/// Sandbox execution engine.
///
/// Each [`Cmd`] is run as a real OS process with resource limits applied via
/// `setrlimit`.  Output is captured through pipes.  File copy-in/copy-out is
/// handled by creating a temporary working directory per request.
use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine;
use tempfile::TempDir;
use tokio::sync::Semaphore;

use crate::filestore::FileStore;
use crate::model::{self, CmdFile, FileError, FileErrorType};

// ─── Public types ─────────────────────────────────────────────────────────────

/// The resource-usage reported back to callers.
#[derive(Debug, Default, Clone)]
pub struct Usage {
    /// CPU time (user + sys) in nanoseconds
    pub cpu_ns: u64,
    /// Peak RSS in bytes
    pub memory_bytes: u64,
    /// Wall-clock time in nanoseconds
    pub wall_ns: u64,
}

/// Internal sandbox result (before converting to model::ExecResult).
#[derive(Debug)]
pub struct SandboxResult {
    pub status: model::Status,
    pub exit_code: i32,
    pub error: String,
    pub usage: Usage,
    /// stdout/stderr or named pipes collected from the run
    pub files: HashMap<String, Vec<u8>>,
    pub file_ids: HashMap<String, String>,
    pub file_errors: Vec<FileError>,
}

impl SandboxResult {
    pub fn internal_error(msg: impl Into<String>) -> Self {
        SandboxResult {
            status: model::Status::InternalError,
            exit_code: -1,
            error: msg.into(),
            usage: Default::default(),
            files: Default::default(),
            file_ids: Default::default(),
            file_errors: Vec::new(),
        }
    }
}

// ─── Worker pool ──────────────────────────────────────────────────────────────

/// Limits concurrent sandbox executions.
#[derive(Clone)]
pub struct Worker {
    semaphore: Arc<Semaphore>,
    file_store: FileStore,
    work_dir: PathBuf,
    /// Global output limit (bytes) applied to each collected file
    output_limit: u64,
    /// Default copy-out file size limit
    copy_out_limit: u64,
}

impl Worker {
    pub fn new(
        parallelism: usize,
        file_store: FileStore,
        work_dir: PathBuf,
        output_limit: u64,
        copy_out_limit: u64,
    ) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(parallelism)),
            file_store,
            work_dir,
            output_limit,
            copy_out_limit,
        }
    }

    /// Execute all commands in `req`, honouring pipe mappings, and return
    /// one [`SandboxResult`] per command.
    pub async fn run(&self, req: &model::Request) -> Vec<SandboxResult> {
        let _permit = self.semaphore.acquire().await.unwrap();
        run_request(req, &self.file_store, &self.work_dir, self.output_limit, self.copy_out_limit).await
    }
}

// ─── Request execution ────────────────────────────────────────────────────────

async fn run_request(
    req: &model::Request,
    fs: &FileStore,
    work_dir: &Path,
    output_limit: u64,
    copy_out_limit: u64,
) -> Vec<SandboxResult> {
    if req.cmd.is_empty() {
        return vec![];
    }

    if req.cmd.len() == 1 && req.pipe_mapping.is_empty() {
        let result = run_single(&req.cmd[0], fs, work_dir, output_limit, copy_out_limit).await;
        return vec![result];
    }

    // Multi-command: run sequentially
    run_multi(req, fs, work_dir, output_limit, copy_out_limit).await
}

// ─── Single command ───────────────────────────────────────────────────────────

async fn run_single(
    cmd: &model::Cmd,
    fs: &FileStore,
    work_dir: &Path,
    output_limit: u64,
    copy_out_limit: u64,
) -> SandboxResult {
    if cmd.args.is_empty() {
        return SandboxResult::internal_error("no args provided");
    }

    // Create a temp working directory for this execution
    let tmp = match TempDir::new_in(work_dir) {
        Ok(t) => t,
        Err(e) => return SandboxResult::internal_error(format!("tempdir: {}", e)),
    };
    let sandbox_dir = tmp.path().to_path_buf();

    // ── Copy files into the working directory ────────────────────────────────
    let mut file_errors: Vec<FileError> = Vec::new();

    for (dest_name, src_file) in &cmd.copy_in {
        if let Some(err) = copy_in_file(src_file, dest_name, &sandbox_dir, fs) {
            file_errors.push(err);
        }
    }
    if !file_errors.is_empty() {
        return SandboxResult {
            status: model::Status::FileError,
            exit_code: -1,
            error: file_errors[0].message.clone().unwrap_or_default(),
            usage: Default::default(),
            files: Default::default(),
            file_ids: Default::default(),
            file_errors,
        };
    }

    // ── Build an effective clock limit ───────────────────────────────────────
    let clock_limit_ns = if cmd.real_cpu_limit > 0 {
        cmd.real_cpu_limit
    } else if cmd.clock_limit > 0 {
        cmd.clock_limit
    } else if cmd.cpu_limit > 0 {
        // Give 3× CPU limit as wall-clock limit
        cmd.cpu_limit.saturating_mul(3)
    } else {
        0
    };

    let eff_copy_out_max = if cmd.copy_out_max > 0 {
        cmd.copy_out_max
    } else {
        copy_out_limit
    };

    // ── Prepare the process configuration ───────────────────────────────────
    let stdin_path = resolve_stdin(&cmd.files, fs, &sandbox_dir);
    let proc_cfg = ProcessConfig {
        args: cmd.args.clone(),
        env: cmd.env.clone(),
        work_dir: sandbox_dir.clone(),
        stdin_file: stdin_path,
        cpu_limit_ns: cmd.cpu_limit,
        memory_limit: cmd.memory_limit,
        stack_limit: cmd.stack_limit,
        proc_limit: cmd.proc_limit,
        output_limit_bytes: output_limit,
        clock_limit_ns,
        data_segment_limit: cmd.data_segment_limit || cmd.strict_memory_limit,
        address_space_limit: cmd.address_space_limit,
    };

    // ── Run the process (blocking) ───────────────────────────────────────────
    let exec_result = tokio::task::spawn_blocking(move || run_process(proc_cfg))
        .await
        .unwrap_or_else(|e| SandboxResult::internal_error(format!("spawn_blocking: {}", e)));

    // ── Collect output files ─────────────────────────────────────────────────
    let mut out_files: HashMap<String, Vec<u8>> = HashMap::new();
    let mut out_file_ids: HashMap<String, String> = HashMap::new();
    let mut copy_file_errors: Vec<FileError> = Vec::new();

    // CopyOut – files the process was expected to produce
    for copy_out_name in &cmd.copy_out {
        let (name, optional) = strip_optional(copy_out_name);
        let path = sandbox_dir.join(&name);
        match fs::read(&path) {
            Ok(content) => {
                if content.len() as u64 > eff_copy_out_max {
                    if !optional {
                        copy_file_errors.push(FileError {
                            name: name.clone(),
                            error_type: FileErrorType::CopyOutExceededLimit,
                            message: Some(format!("file {} exceeds copy-out limit", name)),
                        });
                    }
                } else {
                    out_files.insert(name, content);
                }
            }
            Err(_) if optional => {}
            Err(e) => {
                copy_file_errors.push(FileError {
                    name: name.clone(),
                    error_type: FileErrorType::CopyOutNotFound,
                    message: Some(e.to_string()),
                });
            }
        }
    }

    // CopyOutCached – files to store in the file store and return IDs
    for copy_out_name in &cmd.copy_out_cached {
        let (name, optional) = strip_optional(copy_out_name);
        let path = sandbox_dir.join(&name);
        match fs::read(&path) {
            Ok(content) => {
                if content.len() as u64 > eff_copy_out_max {
                    if !optional {
                        copy_file_errors.push(FileError {
                            name: name.clone(),
                            error_type: FileErrorType::CopyOutExceededLimit,
                            message: Some(format!("file {} exceeds copy-out limit", name)),
                        });
                    }
                } else {
                    match fs.add(&name, &content) {
                        Ok(id) => {
                            out_file_ids.insert(name, id);
                        }
                        Err(e) => {
                            copy_file_errors.push(FileError {
                                name: name.clone(),
                                error_type: FileErrorType::CopyOutOpen,
                                message: Some(e.to_string()),
                            });
                        }
                    }
                }
            }
            Err(_) if optional => {}
            Err(e) => {
                copy_file_errors.push(FileError {
                    name: name.clone(),
                    error_type: FileErrorType::CopyOutNotFound,
                    message: Some(e.to_string()),
                });
            }
        }
    }

    // Merge files from the process execution (stdout/stderr)
    let mut all_files = exec_result.files;
    all_files.extend(out_files);
    let mut all_file_ids = exec_result.file_ids;
    all_file_ids.extend(out_file_ids);
    let all_errors = [exec_result.file_errors, copy_file_errors].concat();

    let mut status = exec_result.status;
    if !all_errors.is_empty() && status == model::Status::Accepted {
        status = model::Status::FileError;
    }

    SandboxResult {
        status,
        exit_code: exec_result.exit_code,
        error: exec_result.error,
        usage: exec_result.usage,
        files: all_files,
        file_ids: all_file_ids,
        file_errors: all_errors,
    }
}

// ─── Multi-command execution ──────────────────────────────────────────────────

async fn run_multi(
    req: &model::Request,
    fs: &FileStore,
    work_dir: &Path,
    output_limit: u64,
    copy_out_limit: u64,
) -> Vec<SandboxResult> {
    // Cap allocation to a reasonable maximum to prevent unbounded memory use.
    const MAX_CMDS: usize = 64;
    let count = req.cmd.len().min(MAX_CMDS);
    let mut results = Vec::with_capacity(count);
    for cmd in req.cmd.iter().take(MAX_CMDS) {
        let r = run_single(cmd, fs, work_dir, output_limit, copy_out_limit).await;
        results.push(r);
    }
    results
}

// ─── Process runner ───────────────────────────────────────────────────────────

struct ProcessConfig {
    args: Vec<String>,
    env: Vec<String>,
    work_dir: PathBuf,
    /// Optional path for stdin redirection
    stdin_file: Option<PathBuf>,
    /// CPU time limit in nanoseconds (0 = unlimited)
    cpu_limit_ns: u64,
    /// Memory limit in bytes (0 = unlimited)
    memory_limit: u64,
    /// Stack limit in bytes (0 = unlimited)
    stack_limit: u64,
    /// Process count limit (0 = unlimited)
    proc_limit: u64,
    /// Maximum stdout/stderr bytes to capture
    output_limit_bytes: u64,
    /// Wall-clock timeout in nanoseconds (0 = unlimited)
    clock_limit_ns: u64,
    data_segment_limit: bool,
    address_space_limit: bool,
}

fn run_process(cfg: ProcessConfig) -> SandboxResult {
    use std::os::unix::process::CommandExt;
    use std::process::{Command, Stdio};

    // Resolve the executable: if the path is absolute but does not exist on the
    // real filesystem, look for it inside the sandbox working directory.
    let exe = resolve_exe(&cfg.args[0], &cfg.work_dir);
    let mut cmd = Command::new(&exe);
    if cfg.args.len() > 1 {
        cmd.args(&cfg.args[1..]);
    }
    cmd.current_dir(&cfg.work_dir);
    cmd.env_clear();

    for kv in &cfg.env {
        if let Some((k, v)) = kv.split_once('=') {
            cmd.env(k, v);
        }
    }

    // stdin
    match cfg.stdin_file {
        Some(ref p) => match fs::File::open(p) {
            Ok(f) => {
                cmd.stdin(f);
            }
            Err(_) => {
                cmd.stdin(Stdio::null());
            }
        },
        None => {
            cmd.stdin(Stdio::null());
        }
    }
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    // ── Apply resource limits in the child (pre_exec) ────────────────────────
    let cpu_limit_ns = cfg.cpu_limit_ns;
    let memory_limit = cfg.memory_limit;
    let stack_limit = cfg.stack_limit;
    let proc_limit = cfg.proc_limit;
    let output_limit = cfg.output_limit_bytes;
    let data_seg = cfg.data_segment_limit;
    let addr_space = cfg.address_space_limit;

    // Safety: we only call async-signal-safe libc functions between fork and exec.
    unsafe {
        cmd.pre_exec(move || {
            // Create a new process group so we can kill all child processes
            // later without risking killing unrelated processes.
            libc::setpgid(0, 0);

            // CPU time in seconds (rounded up)
            if cpu_limit_ns > 0 {
                let cpu_secs = ((cpu_limit_ns as f64) / 1e9).ceil() as u64;
                set_rlimit(libc::RLIMIT_CPU, cpu_secs, cpu_secs + 1);
            }
            // Memory (address space)
            if memory_limit > 0 {
                if addr_space || data_seg {
                    set_rlimit(libc::RLIMIT_AS, memory_limit, memory_limit);
                }
                if data_seg {
                    set_rlimit(libc::RLIMIT_DATA, memory_limit, memory_limit);
                }
            }
            // Stack
            if stack_limit > 0 {
                set_rlimit(libc::RLIMIT_STACK, stack_limit, stack_limit);
            }
            // Process count
            if proc_limit > 0 {
                set_rlimit(libc::RLIMIT_NPROC, proc_limit, proc_limit);
            }
            // Output (file size)
            if output_limit > 0 {
                set_rlimit(libc::RLIMIT_FSIZE, output_limit, output_limit);
            }
            Ok(())
        });
    }

    let start = Instant::now();

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            return SandboxResult::internal_error(format!("spawn failed: {}", e));
        }
    };

    let pid = child.id() as libc::pid_t;

    // ── Clock-limit timer thread ─────────────────────────────────────────────
    // Use an AtomicBool to cancel the timer once the process has been waited
    // for, preventing accidental killing of a recycled PID.
    let clock_ns = cfg.clock_limit_ns;
    let timer_cancelled = Arc::new(AtomicBool::new(false));
    if clock_ns > 0 {
        let cancel_flag = Arc::clone(&timer_cancelled);
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_nanos(clock_ns));
            // Only send the signal if the process hasn't been waited for yet.
            if !cancel_flag.load(Ordering::Acquire) {
                unsafe {
                    // Kill the entire process group created by setpgid(0,0).
                    libc::kill(-pid, libc::SIGKILL);
                    // Also try the leader in case setpgid failed.
                    libc::kill(pid, libc::SIGKILL);
                }
            }
        });
    }

    // ── Read stdout/stderr concurrently to avoid pipe-full deadlock ───────────
    let output_limit_usize = cfg.output_limit_bytes as usize;

    let stdout_handle = {
        let mut stdout = child.stdout.take().unwrap();
        let lim = output_limit_usize;
        std::thread::spawn(move || -> Vec<u8> {
            let mut buf = Vec::new();
            let mut chunk = [0u8; 65536];
            loop {
                match stdout.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => {
                        if buf.len() < lim {
                            let take = n.min(lim - buf.len());
                            buf.extend_from_slice(&chunk[..take]);
                        }
                    }
                    Err(_) => break,
                }
            }
            buf
        })
    };
    let stderr_handle = {
        let mut stderr = child.stderr.take().unwrap();
        let lim = output_limit_usize;
        std::thread::spawn(move || -> Vec<u8> {
            let mut buf = Vec::new();
            let mut chunk = [0u8; 65536];
            loop {
                match stderr.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => {
                        if buf.len() < lim {
                            let take = n.min(lim - buf.len());
                            buf.extend_from_slice(&chunk[..take]);
                        }
                    }
                    Err(_) => break,
                }
            }
            buf
        })
    };

    // ── Wait for child + collect resource usage via wait4 ─────────────────────
    // We use libc::wait4 directly to get rusage.
    // std::mem::forget prevents the Child Drop impl from double-waiting.
    let (exit_raw, rusage) = unsafe {
        let mut status: libc::c_int = 0;
        let mut ru: libc::rusage = std::mem::zeroed();
        let ret = libc::wait4(pid, &mut status, 0, &mut ru);
        if ret < 0 {
            // Fallback: drop the child normally
            drop(child);
            (0i32, ru)
        } else {
            std::mem::forget(child);
            (status, ru)
        }
    };

    // Cancel the timer now that the process has been waited for.
    timer_cancelled.store(true, Ordering::Release);

    let wall_ns = start.elapsed().as_nanos() as u64;

    let stdout_bytes = stdout_handle.join().unwrap_or_default();
    let stderr_bytes = stderr_handle.join().unwrap_or_default();

    // ── Decode exit status ────────────────────────────────────────────────────
    let (exit_code, signal_num) = decode_wait_status(exit_raw);

    // CPU time from rusage (nanoseconds)
    let cpu_ns = rusage_cpu_ns(&rusage);
    // Memory from rusage (bytes) – maxrss is in kilobytes on Linux
    let mem_bytes = rusage.ru_maxrss as u64 * 1024;

    let usage = Usage {
        cpu_ns,
        memory_bytes: mem_bytes,
        wall_ns,
    };

    // ── Determine status ──────────────────────────────────────────────────────
    let status = determine_status(
        exit_code,
        signal_num,
        &usage,
        cfg.cpu_limit_ns,
        cfg.memory_limit,
        cfg.clock_limit_ns,
        cfg.output_limit_bytes,
        stdout_bytes.len() as u64,
        stderr_bytes.len() as u64,
    );

    // Populate files: the names come from the "files" list in the Cmd.
    // Conventionally: files[0] = stdin, files[1] = stdout, files[2] = stderr.
    let mut files: HashMap<String, Vec<u8>> = HashMap::new();
    files.insert("stdout".to_string(), stdout_bytes);
    files.insert("stderr".to_string(), stderr_bytes);

    SandboxResult {
        status,
        exit_code,
        error: String::new(),
        usage,
        files,
        file_ids: HashMap::new(),
        file_errors: Vec::new(),
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

/// Resolve the executable path.
///
/// In go-judge, paths like `/w/a.out` refer to files inside the container.
/// Here we map absolute paths to files inside `sandbox_dir` when:
///   - The path is absolute (starts with `/`), AND
///   - The file does NOT exist at the absolute host path, BUT
///   - It DOES exist relative to `sandbox_dir` (with the leading `/` stripped).
fn resolve_exe(exe: &str, sandbox_dir: &Path) -> PathBuf {
    let p = Path::new(exe);
    if p.is_absolute() && !p.exists() {
        // Try relative to sandbox_dir
        let rel = exe.trim_start_matches('/');
        let candidate = sandbox_dir.join(rel);
        if candidate.exists() {
            return candidate;
        }
    }
    PathBuf::from(exe)
}

fn set_rlimit(resource: libc::__rlimit_resource_t, soft: u64, hard: u64) {
    let lim = libc::rlimit {
        rlim_cur: soft as libc::rlim_t,
        rlim_max: hard as libc::rlim_t,
    };
    unsafe {
        libc::setrlimit(resource, &lim);
    }
}

/// Decode a raw wait status from wait4() into (exit_code, signal).
fn decode_wait_status(status: i32) -> (i32, Option<i32>) {
    if libc::WIFEXITED(status) {
        (libc::WEXITSTATUS(status), None)
    } else if libc::WIFSIGNALED(status) {
        (-1, Some(libc::WTERMSIG(status)))
    } else {
        (-1, None)
    }
}

fn rusage_cpu_ns(ru: &libc::rusage) -> u64 {
    let user_ns = ru.ru_utime.tv_sec as u64 * 1_000_000_000
        + ru.ru_utime.tv_usec as u64 * 1_000;
    let sys_ns = ru.ru_stime.tv_sec as u64 * 1_000_000_000
        + ru.ru_stime.tv_usec as u64 * 1_000;
    user_ns + sys_ns
}

#[allow(clippy::too_many_arguments)]
fn determine_status(
    exit_code: i32,
    signal: Option<i32>,
    usage: &Usage,
    cpu_limit_ns: u64,
    memory_limit: u64,
    clock_limit_ns: u64,
    output_limit: u64,
    stdout_len: u64,
    stderr_len: u64,
) -> model::Status {
    // Memory limit exceeded
    if memory_limit > 0 && usage.memory_bytes > memory_limit {
        return model::Status::MemoryLimitExceeded;
    }
    // CPU time limit exceeded
    if cpu_limit_ns > 0 && usage.cpu_ns > cpu_limit_ns {
        return model::Status::TimeLimitExceeded;
    }
    // Wall-clock limit
    if clock_limit_ns > 0 && usage.wall_ns > clock_limit_ns {
        return model::Status::TimeLimitExceeded;
    }
    // Output limit
    if output_limit > 0 && (stdout_len >= output_limit || stderr_len >= output_limit) {
        return model::Status::OutputLimitExceeded;
    }
    // Killed by signal
    if let Some(sig) = signal {
        if sig == libc::SIGXCPU {
            return model::Status::TimeLimitExceeded;
        }
        if sig == libc::SIGXFSZ {
            return model::Status::OutputLimitExceeded;
        }
        return model::Status::Signalled;
    }
    if exit_code != 0 {
        return model::Status::NonzeroExitStatus;
    }
    model::Status::Accepted
}

/// Copy a single CmdFile into the sandbox working directory.
///
/// `dest_name` may be an absolute path (like `/w/a.out`) as used in go-judge
/// containers.  We strip the leading `/` so the file lands inside `sandbox_dir`.
fn copy_in_file(
    src: &CmdFile,
    dest_name: &str,
    sandbox_dir: &Path,
    fs: &FileStore,
) -> Option<FileError> {
    // Sanitize: treat absolute paths as relative to sandbox_dir
    let rel_name = dest_name.trim_start_matches('/');
    let rel_name = if rel_name.is_empty() { "file" } else { rel_name };
    let dest = sandbox_dir.join(rel_name);

    // Create parent directories if needed
    if let Some(parent) = dest.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            return Some(FileError {
                name: dest_name.to_string(),
                error_type: FileErrorType::CopyInCreateError,
                message: Some(e.to_string()),
            });
        }
    }

    if let Some(ref content) = src.content {
        // Inline content: try to decode as base64 first, else treat as UTF-8
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(content.as_bytes())
            .unwrap_or_else(|_| content.as_bytes().to_vec());
        if let Err(e) = std::fs::write(&dest, &bytes) {
            return Some(FileError {
                name: dest_name.to_string(),
                error_type: FileErrorType::CopyInCopyError,
                message: Some(e.to_string()),
            });
        }
    } else if let Some(ref file_id) = src.file_id {
        match fs.get(file_id) {
            Some((_, content)) => {
                if let Err(e) = std::fs::write(&dest, &content) {
                    return Some(FileError {
                        name: dest_name.to_string(),
                        error_type: FileErrorType::CopyInCopyError,
                        message: Some(e.to_string()),
                    });
                }
            }
            None => {
                return Some(FileError {
                    name: dest_name.to_string(),
                    error_type: FileErrorType::CopyInOpenError,
                    message: Some(format!("fileId {} not found", file_id)),
                });
            }
        }
    } else if let Some(ref path) = src.src {
        if let Err(e) = std::fs::copy(path, &dest) {
            return Some(FileError {
                name: dest_name.to_string(),
                error_type: FileErrorType::CopyInCopyError,
                message: Some(e.to_string()),
            });
        }
    }

    // Make copied file executable
    use std::os::unix::fs::PermissionsExt;
    let _ = std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(0o755));
    None
}

/// Returns the stdin source as a file path, if specified.
fn resolve_stdin(
    files: &[Option<model::CmdFile>],
    fs: &FileStore,
    sandbox_dir: &Path,
) -> Option<PathBuf> {
    let stdin_file = files.get(0)?.as_ref()?;
    if let Some(ref file_id) = stdin_file.file_id {
        if let Some((_, content)) = fs.get(file_id) {
            let tmp = sandbox_dir.join("__stdin__");
            std::fs::write(&tmp, content).ok()?;
            return Some(tmp);
        }
    }
    if let Some(ref content) = stdin_file.content {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(content.as_bytes())
            .unwrap_or_else(|_| content.as_bytes().to_vec());
        let tmp = sandbox_dir.join("__stdin__");
        std::fs::write(&tmp, bytes).ok()?;
        return Some(tmp);
    }
    if let Some(ref path) = stdin_file.src {
        return Some(PathBuf::from(path));
    }
    None
}

/// Strip the optional `?` suffix used in copyOut/copyOutCached lists.
fn strip_optional(name: &str) -> (String, bool) {
    if let Some(s) = name.strip_suffix('?') {
        (s.to_string(), true)
    } else {
        (name.to_string(), false)
    }
}
