use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// File source – mirrors go-judge's CmdFile
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CmdFile {
    /// Local file path on the host
    pub src: Option<String>,
    /// Inline content (string or base64-encoded binary)
    pub content: Option<String>,
    /// File ID from the file store
    pub file_id: Option<String>,
    /// Collector file name (for stdout/stderr capture)
    pub name: Option<String>,
    /// Maximum bytes to capture
    pub max: Option<i64>,
    /// Symlink target
    pub symlink: Option<String>,
    #[serde(default)]
    pub stream_in: bool,
    #[serde(default)]
    pub stream_out: bool,
    #[serde(default)]
    pub pipe: bool,
}

/// A single command to execute inside the sandbox
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Cmd {
    pub args: Vec<String>,
    #[serde(default)]
    pub env: Vec<String>,
    #[serde(default)]
    pub files: Vec<Option<CmdFile>>,

    #[serde(default)]
    pub cpu_limit: u64,
    /// Alias for clockLimit (deprecated but kept for compatibility)
    #[serde(default)]
    pub real_cpu_limit: u64,
    #[serde(default)]
    pub clock_limit: u64,
    #[serde(default)]
    pub memory_limit: u64,
    #[serde(default)]
    pub stack_limit: u64,
    #[serde(default)]
    pub proc_limit: u64,
    #[serde(default)]
    pub cpu_rate_limit: u64,
    #[serde(default)]
    pub cpu_set_limit: String,

    #[serde(default)]
    pub copy_in: HashMap<String, CmdFile>,

    #[serde(default)]
    pub copy_out: Vec<String>,
    #[serde(default)]
    pub copy_out_cached: Vec<String>,
    #[serde(default)]
    pub copy_out_max: u64,
    #[serde(default)]
    pub copy_out_dir: String,
    #[serde(default)]
    pub copy_out_truncate: bool,

    #[serde(default)]
    pub tty: bool,
    #[serde(default)]
    pub strict_memory_limit: bool,
    #[serde(default)]
    pub data_segment_limit: bool,
    #[serde(default)]
    pub address_space_limit: bool,
}

/// Pipe index into a specific command's file-descriptor list
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PipeIndex {
    pub index: usize,
    pub fd: usize,
}

/// A pipe connection between two command file-descriptors
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PipeMap {
    #[serde(rename = "in")]
    pub pipe_in: PipeIndex,
    #[serde(rename = "out")]
    pub pipe_out: PipeIndex,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub max: i64,
    #[serde(default)]
    pub proxy: bool,
    #[serde(default)]
    pub disable_zero_copy: bool,
}

/// Top-level run request
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    #[serde(default)]
    pub request_id: String,
    pub cmd: Vec<Cmd>,
    #[serde(default)]
    pub pipe_mapping: Vec<PipeMap>,
}

// ─── Status ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Invalid,
    Accepted,
    WrongAnswer,
    PartiallyCorrect,
    MemoryLimitExceeded,
    TimeLimitExceeded,
    OutputLimitExceeded,
    FileError,
    NonzeroExitStatus,
    Signalled,
    DangerousSyscall,
    JudgementFailed,
    InvalidInteraction,
    InternalError,
}

impl Status {
    pub fn as_str(&self) -> &'static str {
        match self {
            Status::Invalid => "Invalid",
            Status::Accepted => "Accepted",
            Status::WrongAnswer => "Wrong Answer",
            Status::PartiallyCorrect => "Partially Correct",
            Status::MemoryLimitExceeded => "Memory Limit Exceeded",
            Status::TimeLimitExceeded => "Time Limit Exceeded",
            Status::OutputLimitExceeded => "Output Limit Exceeded",
            Status::FileError => "File Error",
            Status::NonzeroExitStatus => "Nonzero Exit Status",
            Status::Signalled => "Signalled",
            Status::DangerousSyscall => "Dangerous Syscall",
            Status::JudgementFailed => "Judgement Failed",
            Status::InvalidInteraction => "Invalid Interaction",
            Status::InternalError => "Internal Error",
        }
    }
}

impl Serialize for Status {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Status {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        match s.as_str() {
            "Invalid" => Ok(Status::Invalid),
            "Accepted" => Ok(Status::Accepted),
            "Wrong Answer" => Ok(Status::WrongAnswer),
            "Partially Correct" => Ok(Status::PartiallyCorrect),
            "Memory Limit Exceeded" => Ok(Status::MemoryLimitExceeded),
            "Time Limit Exceeded" => Ok(Status::TimeLimitExceeded),
            "Output Limit Exceeded" => Ok(Status::OutputLimitExceeded),
            "File Error" => Ok(Status::FileError),
            "Nonzero Exit Status" => Ok(Status::NonzeroExitStatus),
            "Signalled" => Ok(Status::Signalled),
            "Dangerous Syscall" => Ok(Status::DangerousSyscall),
            "Judgement Failed" => Ok(Status::JudgementFailed),
            "Invalid Interaction" => Ok(Status::InvalidInteraction),
            "Internal Error" => Ok(Status::InternalError),
            _ => Err(serde::de::Error::custom(format!("unknown status: {}", s))),
        }
    }
}

// ─── File Error ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileError {
    pub name: String,
    #[serde(rename = "type")]
    pub error_type: FileErrorType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FileErrorType {
    CopyInOpenError,
    CopyInCreateError,
    CopyInCopyError,
    CopyOutOpen,
    CopyOutNotFound,
    CopyOutExceededLimit,
}

// ─── ExecResult ───────────────────────────────────────────────────────────────

/// The per-command result returned in the REST response.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecResult {
    pub status: Status,
    pub exit_status: i32,
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub error: String,
    /// CPU time in nanoseconds
    pub time: u64,
    /// Memory usage in bytes
    pub memory: u64,
    /// Wall-clock time in nanoseconds
    pub run_time: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proc_peak: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub files: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_ids: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub file_error: Vec<FileError>,
}

/// Top-level run response (array of per-command results)
#[allow(dead_code)]
pub type RunResponse = Vec<ExecResult>;
