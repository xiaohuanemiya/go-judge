mod filestore;
mod handlers;
mod model;
mod sandbox;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    routing::{delete, get, post},
    Router,
};
use clap::Parser;
use handlers::AppState;
use sandbox::Worker;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// rust-judge – a sandboxed code execution service compatible with go-judge
#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// HTTP listening address
    #[arg(long, env = "HTTP_ADDR", default_value = "0.0.0.0:5050")]
    http_addr: String,

    /// Number of concurrent executions
    #[arg(long, env = "PARALLELISM", default_value_t = num_cpus())]
    parallelism: usize,

    /// Working / temp directory (defaults to /tmp/rust-judge)
    #[arg(long, env = "WORK_DIR", default_value = "/tmp/rust-judge")]
    work_dir: PathBuf,

    /// File store directory (defaults to /tmp/rust-judge/files)
    #[arg(long, env = "FILE_STORE_DIR")]
    file_store_dir: Option<PathBuf>,

    /// File TTL in seconds (0 = no expiry)
    #[arg(long, env = "FILE_TIMEOUT", default_value_t = 0)]
    file_timeout: u64,

    /// Default output limit in bytes (per stdout/stderr)
    #[arg(long, env = "OUTPUT_LIMIT", default_value_t = 256 * 1024 * 1024)]
    output_limit: u64,

    /// Default copy-out file size limit in bytes
    #[arg(long, env = "COPY_OUT_LIMIT", default_value_t = 64 * 1024 * 1024)]
    copy_out_limit: u64,

    /// Bearer token for authentication (empty = no auth)
    #[arg(long, env = "AUTH_TOKEN", default_value = "")]
    auth_token: String,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, env = "LOG_LEVEL", default_value = "info")]
    log_level: String,
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // ── Logging ───────────────────────────────────────────────────────────────
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| args.log_level.parse().unwrap_or_else(|_| "info".into())),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // ── Working directory ─────────────────────────────────────────────────────
    std::fs::create_dir_all(&args.work_dir).expect("failed to create work dir");

    let file_store_dir = args
        .file_store_dir
        .clone()
        .unwrap_or_else(|| args.work_dir.join("files"));
    std::fs::create_dir_all(&file_store_dir).expect("failed to create file store dir");

    // ── File store ────────────────────────────────────────────────────────────
    let file_timeout = if args.file_timeout > 0 {
        Some(Duration::from_secs(args.file_timeout))
    } else {
        None
    };
    let fs = filestore::FileStore::new(&file_store_dir, file_timeout);
    if file_timeout.is_some() {
        fs.start_cleanup_task(Duration::from_secs(15));
    }

    // ── Worker ────────────────────────────────────────────────────────────────
    let worker = Worker::new(
        args.parallelism,
        fs.clone(),
        args.work_dir.clone(),
        args.output_limit,
        args.copy_out_limit,
    );

    info!(
        parallelism = args.parallelism,
        work_dir = %args.work_dir.display(),
        "Worker started"
    );

    let state = Arc::new(AppState {
        worker,
        file_store: fs,
        version: env!("CARGO_PKG_VERSION").to_string(),
    });

    // ── Router ────────────────────────────────────────────────────────────────
    let app = build_router(state, &args.auth_token);

    // ── Bind and serve ────────────────────────────────────────────────────────
    let addr: SocketAddr = args
        .http_addr
        .parse()
        .expect("invalid HTTP address");
    info!(addr = %addr, "Starting HTTP server");

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

fn build_router(state: Arc<AppState>, auth_token: &str) -> Router {
    use axum::middleware;

    let public = Router::new()
        .route("/version", get(handlers::handle_version))
        .route("/config", get(handlers::handle_config));

    let protected = Router::new()
        .route("/run", post(handlers::handle_run))
        .route("/file", get(handlers::handle_file_list))
        .route("/file", post(handlers::handle_file_upload))
        .route("/file/:fid", get(handlers::handle_file_get))
        .route("/file/:fid", delete(handlers::handle_file_delete));

    let protected = if !auth_token.is_empty() {
        let token = auth_token.to_string();
        protected.layer(middleware::from_fn(move |req, next| {
            let token = token.clone();
            bearer_auth(req, next, token)
        }))
    } else {
        protected
    };

    public
        .merge(protected)
        .with_state(state)
}

async fn bearer_auth(
    req: axum::extract::Request,
    next: axum::middleware::Next,
    token: String,
) -> Result<axum::response::Response, axum::http::StatusCode> {
    const BEARER: &str = "Bearer ";
    if let Some(auth) = req.headers().get(axum::http::header::AUTHORIZATION) {
        if let Ok(v) = auth.to_str() {
            if v.starts_with(BEARER) && &v[BEARER.len()..] == token {
                return Ok(next.run(req).await);
            }
        }
    }
    Err(axum::http::StatusCode::UNAUTHORIZED)
}
