use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    extract::{Multipart, Path, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use tracing::debug;

use crate::filestore::FileStore;
use crate::model::{self, ExecResult, Request};
use crate::sandbox::Worker;

// ─── Shared application state ─────────────────────────────────────────────────

#[derive(Clone)]
pub struct AppState {
    pub worker: Worker,
    pub file_store: FileStore,
    pub version: String,
}

// ─── POST /run ────────────────────────────────────────────────────────────────

pub async fn handle_run(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Request>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    if req.cmd.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "no cmd provided".to_string()));
    }

    debug!("run request: requestId={}", req.request_id);

    let results = state.worker.run(&req).await;

    // Convert sandbox results to the model::ExecResult format
    let mut out: Vec<ExecResult> = Vec::with_capacity(results.len());
    for (i, r) in results.into_iter().enumerate() {
        let mut files_map: Option<HashMap<String, String>> = None;
        let mut file_ids_map: Option<HashMap<String, String>> = None;

        // Map collected files back to named stdout/stderr.
        // Convention: files[0]=stdin, files[1]=stdout, files[2]=stderr.
        // go-judge collectors are identified by `name` in the CmdFile.
        let cmd = req.cmd.get(i);
        let named = named_collectors(cmd);

        if !r.files.is_empty() {
            let mut m = HashMap::new();
            // Always include stdout/stderr under their canonical or named keys.
            for (internal_key, v) in &r.files {
                let out_key = named
                    .get(internal_key)
                    .cloned()
                    .unwrap_or_else(|| internal_key.clone());
                m.insert(out_key, String::from_utf8_lossy(v).into_owned());
            }
            if !m.is_empty() {
                files_map = Some(m);
            }
        }

        if !r.file_ids.is_empty() {
            file_ids_map = Some(r.file_ids.clone());
        }

        out.push(ExecResult {
            status: r.status,
            exit_status: r.exit_code,
            error: r.error,
            time: r.usage.cpu_ns,
            memory: r.usage.memory_bytes,
            run_time: r.usage.wall_ns,
            proc_peak: None,
            files: files_map,
            file_ids: file_ids_map,
            file_error: r.file_errors,
        });
    }

    Ok(Json(serde_json::to_value(out).unwrap()))
}

/// Extract `internal_key` → `output_name` mapping from a Cmd's files list.
/// files[0]=stdin (ignored), files[1]=stdout, files[2]=stderr.
fn named_collectors(cmd: Option<&model::Cmd>) -> HashMap<String, String> {
    let mut m = HashMap::new();
    if let Some(cmd) = cmd {
        // files[1] = stdout collector
        if let Some(Some(f)) = cmd.files.get(1) {
            if let Some(ref name) = f.name {
                m.insert("stdout".to_string(), name.clone());
            }
        }
        // files[2] = stderr collector
        if let Some(Some(f)) = cmd.files.get(2) {
            if let Some(ref name) = f.name {
                m.insert("stderr".to_string(), name.clone());
            }
        }
    }
    m
}

// ─── GET /file ────────────────────────────────────────────────────────────────

pub async fn handle_file_list(
    State(state): State<Arc<AppState>>,
) -> Json<HashMap<String, String>> {
    Json(state.file_store.list())
}

// ─── POST /file ───────────────────────────────────────────────────────────────

pub async fn handle_file_upload(
    State(state): State<Arc<AppState>>,
    mut multipart: Multipart,
) -> Result<Json<String>, (StatusCode, String)> {
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?
    {
        let name = field.file_name().unwrap_or("file").to_string();
        let data = field
            .bytes()
            .await
            .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

        let id = state
            .file_store
            .add(&name, &data)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        return Ok(Json(id));
    }
    Err((
        StatusCode::BAD_REQUEST,
        "no file in multipart body".to_string(),
    ))
}

// ─── GET /file/:fileId ────────────────────────────────────────────────────────

pub async fn handle_file_get(
    State(state): State<Arc<AppState>>,
    Path(file_id): Path<String>,
) -> Result<Response, StatusCode> {
    match state.file_store.get(&file_id) {
        Some((name, content)) => {
            // Derive a basic MIME type from the extension
            let mime = guess_mime(&name);
            Ok((
                [
                    (
                        header::CONTENT_DISPOSITION,
                        format!("attachment; filename=\"{}\"", name),
                    ),
                    (header::CONTENT_TYPE, mime),
                ],
                content,
            )
                .into_response())
        }
        None => Err(StatusCode::NOT_FOUND),
    }
}

fn guess_mime(name: &str) -> String {
    let ext = std::path::Path::new(name)
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("");
    match ext {
        "txt" => "text/plain",
        "html" | "htm" => "text/html",
        "json" => "application/json",
        "js" => "application/javascript",
        "css" => "text/css",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "pdf" => "application/pdf",
        "zip" => "application/zip",
        _ => "application/octet-stream",
    }
    .to_string()
}

// ─── DELETE /file/:fileId ─────────────────────────────────────────────────────

pub async fn handle_file_delete(
    State(state): State<Arc<AppState>>,
    Path(file_id): Path<String>,
) -> StatusCode {
    if state.file_store.remove(&file_id) {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

// ─── GET /version ─────────────────────────────────────────────────────────────

pub async fn handle_version(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    Json(json!({
        "buildVersion": state.version,
        "rustVersion": env!("CARGO_PKG_VERSION"),
        "platform": std::env::consts::ARCH,
        "os": std::env::consts::OS,
        "copyOutOptional": true,
        "pipeProxy": false,
        "symlink": false,
        "addressSpaceLimit": true,
        "stream": false,
        "procPeak": false,
        "copyOutTruncate": true,
        "pipeProxyZeroCopy": false,
        "fixSymlinkEscape": false,
    }))
}

// ─── GET /config ──────────────────────────────────────────────────────────────

pub async fn handle_config(
    State(_state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    Json(json!({
        "copyOutOptional": true,
        "pipeProxy": false,
        "symlink": false,
        "addressSpaceLimit": true,
        "stream": false,
        "procPeak": false,
        "copyOutTruncate": true,
        "pipeProxyZeroCopy": false,
        "fixSymlinkEscape": false,
    }))
}
