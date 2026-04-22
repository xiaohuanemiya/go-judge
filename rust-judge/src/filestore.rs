use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use dashmap::DashMap;
use tokio::sync::Mutex;
use uuid::Uuid;

/// Metadata about a stored file
#[derive(Debug, Clone)]
pub struct FileEntry {
    pub name: String,
    pub path: PathBuf,
    pub created_at: Instant,
}

/// Thread-safe file store backed by the local filesystem
#[derive(Clone)]
pub struct FileStore {
    inner: Arc<FileStoreInner>,
}

struct FileStoreInner {
    dir: PathBuf,
    files: DashMap<String, FileEntry>,
    timeout: Option<Duration>,
    cleanup: Mutex<()>,
}

impl FileStore {
    /// Create a new `FileStore` rooted at `dir`.
    pub fn new(dir: impl AsRef<Path>, timeout: Option<Duration>) -> Self {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir).ok();
        Self {
            inner: Arc::new(FileStoreInner {
                dir,
                files: DashMap::new(),
                timeout,
                cleanup: Mutex::new(()),
            }),
        }
    }

    /// List all (fileId → filename) entries.
    pub fn list(&self) -> HashMap<String, String> {
        self.inner
            .files
            .iter()
            .map(|e| (e.key().clone(), e.value().name.clone()))
            .collect()
    }

    /// Store content from a byte slice; returns the new `fileId`.
    pub fn add(&self, name: &str, content: &[u8]) -> Result<String> {
        self.expire_old();
        let id = Uuid::new_v4().to_string();
        let path = self.inner.dir.join(&id);
        std::fs::write(&path, content)?;
        self.inner.files.insert(
            id.clone(),
            FileEntry {
                name: name.to_string(),
                path,
                created_at: Instant::now(),
            },
        );
        Ok(id)
    }

    /// Store content from a path (move/copy); returns the new `fileId`.
    #[allow(dead_code)]
    pub fn add_from_path(&self, name: &str, src: &Path) -> Result<String> {
        self.expire_old();
        let id = Uuid::new_v4().to_string();
        let dest = self.inner.dir.join(&id);
        std::fs::copy(src, &dest)?;
        self.inner.files.insert(
            id.clone(),
            FileEntry {
                name: name.to_string(),
                path: dest,
                created_at: Instant::now(),
            },
        );
        Ok(id)
    }

    /// Get file content by `fileId`.  Returns `(name, content)` or `None`.
    pub fn get(&self, id: &str) -> Option<(String, Vec<u8>)> {
        let entry = self.inner.files.get(id)?;
        let content = std::fs::read(&entry.path).ok()?;
        Some((entry.name.clone(), content))
    }

    /// Get file path by `fileId`.  Returns `(name, path)` or `None`.
    #[allow(dead_code)]
    pub fn get_path(&self, id: &str) -> Option<(String, PathBuf)> {
        let entry = self.inner.files.get(id)?;
        Some((entry.name.clone(), entry.path.clone()))
    }

    /// Delete a file by `fileId`.  Returns `true` if it existed.
    pub fn remove(&self, id: &str) -> bool {
        if let Some((_, entry)) = self.inner.files.remove(id) {
            let _ = std::fs::remove_file(&entry.path);
            true
        } else {
            false
        }
    }

    /// Remove expired files (background task helper).
    fn expire_old(&self) {
        if let Some(ttl) = self.inner.timeout {
            let now = Instant::now();
            self.inner.files.retain(|_, v| {
                let alive = now.duration_since(v.created_at) < ttl;
                if !alive {
                    let _ = std::fs::remove_file(&v.path);
                }
                alive
            });
        }
    }

    /// Spawn a background task that periodically expires files.
    pub fn start_cleanup_task(&self, interval: Duration) {
        let store = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                let _guard = store.inner.cleanup.lock().await;
                store.expire_old();
            }
        });
    }
}
