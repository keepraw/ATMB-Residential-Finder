use std::collections::HashMap;
use std::path::{Path, PathBuf};
use log::info;
use serde::{Deserialize, Serialize};

const CHECKPOINT_VERSION: u32 = 1;

#[derive(Serialize, Deserialize)]
struct CheckpointData {
    version: u32,
    /// mailbox detail page link -> resolved street address (line1 merged with line2)
    completed: HashMap<String, String>,
}

impl Default for CheckpointData {
    fn default() -> Self {
        Self {
            version: CHECKPOINT_VERSION,
            completed: HashMap::new(),
        }
    }
}

/// Manages crawl progress persistence so runs can be resumed after interruption.
///
/// Progress is saved to `result/progress.json` (atomically via a `.tmp` file).
/// To restart from scratch, delete that file manually.
pub struct Checkpoint {
    path: PathBuf,
    data: CheckpointData,
}

impl Checkpoint {
    /// Load checkpoint from `path`. If the file doesn't exist, starts fresh.
    pub fn load(path: impl AsRef<Path>) -> color_eyre::Result<Self> {
        let path = path.as_ref().to_path_buf();

        let data = if path.exists() {
            let content = std::fs::read_to_string(&path)?;
            let parsed: CheckpointData = serde_json::from_str(&content)?;
            parsed
        } else {
            CheckpointData::default()
        };

        if !data.completed.is_empty() {
            info!(
                "Loaded checkpoint: {}/{} entries already completed — resuming. \
                 Delete `{}` to start over.",
                data.completed.len(),
                "(unknown total)",
                path.display()
            );
        } else {
            info!("No checkpoint found at `{}`, starting fresh.", path.display());
        }

        Ok(Self { path, data })
    }

    /// Returns the cached street address if this link was already processed.
    pub fn get_completed(&self, link: &str) -> Option<&str> {
        self.data.completed.get(link).map(String::as_str)
    }

    /// Number of already-completed entries.
    pub fn completed_count(&self) -> usize {
        self.data.completed.len()
    }

    /// Persist one completed entry and atomically flush to disk.
    pub fn save_one(&mut self, link: &str, street: &str) -> color_eyre::Result<()> {
        self.data.completed.insert(link.to_string(), street.to_string());
        self.flush()
    }

    /// Write the checkpoint file atomically (write to `.tmp`, then rename).
    fn flush(&self) -> color_eyre::Result<()> {
        // Ensure the output directory exists.
        if let Some(parent) = self.path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)?;
            }
        }

        let tmp_path = self.path.with_extension("json.tmp");
        let content = serde_json::to_string_pretty(&self.data)?;
        std::fs::write(&tmp_path, content)?;
        // Atomic rename: even if the process is killed mid-write, the original
        // checkpoint file is never left in a half-written state.
        std::fs::rename(&tmp_path, &self.path)?;
        Ok(())
    }
}
