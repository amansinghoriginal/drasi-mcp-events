//! Harness cursor state files: `{"cursor": <string|null>}`.

use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use anyhow::Context as _;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorState {
    // Always serialized (null when absent) so the file is self-describing.
    #[serde(default)]
    pub cursor: Option<String>,
}

/// Returns `Ok(None)` when the file does not exist yet.
pub fn load_state(path: &Path) -> anyhow::Result<Option<CursorState>> {
    let bytes = match fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(anyhow::Error::new(e)
                .context(format!("reading state file {}", path.display())))
        }
    };
    let state = serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing state file {}", path.display()))?;
    Ok(Some(state))
}

/// Write-then-rename so a crash mid-write never corrupts the previous state.
pub fn save_state(path: &Path, state: &CursorState) -> anyhow::Result<()> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    let body = serde_json::to_vec(state).context("serializing state")?;
    fs::write(&tmp, body).with_context(|| format!("writing {}", tmp.display()))?;
    fs::rename(&tmp, path)
        .with_context(|| format!("renaming {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "mcp-events-client-state-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

    #[test]
    fn missing_file_loads_as_none() {
        let path = temp_path("missing.json");
        assert_eq!(load_state(&path).unwrap(), None);
    }

    #[test]
    fn round_trip_with_cursor() {
        let path = temp_path("state.json");
        let st = CursorState {
            cursor: Some("17442:9".to_owned()),
        };
        save_state(&path, &st).unwrap();
        assert_eq!(load_state(&path).unwrap(), Some(st));
        // file shape is exactly {"cursor": ...}
        let raw: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(raw, serde_json::json!({"cursor": "17442:9"}));
    }

    #[test]
    fn round_trip_null_cursor() {
        let path = temp_path("null.json");
        save_state(&path, &CursorState::default()).unwrap();
        let raw: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(raw, serde_json::json!({"cursor": null}));
        assert_eq!(load_state(&path).unwrap(), Some(CursorState::default()));
    }

    #[test]
    fn overwrite_keeps_latest() {
        let path = temp_path("overwrite.json");
        save_state(&path, &CursorState { cursor: Some("a".into()) }).unwrap();
        save_state(&path, &CursorState { cursor: Some("b".into()) }).unwrap();
        assert_eq!(
            load_state(&path).unwrap().unwrap().cursor.as_deref(),
            Some("b")
        );
    }
}
