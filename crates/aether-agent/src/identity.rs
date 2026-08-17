//! A node id that survives restarts.
//!
//! Without this, a restarted agent is a brand-new node: the controller cannot
//! tell it apart from an extra machine, and its history is lost. The id lives
//! in a small file next to the agent.

use std::path::{Path, PathBuf};

use aether_core::NodeId;
use tracing::info;

/// File the id is kept in, under the user's data directory when one is known.
pub fn default_identity_path() -> PathBuf {
    let base = std::env::var_os("APPDATA")
        .or_else(|| std::env::var_os("XDG_DATA_HOME"))
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/share")))
        .unwrap_or_else(|| PathBuf::from("."));
    base.join("aethermesh").join("node-id")
}

/// Reads the node id from `path`, creating one on first run.
///
/// A file that does not contain a valid id is replaced rather than fatal: a
/// truncated write should not keep a node out of the mesh forever.
pub fn load_or_create(path: &Path) -> std::io::Result<NodeId> {
    if let Ok(contents) = std::fs::read_to_string(path) {
        if let Ok(node_id) = contents.trim().parse::<NodeId>() {
            return Ok(node_id);
        }
    }

    let node_id = NodeId::generate();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, node_id.to_string())?;
    info!(%node_id, path = %path.display(), "created node identity");
    Ok(node_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("aethermesh-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn the_id_is_created_once_and_then_reused() {
        let path = temp_dir("identity").join("node-id");
        let _ = std::fs::remove_file(&path);

        let first = load_or_create(&path).unwrap();
        let second = load_or_create(&path).unwrap();

        assert_eq!(first, second);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), first.to_string());
    }

    #[test]
    fn a_corrupt_file_is_replaced() {
        let path = temp_dir("corrupt").join("node-id");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "not-a-uuid").unwrap();

        let node_id = load_or_create(&path).unwrap();

        assert_eq!(std::fs::read_to_string(&path).unwrap(), node_id.to_string());
    }

    #[test]
    fn missing_parent_directories_are_created() {
        let path = temp_dir("nested").join("a/b/node-id");
        let _ = std::fs::remove_dir_all(path.parent().unwrap());

        assert!(load_or_create(&path).is_ok());
        assert!(path.exists());
    }
}
