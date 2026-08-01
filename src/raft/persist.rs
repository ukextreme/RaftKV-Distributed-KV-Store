use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Serialize, Deserialize};
use super::log::LogEntry;

/// The persistent state of a Raft node.
///
/// These three fields are the ONLY things that must survive a restart.
/// Everything else (commit_index, last_applied, role, next_index,
/// match_index) is volatile and gets rebuilt after restart.
///
/// From the Raft paper, Section 5.2:
///   "Persistent state on all servers:
///    (Updated on stable storage before responding to RPCs)"
///
/// That last line is the critical constraint — we must write this
/// to disk BEFORE sending any message that depends on it.
#[derive(Debug, Serialize, Deserialize)]
pub struct PersistentState {
    /// The latest term this node has seen.
    pub current_term: u64,

    /// Who this node voted for in the current term (None if no vote yet).
    pub voted_for: Option<u64>,

    /// The complete Raft log (including the sentinel at index 0).
    pub log: Vec<LogEntry>,
}

/// Handles reading and writing persistent Raft state to disk.
///
/// Uses a simple JSON file. A production system would use a more
/// efficient binary format and atomic writes (write to a temp file,
/// then rename — which is atomic on most filesystems). We keep it
/// simple for clarity.
pub struct StatePersister {
    /// Path to the JSON file where state is saved.
    path: PathBuf,
}


impl StatePersister {
    /// Create a new persister that saves to the given path.
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        StatePersister {
            path: path.as_ref().to_path_buf(),
        }
    }

    /// Save the persistent state to disk.
    ///
    /// This must be called BEFORE sending any message that depends
    /// on the state change. For example:
    ///   - After updating current_term → before sending any response
    ///   - After setting voted_for → before sending the vote response
    ///   - After appending to the log → before sending AppendEntries
    ///
    /// The sequence is always:
    ///   1. Update in-memory state
    ///   2. Call save() to write to disk
    ///   3. Send messages / return actions
    pub fn save(&self, state: &PersistentState) -> io::Result<()> {
        // Serialize the state to a pretty-printed JSON string.
        // serde_json::to_string_pretty formats with indentation
        // so you can read the file with `cat` for debugging.
        // serde_json::to_string would be more compact but harder to read.
        let json = serde_json::to_string_pretty(state)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // Write the JSON to the file.
        // fs::write is an atomic-ish operation: it creates/truncates
        // the file and writes the entire content. It's not truly atomic
        // (a crash mid-write could leave a partial file), but for our
        // purposes it's sufficient.
        //
        // A production system would:
        //   1. Write to a temporary file (e.g., state.json.tmp)
        //   2. fsync the temporary file
        //   3. Rename temp → state.json (rename is atomic on Linux)
        //   4. fsync the directory
        // This guarantees the file is either the old version or the
        // new version, never a partial write.
        fs::write(&self.path, json)?;

        Ok(())
    }

    /// Load the persistent state from disk.
    ///
    /// Returns None if the file doesn't exist (first startup).
    /// Returns an error if the file exists but can't be parsed
    /// (corrupted state — a serious problem in production).
    pub fn load(&self) -> io::Result<Option<PersistentState>> {
        // Check if the file exists
        if !self.path.exists() {
            return Ok(None);
        }

        // Read the entire file into a string
        let json = fs::read_to_string(&self.path)?;

        // Parse the JSON back into a PersistentState struct.
        // serde_json::from_str is the reverse of to_string_pretty.
        // It reads the JSON and reconstructs the struct, including
        // all nested types (Vec<LogEntry>, LogCommand variants, etc.).
        let state: PersistentState = serde_json::from_str(&json)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(Some(state))
    }
}