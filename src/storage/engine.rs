use std::io;
use std::path::{Path, PathBuf};

use super::memtable::Memtable;
use super::wal::{Operation, Wal, WalEntry};

/// The storage engine: ties WAL and memtable together.
///
/// This is the only thing the rest of the database talks to.
/// It provides get/put/delete and handles durability + fast reads
/// internally. Nobody outside this module needs to know that
/// a WAL or memtable exists.
pub struct StorageEngine {
    wal: Wal,
    memtable: Memtable,
    data_dir: PathBuf,
}


impl StorageEngine {
    /// Create or reopen a storage engine at the given directory.
    ///
    /// If the directory already has a WAL file from a previous run,
    /// we replay it to rebuild the memtable — this is crash recovery.
    /// If it's a fresh directory, we start empty.
    pub fn new<P: AsRef<Path>>(data_dir: P) -> io::Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();

        // Create the data directory if it doesn't exist.
        // create_dir_all is like "mkdir -p" — creates parent directories too.
        std::fs::create_dir_all(&data_dir)?;

        // The WAL file lives inside the data directory.
        // .join() appends a path component — like Python's os.path.join.
        // data_dir = "/tmp/mydb" → wal_path = "/tmp/mydb/wal.log"
        let wal_path = data_dir.join("wal.log");
        let wal = Wal::new(&wal_path)?;

        let mut memtable = Memtable::new();

        // --- CRASH RECOVERY ---
        // If a WAL file exists from a previous run, replay it
        // to rebuild the memtable. This is where durability pays off:
        // every entry that was appended and synced is now restored.
        let entries = wal.replay()?;
        let entry_count = entries.len();

        for entry in entries {
            match entry.operation {
                Operation::Put => {
                    // entry.value is Option<String>
                    // For a Put, it should always be Some(value)
                    // .unwrap_or_default() gives "" if somehow None
                    let value = entry.value.unwrap_or_default();
                    memtable.put(entry.key, value);
                }
                Operation::Delete => {
                    memtable.delete(entry.key);
                }
            }
        }

        if entry_count > 0 {
            println!("Recovered {} entries from WAL", entry_count);
        }

        Ok(StorageEngine {
            wal,
            memtable,
            data_dir,
        })
    }

    /// Store a key-value pair.
    ///
    /// The write goes to the WAL first (for durability), then to the
    /// memtable (for fast reads). If the WAL write fails, we don't
    /// update the memtable — this keeps them in sync.
    pub fn put(&mut self, key: String, value: String) -> io::Result<()> {
        // Step 1: Write to WAL FIRST — this is the "write-ahead" part.
        // If this fails, we return the error and the memtable is untouched.
        // The database stays consistent: WAL and memtable always agree.
        let entry = WalEntry {
            operation: Operation::Put,
            key: key.clone(),          // clone because we need the key for both
            value: Some(value.clone()), // WAL and memtable
        };
        self.wal.append(&entry)?;

        // Step 2: Only after the WAL write succeeds, update the memtable.
        self.memtable.put(key, value);

        Ok(())
    }

    /// Delete a key.
    ///
    /// Same pattern as put: WAL first, then memtable.
    pub fn delete(&mut self, key: String) -> io::Result<()> {
        let entry = WalEntry {
            operation: Operation::Delete,
            key: key.clone(),
            value: None,
        };
        self.wal.append(&entry)?;

        self.memtable.delete(key);

        Ok(())
    }

    /// Look up a key.
    ///
    /// Checks the memtable only (for now). Returns:
    ///   Some(value) — the key exists
    ///   None        — the key doesn't exist or was deleted
    ///
    /// Notice this is simpler than the memtable's triple-Option return.
    /// The storage engine flattens it: the caller doesn't need to know
    /// about tombstones. That's an internal concern.
    pub fn get(&self, key: &str) -> Option<String> {
        match self.memtable.get(key) {
            Some(Some(value)) => Some(value.to_string()),
            Some(None) => None,        // tombstone — key was deleted
            None => None,              // key was never written
        }
    }

    /// Return the path to the data directory.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }
}