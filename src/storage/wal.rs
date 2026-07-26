use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

/// The type of operation stored in the WAL.
/// Each entry is either setting a value or deleting a key.
#[derive(Debug, Clone, PartialEq)]
pub enum Operation {
    Put,
    Delete,
}

/// A single entry in the write-ahead log.
/// This is what gets serialized to disk for every mutation.
#[derive(Debug, Clone)]
pub struct WalEntry {
    pub operation: Operation,
    pub key: String,
    pub value: Option<String>, // Some("...") for Put, None for Delete
}

/// The write-ahead log itself.
/// Wraps a file handle and provides append + replay.
pub struct Wal {
    writer: BufWriter<File>,
    path: PathBuf,
}

impl Wal {
    /// Create a new WAL, opening (or creating) the log file at the given path.
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();

        let file = OpenOptions::new()
            .create(true)    // Create the file if it doesn't exist
            .append(true)    // All writes go to the end (never overwrite)
            .open(&path)?;   // Actually open it; ? propagates errors

        let writer = BufWriter::new(file);

        Ok(Wal { writer, path })
    }

    /// Serialize a WalEntry into the text format: "PUT|key|value\n" or "DELETE|key\n"
    fn serialize_entry(entry: &WalEntry) -> String {
        match &entry.operation {
            Operation::Put => {
                // .as_deref() converts Option<String> to Option<&str>
                // .unwrap_or("") gives us "" if somehow None (shouldn't happen for Put)
                let value = entry.value.as_deref().unwrap_or("");
                format!("PUT|{}|{}\n", entry.key, value)
            }
            Operation::Delete => {
                format!("DELETE|{}\n", entry.key)
            }
        }
    }

    /// Append a single entry to the WAL and flush to disk.
    /// This is the core durability guarantee — after this returns Ok(()),
    /// the entry is on disk and will survive a crash.
    pub fn append(&mut self, entry: &WalEntry) -> io::Result<()> {
        let serialized = Self::serialize_entry(entry);
        self.writer.write_all(serialized.as_bytes())?;
        self.sync()?;
        Ok(())
    }

    /// Force all buffered data to the physical disk.
    /// After this returns, the data survives power loss.
    pub fn sync(&mut self) -> io::Result<()> {
        // Step 1: Flush the BufWriter's internal buffer to the OS
        self.writer.flush()?;

        // Step 2: Get a reference to the underlying File inside the BufWriter
        // .get_ref() returns &File without consuming the BufWriter
        let file = self.writer.get_ref();

        // Step 3: Tell the OS to write its page cache to the physical disk
        // This is the actual fsync system call
        file.sync_all()?;

        Ok(())
    }

    /// Read all entries from the WAL file.
    /// Called on startup to rebuild state after a crash.
    pub fn replay(&self) -> io::Result<Vec<WalEntry>> {
        let mut entries = Vec::new();
        let mut contents = String::new();

        // Open the file separately for reading
        let mut file = File::open(&self.path)?;
        file.read_to_string(&mut contents)?;

        for line in contents.lines() {
            let parts: Vec<&str> = line.split('|').collect();

            let entry = match parts.as_slice() {
                ["PUT", key, value] => WalEntry {
                    operation: Operation::Put,
                    key: key.to_string(),
                    value: Some(value.to_string()),
                },
                ["DELETE", key] => WalEntry {
                    operation: Operation::Delete,
                    key: key.to_string(),
                    value: None,
                },
                _ => continue, // Skip malformed lines
            };

            entries.push(entry);
        }

        Ok(entries)
    }
}