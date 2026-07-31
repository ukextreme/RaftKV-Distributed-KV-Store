/// A single entry in the Raft log.
///
/// Every operation that changes the database becomes a log entry.
/// The entry records WHAT to do (the command) and WHEN it was
/// proposed (the term). The term is crucial for Raft's safety —
/// it tells you which leader created this entry.
#[derive(Debug, Clone, PartialEq)]
pub struct LogEntry {
    /// The term when this entry was created by the leader.
    /// Used to detect stale entries and resolve conflicts.
    pub term: u64,

    /// The actual command to execute.
    /// For now, this is a simple enum — Put or Delete.
    /// A production system might use a more efficient binary format.
    pub command: LogCommand,
}

/// The command stored in a log entry.
/// This is what actually gets applied to the storage engine.
#[derive(Debug, Clone, PartialEq)]
pub enum LogCommand {
    /// Store a key-value pair
    Put { key: String, value: String },

    /// Delete a key
    Delete { key: String },

    /// A no-op entry. Leaders append this on election to commit
    /// entries from previous terms. We'll use this later.
    Noop,
}

/// The Raft log: an ordered, indexed sequence of entries.
///
/// Indices start at 1 (not 0). Index 0 is a sentinel — it
/// represents "before the log began" and has term 0. This
/// simplifies the boundary conditions in Raft's algorithm
/// significantly, because you never have to special-case
/// "what if the log is empty?"
///
/// Think of it as:
///   Index: 0        1        2        3
///   Entry: sentinel  first    second   third
///   Term:  0        1        1        2
pub struct RaftLog {
    /// The actual entries. entries[0] is the sentinel.
    entries: Vec<LogEntry>,
}


impl RaftLog {
    /// Create a new log with just the sentinel entry at index 0.
    pub fn new() -> Self {
        RaftLog {
            // Start with one entry: the sentinel at index 0, term 0
            entries: vec![LogEntry {
                term: 0,
                command: LogCommand::Noop,
            }],
        }
    }

    /// Get the entry at the given index.
    /// Returns None if the index is out of bounds.
    ///
    /// Note: index 0 returns the sentinel, which is valid but
    /// should never be applied to the state machine.
    pub fn get(&self, index: usize) -> Option<&LogEntry> {
        self.entries.get(index)
    }

    /// The index of the last entry in the log.
    /// Returns 0 if the log only contains the sentinel (empty log).
    pub fn last_index(&self) -> usize {
        // .len() includes the sentinel, so subtract 1
        // A fresh log has len=1 (just sentinel), so last_index=0
        self.entries.len() - 1
    }

    /// The term of the last entry in the log.
    /// Returns 0 if the log only contains the sentinel.
    pub fn last_term(&self) -> u64 {
        // .last() returns Option<&LogEntry> — the last element
        // .map() transforms it — extract just the term
        // .unwrap_or(0) — if somehow empty (impossible), use 0
        self.entries.last().map(|e| e.term).unwrap_or(0)
    }

    /// The term of the entry at the given index.
    /// Returns 0 if the index is out of bounds (treats missing
    /// entries as having term 0, matching the sentinel convention).
    pub fn term_at(&self, index: usize) -> u64 {
        self.entries.get(index).map(|e| e.term).unwrap_or(0)
    }

    /// Append a new entry to the end of the log.
    /// Returns the index of the newly appended entry.
    pub fn append(&mut self, entry: LogEntry) -> usize {
        self.entries.push(entry);
        self.last_index()
    }

    /// Append multiple entries at once.
    /// Returns the index of the last appended entry.
    pub fn append_entries(&mut self, entries: Vec<LogEntry>) -> usize {
        // .extend() appends all elements from an iterator
        // to the end of the vector. More efficient than
        // calling .push() in a loop because it can pre-allocate
        // space for all the new elements at once.
        self.entries.extend(entries);
        self.last_index()
    }

    /// Truncate the log from the given index onwards.
    ///
    /// This is used when a follower discovers its log conflicts
    /// with the leader's. The leader's log is authoritative, so
    /// the follower removes the conflicting entries and replaces
    /// them with the leader's version.
    ///
    /// Example: if the log has entries at indices [0,1,2,3,4]
    /// and you call truncate_from(3), entries at 3 and 4 are
    /// removed, leaving [0,1,2].
    pub fn truncate_from(&mut self, index: usize) {
        // .truncate(n) keeps the first n elements and drops the rest.
        // We want to keep indices 0 through index-1, which is
        // exactly `index` elements (because of 0-indexing).
        if index < self.entries.len() {
            self.entries.truncate(index);
        }
    }

    /// Get a slice of entries from start_index to the end.
    /// Used when the leader needs to send entries to a follower.
    ///
    /// Returns an empty slice if start_index is beyond the log.
    pub fn entries_from(&self, start_index: usize) -> &[LogEntry] {
        if start_index >= self.entries.len() {
            // Return an empty slice — no entries to send
            &[]
        } else {
            // &self.entries[start_index..] — a slice from start_index
            // to the end. This is a reference, not a copy — it points
            // into the existing vector without allocating new memory.
            &self.entries[start_index..]
        }
    }

    /// How many real entries are in the log (excluding the sentinel).
    pub fn len(&self) -> usize {
        // Subtract 1 for the sentinel
        self.entries.len() - 1
    }
}