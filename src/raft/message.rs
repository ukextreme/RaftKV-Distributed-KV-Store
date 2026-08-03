use serde::{Serialize, Deserialize};
use super::log::LogEntry;

// ============================================================
// RequestVote RPC
// ============================================================
// Sent by a CANDIDATE to all other nodes during an election.
// "I want to be leader. Here's my credentials. Vote for me?"

/// The request a candidate sends to ask for votes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteRequest {
    /// The candidate's current term.
    /// If the receiver's term is higher, the candidate is stale
    /// and the receiver rejects the vote immediately.
    pub term: u64,

    /// The candidate's node ID.
    /// So the receiver knows who's asking.
    pub candidate_id: u64,

    /// Index of the candidate's last log entry.
    /// Used for the "up-to-date" check: a node only votes for
    /// a candidate whose log is at least as complete as its own.
    pub last_log_index: usize,

    /// Term of the candidate's last log entry.
    /// Also part of the up-to-date check. The comparison is:
    /// first compare last_log_term (higher is more up-to-date),
    /// then if equal, compare last_log_index (higher is more).
    pub last_log_term: u64,
}

/// The response to a vote request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    /// The receiver's current term.
    /// If higher than the candidate's term, the candidate
    /// learns it's stale and steps down to follower.
    pub term: u64,

    /// Did the receiver grant its vote?
    /// true = "yes, I vote for you"
    /// false = "no" (already voted for someone else, or
    ///         the candidate's log isn't up-to-date enough)
    pub vote_granted: bool,
}

// ============================================================
// AppendEntries RPC
// ============================================================
// Sent by the LEADER to followers. Two purposes:
// 1. With entries: replicate new log entries
// 2. Without entries (empty): heartbeat to prevent elections

/// The request a leader sends to replicate log entries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    /// The leader's current term.
    pub term: u64,

    /// The leader's node ID.
    /// So followers know who the current leader is
    /// (useful for redirecting client requests).
    pub leader_id: u64,

    /// Index of the log entry immediately BEFORE the new ones.
    /// The follower checks that its log matches at this point.
    /// If it doesn't, the logs have diverged and the follower
    /// rejects the request — the leader will then back up and
    /// retry with earlier entries.
    pub prev_log_index: usize,

    /// Term of the entry at prev_log_index.
    /// Part of the consistency check — the follower verifies
    /// both the index AND the term match.
    pub prev_log_term: u64,

    /// The new entries to append (may be empty for heartbeats).
    /// In normal operation, this is usually one entry at a time,
    /// but after a follower falls behind, the leader sends a
    /// batch to catch it up.
    pub entries: Vec<LogEntry>,

    /// The leader's commit index — the highest log index known
    /// to be committed (replicated to a majority). The follower
    /// uses this to advance its own commit index and apply
    /// entries to its state machine.
    pub leader_commit: usize,
}

/// The response to an AppendEntries request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    /// The follower's current term.
    /// If higher than the leader's, the leader discovers it's
    /// stale and steps down.
    pub term: u64,

    /// Did the follower accept the entries?
    /// false means the consistency check failed — the follower's
    /// log doesn't match at prev_log_index/prev_log_term.
    /// The leader will decrement prev_log_index and retry.
    pub success: bool,

    /// Optimization: if the follower rejects, it tells the leader
    /// how far back to jump. Without this, the leader decrements
    /// by 1 each time, which is slow if the follower is far behind.
    /// This is an optimization from the Raft paper's section 5.3.
    pub match_index: Option<usize>,
}