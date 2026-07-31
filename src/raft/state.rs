use super::log::RaftLog;

/// The three possible roles a Raft node can be in.
///
/// Every node starts as a Follower. If it doesn't hear from a
/// leader within a random timeout, it becomes a Candidate and
/// starts an election. If it wins the election, it becomes the
/// Leader. If it discovers a higher term, it reverts to Follower.
///
/// The state transitions:
///   Follower → Candidate (election timeout, no heartbeat received)
///   Candidate → Leader (won election, got majority votes)
///   Candidate → Follower (discovered higher term, or another leader)
///   Leader → Follower (discovered higher term)
#[derive(Debug, Clone, PartialEq)]
pub enum NodeRole {
    Follower,
    Candidate,
    Leader,
}

/// The complete state of a Raft node.
///
/// Divided into three categories per the Raft paper:
///   1. Persistent state — survives restarts (term, voted_for, log)
///   2. Volatile state on all servers — lost on restart, rebuilt
///   3. Volatile state on leaders only — tracking follower progress
pub struct RaftState {
    // === Identity ===

    /// This node's unique ID within the cluster.
    pub id: u64,

    /// IDs of all nodes in the cluster (including self).
    pub peers: Vec<u64>,

    // === Persistent state (must survive restarts) ===
    // In a production system, these are written to disk after
    // every change. We'll add persistence in M3d.

    /// The latest term this node has seen.
    /// Monotonically increases. Never decreases.
    /// A node updates this when it sees a higher term in any
    /// message, and immediately steps down to Follower.
    pub current_term: u64,

    /// Who this node voted for in the current term.
    /// None if it hasn't voted yet. At most one vote per term —
    /// this is how Raft prevents two leaders in the same term.
    pub voted_for: Option<u64>,

    /// The replicated log.
    pub log: RaftLog,

    // === Volatile state (all servers) ===

    /// Index of the highest log entry known to be committed.
    /// "Committed" means replicated to a majority — safe and
    /// permanent. Entries up to this index can be applied to
    /// the state machine.
    pub commit_index: usize,

    /// Index of the highest log entry applied to the state machine.
    /// Always <= commit_index. The gap between commit_index and
    /// last_applied represents entries that are committed but
    /// haven't been executed yet. The node applies them in order.
    pub last_applied: usize,

    // === Volatile state (leaders only) ===

    /// For each follower: the next log index to send to that follower.
    /// Initialized to leader's last_log_index + 1 after election.
    /// Decremented when AppendEntries is rejected (follower's log
    /// is behind), incremented when it succeeds.
    ///
    /// Stored as a Vec of (node_id, next_index) pairs.
    /// Only meaningful when this node is the Leader.
    pub next_index: Vec<(u64, usize)>,

    /// For each follower: the highest log index known to be
    /// replicated on that follower. Used by the leader to
    /// calculate the commit_index (find the highest index
    /// replicated on a majority).
    ///
    /// Stored as a Vec of (node_id, match_index) pairs.
    /// Only meaningful when this node is the Leader.
    pub match_index: Vec<(u64, usize)>,

    // === Current role ===

    /// What role this node is currently playing.
    pub role: NodeRole,

    /// Who the current leader is, if known.
    /// Followers and candidates track this so they can redirect
    /// client requests to the leader.
    pub leader_id: Option<u64>,
}


impl RaftState {
    /// Create a new Raft node with the given ID and peer list.
    /// Starts as a Follower with term 0 and an empty log.
    pub fn new(id: u64, peers: Vec<u64>) -> Self {
        RaftState {
            id,
            peers,
            current_term: 0,
            voted_for: None,
            log: RaftLog::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: Vec::new(),
            match_index: Vec::new(),
            role: NodeRole::Follower,
            leader_id: None,
        }
    }

    /// Transition to Follower role.
    ///
    /// Called when the node discovers a higher term, or when a
    /// candidate loses an election. Clears leader-specific state
    /// and resets the vote if the term changed.
    pub fn become_follower(&mut self, term: u64) {
        // If the term is higher than ours, update and clear our vote.
        // We can only vote once per term, and a new term means
        // a new election — any previous vote is irrelevant.
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
        }
        self.role = NodeRole::Follower;
        // Clear leader-only state
        self.next_index.clear();
        self.match_index.clear();
    }

    /// Transition to Candidate role and start an election.
    ///
    /// The node increments its term, votes for itself, and will
    /// then send RequestVote RPCs to all other nodes.
    pub fn become_candidate(&mut self) {
        self.current_term += 1;
        self.role = NodeRole::Candidate;
        self.voted_for = Some(self.id); // Vote for yourself
        self.leader_id = None;          // No known leader during election
    }

    /// Transition to Leader role after winning an election.
    ///
    /// Initializes next_index and match_index for all followers.
    pub fn become_leader(&mut self) {
        self.role = NodeRole::Leader;
        self.leader_id = Some(self.id);

        // Initialize next_index for each peer to our last log index + 1
        // (optimistic: assume they're caught up)
        let last = self.log.last_index() + 1;

        self.next_index = self.peers
            .iter()                        // iterate over peer IDs
            .filter(|&&peer_id| peer_id != self.id)  // skip self
            .map(|&peer_id| (peer_id, last))         // pair each with last+1
            .collect();                    // collect into Vec<(u64, usize)>

        // Initialize match_index for each peer to 0
        // (pessimistic: we don't know what they have)
        self.match_index = self.peers
            .iter()
            .filter(|&&peer_id| peer_id != self.id)
            .map(|&peer_id| (peer_id, 0))
            .collect();
    }

    /// How many nodes are needed for a majority (quorum)?
    /// For 5 nodes: 3. For 3 nodes: 2.
    pub fn majority_count(&self) -> usize {
        // Integer division rounds down, so (5/2)+1 = 3, (3/2)+1 = 2
        (self.peers.len() / 2) + 1
    }
}