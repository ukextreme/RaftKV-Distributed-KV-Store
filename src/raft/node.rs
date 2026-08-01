use std::collections::HashSet;

use super::log::{LogCommand, LogEntry, RaftLog};
use super::message::{
    AppendEntriesRequest, AppendEntriesResponse,
    RequestVoteRequest, RequestVoteResponse,
};
use super::state::{NodeRole, RaftState};

/// An action the Raft node wants the outside world to perform.
///
/// The node itself doesn't send network messages or write to disk —
/// it produces these actions, and the runtime (transport layer)
/// executes them. This separation keeps the Raft logic pure and testable.
#[derive(Debug)]
pub enum Action {
    /// Send a RequestVote RPC to a specific node.
    SendRequestVote {
        to: u64,
        request: RequestVoteRequest,
    },

    /// Send a RequestVote response back to the candidate.
    SendRequestVoteResponse {
        to: u64,
        response: RequestVoteResponse,
    },

    /// Send an AppendEntries RPC to a specific node.
    SendAppendEntries {
        to: u64,
        request: AppendEntriesRequest,
    },

    /// Send an AppendEntries response back to the leader.
    SendAppendEntriesResponse {
        to: u64,
        response: AppendEntriesResponse,
    },

    /// Apply a committed log entry to the state machine.
    /// This is how Raft tells the storage engine to execute a command.
    ApplyEntry {
        index: usize,
        command: LogCommand,
    },
}

/// Configuration for timeout behavior.
/// In a real system these would be milliseconds.
/// Here they're in "ticks" — each call to tick() is one tick.
const ELECTION_TIMEOUT_MIN: u64 = 10;
const ELECTION_TIMEOUT_MAX: u64 = 20;
const HEARTBEAT_INTERVAL: u64 = 3;

/// The core Raft node.
///
/// This is a state machine: you feed it inputs (messages and ticks)
/// and it produces outputs (actions to perform). It holds all the
/// Raft state plus election-specific bookkeeping.
pub struct RaftNode {
    /// All persistent and volatile Raft state.
    pub state: RaftState,

    /// Ticks remaining until election timeout fires.
    /// When this reaches 0, a follower/candidate starts an election.
    /// Reset whenever we hear from the leader (heartbeat or AppendEntries).
    election_timeout: u64,

    /// The randomized timeout value for this election period.
    /// Randomized to prevent all nodes from timing out simultaneously.
    election_timeout_duration: u64,

    /// Ticks remaining until the leader sends the next heartbeat.
    /// Only meaningful when this node is the leader.
    heartbeat_timeout: u64,

    /// Set of node IDs that have voted for us in the current election.
    /// Only meaningful when this node is a candidate.
    votes_received: HashSet<u64>,

    /// A simple counter used for pseudo-random timeout generation.
    /// A real system would use a proper random number generator,
    /// but for our purposes this is sufficient and avoids adding
    /// a dependency.
    random_seed: u64,
}

impl RaftNode {
    /// Create a new Raft node.
    pub fn new(id: u64, peers: Vec<u64>) -> Self {
        let mut node = RaftNode {
            state: RaftState::new(id, peers),
            election_timeout: 0,
            election_timeout_duration: 0,
            heartbeat_timeout: HEARTBEAT_INTERVAL,
            votes_received: HashSet::new(),
            random_seed: id, // Seed with node ID so each node gets different timeouts
        };
        node.reset_election_timeout();
        node
    }

    /// Generate a pseudo-random election timeout between MIN and MAX.
    ///
    /// Uses a simple linear congruential generator (LCG) — the same
    /// basic approach as early C rand() implementations. Not
    /// cryptographically secure, but perfectly fine for jittering
    /// election timeouts.
    fn reset_election_timeout(&mut self) {
        // LCG formula: seed = (seed * a + c) % m
        // These constants are from Numerical Recipes
        self.random_seed = self.random_seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);

        // Map the random seed to our timeout range
        let range = ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN;
        let random_offset = (self.random_seed % range) + ELECTION_TIMEOUT_MIN;

        self.election_timeout_duration = random_offset;
        self.election_timeout = random_offset;
    }

    /// Process one unit of time passing.
    ///
    /// This is called periodically by the runtime. It handles:
    /// - Election timeouts (followers/candidates → start election)
    /// - Heartbeat intervals (leaders → send heartbeats)
    ///
    /// Returns a list of actions to perform.
    pub fn tick(&mut self) -> Vec<Action> {
        match self.state.role {
            NodeRole::Follower | NodeRole::Candidate => {
                self.tick_election()
            }
            NodeRole::Leader => {
                self.tick_heartbeat()
            }
        }
    }

    /// Handle election timeout for followers and candidates.
    ///
    /// Decrements the timer. When it reaches 0, starts an election.
    fn tick_election(&mut self) -> Vec<Action> {
        // Decrement the countdown
        if self.election_timeout > 0 {
            self.election_timeout -= 1;
        }

        // If the timer hasn't expired, nothing to do
        if self.election_timeout > 0 {
            return Vec::new();
        }

        // Timer expired! Start an election.
        self.start_election()
    }

    /// Handle heartbeat interval for leaders.
    ///
    /// Decrements the timer. When it reaches 0, sends heartbeats
    /// to all followers.
    fn tick_heartbeat(&mut self) -> Vec<Action> {
        if self.heartbeat_timeout > 0 {
            self.heartbeat_timeout -= 1;
        }

        if self.heartbeat_timeout > 0 {
            return Vec::new();
        }

        // Time to send heartbeats
        self.heartbeat_timeout = HEARTBEAT_INTERVAL;
        self.send_heartbeats()
    }

    /// Start a new election.
    ///
    /// 1. Transition to Candidate
    /// 2. Vote for ourselves
    /// 3. Reset election timeout (in case we need to retry)
    /// 4. Send RequestVote to all other nodes
    fn start_election(&mut self) -> Vec<Action> {
        // Transition to candidate — increments term, votes for self
        self.state.become_candidate();

        // Track our own vote
        self.votes_received.clear();
        self.votes_received.insert(self.state.id);

        // Reset the timeout — if this election fails (vote split),
        // we'll wait a new random duration before trying again
        self.reset_election_timeout();

        let mut actions = Vec::new();

        // Check if we're the only node (single-node cluster)
        // If so, we already have a majority (1 out of 1)
        if self.state.peers.len() == 1 {
            self.state.become_leader();
            return actions;
        }

        // Send RequestVote to every other node
        for &peer_id in &self.state.peers {
            // Skip ourselves — we already voted for ourselves
            if peer_id == self.state.id {
                continue;
            }

            actions.push(Action::SendRequestVote {
                to: peer_id,
                request: RequestVoteRequest {
                    term: self.state.current_term,
                    candidate_id: self.state.id,
                    last_log_index: self.state.log.last_index(),
                    last_log_term: self.state.log.last_term(),
                },
            });
        }

        actions
    }

    /// Send heartbeats (empty AppendEntries) to all followers.
    ///
    /// Heartbeats serve two purposes:
    /// 1. Prevent followers from starting unnecessary elections
    /// 2. Carry the leader's commit_index so followers can advance theirs
    /// Send heartbeats (with any pending entries) to all followers.
    ///
    /// This replaces the old send_heartbeats that always sent empty
    /// messages. Now it sends whatever entries each follower needs.
    /// If a follower is fully caught up, the entries list will be
    /// empty — which is exactly a heartbeat.
    fn send_heartbeats(&self) -> Vec<Action> {
        self.replicate_to_all_peers()
    }

    /// Handle an incoming RequestVote request.
    ///
    /// Called when another node is running for leader and asks us to vote.
    /// We vote yes only if:
    ///   1. The candidate's term is at least as high as ours
    ///   2. We haven't already voted for someone else this term
    ///   3. The candidate's log is at least as up-to-date as ours
    ///
    /// Returns actions (the vote response to send back).
    pub fn handle_request_vote(
        &mut self,
        request: RequestVoteRequest,
    ) -> Vec<Action> {
        let mut actions = Vec::new();

        // Rule 1: If the candidate's term is less than ours,
        // they're stale — reject immediately.
        if request.term < self.state.current_term {
            actions.push(Action::SendRequestVoteResponse {
                to: request.candidate_id,
                response: RequestVoteResponse {
                    term: self.state.current_term,
                    vote_granted: false,
                },
            });
            return actions;
        }

        // If the candidate's term is higher than ours, we're stale.
        // Step down to follower and update our term.
        // This is a critical Raft rule: any node that sees a higher
        // term immediately steps down, regardless of its current role.
        if request.term > self.state.current_term {
            self.state.become_follower(request.term);
        }

        // Rule 2: Check if we can vote for this candidate.
        // We can vote if:
        //   - We haven't voted yet this term (voted_for is None), OR
        //   - We already voted for this same candidate (idempotent)
        let can_vote = match self.state.voted_for {
            None => true,
            Some(id) => id == request.candidate_id,
        };

        // Rule 3: The candidate's log must be at least as up-to-date
        // as ours. "Up-to-date" is defined as:
        //   - Higher last_log_term wins, OR
        //   - If same last_log_term, higher last_log_index wins
        //
        // This is the ELECTION RESTRICTION — the most important
        // safety property in Raft. It guarantees that a leader
        // always has every committed entry. Without this, a node
        // with a stale log could win an election and overwrite
        // committed data.
        let candidate_up_to_date = {
            let our_last_term = self.state.log.last_term();
            let our_last_index = self.state.log.last_index();

            // Compare: first by term (higher is better),
            // then by index (higher is better if terms are equal)
            request.last_log_term > our_last_term
                || (request.last_log_term == our_last_term
                    && request.last_log_index >= our_last_index)
        };

        let vote_granted = can_vote && candidate_up_to_date;

        if vote_granted {
            // Record our vote
            self.state.voted_for = Some(request.candidate_id);
            // Reset election timeout — we just heard from a valid
            // candidate, so don't start our own election
            self.reset_election_timeout();
        }

        actions.push(Action::SendRequestVoteResponse {
            to: request.candidate_id,
            response: RequestVoteResponse {
                term: self.state.current_term,
                vote_granted,
            },
        });

        actions
    }

    /// Handle an incoming RequestVote response.
    ///
    /// Called when we're a candidate and a node replies to our vote request.
    /// If we get enough votes (a majority), we become the leader.
    pub fn handle_request_vote_response(
        &mut self,
        from: u64,
        response: RequestVoteResponse,
    ) -> Vec<Action> {
        // If we're no longer a candidate (maybe we already won,
        // or stepped down), ignore the response.
        if self.state.role != NodeRole::Candidate {
            return Vec::new();
        }

        // If the response has a higher term, we're stale.
        // Step down immediately.
        if response.term > self.state.current_term {
            self.state.become_follower(response.term);
            return Vec::new();
        }

        // Ignore responses from old terms
        if response.term != self.state.current_term {
            return Vec::new();
        }

        // Count the vote if granted
        if response.vote_granted {
            self.votes_received.insert(from);
        }

        // Check if we have a majority
        if self.votes_received.len() >= self.state.majority_count() {
            // We won the election!
            self.state.become_leader();
            self.heartbeat_timeout = 0; // Send heartbeats immediately

            // Immediately send heartbeats to assert leadership
            // Setting heartbeat_timeout to 0 means the next tick()
            // will trigger send_heartbeats(). But we can also
            // send them right now for faster convergence.
            return self.send_heartbeats();
        }

        Vec::new()
    }

    /// Handle an incoming AppendEntries request.
    ///
    /// This is the most complex handler in Raft. It handles:
    /// - Heartbeats (empty entries) — reset election timeout
    /// - Log replication (with entries) — append to our log
    /// - Commit advancement — update our commit_index
    ///
    /// For M3b, we implement the heartbeat and basic structure.
    /// Log replication details come in M3c.
    pub fn handle_append_entries(
        &mut self,
        request: AppendEntriesRequest,
    ) -> Vec<Action> {
        let mut actions = Vec::new();

        // If the leader's term is less than ours, reject.
        // This "leader" is stale and should step down.
        if request.term < self.state.current_term {
            actions.push(Action::SendAppendEntriesResponse {
                to: request.leader_id,
                response: AppendEntriesResponse {
                    term: self.state.current_term,
                    success: false,
                    match_index: None,
                },
            });
            return actions;
        }

        // Valid leader — update our state.
        // If term is higher or equal, accept this node as leader.
        // become_follower handles the term update and vote clearing.
        self.state.become_follower(request.term);
        self.state.leader_id = Some(request.leader_id);

        // Reset election timeout — we heard from the leader,
        // so don't start an unnecessary election.
        self.reset_election_timeout();

        // === LOG CONSISTENCY CHECK ===
        // Verify that our log matches the leader's at prev_log_index.
        // If it doesn't, our logs have diverged and we reject.

        let prev_term = self.state.log.term_at(request.prev_log_index);

        if prev_term != request.prev_log_term {
            // Our log doesn't match at the specified point.
            // Tell the leader to back up and try with earlier entries.
            actions.push(Action::SendAppendEntriesResponse {
                to: request.leader_id,
                response: AppendEntriesResponse {
                    term: self.state.current_term,
                    success: false,
                    // Tell the leader where our log ends so it can
                    // jump back efficiently instead of decrementing by 1
                    match_index: Some(self.state.log.last_index()),
                },
            });
            return actions;
        }

        // === APPEND NEW ENTRIES ===
        // The consistency check passed. Now append any new entries.

        if !request.entries.is_empty() {
            // Check for conflicts: if an existing entry has the same
            // index but a different term, delete it and everything after.
            let mut insert_index = request.prev_log_index + 1;

            for entry in &request.entries {
                let existing_term = self.state.log.term_at(insert_index);

                if existing_term == 0 && insert_index > self.state.log.last_index() {
                    // We've gone past the end of our log — no conflict,
                    // just append from here
                    break;
                }

                if existing_term != entry.term {
                    // Conflict! Truncate from this point
                    self.state.log.truncate_from(insert_index);
                    break;
                }

                // Entry matches — skip it (already have it)
                insert_index += 1;
            }

            // Append any entries we don't already have
            let new_entries_start = insert_index - (request.prev_log_index + 1);
            let new_entries: Vec<LogEntry> = request.entries
                [new_entries_start..]
                .to_vec();

            if !new_entries.is_empty() {
                self.state.log.append_entries(new_entries);
            }
        }

        // === ADVANCE COMMIT INDEX ===
        // The leader tells us its commit_index. We advance ours
        // to the minimum of the leader's commit and our last index
        // (we can't commit entries we don't have yet).
        if request.leader_commit > self.state.commit_index {
            let new_commit = std::cmp::min(
                request.leader_commit,
                self.state.log.last_index(),
            );

            // Apply newly committed entries to the state machine
            actions.extend(self.apply_committed_entries(new_commit));
        }

        // Success! Tell the leader how far our log extends.
        actions.push(Action::SendAppendEntriesResponse {
            to: request.leader_id,
            response: AppendEntriesResponse {
                term: self.state.current_term,
                success: true,
                match_index: Some(self.state.log.last_index()),
            },
        });

        actions
    }

    /// Apply committed but not-yet-applied entries to the state machine.
    ///
    /// Entries between last_applied+1 and new_commit_index get applied
    /// in order. Returns ApplyEntry actions for each one.
    fn apply_committed_entries(&mut self, new_commit_index: usize) -> Vec<Action> {
        let mut actions = Vec::new();

        // Apply entries one by one, in order
        while self.state.last_applied < new_commit_index {
            self.state.last_applied += 1;
            let index = self.state.last_applied;

            // Get the entry at this index and produce an apply action
            if let Some(entry) = self.state.log.get(index) {
                match &entry.command {
                    LogCommand::Noop => {
                        // No-ops don't produce apply actions —
                        // they exist only for Raft bookkeeping
                    }
                    cmd => {
                        actions.push(Action::ApplyEntry {
                            index,
                            command: cmd.clone(),
                        });
                    }
                }
            }
        }

        // Update commit_index
        self.state.commit_index = new_commit_index;

        actions
    }
    /// Handle an incoming AppendEntries response (leader only).
    ///
    /// Updates next_index and match_index for the responding follower.
    /// If enough followers have replicated an entry, advance commit_index.
    pub fn handle_append_entries_response(
        &mut self,
        from: u64,
        response: AppendEntriesResponse,
    ) -> Vec<Action> {
        // Only leaders process these
        if self.state.role != NodeRole::Leader {
            return Vec::new();
        }

        // If the follower has a higher term, step down
        if response.term > self.state.current_term {
            self.state.become_follower(response.term);
            return Vec::new();
        }

        if response.success {
            // The follower accepted our entries.
            // Update match_index and next_index for this follower.
            if let Some(match_idx) = response.match_index {
                // Find this follower in our tracking arrays and update
                for (id, mi) in &mut self.state.match_index {
                    if *id == from {
                        *mi = match_idx;
                        break;
                    }
                }
                for (id, ni) in &mut self.state.next_index {
                    if *id == from {
                        *ni = match_idx + 1;
                        break;
                    }
                }
            }

            // Check if we can advance the commit index.
            // An entry is committed if it's replicated on a majority
            // AND it's from the current term.
            return self.maybe_advance_commit_index();
        } else {
            // The follower rejected — its log doesn't match.
            // Decrement next_index for this follower and we'll
            // retry with earlier entries on the next heartbeat.
            if let Some(match_idx) = response.match_index {
                // Follower told us where its log ends — jump there
                for (id, ni) in &mut self.state.next_index {
                    if *id == from {
                        *ni = match_idx + 1;
                        break;
                    }
                }
            } else {
                // No hint — decrement by 1
                for (id, ni) in &mut self.state.next_index {
                    if *id == from {
                        if *ni > 1 {
                            *ni -= 1;
                        }
                        break;
                    }
                }
            }
        }

        Vec::new()
    }

    /// Check if any new entries can be committed.
    ///
    /// An entry at index N is committed if:
    ///   1. A majority of nodes have match_index >= N
    ///   2. The entry at index N is from the current term
    ///
    /// Condition 2 is subtle but critical: a leader can only commit
    /// entries from its OWN term, not previous terms. Entries from
    /// previous terms get committed indirectly when a current-term
    /// entry after them is committed. This prevents a subtle safety
    /// violation described in Section 5.4.2 of the Raft paper.
    fn maybe_advance_commit_index(&mut self) -> Vec<Action> {
        let old_commit = self.state.commit_index;

        // Check each index from commit_index+1 to last_index
        for index in (self.state.commit_index + 1)..=self.state.log.last_index() {
            // Condition 2: only commit entries from our current term
            if self.state.log.term_at(index) != self.state.current_term {
                continue;
            }

            // Condition 1: count how many nodes have this entry
            // Start at 1 because the leader always has it
            let mut replication_count = 1;

            for &(_, match_idx) in &self.state.match_index {
                if match_idx >= index {
                    replication_count += 1;
                }
            }

            // If a majority has it, this entry (and all before it) are committed
            if replication_count >= self.state.majority_count() {
                self.state.commit_index = index;
            }
        }

        // Apply any newly committed entries
        if self.state.commit_index > old_commit {
            return self.apply_committed_entries(self.state.commit_index);
        }

        Vec::new()
    }

    /// Propose a new command to the cluster.
    ///
    /// Only the leader can accept proposals. The command is:
    /// 1. Appended to the leader's log
    /// 2. Immediately sent to all followers for replication
    ///
    /// The command is NOT committed yet — it's only committed once
    /// a majority of nodes have replicated it. The caller should
    /// wait for the ApplyEntry action to know it's safe.
    ///
    /// Returns actions (AppendEntries to send to followers).
    /// Returns None if this node is not the leader.
    pub fn propose(&mut self, command: LogCommand) -> Option<Vec<Action>> {
        // Only leaders can accept proposals
        if self.state.role != NodeRole::Leader {
            return None;
        }

        // Append the command to our log with the current term
        let entry = LogEntry {
            term: self.state.current_term,
            command,
        };
        self.state.log.append(entry);

        // Immediately replicate to all followers
        // Don't wait for the next heartbeat — latency matters
        let actions = self.replicate_to_all_peers();

        Some(actions)
    }

    /// Send pending log entries to all followers.
    ///
    /// For each follower, checks next_index to determine which
    /// entries they need, and sends an AppendEntries with those entries.
    /// If a follower is fully caught up, this sends an empty
    /// AppendEntries (a heartbeat).
    fn replicate_to_all_peers(&self) -> Vec<Action> {
        let mut actions = Vec::new();

        for &peer_id in &self.state.peers {
            if peer_id == self.state.id {
                continue;
            }

            if let Some(action) = self.replicate_to_peer(peer_id) {
                actions.push(action);
            }
        }

        actions
    }

    /// Send pending log entries to a specific follower.
    ///
    /// Looks up next_index for this follower to determine:
    /// - prev_log_index and prev_log_term (for the consistency check)
    /// - which entries to send (everything from next_index onwards)
    ///
    /// Returns None if the peer isn't found in our tracking arrays
    /// (shouldn't happen, but defensive programming).
    fn replicate_to_peer(&self, peer_id: u64) -> Option<Action> {
        // Find next_index for this peer
        // .iter() gives us references to the (id, index) tuples
        // .find() returns the first element matching the condition
        // It returns Option<&(u64, usize)> — None if not found
        let next_idx = self.state.next_index
            .iter()
            .find(|(id, _)| *id == peer_id)
            .map(|(_, idx)| *idx)?;

        // prev_log_index is the entry just before what we're sending
        // next_idx is where we start sending, so prev is next_idx - 1
        let prev_log_index = if next_idx > 0 { next_idx - 1 } else { 0 };
        let prev_log_term = self.state.log.term_at(prev_log_index);

        // Get the entries to send: everything from next_idx onwards
        // .to_vec() makes a copy — we need to own the entries to put
        // them in the message, but the log retains its copy too
        let entries = self.state.log.entries_from(next_idx).to_vec();

        Some(Action::SendAppendEntries {
            to: peer_id,
            request: AppendEntriesRequest {
                term: self.state.current_term,
                leader_id: self.state.id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.state.commit_index,
            },
        })
    }
}