mod storage;
mod network;
mod raft;

use network::server::Server;

fn main() {
    println!("raftkv starting...");

    // Start the server on port 6380 (not 6379, to avoid
    // conflicting with any real Redis that might be running)
    let mut server = match Server::new("127.0.0.1:6380", "/tmp/raftkv-data") {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to start server: {}", e);
            std::process::exit(1);
        }
    };

    if let Err(e) = server.run() {
        eprintln!("Server error: {}", e);
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::wal::{Operation, Wal, WalEntry};
    use std::fs;

    #[test]
    fn test_wal_append_and_replay() {
        let test_path = "/tmp/test_wal.log";

        // Clean up from any previous test run
        let _ = fs::remove_file(test_path);

        // Create a WAL and append some entries
        {
            let mut wal = Wal::new(test_path).expect("Failed to create WAL");

            wal.append(&WalEntry {
                operation: Operation::Put,
                key: "name".to_string(),
                value: Some("uday".to_string()),
            })
            .expect("Failed to append");

            wal.append(&WalEntry {
                operation: Operation::Put,
                key: "balance".to_string(),
                value: Some("5000".to_string()),
            })
            .expect("Failed to append");

            wal.append(&WalEntry {
                operation: Operation::Delete,
                key: "name".to_string(),
                value: None,
            })
            .expect("Failed to append");
        }
        // The WAL is dropped (closed) here because the block ended.
        // This simulates a restart — the file is closed, we reopen it.

        // Reopen and replay
        let wal = Wal::new(test_path).expect("Failed to reopen WAL");
        let entries = wal.replay().expect("Failed to replay");

        // Verify we got all 3 entries back
        assert_eq!(entries.len(), 3);

        assert_eq!(entries[0].key, "name");
        assert_eq!(entries[0].operation, Operation::Put);
        assert_eq!(entries[0].value, Some("uday".to_string()));

        assert_eq!(entries[1].key, "balance");
        assert_eq!(entries[1].operation, Operation::Put);
        assert_eq!(entries[1].value, Some("5000".to_string()));

        assert_eq!(entries[2].key, "name");
        assert_eq!(entries[2].operation, Operation::Delete);
        assert_eq!(entries[2].value, None);

        // Clean up
        let _ = fs::remove_file(test_path);
    }

    #[test]
    fn test_memtable_basic_operations() {
        use crate::storage::memtable::Memtable;

        let mut table = Memtable::new();

        // Test 1: empty table returns None (key not found)
        assert!(table.get("name").is_none());
        assert!(table.is_empty());

        // Test 2: put a value and read it back
        table.put("name".to_string(), "uday".to_string());
        assert_eq!(table.get("name"), Some(Some("uday")));
        assert_eq!(table.len(), 1);

        // Test 3: overwrite the value
        table.put("name".to_string(), "pranay".to_string());
        assert_eq!(table.get("name"), Some(Some("pranay")));
        assert_eq!(table.len(), 1); // still 1 entry, not 2

        // Test 4: add more keys
        table.put("balance".to_string(), "5000".to_string());
        assert_eq!(table.len(), 2);
        assert_eq!(table.get("balance"), Some(Some("5000")));

        // Test 5: delete a key — should become a tombstone
        table.delete("name".to_string());
        // This is Some(None), NOT None — the key exists but is dead
        assert_eq!(table.get("name"), Some(None));

        // Test 6: a key that was never inserted is different from deleted
        assert!(table.get("nonexistent").is_none()); // None — never seen
        assert_eq!(table.get("name"), Some(None));   // Some(None) — tombstone

        // Test 7: size tracking
        assert!(table.size() > 0);

        // Test 8: clear
        table.clear();
        assert!(table.is_empty());
        assert_eq!(table.size(), 0);
        assert!(table.get("name").is_none()); // gone after clear
    }

    #[test]
    fn test_engine_crash_recovery() {
        use crate::storage::engine::StorageEngine;
        use std::fs;

        let test_dir = "/tmp/test_engine_recovery";

        // Clean up from previous runs
        let _ = fs::remove_dir_all(test_dir);

        // === FIRST "LIFETIME" OF THE DATABASE ===
        // Write some data, then drop (simulate crash)
        {
            let mut engine = StorageEngine::new(test_dir)
                .expect("Failed to create engine");

            engine.put("name".to_string(), "uday".to_string())
                .expect("Failed to put");
            engine.put("balance".to_string(), "5000".to_string())
                .expect("Failed to put");
            engine.put("city".to_string(), "kharagpur".to_string())
                .expect("Failed to put");
            engine.delete("city".to_string())
                .expect("Failed to delete");

            // Verify reads work during normal operation
            assert_eq!(engine.get("name"), Some("uday".to_string()));
            assert_eq!(engine.get("balance"), Some("5000".to_string()));
            assert_eq!(engine.get("city"), None); // deleted
        }
        // engine is dropped here — simulates a crash
        // All in-memory state (the memtable) is gone

        // === SECOND "LIFETIME" — RECOVERY ===
        // Reopen the database. The WAL should restore everything.
        {
            let engine = StorageEngine::new(test_dir)
                .expect("Failed to reopen engine");

            // Everything that was written should be back
            assert_eq!(engine.get("name"), Some("uday".to_string()));
            assert_eq!(engine.get("balance"), Some("5000".to_string()));

            // The delete should also be recovered — city should still be gone
            assert_eq!(engine.get("city"), None);

            // Keys that never existed should still not exist
            assert_eq!(engine.get("nonexistent"), None);
        }

        // Clean up
        let _ = fs::remove_dir_all(test_dir);
    }

    #[test]
    fn test_raft_log_operations() {
        use crate::raft::log::{LogCommand, LogEntry, RaftLog};

        let mut log = RaftLog::new();

        // Fresh log has just the sentinel
        assert_eq!(log.last_index(), 0);
        assert_eq!(log.last_term(), 0);
        assert_eq!(log.len(), 0);

        // Append an entry at term 1
        let idx = log.append(LogEntry {
            term: 1,
            command: LogCommand::Put {
                key: "name".to_string(),
                value: "uday".to_string(),
            },
        });
        assert_eq!(idx, 1);              // First real entry is at index 1
        assert_eq!(log.last_index(), 1);
        assert_eq!(log.last_term(), 1);
        assert_eq!(log.len(), 1);

        // Append another at term 1
        log.append(LogEntry {
            term: 1,
            command: LogCommand::Put {
                key: "balance".to_string(),
                value: "5000".to_string(),
            },
        });
        assert_eq!(log.last_index(), 2);
        assert_eq!(log.len(), 2);

        // Append one at term 2 (new leader)
        log.append(LogEntry {
            term: 2,
            command: LogCommand::Delete {
                key: "name".to_string(),
            },
        });
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.last_term(), 2);

        // term_at works correctly
        assert_eq!(log.term_at(0), 0);  // sentinel
        assert_eq!(log.term_at(1), 1);
        assert_eq!(log.term_at(2), 1);
        assert_eq!(log.term_at(3), 2);
        assert_eq!(log.term_at(99), 0); // out of bounds = 0

        // entries_from returns the right slice
        let from_2 = log.entries_from(2);
        assert_eq!(from_2.len(), 2); // entries at index 2 and 3

        // Truncate from index 3 — removes entry at index 3
        log.truncate_from(3);
        assert_eq!(log.last_index(), 2);
        assert_eq!(log.last_term(), 1); // back to term 1
        assert_eq!(log.len(), 2);
    }

    #[test]
    fn test_raft_state_transitions() {
        use crate::raft::state::{NodeRole, RaftState};

        let mut state = RaftState::new(1, vec![1, 2, 3]);

        // Starts as follower, term 0
        assert_eq!(state.role, NodeRole::Follower);
        assert_eq!(state.current_term, 0);
        assert_eq!(state.voted_for, None);

        // Become candidate — term increments, votes for self
        state.become_candidate();
        assert_eq!(state.role, NodeRole::Candidate);
        assert_eq!(state.current_term, 1);
        assert_eq!(state.voted_for, Some(1)); // voted for self

        // Win election — become leader
        state.become_leader();
        assert_eq!(state.role, NodeRole::Leader);
        assert_eq!(state.leader_id, Some(1));
        // next_index initialized for peers 2 and 3
        assert_eq!(state.next_index.len(), 2);
        assert_eq!(state.match_index.len(), 2);

        // Discover higher term — step down to follower
        state.become_follower(5);
        assert_eq!(state.role, NodeRole::Follower);
        assert_eq!(state.current_term, 5);
        assert_eq!(state.voted_for, None); // vote cleared for new term
        assert!(state.next_index.is_empty()); // leader state cleared

        // Majority of 3 nodes = 2
        assert_eq!(state.majority_count(), 2);
    }

    #[test]
    fn test_leader_election() {
        use crate::raft::node::{Action, RaftNode};
        use crate::raft::state::NodeRole;
        use crate::raft::message::RequestVoteResponse;

        // Create a 3-node cluster
        let peers = vec![1, 2, 3];
        let mut node1 = RaftNode::new(1, peers.clone());
        let mut node2 = RaftNode::new(2, peers.clone());
        let mut node3 = RaftNode::new(3, peers.clone());

        // All start as followers
        assert_eq!(node1.state.role, NodeRole::Follower);
        assert_eq!(node2.state.role, NodeRole::Follower);
        assert_eq!(node3.state.role, NodeRole::Follower);

        // Tick node1 until it times out and starts an election.
        // We tick up to 25 times (max timeout is 20).
        let mut election_actions = Vec::new();
        for _ in 0..25 {
            let actions = node1.tick();
            if !actions.is_empty() {
                election_actions = actions;
                break;
            }
        }

        // Node1 should be a candidate now
        assert_eq!(node1.state.role, NodeRole::Candidate);
        assert_eq!(node1.state.current_term, 1);

        // It should have sent RequestVote to nodes 2 and 3
        assert_eq!(election_actions.len(), 2);

        // Extract the vote requests and deliver them
        for action in &election_actions {
            match action {
                Action::SendRequestVote { to, request } => {
                    // Deliver the vote request to the target node
                    let response_actions = if *to == 2 {
                        node2.handle_request_vote(request.clone())
                    } else {
                        node3.handle_request_vote(request.clone())
                    };

                    // Each should respond with a vote
                    assert_eq!(response_actions.len(), 1);

                    // Deliver the response back to node1
                    if let Action::SendRequestVoteResponse { response, .. } =
                        &response_actions[0]
                    {
                        assert!(response.vote_granted);
                        node1.handle_request_vote_response(
                            *to,
                            response.clone(),
                        );
                    }
                }
                _ => panic!("Expected SendRequestVote action"),
            }
        }

        // Node1 should now be the leader!
        assert_eq!(node1.state.role, NodeRole::Leader);
        assert_eq!(node1.state.current_term, 1);
        assert_eq!(node1.state.leader_id, Some(1));

        // Nodes 2 and 3 should be followers at term 1
        assert_eq!(node2.state.role, NodeRole::Follower);
        assert_eq!(node2.state.current_term, 1);
        assert_eq!(node3.state.role, NodeRole::Follower);
        assert_eq!(node3.state.current_term, 1);
    }

    #[test]
    fn test_log_replication() {
        use crate::raft::log::LogCommand;
        use crate::raft::node::{Action, RaftNode};
        use crate::raft::state::NodeRole;

        // === SETUP: elect node1 as leader ===
        let peers = vec![1, 2, 3];
        let mut node1 = RaftNode::new(1, peers.clone());
        let mut node2 = RaftNode::new(2, peers.clone());
        let mut node3 = RaftNode::new(3, peers.clone());

        // Fast-forward node1 to become leader
        // (We tested election in detail already; here we just
        //  set it up quickly to test replication)

        // Tick node1 until it starts an election
        let mut vote_requests = Vec::new();
        for _ in 0..25 {
            let actions = node1.tick();
            if !actions.is_empty() {
                vote_requests = actions;
                break;
            }
        }
        assert_eq!(node1.state.role, NodeRole::Candidate);

        // Deliver votes from node2 and node3
        for action in &vote_requests {
            if let Action::SendRequestVote { to, request } = action {
                let responses = if *to == 2 {
                    node2.handle_request_vote(request.clone())
                } else {
                    node3.handle_request_vote(request.clone())
                };

                for resp_action in &responses {
                    if let Action::SendRequestVoteResponse { response, .. } = resp_action {
                        node1.handle_request_vote_response(*to, response.clone());
                    }
                }
            }
        }
        assert_eq!(node1.state.role, NodeRole::Leader);

        // === TEST: propose a write and replicate it ===

        // Client sends: SET name uday
        let actions = node1.propose(LogCommand::Put {
            key: "name".to_string(),
            value: "uday".to_string(),
        }).expect("Leader should accept proposal");

        // The leader should send AppendEntries to both followers
        assert_eq!(actions.len(), 2);

        // Verify the leader's log has the entry
        assert_eq!(node1.state.log.last_index(), 1);
        assert_eq!(node1.state.log.last_term(), 1);

        // Entry is NOT committed yet — only the leader has it
        assert_eq!(node1.state.commit_index, 0);

        // === Deliver AppendEntries to followers ===

        let mut all_response_actions = Vec::new();

        for action in &actions {
            if let Action::SendAppendEntries { to, request } = action {
                let response_actions = if *to == 2 {
                    node2.handle_append_entries(request.clone())
                } else {
                    node3.handle_append_entries(request.clone())
                };

                // Each follower should accept and respond
                for resp_action in response_actions {
                    all_response_actions.push((*to, resp_action));
                }
            }
        }

        // Verify followers have the entry in their logs
        assert_eq!(node2.state.log.last_index(), 1);
        assert_eq!(node3.state.log.last_index(), 1);

        // === Deliver responses back to leader ===

        let mut apply_actions = Vec::new();

        for (from, action) in &all_response_actions {
            if let Action::SendAppendEntriesResponse { response, .. } = action {
                let leader_actions = node1.handle_append_entries_response(
                    *from,
                    response.clone(),
                );

                // Collect any ApplyEntry actions
                for a in leader_actions {
                    if matches!(&a, Action::ApplyEntry { .. }) {
                        apply_actions.push(a);
                    }
                }
            }
        }

        // === Verify: entry is now committed ===

        // Leader's commit_index should have advanced to 1
        assert_eq!(node1.state.commit_index, 1);

        // The leader should have produced an ApplyEntry action
        assert!(!apply_actions.is_empty());

        // Verify the apply action contains the right command
        if let Action::ApplyEntry { index, command } = &apply_actions[0] {
            assert_eq!(*index, 1);
            match command {
                LogCommand::Put { key, value } => {
                    assert_eq!(key, "name");
                    assert_eq!(value, "uday");
                }
                _ => panic!("Expected Put command"),
            }
        } else {
            panic!("Expected ApplyEntry action");
        }

        // === TEST: propose a second write ===

        let actions = node1.propose(LogCommand::Put {
            key: "balance".to_string(),
            value: "5000".to_string(),
        }).expect("Leader should accept proposal");

        // Deliver to node2 only (node3 is "slow" / partitioned)
        for action in &actions {
            if let Action::SendAppendEntries { to, request } = action {
                if *to == 2 {
                    let responses = node2.handle_append_entries(request.clone());

                    for resp_action in &responses {
                        if let Action::SendAppendEntriesResponse { response, .. } = resp_action {
                            node1.handle_append_entries_response(2, response.clone());
                        }
                    }
                }
                // Skip node3 — simulating it being slow/unreachable
            }
        }

        // Should STILL commit — node1 + node2 = 2 out of 3 = majority
        assert_eq!(node1.state.commit_index, 2);

        // Node2 has both entries
        assert_eq!(node2.state.log.last_index(), 2);

        // Node3 still only has the first entry
        assert_eq!(node3.state.log.last_index(), 1);

        // === Verify: non-leader cannot propose ===

        let result = node2.propose(LogCommand::Put {
            key: "test".to_string(),
            value: "fail".to_string(),
        });
        assert!(result.is_none()); // Followers can't propose
    }
    #[test]
    fn test_raft_persistence() {
        use crate::raft::log::{LogCommand, LogEntry};
        use crate::raft::node::RaftNode;
        use crate::raft::state::NodeRole;
        use std::fs;

        let test_dir = "/tmp/test_raft_persist";
        let _ = fs::remove_dir_all(test_dir);

        let peers = vec![1, 2, 3];

        // === FIRST LIFETIME: create node, start election, add entries ===
        {
            let mut node = RaftNode::with_persistence(1, peers.clone(), test_dir)
                .expect("Failed to create node");

            // Tick until election starts
            for _ in 0..25 {
                let actions = node.tick();
                if !actions.is_empty() {
                    break;
                }
            }

            assert_eq!(node.state.current_term, 1);
            assert_eq!(node.state.voted_for, Some(1));

            // Manually become leader and add a log entry
            node.state.become_leader();
            node.state.log.append(LogEntry {
                term: 1,
                command: LogCommand::Put {
                    key: "name".to_string(),
                    value: "uday".to_string(),
                },
            });

            // Save the state (normally done by handler methods,
            // but we modified state directly here)
            node.persist_for_test();
        }
        // Node dropped — simulates crash. Memory wiped.

        // === SECOND LIFETIME: recover from disk ===
        {
            let node = RaftNode::with_persistence(1, peers.clone(), test_dir)
                .expect("Failed to recover node");

            // Persistent state: restored
            assert_eq!(node.state.current_term, 1);
            assert_eq!(node.state.voted_for, Some(1));
            assert_eq!(node.state.log.last_index(), 1);
            assert_eq!(node.state.log.last_term(), 1);

            // Check the actual log entry content
            let entry = node.state.log.get(1).expect("Entry should exist");
            assert_eq!(entry.term, 1);
            match &entry.command {
                LogCommand::Put { key, value } => {
                    assert_eq!(key, "name");
                    assert_eq!(value, "uday");
                }
                _ => panic!("Expected Put command"),
            }

            // Volatile state: reset to defaults
            assert_eq!(node.state.role, NodeRole::Follower);
            assert_eq!(node.state.commit_index, 0);
            assert_eq!(node.state.last_applied, 0);
        }

        let _ = fs::remove_dir_all(test_dir);
    }
}