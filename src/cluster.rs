use crate::raft::log::LogCommand;
use crate::raft::node::{Action, RaftNode};
use crate::raft::state::NodeRole;
use crate::storage::engine::StorageEngine;
use crate::network::protocol::{Command, Response};

/// A complete cluster node: Raft consensus + storage engine.
///
/// This is the integration layer that connects the pieces:
/// - Client commands come in through handle_client_command()
/// - Writes go through Raft (propose → replicate → commit → apply)
/// - Reads go directly to the storage engine
/// - Raft actions are processed (apply entries, queue messages for peers)
///
/// In a multi-node setup, each physical server runs one ClusterNode.
pub struct ClusterNode {
    /// The Raft consensus module.
    pub raft: RaftNode,

    /// The local storage engine (WAL + memtable).
    storage: StorageEngine,

    /// Outbound messages queued for delivery to peer nodes.
    /// The transport layer drains this queue and sends them over TCP.
    /// Key: destination node ID. Value: list of serialized messages.
    pub outbound_messages: Vec<PeerMessage>,
}

/// A message to be sent to a peer node.
/// The transport layer reads these and delivers them over the network.
#[derive(Debug)]
pub struct PeerMessage {
    pub to: u64,
    pub payload: MessagePayload,
}

/// The actual content of a peer message.
/// Wraps the Raft RPC types so they can be routed.
#[derive(Debug)]
pub enum MessagePayload {
    RequestVote(crate::raft::message::RequestVoteRequest),
    RequestVoteResponse(crate::raft::message::RequestVoteResponse),
    AppendEntries(crate::raft::message::AppendEntriesRequest),
    AppendEntriesResponse(crate::raft::message::AppendEntriesResponse),
}

impl ClusterNode {
    /// Create a new cluster node.
    pub fn new(
        node_id: u64,
        peers: Vec<u64>,
        data_dir: &str,
    ) -> std::io::Result<Self> {
        // Each node gets its own subdirectory for storage
        let storage_dir = format!("{}/storage", data_dir);
        let storage = StorageEngine::new(&storage_dir)?;

        let raft = RaftNode::with_persistence(node_id, peers, data_dir)?;

        Ok(ClusterNode {
            raft,
            storage,
            outbound_messages: Vec::new(),
        })
    }

    /// Handle a client command (from redis-cli or any RESP client).
    ///
    /// This is the main entry point for client requests.
    /// - Reads go directly to the storage engine (fast, local)
    /// - Writes go through Raft (propose → replicate → commit → apply)
    pub fn handle_client_command(&mut self, command: Command) -> Response {
        match command {
            Command::Get { key } => {
                match self.storage.get(&key) {
                    Some(value) => Response::BulkString(value),
                    None => Response::Null,
                }
            }

            Command::Set { key, value } => {
                // Only the leader can accept writes
                if self.raft.state.role != NodeRole::Leader {
                    return Response::Error(
                        "NOTLEADER - this node is not the leader".to_string()
                    );
                }

                // Propose through Raft
                let command = LogCommand::Put { key, value };
                match self.raft.propose(command) {
                    Some(actions) => {
                        self.process_actions(actions);
                        Response::SimpleString("OK".to_string())
                    }
                    None => {
                        Response::Error("failed to propose".to_string())
                    }
                }
            }

            Command::Del { key } => {
                if self.raft.state.role != NodeRole::Leader {
                    return Response::Error(
                        "NOTLEADER - this node is not the leader".to_string()
                    );
                }

                let command = LogCommand::Delete { key };
                match self.raft.propose(command) {
                    Some(actions) => {
                        self.process_actions(actions);
                        Response::Integer(1)
                    }
                    None => {
                        Response::Error("failed to propose".to_string())
                    }
                }
            }

            Command::Ping => {
                Response::SimpleString("PONG".to_string())
            }

            Command::Unknown { name } => {
                Response::Error(format!("unknown command '{}'", name))
            }
        }
    }

    /// Process a Raft tick (called periodically by the tick thread).
    ///
    /// This drives elections and heartbeats. Returns nothing —
    /// actions are processed internally.
    pub fn tick(&mut self) {
        let actions = self.raft.tick();
        self.process_actions(actions);
    }

    /// Process actions produced by the Raft node.
    ///
    /// This is the bridge between Raft's pure logic and the real world:
    /// - ApplyEntry → execute against the storage engine
    /// - Send* → queue for delivery to peer nodes
    pub fn process_actions(&mut self, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::ApplyEntry { index: _, command } => {
                    // Apply the committed command to the storage engine.
                    // This is where Raft meets storage — the moment a
                    // replicated, committed command actually takes effect.
                    match command {
                        LogCommand::Put { key, value } => {
                            if let Err(e) = self.storage.put(key, value) {
                                eprintln!("Failed to apply put: {}", e);
                            }
                        }
                        LogCommand::Delete { key } => {
                            if let Err(e) = self.storage.delete(key) {
                                eprintln!("Failed to apply delete: {}", e);
                            }
                        }
                        LogCommand::Noop => {
                            // No-ops exist for Raft bookkeeping only
                        }
                    }
                }

                Action::SendRequestVote { to, request } => {
                    self.outbound_messages.push(PeerMessage {
                        to,
                        payload: MessagePayload::RequestVote(request),
                    });
                }

                Action::SendRequestVoteResponse { to, response } => {
                    self.outbound_messages.push(PeerMessage {
                        to,
                        payload: MessagePayload::RequestVoteResponse(response),
                    });
                }

                Action::SendAppendEntries { to, request } => {
                    self.outbound_messages.push(PeerMessage {
                        to,
                        payload: MessagePayload::AppendEntries(request),
                    });
                }

                Action::SendAppendEntriesResponse { to, response } => {
                    self.outbound_messages.push(PeerMessage {
                        to,
                        payload: MessagePayload::AppendEntriesResponse(response),
                    });
                }
            }
        }
    }

    /// Drain all outbound messages (called by the transport layer).
    pub fn take_outbound_messages(&mut self) -> Vec<PeerMessage> {
        // .drain(..) removes all elements from the vector and
        // returns them as an iterator. .collect() gathers them
        // into a new Vec. The original vector is left empty.
        // This is a "take all and clear" operation — the transport
        // gets the messages, and the queue is reset.
        self.outbound_messages.drain(..).collect()
    }
}