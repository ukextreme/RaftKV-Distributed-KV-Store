use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use serde::{Serialize, Deserialize};

use crate::cluster::{ClusterNode, MessagePayload, PeerMessage};
use crate::raft::message::{
    AppendEntriesRequest, AppendEntriesResponse,
    RequestVoteRequest, RequestVoteResponse,
};

/// A network message between Raft peers.
///
/// This wraps the four Raft RPC types into a single enum
/// that can be serialized to JSON and sent over TCP.
/// The `from` field identifies the sender so the receiver
/// knows who to send the response back to.
#[derive(Debug, Serialize, Deserialize)]
pub enum NetworkMessage {
    RequestVote {
        from: u64,
        request: RequestVoteRequest,
    },
    RequestVoteResponse {
        from: u64,
        response: RequestVoteResponse,
    },
    AppendEntries {
        from: u64,
        request: AppendEntriesRequest,
    },
    AppendEntriesResponse {
        from: u64,
        response: AppendEntriesResponse,
    },
}

/// Configuration for a single node in the cluster.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub id: u64,
    pub client_port: u16,
    pub peer_port: u16,
}

/// The transport layer: handles sending and receiving Raft
/// messages between nodes over TCP.
///
/// Uses newline-delimited JSON: each message is one line of
/// JSON followed by \n. Simple to parse, easy to debug with
/// tools like netcat, and sufficient for our throughput needs.
pub struct PeerTransport {
    /// This node's ID.
    node_id: u64,

    /// Map from node ID to peer address ("127.0.0.1:port").
    pub peer_addresses: HashMap<u64, String>,
}

impl PeerTransport {
    /// Create a new transport for the given node.
    pub fn new(node_id: u64, all_nodes: &[NodeConfig]) -> Self {
        let mut peer_addresses = HashMap::new();

        for config in all_nodes {
            if config.id != node_id {
                peer_addresses.insert(
                    config.id,
                    format!("127.0.0.1:{}", config.peer_port),
                );
            }
        }

        PeerTransport {
            node_id,
            peer_addresses,
        }
    }

    /// Send outbound messages to peer nodes.
    ///
    /// Takes the queued PeerMessages from the ClusterNode and
    /// delivers them over TCP. Uses short-lived connections:
    /// connect, send, close. Not efficient (a production system
    /// would maintain persistent connections), but correct and
    /// simple.
    ///
    /// Failed sends are silently ignored — Raft is designed to
    /// tolerate message loss. The leader will retry on the next
    /// heartbeat, and elections will retry on timeout.
    pub fn send_messages(&self, messages: Vec<PeerMessage>) {
        for msg in messages {
            let addr = match self.peer_addresses.get(&msg.to) {
                Some(addr) => addr.clone(),
                None => continue, // Unknown peer, skip
            };

            // Convert PeerMessage to NetworkMessage
            let network_msg = match msg.payload {
                MessagePayload::RequestVote(req) => {
                    NetworkMessage::RequestVote {
                        from: self.node_id,
                        request: req,
                    }
                }
                MessagePayload::RequestVoteResponse(resp) => {
                    NetworkMessage::RequestVoteResponse {
                        from: self.node_id,
                        response: resp,
                    }
                }
                MessagePayload::AppendEntries(req) => {
                    NetworkMessage::AppendEntries {
                        from: self.node_id,
                        request: req,
                    }
                }
                MessagePayload::AppendEntriesResponse(resp) => {
                    NetworkMessage::AppendEntriesResponse {
                        from: self.node_id,
                        response: resp,
                    }
                }
            };

            // Serialize to JSON
            let json = match serde_json::to_string(&network_msg) {
                Ok(j) => j,
                Err(_) => continue,
            };

            // Connect and send. If anything fails, skip silently.
            // Raft handles message loss gracefully — the leader
            // retries, elections retry, nothing depends on a single
            // message getting through.
            if let Ok(mut stream) = TcpStream::connect_timeout(
                &addr.parse().unwrap(),
                Duration::from_millis(100),
            ) {
                let line = format!("{}\n", json);
                let _ = stream.write_all(line.as_bytes());
                let _ = stream.flush();
            }
            // Connection failure is normal — the peer might be down.
            // Raft tolerates this by design.
        }
    }

    /// Start a listener thread that accepts incoming peer messages
    /// and feeds them to the ClusterNode.
    ///
    /// Runs in a background thread. Incoming messages are parsed
    /// and dispatched to the appropriate Raft handler.
    pub fn start_listener(
        node_id: u64,
        peer_port: u16,
        cluster_node: Arc<Mutex<ClusterNode>>,
    ) -> std::io::Result<()> {
        let addr = format!("127.0.0.1:{}", peer_port);
        let listener = TcpListener::bind(&addr)?;

        println!("Node {} peer listener on {}", node_id, addr);

        // Set non-blocking would complicate things; instead,
        // we spawn a thread that blocks on accept.
        thread::spawn(move || {
            for stream in listener.incoming() {
                let stream = match stream {
                    Ok(s) => s,
                    Err(_) => continue,
                };

                // Read one message per connection
                // (our send side connects, sends one message, closes)
                let reader = BufReader::new(stream);

                for line in reader.lines() {
                    let line = match line {
                        Ok(l) => l,
                        Err(_) => break,
                    };

                    if line.is_empty() {
                        continue;
                    }

                    // Parse the JSON message
                    let msg: NetworkMessage = match serde_json::from_str(&line) {
                        Ok(m) => m,
                        Err(e) => {
                            eprintln!("Failed to parse peer message: {}", e);
                            continue;
                        }
                    };

                    // Lock the cluster node and dispatch the message
                    let mut node = cluster_node.lock().unwrap();
                    Self::dispatch_message(&mut node, msg);
                }
            }
        });

        Ok(())
    }

    /// Dispatch a received network message to the appropriate
    /// Raft handler on the ClusterNode.
    fn dispatch_message(node: &mut ClusterNode, msg: NetworkMessage) {
        let actions = match msg {
            NetworkMessage::RequestVote { from: _, request } => {
                node.raft.handle_request_vote(request)
            }
            NetworkMessage::RequestVoteResponse { from, response } => {
                node.raft.handle_request_vote_response(from, response)
            }
            NetworkMessage::AppendEntries { from: _, request } => {
                node.raft.handle_append_entries(request)
            }
            NetworkMessage::AppendEntriesResponse { from, response } => {
                node.raft.handle_append_entries_response(from, response)
            }
        };

        // Process any actions produced by the handler
        node.process_actions(actions);
    }

    /// Create a transport from an existing address map.
    /// Used by the tick thread which needs its own transport instance.
    pub fn from_addresses(node_id: u64, peer_addresses: HashMap<u64, String>) -> Self {
        PeerTransport {
            node_id,
            peer_addresses,
        }
    }
}