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

        // Check for environment variable overrides (for Docker)
        for config in all_nodes {
            if config.id != node_id {
                let host_var = format!("PEER_HOST_{}", config.id);
                let host = std::env::var(&host_var)
                    .unwrap_or_else(|_| "127.0.0.1".to_string());

                peer_addresses.insert(
                    config.id,
                    format!("{}:{}", host, config.peer_port),
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
                None => continue,
            };

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

            let json = match serde_json::to_string(&network_msg) {
                Ok(j) => j,
                Err(_) => continue,
            };

            // Spawn a thread for each send so that a dead node
            // doesn't block messages to live nodes.
            // Without this, connecting to the dead node blocks
            // for the timeout duration, and by then the live
            // node has already started a new election.
            thread::spawn(move || {
                use std::net::ToSocketAddrs;

                let sock_addr = match addr.to_socket_addrs() {
                    Ok(mut addrs) => match addrs.next() {
                        Some(a) => a,
                        None => return,
                    },
                    Err(_) => return,
                };

                if let Ok(mut stream) = TcpStream::connect_timeout(
                    &sock_addr,
                    Duration::from_millis(50),
                ) {
                    let line = format!("{}\n", json);
                    let _ = stream.write_all(line.as_bytes());
                    let _ = stream.flush();
                }
            });
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
        all_nodes: Vec<NodeConfig>,
    ) -> std::io::Result<()> {
        let addr = format!("0.0.0.0:{}", peer_port);
        let listener = TcpListener::bind(&addr)?;

        println!("Node {} peer listener on {}", node_id, addr);

        // Create a transport for sending responses immediately
        let transport = PeerTransport::new(node_id, &all_nodes);

        thread::spawn(move || {
            for stream in listener.incoming() {
                let stream = match stream {
                    Ok(s) => {
                        // Set a read timeout so we don't block forever
                        // waiting for more data on a closed connection
                        s.set_read_timeout(Some(Duration::from_millis(500))).ok();
                        s
                    }
                    Err(_) => continue,
                };

                let reader = BufReader::new(stream);

                for line in reader.lines() {
                    let line = match line {
                        Ok(l) => l,
                        Err(_) => break,
                    };

                    if line.is_empty() {
                        continue;
                    }

                    let msg: NetworkMessage = match serde_json::from_str(&line) {
                        Ok(m) => m,
                        Err(e) => {
                            eprintln!("Failed to parse peer message: {}", e);
                            continue;
                        }
                    };

                    // Lock, process, drain messages, then unlock BEFORE sending
                    let outbound = {
                        let mut node = cluster_node.lock().unwrap();
                        Self::dispatch_message(&mut node, msg);
                        node.take_outbound_messages()
                    };
                    // Lock released here

                    // Send outbound messages IMMEDIATELY — don't wait for tick thread
                    if !outbound.is_empty() {
                        transport.send_messages(outbound);
                    }
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