use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use crate::metrics;
use crate::cluster::ClusterNode;
use crate::transport::{NodeConfig, PeerTransport};
use super::protocol::{Command, Response};

/// The database server.
pub struct Server {
    node: Arc<Mutex<ClusterNode>>,
    addr: String,
    transport: PeerTransport,
    peer_port: u16,
    node_id: u64,
    all_nodes: Vec<NodeConfig>,
}

impl Server {
    /// Create a new server for a multi-node cluster.
    pub fn new(
        node_id: u64,
        peers: Vec<u64>,
        client_addr: &str,
        data_dir: &str,
        all_nodes: &[NodeConfig],
    ) -> std::io::Result<Self> {
        let cluster_node = ClusterNode::new(node_id, peers, data_dir)?;

        let peer_port = all_nodes
            .iter()
            .find(|n| n.id == node_id)
            .map(|n| n.peer_port)
            .expect("Node ID not found in config");

        let transport = PeerTransport::new(node_id, all_nodes);

        Ok(Server {
            node: Arc::new(Mutex::new(cluster_node)),
            addr: client_addr.to_string(),
            transport,
            peer_port,
            node_id,
            all_nodes: all_nodes.to_vec(),
        })
    }

    /// Start the server: peer listener + tick thread + client accept loop.
    pub fn run(&self) -> std::io::Result<()> {
        // === START THE METRICS SERVER ===
        let metrics_port = 9100 + self.node_id as u16;
        metrics::start_metrics_server(
            metrics_port,
            self.node_id,
            Arc::clone(&self.node),
        );
        // === START THE PEER LISTENER ===
        PeerTransport::start_listener(
            self.node_id,
            self.peer_port,
            Arc::clone(&self.node),
            self.all_nodes.clone(),
        )?;

        // === START THE TICK THREAD ===
        let tick_node = Arc::clone(&self.node);

        // We need to send outbound messages from the tick thread.
        // Create a second transport for sending.
        let peer_addresses: std::collections::HashMap<u64, String> = self.transport
            .peer_addresses
            .clone();
        let tick_node_id = self.node_id;

        thread::spawn(move || {
            // Create a transport for sending in this thread
            let transport = PeerTransport::from_addresses(tick_node_id, peer_addresses);

            loop {
                thread::sleep(Duration::from_millis(100));

                let messages = {
                    let mut node = tick_node.lock().unwrap();
                    node.tick();
                    node.take_outbound_messages()
                };
                // Lock is released here before sending over network.
                // This is critical: sending over TCP can block, and
                // we don't want to hold the mutex during network I/O.

                if !messages.is_empty() {
                    transport.send_messages(messages);
                }
            }
        });

        // === START THE CLIENT ACCEPT LOOP ===
        let listener = TcpListener::bind(&self.addr)?;
        println!(
            "Node {} client server on {}",
            self.node_id, self.addr
        );

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    self.handle_client(stream);
                }
                Err(e) => {
                    eprintln!("Failed to accept connection: {}", e);
                }
            }
        }

        Ok(())
    }

    fn handle_client(&self, stream: TcpStream) {
        let reader_stream = match stream.try_clone() {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Failed to clone stream: {}", e);
                return;
            }
        };

        let reader = BufReader::new(reader_stream);
        let mut writer = stream;
        let mut lines_iter = reader.lines();

        loop {
            let first_line = match lines_iter.next() {
                Some(Ok(line)) => line,
                Some(Err(_)) => break,
                None => break,
            };

            if !first_line.starts_with('*') {
                let response = Response::Error("invalid protocol".to_string());
                let _ = writer.write_all(response.serialize().as_bytes());
                continue;
            }

            let num_args: usize = match first_line[1..].parse() {
                Ok(n) => n,
                Err(_) => {
                    let response = Response::Error("invalid argument count".to_string());
                    let _ = writer.write_all(response.serialize().as_bytes());
                    continue;
                }
            };

            let mut args: Vec<String> = Vec::new();
            let mut parse_failed = false;

            for _ in 0..num_args {
                let len_line = match lines_iter.next() {
                    Some(Ok(line)) => line,
                    _ => { parse_failed = true; break; }
                };

                if !len_line.starts_with('$') {
                    parse_failed = true;
                    break;
                }

                let data = match lines_iter.next() {
                    Some(Ok(line)) => line,
                    _ => { parse_failed = true; break; }
                };

                args.push(data);
            }

            if parse_failed || args.is_empty() {
                let response = Response::Error("protocol parse error".to_string());
                let _ = writer.write_all(response.serialize().as_bytes());
                continue;
            }

            let command_name = args[0].to_uppercase();
            let command = match command_name.as_str() {
                "GET" if args.len() == 2 => Command::Get {
                    key: args[1].clone(),
                },
                "SET" if args.len() == 3 => Command::Set {
                    key: args[1].clone(),
                    value: args[2].clone(),
                },
                "DEL" if args.len() == 2 => Command::Del {
                    key: args[1].clone(),
                },
                "PING" => Command::Ping,
                _ => Command::Unknown {
                    name: command_name,
                },
            };

            let response = {
                let mut node = self.node.lock().unwrap();

                // Also send any outbound messages generated by
                // client command processing
                let resp = node.handle_client_command(command);
                let pending = node.pending_commit_index;
                let messages = node.take_outbound_messages();
                drop(node); // Release lock before network I/O

                if !messages.is_empty() {
                    self.transport.send_messages(messages);
                }

                // A write is only safe once a majority has replicated it.
                // Replying earlier means an acknowledged write can vanish
                // when the leader fails, so hold the reply until then.
                match pending {
                    Some(idx) => self.await_commit(idx, resp),
                    None => resp,
                }
            };

            let serialized = response.serialize();
            if writer.write_all(serialized.as_bytes()).is_err() {
                break;
            }
        }
    }

    /// Block until `index` is committed by a majority, then return `ok`.
    ///
    /// Polls instead of using a condvar because commit progress is applied by
    /// the tick and peer-listener threads, which need the same mutex — so this
    /// must not hold the lock while waiting.
    fn await_commit(&self, index: usize, ok: Response) -> Response {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            {
                let node = self.node.lock().unwrap();
                if node.raft.state.commit_index >= index {
                    return ok;
                }
            }
            if Instant::now() >= deadline {
                return Response::Error(
                    "TIMEOUT - entry appended but not committed by a majority"
                        .to_string(),
                );
            }
            thread::sleep(Duration::from_micros(200));
        }
    }
}