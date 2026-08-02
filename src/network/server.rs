use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::cluster::ClusterNode;
use super::protocol::{Command, Response};

/// The database server.
///
/// Manages client connections and a background tick thread.
/// The ClusterNode is shared between the main thread (handling
/// client commands) and the tick thread (driving Raft elections
/// and heartbeats) via Arc<Mutex<>>.
pub struct Server {
    /// The cluster node, shared between threads.
    ///
    /// Arc = Atomic Reference Count: allows multiple threads to
    ///       own the same data. When the last Arc is dropped,
    ///       the data is freed.
    /// Mutex = Mutual Exclusion: only one thread can access the
    ///         inner data at a time. .lock() acquires access,
    ///         and the lock is released when the guard is dropped.
    ///
    /// Together, Arc<Mutex<T>> is THE standard Rust pattern for
    /// "shared mutable state across threads."
    node: Arc<Mutex<ClusterNode>>,
    addr: String,
}

impl Server {
    /// Create a new server backed by a cluster node.
    pub fn new(
        node_id: u64,
        peers: Vec<u64>,
        addr: &str,
        data_dir: &str,
    ) -> std::io::Result<Self> {
        let cluster_node = ClusterNode::new(node_id, peers, data_dir)?;

        Ok(Server {
            node: Arc::new(Mutex::new(cluster_node)),
            addr: addr.to_string(),
        })
    }

    /// Start the server: tick thread + client accept loop.
    pub fn run(&self) -> std::io::Result<()> {
        // === START THE TICK THREAD ===

        // Arc::clone creates a new reference to the same data.
        // It doesn't copy the ClusterNode — it increments the
        // reference count. Both `tick_node` and `self.node` point
        // at the same Mutex<ClusterNode>.
        let tick_node = Arc::clone(&self.node);

        // thread::spawn creates a new OS thread.
        // The `move` keyword transfers ownership of `tick_node`
        // into the closure — the new thread now owns its Arc handle.
        // Without `move`, the closure would try to borrow from
        // the current scope, which doesn't live long enough.
        thread::spawn(move || {
            loop {
                // Sleep for 50ms between ticks.
                // This means our "tick" unit is ~50ms.
                // Election timeout of 10-20 ticks = 500ms-1000ms.
                // Heartbeat interval of 3 ticks = 150ms.
                thread::sleep(Duration::from_millis(50));

                // Lock the mutex to get exclusive access.
                // .lock() returns a Result — it can fail if another
                // thread panicked while holding the lock (a "poisoned"
                // mutex). .unwrap() crashes on poison; in production
                // you'd handle this more gracefully.
                let mut node = tick_node.lock().unwrap();

                // Tick the Raft node
                node.tick();

                // In M3f, we'd also drain and deliver outbound messages here.
                // For now, single-node, there are no peers to send to.
            }
            // Lock is automatically released here when `node` goes out of scope.
            // This is RAII — the same principle as your WAL file handles.
        });

        // === START THE CLIENT ACCEPT LOOP ===

        let listener = TcpListener::bind(&self.addr)?;
        println!("raftkv server listening on {}", self.addr);

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let addr = stream
                        .peer_addr()
                        .map(|a| a.to_string())
                        .unwrap_or_else(|_| "unknown".to_string());
                    println!("Client connected: {}", addr);

                    self.handle_client(stream);

                    println!("Client disconnected: {}", addr);
                }
                Err(e) => {
                    eprintln!("Failed to accept connection: {}", e);
                }
            }
        }

        Ok(())
    }

    /// Handle a single client connection.
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

            // === THE KEY CHANGE FROM M2 ===
            // Instead of calling storage engine directly, we go
            // through the ClusterNode, which routes through Raft.
            let response = {
                // Lock the mutex — this blocks if the tick thread
                // currently holds the lock. The lock is released
                // at the end of this block.
                let mut node = self.node.lock().unwrap();
                node.handle_client_command(command)
            };
            // Mutex released here — tick thread can proceed

            let serialized = response.serialize();
            if writer.write_all(serialized.as_bytes()).is_err() {
                break;
            }
        }
    }
}