use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};

use crate::storage::engine::StorageEngine;
use super::protocol::{Command, Response};

/// The database server.
/// Listens on a TCP port and handles client connections,
/// translating RESP commands into storage engine operations.
pub struct Server {
    storage: StorageEngine,
    addr: String,
}


impl Server {
    /// Create a new server backed by a storage engine at the given directory.
    pub fn new(addr: &str, data_dir: &str) -> std::io::Result<Self> {
        let storage = StorageEngine::new(data_dir)?;
        Ok(Server {
            storage,
            addr: addr.to_string(),
        })
    }

    /// Start listening for connections and handle them.
    ///
    /// This function runs forever (until the process is killed).
    /// It accepts one connection at a time — a real database would
    /// use threads or async I/O to handle many clients concurrently,
    /// but single-threaded is correct and simple for now.
    pub fn run(&mut self) -> std::io::Result<()> {
        // Bind to the address and port.
        // TcpListener::bind is like calling socket() + bind() + listen()
        // in C — Rust combines them into one call.
        let listener = TcpListener::bind(&self.addr)?;
        println!("raftkv server listening on {}", self.addr);

        // .incoming() returns an iterator of new connections.
        // Each time a client connects, this yields a new TcpStream.
        // It blocks (waits) when no client is connecting.
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    // Get the client's address for logging
                    let addr = stream
                        .peer_addr()
                        .map(|a| a.to_string())
                        .unwrap_or_else(|_| "unknown".to_string());
                    println!("Client connected: {}", addr);

                    // Handle this client's commands until they disconnect
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
    ///
    /// Reads commands in a loop, processes each one, and sends
    /// the response back. Runs until the client disconnects.
    fn handle_client(&mut self, stream: TcpStream) {
        // BufReader wraps the TCP stream and adds buffered reading.
        // .lines() will give us one complete line at a time.
        // But RESP is multi-line, so we need to read raw lines
        // and accumulate them into complete messages.

        // We clone the stream because we need one handle for reading
        // (wrapped in BufReader) and one for writing. try_clone()
        // creates a second handle to the same underlying connection —
        // like dup() in C. Both handles refer to the same socket.
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
        
        // Main command loop: read and process commands until disconnection
        loop {
            // Read the first line of a RESP message
            // .lines() returns Option<Result<String, Error>>:
            //   None → client disconnected (stream closed)
            //   Some(Err(...)) → read error
            //   Some(Ok(line)) → a line of text
            let first_line = match lines_iter.next() {
                Some(Ok(line)) => line,
                Some(Err(_)) => break,   // Read error — disconnect
                None => break,           // Client disconnected
            };

            // Parse the RESP message starting from this first line
            if !first_line.starts_with('*') {
                // Not a RESP array — skip it
                let response = Response::Error("invalid protocol".to_string());
                let _ = writer.write_all(response.serialize().as_bytes());
                continue;
            }

            // Read the number of arguments
            let num_args: usize = match first_line[1..].parse() {
                Ok(n) => n,
                Err(_) => {
                    let response = Response::Error("invalid argument count".to_string());
                    let _ = writer.write_all(response.serialize().as_bytes());
                    continue;
                }
            };

            // Read all the argument pairs ($len + data)
            let mut args: Vec<String> = Vec::new();
            let mut parse_failed = false;

            for _ in 0..num_args {
                // Read the $<len> line
                let len_line = match lines_iter.next() {
                    Some(Ok(line)) => line,
                    _ => { parse_failed = true; break; }
                };

                if !len_line.starts_with('$') {
                    parse_failed = true;
                    break;
                }

                // Read the data line
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

            // Build the command from parsed arguments
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

            // Execute the command against the storage engine
            let response = self.execute(command);

            // Send the response back to the client
            let serialized = response.serialize();
            if writer.write_all(serialized.as_bytes()).is_err() {
                break; // Write failed — client probably disconnected
            }
        }
    }

    /// Execute a parsed command against the storage engine.
    ///
    /// This is where the network layer meets the storage layer.
    /// Clean separation: the command is already parsed, the response
    /// is just data — no networking concerns leak in here.
    fn execute(&mut self, command: Command) -> Response {
        match command {
            Command::Get { key } => {
                match self.storage.get(&key) {
                    Some(value) => Response::BulkString(value),
                    None => Response::Null,
                }
            }
            Command::Set { key, value } => {
                match self.storage.put(key, value) {
                    Ok(()) => Response::SimpleString("OK".to_string()),
                    Err(e) => Response::Error(format!("storage error: {}", e)),
                }
            }
            Command::Del { key } => {
                match self.storage.delete(key) {
                    Ok(()) => Response::Integer(1),
                    Err(e) => Response::Error(format!("storage error: {}", e)),
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
}