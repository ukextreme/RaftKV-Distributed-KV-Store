/// A command parsed from a client request.
/// These are the only commands our database supports.
#[derive(Debug)]
pub enum Command {
    Get { key: String },
    Set { key: String, value: String },
    Del { key: String },
    Ping,
    Unknown { name: String },
}

/// A response to send back to the client.
/// Each variant maps to a RESP response format.
#[derive(Debug)]
pub enum Response {
    SimpleString(String),   // +OK\r\n
    BulkString(String),     // $<len>\r\n<data>\r\n
    Null,                   // $-1\r\n (key not found)
    Error(String),          // -ERR ...\r\n
    Integer(i64),           // :<number>\r\n
}


impl Command {
    /// Parse a RESP message into a Command.
    ///
    /// RESP format from clients is always an array of bulk strings:
    ///   *<num_elements>\r\n
    ///   $<len>\r\n<data>\r\n
    ///   $<len>\r\n<data>\r\n
    ///   ...
    ///
    /// Example: SET name uday arrives as:
    ///   *3\r\n$3\r\nSET\r\n$4\r\nname\r\n$4\r\nuday\r\n
    pub fn parse(input: &str) -> Option<Command> {
        let mut lines = input.lines();

        // First line: *N where N is the number of arguments
        // Example: *3 means 3 arguments follow (like SET, name, uday)
        let first_line = lines.next()?;

        // The ? at the end is different here — on Option, it means
        // "if this is None, return None from the whole function."
        // Same concept as with Result: early return on failure.

        if !first_line.starts_with('*') {
            return None; // Not a valid RESP array
        }

        // Parse the number after '*'
        // &first_line[1..] is a slice: everything after the first character
        // .parse::<usize>() tries to convert the string to a number
        // .ok()? converts Result to Option, then ? returns None on failure
        let num_args: usize = first_line[1..].parse().ok()?;

        // Read each argument: pairs of $<len> and <data> lines
        let mut args: Vec<String> = Vec::new();

        for _ in 0..num_args {
            // Read the $<len> line — we don't actually need the length
            // because .lines() already splits by line boundaries,
            // but we must consume it to advance the iterator
            let len_line = lines.next()?;
            if !len_line.starts_with('$') {
                return None;
            }

            // Read the actual data
            let data = lines.next()?;
            args.push(data.to_string());
        }

        if args.is_empty() {
            return None;
        }

        // Convert the first argument (the command name) to uppercase
        // so "set", "SET", and "Set" all work
        let command_name = args[0].to_uppercase();

        // Match the command name and extract the right arguments
        match command_name.as_str() {
            "GET" => {
                if args.len() != 2 {
                    return None;
                }
                // args[1].clone() — we need to take ownership of the string
                // out of the vector. clone() copies it. We could also use
                // .remove() to move it out, but clone is clearer here.
                Some(Command::Get {
                    key: args[1].clone(),
                })
            }
            "SET" => {
                if args.len() != 3 {
                    return None;
                }
                Some(Command::Set {
                    key: args[1].clone(),
                    value: args[2].clone(),
                })
            }
            "DEL" => {
                if args.len() != 2 {
                    return None;
                }
                Some(Command::Del {
                    key: args[1].clone(),
                })
            }
            "PING" => Some(Command::Ping),
            _ => Some(Command::Unknown {
                name: command_name,
            }),
        }
    }
}


impl Response {
    /// Convert a Response into the RESP wire format.
    ///
    /// Each variant has its own prefix character:
    ///   + for simple strings
    ///   $ for bulk strings (includes length)
    ///   - for errors
    ///   : for integers
    pub fn serialize(&self) -> String {
        match self {
            // +OK\r\n — simple string, prefixed with +
            Response::SimpleString(s) => format!("+{}\r\n", s),

            // $<len>\r\n<data>\r\n — bulk string with explicit length
            // The length tells the client exactly how many bytes to read
            Response::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s),

            // $-1\r\n — null bulk string (length of -1 means "doesn't exist")
            Response::Null => "$-1\r\n".to_string(),

            // -ERR message\r\n — error, prefixed with -
            Response::Error(msg) => format!("-ERR {}\r\n", msg),

            // :<number>\r\n — integer, prefixed with :
            Response::Integer(n) => format!(":{}\r\n", n),
        }
    }
}