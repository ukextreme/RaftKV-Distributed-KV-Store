use std::io::Write;
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::thread;

use crate::cluster::ClusterNode;
use crate::raft::state::NodeRole;

/// Start a metrics HTTP server on the given port.
///
/// Prometheus scrapes this endpoint every few seconds.
/// The response is plain text in Prometheus's exposition format:
///
///   # HELP metric_name Description
///   # TYPE metric_name gauge
///   metric_name{label="value"} 42
///
/// Each line is one metric. Labels add dimensions (like which node).
/// "gauge" means the value can go up and down (like current term).
/// "counter" means it only goes up (like total requests handled).
pub fn start_metrics_server(
    port: u16,
    node_id: u64,
    cluster_node: Arc<Mutex<ClusterNode>>,
) {
    thread::spawn(move || {
        let addr = format!("0.0.0.0:{}", port);
        let listener = match TcpListener::bind(&addr) {
            Ok(l) => l,
            Err(e) => {
                eprintln!("Metrics server failed to bind on {}: {}", addr, e);
                return;
            }
        };

        println!("Node {} metrics server on {}", node_id, addr);

        for stream in listener.incoming() {
            let mut stream = match stream {
                Ok(s) => s,
                Err(_) => continue,
            };

            // Read the HTTP request (we don't actually parse it —
            // we respond to everything with metrics)
            let mut buf = [0u8; 1024];
            let _ = std::io::Read::read(&mut stream, &mut buf);

            // Generate metrics from the current cluster state
            let body = {
                let node = cluster_node.lock().unwrap();
                generate_metrics(&node, node_id)
            };
            // Lock released here — same pattern as the client handler

            // Write a minimal HTTP response
            // HTTP/1.1 requires a status line, Content-Type header,
            // and the body. Prometheus expects text/plain.
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );

            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
        }
    });
}

/// Generate Prometheus-format metrics from the cluster node state.
///
/// Each metric has:
///   - A name (snake_case, prefixed with "raftkv_")
///   - A type (gauge or counter)
///   - Labels in {} that add dimensions
///   - A numeric value
fn generate_metrics(node: &ClusterNode, node_id: u64) -> String {
    let raft = &node.raft;
    let state = &raft.state;

    // Determine role as a number for Prometheus:
    // 0 = follower, 1 = candidate, 2 = leader
    let role_num = match state.role {
        NodeRole::Follower => 0,
        NodeRole::Candidate => 1,
        NodeRole::Leader => 2,
    };

    // Role as a string label for human-readable dashboards
    let role_str = match state.role {
        NodeRole::Follower => "follower",
        NodeRole::Candidate => "candidate",
        NodeRole::Leader => "leader",
    };

    let mut output = String::new();

    // --- Raft state metrics ---

    output.push_str("# HELP raftkv_current_term The current Raft term\n");
    output.push_str("# TYPE raftkv_current_term gauge\n");
    output.push_str(&format!(
        "raftkv_current_term{{node=\"{}\"}} {}\n",
        node_id, state.current_term
    ));

    output.push_str("# HELP raftkv_role Current role (0=follower, 1=candidate, 2=leader)\n");
    output.push_str("# TYPE raftkv_role gauge\n");
    output.push_str(&format!(
        "raftkv_role{{node=\"{}\",role=\"{}\"}} {}\n",
        node_id, role_str, role_num
    ));

    output.push_str("# HELP raftkv_commit_index The highest committed log index\n");
    output.push_str("# TYPE raftkv_commit_index gauge\n");
    output.push_str(&format!(
        "raftkv_commit_index{{node=\"{}\"}} {}\n",
        node_id, state.commit_index
    ));

    output.push_str("# HELP raftkv_last_applied The highest applied log index\n");
    output.push_str("# TYPE raftkv_last_applied gauge\n");
    output.push_str(&format!(
        "raftkv_last_applied{{node=\"{}\"}} {}\n",
        node_id, state.last_applied
    ));

    output.push_str("# HELP raftkv_log_length Number of entries in the Raft log\n");
    output.push_str("# TYPE raftkv_log_length gauge\n");
    output.push_str(&format!(
        "raftkv_log_length{{node=\"{}\"}} {}\n",
        node_id, state.log.last_index()
    ));

    output.push_str("# HELP raftkv_is_leader Whether this node is the leader (1=yes, 0=no)\n");
    output.push_str("# TYPE raftkv_is_leader gauge\n");
    output.push_str(&format!(
        "raftkv_is_leader{{node=\"{}\"}} {}\n",
        node_id,
        if state.role == NodeRole::Leader { 1 } else { 0 }
    ));

    // --- Leader-specific metrics ---

    if let Some(leader_id) = state.leader_id {
        output.push_str("# HELP raftkv_known_leader The node ID of the known leader\n");
        output.push_str("# TYPE raftkv_known_leader gauge\n");
        output.push_str(&format!(
            "raftkv_known_leader{{node=\"{}\"}} {}\n",
            node_id, leader_id
        ));
    }

    output
}