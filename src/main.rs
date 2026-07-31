mod storage;
mod network;

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
}