mod storage;

fn main() {
    println!("raftkv starting...");
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
}