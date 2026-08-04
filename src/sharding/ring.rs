use std::collections::BTreeMap;

/// A consistent hashing ring with virtual nodes.
///
/// Maps keys to shard IDs using a ring of hash values.
/// Each shard gets multiple "virtual nodes" (points on the ring)
/// to ensure even distribution of keys across shards.
///
/// Uses a BTreeMap internally — the sorted nature of BTreeMap
/// lets us efficiently find "the next point clockwise" by doing
/// a range query. This is O(log n) per lookup, where n is the
/// total number of virtual nodes across all shards.
pub struct HashRing {
    /// Maps positions on the ring to shard IDs.
    /// Each shard has `num_virtual_nodes` entries here.
    /// BTreeMap keeps them sorted, so finding "the next one
    /// after hash X" is a single range query.
    ring: BTreeMap<u64, u64>,

    /// How many virtual nodes each shard gets.
    /// More virtual nodes = more even distribution.
    /// 64 is a common choice that gives good uniformity.
    num_virtual_nodes: u32,
}

impl HashRing {
    /// Create a new empty ring with the given number of virtual
    /// nodes per shard.
    pub fn new(num_virtual_nodes: u32) -> Self {
        HashRing {
            ring: BTreeMap::new(),
            num_virtual_nodes,
        }
    }

    /// Add a shard to the ring.
    ///
    /// Creates `num_virtual_nodes` points on the ring for this shard.
    /// Each virtual node is placed at a different hash position
    /// computed from the shard ID and virtual node index.
    pub fn add_shard(&mut self, shard_id: u64) {
        for i in 0..self.num_virtual_nodes {
            // Create a unique string for each virtual node.
            // "shard-3-vnode-17" → hash this to get a ring position.
            let key = format!("shard-{}-vnode-{}", shard_id, i);
            let position = self.hash(&key);
            self.ring.insert(position, shard_id);
        }
    }

    /// Remove a shard from the ring.
    ///
    /// Removes all virtual nodes belonging to this shard.
    /// Keys that were assigned to this shard will automatically
    /// fall to the next shard clockwise — no explicit migration needed.
    pub fn remove_shard(&mut self, shard_id: u64) {
        // .retain() keeps only entries where the closure returns true.
        // We keep entries whose shard_id is NOT the one we're removing.
        self.ring.retain(|_, &mut id| id != shard_id);
    }

    /// Find which shard owns a given key.
    ///
    /// Hashes the key to a position on the ring, then walks
    /// clockwise (ascending) to find the first shard.
    /// If we go past the maximum, we wrap around to the beginning
    /// (that's the "ring" part).
    ///
    /// Returns None if the ring is empty (no shards).
    pub fn get_shard(&self, key: &str) -> Option<u64> {
        if self.ring.is_empty() {
            return None;
        }

        let hash = self.hash(key);

        // .range(hash..) gives an iterator over all entries with
        // position >= hash. The first entry is "the next point
        // clockwise on the ring."
        //
        // .next() gets that first entry.
        //
        // If there's no entry with position >= hash (we're past
        // the last point on the ring), we wrap around to the
        // first entry (.iter().next()) — this is the "ring"
        // wrapping behavior.
        match self.ring.range(hash..).next() {
            Some((_, &shard_id)) => Some(shard_id),
            None => {
                // Wrap around to the beginning of the ring
                self.ring.iter().next().map(|(_, &id)| id)
            }
        }
    }

    /// Count how many shards are on the ring.
    ///
    /// Note: this counts unique shard IDs, not virtual nodes.
    /// A ring with 3 shards and 64 virtual nodes each has
    /// 192 ring entries but only 3 shards.
    pub fn shard_count(&self) -> usize {
        let mut unique: Vec<u64> = self.ring.values().copied().collect();
        unique.sort();
        unique.dedup();
        unique.len()
    }

    /// Get all unique shard IDs on the ring.
    pub fn shard_ids(&self) -> Vec<u64> {
        let mut ids: Vec<u64> = self.ring.values().copied().collect();
        ids.sort();
        ids.dedup();
        ids
    }

    /// Hash a string to a u64 value (a position on the ring).
    ///
    /// Uses FNV-1a, a simple non-cryptographic hash function.
    /// It's fast, has good distribution, and is easy to implement.
    /// We don't need cryptographic strength — we just need keys
    /// to spread evenly around the ring.
    fn hash(&self, key: &str) -> u64 {
        // FNV-1a base
        let mut hash: u64 = 0xcbf29ce484222325;
        let prime: u64 = 0x100000001b3;

        for byte in key.as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(prime);
        }

        // Murmur3-style finalizer: mixes the bits so that
        // small input changes affect all output bits.
        // Without this, similar inputs ("key_1", "key_2") can
        // produce hash values that cluster in the same region.
        hash ^= hash >> 33;
        hash = hash.wrapping_mul(0xff51afd7ed558ccd);
        hash ^= hash >> 33;
        hash = hash.wrapping_mul(0xc4ceb9fe1a85ec53);
        hash ^= hash >> 33;

        hash
    }
}