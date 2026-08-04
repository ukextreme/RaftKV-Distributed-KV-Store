use super::ring::HashRing;

/// Shard configuration: which shard owns which nodes,
/// and how to reach them.
#[derive(Debug, Clone)]
pub struct ShardConfig {
    /// The shard ID.
    pub shard_id: u64,

    /// Node IDs that belong to this shard's Raft group.
    pub node_ids: Vec<u64>,

    /// The client port of the leader for this shard.
    /// Updated dynamically as leaders change.
    pub leader_port: Option<u16>,
}

/// The request router: maps keys to shards and shards to nodes.
///
/// Sits in front of the cluster and directs each request to the
/// correct shard based on the key. In a production system, this
/// would be a separate proxy process (like Redis Cluster's
/// smart client or CockroachDB's gateway). Here, each node
/// runs its own router.
pub struct Router {
    /// The consistent hashing ring.
    ring: HashRing,

    /// Configuration for each shard.
    shards: Vec<ShardConfig>,
}

impl Router {
    /// Create a new router with the given shard configuration.
    ///
    /// Each shard is added to the hash ring so that keys get
    /// distributed across shards.
    pub fn new(shards: Vec<ShardConfig>, virtual_nodes: u32) -> Self {
        let mut ring = HashRing::new(virtual_nodes);

        for shard in &shards {
            ring.add_shard(shard.shard_id);
        }

        Router { ring, shards }
    }

    /// Determine which shard owns a given key.
    ///
    /// Returns the ShardConfig for the owning shard, or None
    /// if the ring is empty.
    pub fn route(&self, key: &str) -> Option<&ShardConfig> {
        let shard_id = self.ring.get_shard(key)?;

        self.shards.iter().find(|s| s.shard_id == shard_id)
    }

    /// Get the shard ID for a given key.
    pub fn get_shard_id(&self, key: &str) -> Option<u64> {
        self.ring.get_shard(key)
    }

    /// Check if a key belongs to a specific shard.
    ///
    /// Used by nodes to determine if they should handle a
    /// request or redirect it to another shard.
    pub fn key_belongs_to_shard(&self, key: &str, shard_id: u64) -> bool {
        match self.ring.get_shard(key) {
            Some(id) => id == shard_id,
            None => false,
        }
    }

    /// Get configuration for a specific shard.
    pub fn get_shard(&self, shard_id: u64) -> Option<&ShardConfig> {
        self.shards.iter().find(|s| s.shard_id == shard_id)
    }

    /// Add a new shard to the router.
    ///
    /// This doesn't migrate any data — it just makes the ring
    /// aware of the new shard so future requests get routed to it.
    /// Data migration would be handled separately in a production system.
    pub fn add_shard(&mut self, config: ShardConfig) {
        self.ring.add_shard(config.shard_id);
        self.shards.push(config);
    }

    /// Remove a shard from the router.
    pub fn remove_shard(&mut self, shard_id: u64) {
        self.ring.remove_shard(shard_id);
        self.shards.retain(|s| s.shard_id != shard_id);
    }

    /// Get the total number of shards.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }
}