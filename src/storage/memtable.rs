use std::collections::BTreeMap;

/// The memtable: an in-memory sorted key-value store.
///
/// Every write goes here for fast lookups. Keys are sorted,
/// which makes future SSTable flushing efficient.
///
/// A None value means the key has been deleted — we store the
/// deletion explicitly (called a "tombstone") rather than just
/// removing the key. Why? Because in a layered storage engine,
/// older data might exist on disk. If we just removed the key
/// from memory, a read would fall through to disk and find the
/// old value — the delete would be invisible. The tombstone
/// says "this key is dead, don't look further."
pub struct Memtable {
    entries: BTreeMap<String, Option<String>>,
    size_bytes: usize,
}

impl Memtable {
    /// Create a new, empty memtable.
    pub fn new() -> Self {
        Memtable {
            entries: BTreeMap::new(),
            size_bytes: 0,
        }
    }

    /// Insert or update a key-value pair.
    /// Returns the approximate size increase in bytes.
    pub fn put(&mut self, key: String, value: String) -> usize {
        // Calculate how many bytes this entry takes up.
        // This is approximate — we count key length + value length.
        // Real databases track this more precisely, but this is
        // close enough for deciding when to flush.
        let added_size = key.len() + value.len();

        // .insert() puts the key-value pair into the BTreeMap.
        // If the key already existed, it replaces the old value
        // and returns Some(old_value). If it's new, returns None.
        // We wrap the value in Some() because a present value
        // is Some("..."), while a tombstone would be None.
        self.entries.insert(key, Some(value));

        self.size_bytes += added_size;
        added_size
    }

    /// Mark a key as deleted by inserting a tombstone (None).
    /// The key isn't removed — it's explicitly marked dead.
    pub fn delete(&mut self, key: String) -> usize {
        let added_size = key.len();
        self.entries.insert(key, None);
        self.size_bytes += added_size;
        added_size
    }

    /// Look up a key.
    ///
    /// Returns:
    ///   Some(Some(value)) — key exists and has a value
    ///   Some(None)        — key was explicitly deleted (tombstone)
    ///   None              — key is not in the memtable at all
    ///
    /// The caller needs to distinguish between "deleted" and "not found"
    /// because they mean different things: "deleted" means stop looking,
    /// "not found" means check the next layer (SSTables on disk).
    pub fn get(&self, key: &str) -> Option<Option<&str>> {
        // .get() on a BTreeMap returns Option<&V>
        //   - If the key exists: Some(&value)  where value is Option<String>
        //   - If the key is missing: None
        //
        // .map() transforms the inner value if it's Some, leaves None alone.
        // Inside the map, we convert &Option<String> to Option<&str>:
        //   - &Some(ref s) means "borrow the String inside the Some"
        //   - s.as_str() converts &String to &str (a lighter reference)
        self.entries.get(key).map(|value| {
            value.as_deref()
        })
    }

    /// How many bytes the memtable is approximately using.
    pub fn size(&self) -> usize {
        self.size_bytes
    }

    /// How many entries (including tombstones) are in the memtable.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Is the memtable empty?
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get all entries as a sorted iterator.
    /// Used when flushing to an SSTable — we need them in key order.
    /// Returns pairs of (key, optional_value).
    pub fn iter(&self) -> impl Iterator<Item = (&String, &Option<String>)> {
        self.entries.iter()
    }

    /// Clear the memtable completely. Called after flushing to disk.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.size_bytes = 0;
    }
}