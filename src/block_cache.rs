use std::{
    collections::{BTreeMap, HashMap},
    mem::size_of,
    sync::{Arc, Mutex, PoisonError},
};

use crate::sstable::Block;

pub(crate) const DEFAULT_CAPACITY: usize = 4 * 1024 * 1024;
type Key = (u64, usize);

pub(crate) struct BlockCache {
    capacity: usize,
    state: Mutex<CacheState>,
}

#[derive(Default)]
struct CacheState {
    blocks: HashMap<Key, CacheEntry>,
    recency: BTreeMap<u64, Key>,
    clock: u64,
    bytes: usize,
}

struct CacheEntry {
    block: Arc<Block>,
    age: u64,
    bytes: usize,
}

impl BlockCache {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            state: Mutex::new(CacheState::default()),
        }
    }

    pub(crate) fn get(&self, key: Key) -> Option<Arc<Block>> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let age = state.next_age();
        let entry = state.blocks.get_mut(&key)?;
        let block = Arc::clone(&entry.block);
        let previous = std::mem::replace(&mut entry.age, age);
        state.recency.remove(&previous);
        state.recency.insert(age, key);
        Some(block)
    }

    pub(crate) fn insert(&self, key: Key, block: Arc<Block>) -> Arc<Block> {
        // Allow for both index entries and the Arc allocation in addition to its payload.
        const ENTRY_BYTES: usize = 2 * size_of::<(Key, CacheEntry)>() + size_of::<(u64, Key)>();
        let bytes = ENTRY_BYTES.saturating_add(block.heap_bytes());
        if bytes > self.capacity {
            return block;
        }

        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(existing) = state.blocks.get(&key) {
            // A concurrent miss may already have installed this immutable block.
            return Arc::clone(&existing.block);
        }
        while self.capacity - state.bytes < bytes {
            let (_, oldest) = state
                .recency
                .pop_first()
                .expect("cache has charged entries");
            let evicted = state.blocks.remove(&oldest).expect("matching cache entry");
            state.bytes -= evicted.bytes;
        }
        let age = state.next_age();
        state.recency.insert(age, key);
        state.blocks.insert(
            key,
            CacheEntry {
                block: Arc::clone(&block),
                age,
                bytes,
            },
        );
        state.bytes += bytes;
        block
    }

    #[cfg(test)]
    pub(crate) fn occupied(&self) -> usize {
        self.state.lock().unwrap().bytes
    }
}

impl CacheState {
    fn next_age(&mut self) -> u64 {
        if self.clock == u64::MAX {
            let order = std::mem::take(&mut self.recency);
            self.clock = 0;
            for (_, key) in order {
                self.blocks.get_mut(&key).expect("matching cache entry").age = self.clock;
                self.recency.insert(self.clock, key);
                self.clock += 1;
            }
        }
        self.clock += 1;
        self.clock
    }
}
