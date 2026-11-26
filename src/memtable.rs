use std::{collections::BTreeMap, mem::size_of};

pub(crate) enum Entry {
    Value(Vec<u8>),
    Tombstone,
}

impl Entry {
    pub(crate) fn payload_len(&self) -> usize {
        match self {
            Self::Value(value) => value.len(),
            Self::Tombstone => 0,
        }
    }
}

#[derive(Default)]
pub(crate) struct MemTable {
    entries: BTreeMap<Vec<u8>, Entry>,
    bytes: usize,
}

impl MemTable {
    pub(crate) fn insert(&mut self, key: impl AsRef<[u8]> + Into<Vec<u8>>, entry: Entry) {
        if let Some(existing) = self.entries.get_mut(key.as_ref()) {
            self.bytes -= existing.payload_len();
            self.bytes += entry.payload_len();
            *existing = entry;
        } else {
            self.bytes += key.as_ref().len() + entry.payload_len() + size_of::<(Vec<u8>, Entry)>();
            self.entries.insert(key.into(), entry);
        }
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<&Entry> {
        self.entries.get(key)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&[u8], &Entry)> {
        self.entries
            .iter()
            .map(|(key, entry)| (key.as_slice(), entry))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) fn approximate_bytes(&self) -> usize {
        self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_binary_keys_in_lexicographic_order() {
        let mut table = MemTable::default();
        for key in [&b"\xff"[..], b"\0\xff", b"\0", b"\0\0"] {
            table.insert(key, Entry::Value(Vec::new()));
        }

        let keys: Vec<_> = table.entries.keys().map(Vec::as_slice).collect();
        assert_eq!(keys, [&b"\0"[..], b"\0\0", b"\0\xff", b"\xff"]);
    }

    #[test]
    fn distinguishes_tombstones_from_missing_keys_and_empty_values() {
        let mut table = MemTable::default();
        table.insert(b"key", Entry::Value(Vec::new()));
        assert!(matches!(table.get(b"key"), Some(Entry::Value(value)) if value.is_empty()));

        table.insert(b"key", Entry::Tombstone);
        assert!(matches!(table.get(b"key"), Some(Entry::Tombstone)));
        assert!(table.get(b"missing").is_none());

        table.insert(b"key", Entry::Value(vec![42]));
        assert!(matches!(table.get(b"key"), Some(Entry::Value(value)) if value == &[42]));
        assert_eq!(table.entries.len(), 1);
    }
}
