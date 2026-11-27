const HASHES: u32 = 7;
const BITS_PER_KEY: usize = 10;
const MIN_BYTES: usize = 8;
// A regular block holds at most 409 records of at least ten bytes each.
// An oversized block contains one record, so 512 bytes also covers it.
pub(crate) const MAX_BYTES: usize = 512;

pub(crate) fn hash(key: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for &byte in key {
        hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
    }
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51afd7ed558ccd);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xc4ceb9fe1a85ec53);
    hash ^ (hash >> 33)
}

pub(crate) fn build(hashes: &[u64]) -> Box<[u8]> {
    let len = (hashes.len() * BITS_PER_KEY).div_ceil(8).max(MIN_BYTES);
    debug_assert!(len <= MAX_BYTES);
    let mut filter = vec![0; len];
    for &hash in hashes {
        for bit in positions(hash, len) {
            filter[bit / 8] |= 1 << (bit % 8);
        }
    }
    filter.into_boxed_slice()
}

pub(crate) fn valid_len(len: usize) -> bool {
    (MIN_BYTES..=MAX_BYTES).contains(&len)
}

pub(crate) fn might_contain(filter: &[u8], hash: u64) -> bool {
    positions(hash, filter.len()).all(|bit| filter[bit / 8] & (1 << (bit % 8)) != 0)
}

fn positions(hash: u64, len: usize) -> impl Iterator<Item = usize> {
    // Double hashing derives the probe sequence from the two halves.
    let start = hash as u32;
    let step = ((hash >> 32) as u32) | 1;
    (0..HASHES).map(move |i| start.wrapping_add(i.wrapping_mul(step)) as usize % (len * 8))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_all_inserted_keys_and_rejects_most_absent_keys() {
        let keys: Vec<_> = (0..400_u64).map(|i| hash(&i.to_le_bytes())).collect();
        let filter = build(&keys);
        assert!(valid_len(filter.len()));
        assert!(keys.iter().all(|&hash| might_contain(&filter, hash)));
        let false_positives = (400..10_400_u64)
            .filter(|i| might_contain(&filter, hash(&i.to_le_bytes())))
            .count();
        assert!(false_positives < 250, "{false_positives} false positives");
    }
}
