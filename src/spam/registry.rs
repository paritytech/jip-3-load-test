//! Shared validator registry for pair-wise event generation.
//!
//! Pre-computes N validator PeerIds at startup. Immutable after construction,
//! shared across all worker threads via Arc.

use jam_std_common::{ed25519, PeerId};

/// Registry of all simulated validators, keyed by index 0..count.
///
/// PeerId derivation matches the existing spammer convention:
///   bytes[0..8] = node_id.to_le_bytes(), rest zeros
/// where node_id = start_id + index.
pub struct ValidatorRegistry {
    start_id: u64,
    peer_ids: Vec<PeerId>,
}

impl ValidatorRegistry {
    /// Create a registry with `count` validators starting at `start_id`.
    ///
    /// Validator index `i` has node_id = `start_id + i`.
    pub fn new(start_id: u64, count: usize) -> Self {
        let peer_ids = (0..count)
            .map(|i| {
                let node_id = start_id + i as u64;
                let mut bytes = [0u8; 32];
                bytes[0..8].copy_from_slice(&node_id.to_le_bytes());
                PeerId(ed25519::Public::from(bytes))
            })
            .collect();
        Self { start_id, peer_ids }
    }

    /// Total number of validators in the registry.
    #[allow(dead_code)]
    pub fn count(&self) -> usize {
        self.peer_ids.len()
    }

    /// Get the PeerId for validator at the given index.
    pub fn peer_id(&self, index: usize) -> PeerId {
        self.peer_ids[index]
    }

    /// Convert a node_id back to its registry index.
    /// Returns None if the node_id is outside the registry range.
    pub fn index_of_node(&self, node_id: u64) -> Option<usize> {
        if node_id < self.start_id {
            return None;
        }
        let idx = (node_id - self.start_id) as usize;
        if idx < self.peer_ids.len() {
            Some(idx)
        } else {
            None
        }
    }

    /// Pick `n` random other validators (excluding `my_index`).
    /// Returns indices. If n >= count-1, returns all other validators.
    pub fn pick_others(
        &self,
        my_index: usize,
        n: usize,
        rng: &mut impl rand::Rng,
    ) -> Vec<usize> {
        use rand::seq::SliceRandom;

        let count = self.peer_ids.len();
        let mut pool: Vec<usize> = (0..count).filter(|&i| i != my_index).collect();

        if n >= pool.len() {
            return pool;
        }

        pool.partial_shuffle(rng, n);
        pool.truncate(n);
        pool
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_id_matches_existing_derivation() {
        let reg = ValidatorRegistry::new(0, 3);
        // Index 0 => node_id=0 => first 8 bytes are 0u64 LE
        let mut expected = [0u8; 32];
        expected[0..8].copy_from_slice(&0u64.to_le_bytes());
        assert_eq!(reg.peer_id(0), PeerId(ed25519::Public::from(expected)));

        // Index 1 => node_id=1
        expected = [0u8; 32];
        expected[0..8].copy_from_slice(&1u64.to_le_bytes());
        assert_eq!(reg.peer_id(1), PeerId(ed25519::Public::from(expected)));
    }

    #[test]
    fn peer_id_with_nonzero_start() {
        let reg = ValidatorRegistry::new(100, 3);
        let mut expected = [0u8; 32];
        expected[0..8].copy_from_slice(&100u64.to_le_bytes());
        assert_eq!(reg.peer_id(0), PeerId(ed25519::Public::from(expected)));
    }

    #[test]
    fn index_of_node_round_trips() {
        let reg = ValidatorRegistry::new(100, 50);
        assert_eq!(reg.index_of_node(100), Some(0));
        assert_eq!(reg.index_of_node(149), Some(49));
        assert_eq!(reg.index_of_node(99), None);
        assert_eq!(reg.index_of_node(150), None);
    }

    #[test]
    fn pick_others_excludes_self() {
        let reg = ValidatorRegistry::new(0, 5);
        let mut rng = rand::rng();
        let others = reg.pick_others(2, 3, &mut rng);
        assert_eq!(others.len(), 3);
        assert!(!others.contains(&2));
    }

    #[test]
    fn pick_others_caps_at_count_minus_one() {
        let reg = ValidatorRegistry::new(0, 5);
        let mut rng = rand::rng();
        let others = reg.pick_others(2, 100, &mut rng);
        assert_eq!(others.len(), 4);
        assert!(!others.contains(&2));
    }

    #[test]
    fn count_returns_correct_value() {
        let reg = ValidatorRegistry::new(0, 1024);
        assert_eq!(reg.count(), 1024);
    }
}
