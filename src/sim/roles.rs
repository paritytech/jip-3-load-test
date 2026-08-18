//! Validator role assignment: guarantor trios per core, rotating every
//! `rotation_period` slots.
//!
//! Real polkajam derives assignments from epoch entropy (GP); for telemetry
//! realism a fresh seeded permutation per rotation gives the same observable
//! properties: every validator serves exactly one core per rotation
//! (V == 3·C), assignments change on rotation boundaries, and the mapping is
//! identical for every node in the network.

use rand::seq::SliceRandom;
use rand::SeedableRng;

use super::ids::subseed;

/// Guarantor trios per core for one rotation window.
pub struct Assignment {
    pub cores: Vec<[u16; 3]>,
}

pub fn assignment(seed: u64, v: usize, c: usize, rotation_index: u64) -> Assignment {
    debug_assert_eq!(v, c * 3, "val_count must be core_count * 3");
    let mut vals: Vec<u16> = (0..v as u16).collect();
    let mut rng = rand::rngs::StdRng::seed_from_u64(subseed(seed, "rotation", rotation_index));
    vals.shuffle(&mut rng);
    let cores = vals
        .chunks_exact(3)
        .map(|t| [t[0], t[1], t[2]])
        .collect();
    Assignment { cores }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_validator_serves_exactly_one_core() {
        let a = assignment(42, 78, 26, 5);
        assert_eq!(a.cores.len(), 26);
        let mut seen = std::collections::HashSet::new();
        for trio in &a.cores {
            for &val in trio {
                assert!(val < 78);
                assert!(seen.insert(val), "validator {} assigned twice", val);
            }
        }
        assert_eq!(seen.len(), 78);
    }

    #[test]
    fn deterministic_per_rotation_and_changes_between() {
        let a = assignment(42, 78, 26, 5);
        let b = assignment(42, 78, 26, 5);
        assert_eq!(a.cores, b.cores);
        let c = assignment(42, 78, 26, 6);
        assert_ne!(a.cores, c.cores);
    }
}
