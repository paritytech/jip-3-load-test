//! Deterministic identity and hash derivation.
//!
//! Everything visually hash-like in the simulated network (block hashes, peer
//! ids, the genesis hash) is derived from the world seed through a small
//! non-cryptographic mixer, so a given seed reproduces the identical network.

/// SplitMix64 step — good avalanche, zero dependencies.
fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn fnv1a(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01B3);
    }
    h
}

/// Derive 32 bytes from (seed, domain, a, b).
pub fn h32(seed: u64, domain: &str, a: u64, b: u64) -> [u8; 32] {
    let mut state = seed ^ fnv1a(domain).rotate_left(17) ^ a.rotate_left(31) ^ b.rotate_left(47);
    // Extra mixing round so (a, b) permutations don't collide.
    splitmix(&mut state);
    let mut out = [0u8; 32];
    for chunk in out.chunks_exact_mut(8) {
        chunk.copy_from_slice(&splitmix(&mut state).to_le_bytes());
    }
    out
}

/// Derive a child RNG seed from (seed, domain, a).
pub fn subseed(seed: u64, domain: &str, a: u64) -> u64 {
    let mut state = seed ^ fnv1a(domain) ^ a.rotate_left(23);
    splitmix(&mut state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_and_distinct() {
        assert_eq!(h32(1, "block", 5, 0), h32(1, "block", 5, 0));
        assert_ne!(h32(1, "block", 5, 0), h32(1, "block", 6, 0));
        assert_ne!(h32(1, "block", 5, 0), h32(1, "peer", 5, 0));
        assert_ne!(h32(1, "block", 5, 0), h32(2, "block", 5, 0));
        assert_ne!(h32(1, "x", 5, 7), h32(1, "x", 7, 5));
    }
}
