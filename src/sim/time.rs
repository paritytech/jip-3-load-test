//! Simulation time.
//!
//! Sim time is expressed in microseconds since the JAM Common Era — the same
//! unit as the JIP-3 `Timestamp`, so materialized events carry sim time
//! directly. Tart stores this embedded timestamp (not arrival time), which is
//! what makes backfill possible.

use jam_types::JAM_COMMON_ERA;

/// Microseconds since the JAM Common Era (2025-01-01 12:00 UTC).
pub type SimTime = u64;

pub const MICROS_PER_SEC: u64 = 1_000_000;
pub const MICROS_PER_MS: u64 = 1_000;

/// Current wall clock as sim time.
pub fn wall_now() -> SimTime {
    let unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before unix epoch");
    (unix.as_micros() as u64).saturating_sub(JAM_COMMON_ERA * MICROS_PER_SEC)
}

/// Convert an RFC3339 timestamp ("2026-08-12T10:00:00Z") to sim time.
pub fn parse_rfc3339(s: &str) -> anyhow::Result<SimTime> {
    let t = humantime::parse_rfc3339(s)
        .map_err(|e| anyhow::anyhow!("invalid RFC3339 timestamp '{}': {}", s, e))?;
    let unix = t
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| anyhow::anyhow!("timestamp '{}' before unix epoch", s))?;
    let jce = (unix.as_micros() as u64)
        .checked_sub(JAM_COMMON_ERA * MICROS_PER_SEC)
        .ok_or_else(|| anyhow::anyhow!("timestamp '{}' before the JAM Common Era", s))?;
    Ok(jce)
}

/// Slot arithmetic on sim time.
#[derive(Clone, Copy, Debug)]
pub struct SlotMath {
    pub slot_period_us: u64,
    pub epoch_period: u32,
}

impl SlotMath {
    pub fn slot_of(&self, t: SimTime) -> u32 {
        (t / self.slot_period_us) as u32
    }

    pub fn slot_start(&self, slot: u32) -> SimTime {
        slot as u64 * self.slot_period_us
    }

    /// Position of the slot within its epoch (0..epoch_period).
    pub fn slot_phase(&self, slot: u32) -> u32 {
        slot % self.epoch_period
    }

    #[allow(dead_code)]
    pub fn epoch_of(&self, slot: u32) -> u32 {
        slot / self.epoch_period
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_math() {
        let m = SlotMath { slot_period_us: 6 * MICROS_PER_SEC, epoch_period: 600 };
        assert_eq!(m.slot_of(0), 0);
        assert_eq!(m.slot_of(5_999_999), 0);
        assert_eq!(m.slot_of(6_000_000), 1);
        assert_eq!(m.slot_start(10), 60_000_000);
        assert_eq!(m.slot_phase(600), 0);
        assert_eq!(m.slot_phase(1234), 34);
        assert_eq!(m.epoch_of(1234), 2);
    }
}
