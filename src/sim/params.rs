//! Simulation parameters: protocol constants plus the behavioral profile.
//!
//! The protocol side comes from `jam_types::ProtocolParameters` (tiny/mini/full,
//! selected the same way polkajam's chainspec does — by validator count). The
//! behavioral side lives in a `Profile` (see profile.rs), loadable from TOML.

use jam_types::ProtocolParameters;

use super::profile::Profile;
use super::time::{SlotMath, MICROS_PER_SEC};

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum ParamsKind {
    /// V=6, C=2, epoch 12 — local development scale
    Tiny,
    /// V=78, C=26, epoch 600 — JAM Toaster scale
    Mini,
    /// V=1023, C=341, epoch 600 — production scale
    Full,
}

pub struct SimParams {
    pub proto: ProtocolParameters,
    /// Validator count (== node count in the current design).
    pub v: usize,
    /// Core count.
    pub c: usize,
    pub slots: SlotMath,
    /// Behavioral knobs.
    pub p: Profile,
    /// Status timer period — a polkajam constant (2 s), not a profile knob.
    pub status_period_us: u64,
}

impl SimParams {
    pub fn new(kind: ParamsKind, profile: Profile) -> Self {
        let proto = match kind {
            ParamsKind::Tiny => ProtocolParameters::tiny(),
            ParamsKind::Mini => ProtocolParameters::mini(),
            ParamsKind::Full => ProtocolParameters::full(),
        };
        let v = proto.val_count as usize;
        let c = proto.core_count as usize;
        let slots = SlotMath {
            slot_period_us: proto.slot_period_sec as u64 * MICROS_PER_SEC,
            epoch_period: proto.epoch_period,
        };
        Self { proto, v, c, slots, p: profile, status_period_us: 2 * MICROS_PER_SEC }
    }

    pub fn epoch_tail_start(&self) -> u32 {
        self.proto.epoch_tail_start
    }
}
