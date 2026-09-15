//! Traffic profile: every behavioral knob of the simulator in one loadable
//! TOML document. Defaults reproduce the JAM Toaster reference run
//! (2026-08-01..03); `calibrate` regenerates a profile from any live tart.
//!
//! Latency ranges are `[min_ms, max_ms]` pairs sampled uniformly.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Profile {
    pub network: NetworkKnobs,
    pub chain: ChainKnobs,
    pub wp: WpKnobs,
    pub segments: SegmentKnobs,
    pub audit: AuditKnobs,
    pub background: BackgroundKnobs,
    /// Reference event-type mix as shares of total volume (type id → share),
    /// used by `verify` to compare an observed window against expectations.
    pub reference_mix: std::collections::BTreeMap<String, f64>,
}

impl Profile {
    pub fn load(path: &std::path::Path) -> anyhow::Result<Profile> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("cannot read profile {}: {}", path.display(), e))?;
        toml::from_str(&text)
            .map_err(|e| anyhow::anyhow!("cannot parse profile {}: {}", path.display(), e))
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct NetworkKnobs {
    /// Nodes actually online and emitting; the rest of the validator set is
    /// registered but silent (the Toaster reference ran ~63 of 78). Offline
    /// trio members surface as share failures, offline proxies as ticket
    /// failures. Default: the full validator set.
    pub active_nodes: Option<usize>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ChainKnobs {
    /// Probability that a slot produces no block.
    pub missed_slot_prob: f64,
    /// Authoring claim delay after the slot boundary, ms.
    pub authoring_jitter_ms: (u64, u64),
    /// Block build time from claim to Authored, ms.
    pub authoring_dur_ms: (u64, u64),
    /// Import verify stage, ms.
    pub verify_ms: (u64, u64),
    /// Import execute stage, ms.
    pub exec_ms: (u64, u64),
    /// Gossip hop delay, ms.
    pub gossip_hop_ms: (u64, u64),
    /// Network edge delay, ms.
    pub net_ms: (u64, u64),
    /// Poisson λ for duplicate announce edges per node per block.
    pub announce_dup_lambda: f64,
    /// Finalization lag bounds in slots.
    pub finality_lag: (u32, u32),
    /// Probability per slot that the finality lag steps.
    pub finality_lag_switch_prob: f64,
    /// Per-node clock drift bound, µs (NTP scale, not the protocol's 250 ms).
    pub clock_drift_max_us: i64,
    /// Mean tickets per block outside the epoch tail.
    pub tickets_per_block_mean: f64,
}

impl Default for ChainKnobs {
    fn default() -> Self {
        Self {
            missed_slot_prob: 0.058,
            authoring_jitter_ms: (20, 250),
            authoring_dur_ms: (120, 550),
            verify_ms: (5, 35),
            exec_ms: (15, 90),
            gossip_hop_ms: (5, 45),
            net_ms: (8, 70),
            announce_dup_lambda: 4.8,
            finality_lag: (2, 3),
            finality_lag_switch_prob: 0.08,
            clock_drift_max_us: 5_000,
            tickets_per_block_mean: 2.2,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct WpKnobs {
    /// Poisson rate of work-package arrivals per core per slot.
    pub per_core_per_slot: f64,
    /// Probability an accepted WP dies at the authorization stage.
    pub early_fail: f64,
    /// Probability a share to one secondary fails (99, no 91 on that side).
    pub share_fail: f64,
    /// Probability one guarantee send to an ONLINE recipient fails (107).
    /// Sends to offline validators always fail, instantly — that share comes
    /// from `network.active_nodes`, not from here.
    pub guarantee_send_fail: f64,
    /// Probability the guarantee-receive path fails on one receiver (rare).
    pub guarantee_recv_fail: f64,
    /// Probability one assurance send to an ONLINE recipient fails (127);
    /// offline recipients always fail instantly.
    pub assurance_send_fail: f64,
    /// Probability a shard request fails (122).
    pub shard_req_fail: f64,
    /// Distinct service ids active on the network.
    pub services: usize,
    /// Builder nodes submitting work packages.
    pub builders: usize,

    // --- Timing shape, measured on live Toaster nodes (2026-09-09) ---
    /// DistributingAssurance fires this long before the slot boundary, ms
    /// (polkajam `SEND_ASSURANCE_BEFORE` = 2 s; observed at 4000 ms ± 4).
    pub assurance_lead_ms: u64,
    /// All AssuranceSent/Failed events land within this many µs of
    /// DistributingAssurance — the real node stamps them at send initiation,
    /// en masse (observed: 76 events within ~1 ms).
    pub assurance_burst_us: u64,
    /// AssuranceDistributed after DistributingAssurance, ms range
    /// (observed 20..600, median 446).
    pub assurance_done_ms: (u64, u64),
    /// All SendingGuarantee events land within this many ms of GuaranteeBuilt
    /// (observed: 77 within 0..25 ms).
    pub guarantee_burst_ms: u64,
    /// GuaranteeSent after its SendingGuarantee, ms range (observed 1..6).
    pub guarantee_ack_ms: (u64, u64),
    /// GuaranteesDistributed after GuaranteeBuilt, ms range (observed 130..600).
    pub guarantee_done_ms: (u64, u64),
    /// LAN delivery for paired receive events, µs range (observed: median
    /// ~1 ms, tail to 30 ms — a 15% tail is added on top of this range).
    pub lan_us: (u64, u64),
}

impl Default for WpKnobs {
    fn default() -> Self {
        Self {
            per_core_per_slot: 0.83,
            early_fail: 0.12,
            share_fail: 0.25,
            // Residual (online-recipient) failure shares: the Toaster's
            // observed 15% was almost entirely its offline validators.
            guarantee_send_fail: 0.026,
            guarantee_recv_fail: 0.0008,
            assurance_send_fail: 0.0,
            shard_req_fail: 0.003,
            services: 6,
            builders: 4,
            assurance_lead_ms: 2_000,
            assurance_burst_us: 1_500,
            assurance_done_ms: (20, 620),
            guarantee_burst_ms: 25,
            guarantee_ack_ms: (1, 6),
            guarantee_done_ms: (130, 600),
            lan_us: (300, 4_000),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct SegmentKnobs {
    /// Share of WPs that import segments at all.
    pub has_imports: f64,
    /// Mean direct segment-request batches per importing WP.
    pub batches_mean: f64,
    /// Probability one direct segment request fails (175).
    pub req_fail: f64,
    /// Probability a failed direct request escalates to the shard path.
    pub escalate: f64,
    /// Probability one segment-shard request fails (164).
    pub shard_fail: f64,
}

impl Default for SegmentKnobs {
    fn default() -> Self {
        Self {
            has_imports: 0.8,
            batches_mean: 330.0,
            req_fail: 0.097,
            escalate: 0.42,
            shard_fail: 0.18,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct AuditKnobs {
    /// Mean auditors fetching each included report.
    pub fetchers_mean: f64,
    /// Probability an audit fetch takes the bundle-shard path.
    pub shard_path: f64,
    /// Probability one direct bundle request fails (150).
    pub req_fail: f64,
}

impl Default for AuditKnobs {
    fn default() -> Self {
        Self { fetchers_mean: 19.0, shard_path: 0.005, req_fail: 0.006 }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct BackgroundKnobs {
    /// Mean dead-peer dials per node per slot (~99% fail, like the Toaster).
    pub dials_per_slot: f64,
    /// Long-dead peer identities the fleet keeps redialing.
    pub dead_peers: u64,
    /// Probability a dial to a dead peer succeeds.
    pub dial_ok: f64,
    /// Probability one ticket transfer leg fails (83).
    pub ticket_fail: f64,
    /// Fraction of validators a proxy forwards each ticket to.
    pub proxy_fanout: f64,
    /// Probability a preimage story starts in a given slot.
    pub preimage_per_slot: f64,
    /// Probability one announce edge fails (190).
    pub announce_fail: f64,
    /// Probability of a PeerMisbehaved per node per slot.
    pub misbehave_per_slot: f64,
    /// Probability of a disconnect/reconnect flap per node per slot.
    pub flap_per_slot: f64,
    /// Mean block-sync requests per node per slot (nodes catching up; the
    /// Toaster reference runs ~4).
    pub block_req_per_slot: f64,
    /// Probability one side of a block request fails (65).
    pub block_req_fail: f64,
}

impl Default for BackgroundKnobs {
    fn default() -> Self {
        Self {
            dials_per_slot: 7.2,
            dead_peers: 6,
            dial_ok: 0.01,
            ticket_fail: 0.09,
            proxy_fanout: 0.4,
            preimage_per_slot: 0.15,
            announce_fail: 0.03,
            misbehave_per_slot: 0.0035,
            flap_per_slot: 0.0015,
            block_req_per_slot: 4.0,
            block_req_fail: 0.45,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_toml_yields_defaults() {
        let p: Profile = toml::from_str("").expect("empty profile parses");
        assert_eq!(p.wp.per_core_per_slot, 0.83);
        assert_eq!(p.segments.batches_mean, 330.0);
        assert_eq!(p.chain.finality_lag, (2, 3));
    }

    #[test]
    fn partial_override_keeps_rest() {
        let p: Profile = toml::from_str(
            "[wp]\nper_core_per_slot = 0.1\n[segments]\nbatches_mean = 42.0\n",
        )
        .expect("parses");
        assert_eq!(p.wp.per_core_per_slot, 0.1);
        assert_eq!(p.segments.batches_mean, 42.0);
        assert_eq!(p.wp.share_fail, 0.25);
        assert_eq!(p.background.dead_peers, 6);
    }

    #[test]
    fn unknown_field_is_rejected() {
        assert!(toml::from_str::<Profile>("[wp]\nno_such_knob = 1\n").is_err());
    }
}
