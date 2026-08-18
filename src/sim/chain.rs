//! Phase-1 chain simulation: slots, authorship, block propagation, finality,
//! Status ticks and node startup.
//!
//! Timing and role rules follow the polkajam emission model
//! (docs/research/polkajam-emission-model.md):
//! - exactly one author per produced slot, chosen by a deterministic lottery;
//!   the author emits Authoring(40) → Authored(42) → BlockExecuted(47) and
//!   never Importing;
//! - every other node receives the block via gossip and emits
//!   BlockAnnounced(62, Remote) → Importing(43) → BlockVerified(45) →
//!   BlockExecuted(47), each strictly after the author's chain;
//! - BestBlockChanged(11) fires per node once its import completes, so
//!   cross-node convergence deltas are positive;
//! - FinalizedBlockChanged(12) trails the head by 2–3 slots;
//! - Status(10) runs on a free 2 s timer whose phase drifts against slots.

use rand::seq::SliceRandom;
use rand::Rng;
use rand_distr::{Distribution, Poisson};

use super::time::{SimTime, MICROS_PER_MS, MICROS_PER_SEC};
use super::world::{id_slot, BlockRec, Emit, World};

impl World {
    /// Schedule node startup traffic and the recurring drivers. Call once
    /// before `run()`.
    pub fn bootstrap(&mut self) {
        let act = self.act();
        let t0 = self.start_ts;

        // Window over which a node's stream-open events are spread. Scaled
        // with the fleet so the per-node queue (200) never overflows while the
        // sender is still ramping up its connection: at full scale (V=1023)
        // the burst must trickle, at mini it can be quick like a real start.
        let stream_open_window_ms = (act as u64 * 40).max(2_300);

        for n in 0..act {
            let up = t0 + self.rng.random_range(0..1_500 * MICROS_PER_MS);

            // A node comes up unsynced and reports sync shortly after its
            // first imports complete (~2 slots in).
            self.schedule(up, n, Emit::SyncStatus { synced: false });
            let synced_at =
                up + 2 * self.params.slots.slot_period_us + self.rng.random_range(0..MICROS_PER_SEC);
            self.schedule(synced_at, n, Emit::SyncStatus { synced: true });

            // Announcement streams to every peer, both ends emitting 60 with
            // the opener side decided by a deterministic tiebreak (mirrors
            // polkajam's peer-manager topology rule).
            for peer in 0..act {
                if peer == n {
                    continue;
                }
                let at = up + self.rng.random_range(200..stream_open_window_ms) * MICROS_PER_MS;
                let opener_local = open_tiebreak(n, peer);
                let self_id = id_slot();
                self.stream_ids.insert((n, peer), self_id.clone());
                self.schedule(at, n, Emit::StreamOpened { peer, opener_local, self_id });
            }

            // Status timer: random initial phase, then every 2 s (rescheduled
            // from the tick handler).
            let phase = self.rng.random_range(0..self.params.status_period_us);
            self.schedule_status_tick(up + phase, n);
        }

        // First authored slot: the one after next, giving senders ramp time.
        let first_slot = self.params.slots.slot_of(t0) + 2;
        self.schedule_slot_start(first_slot);
    }

    pub(super) fn on_status_tick(&mut self, node: u16, at: SimTime) {
        self.materialize(node, at, Emit::Status);
        let next = at + self.params.status_period_us;
        if self.end_ts.is_none_or(|end| next < end) {
            self.schedule_status_tick(next, node);
        }
    }

    pub(super) fn on_slot_start(&mut self, slot: u32) {
        self.schedule_slot_start(slot + 1);
        self.walk_clock_drift();

        // Work packages arrive and assurances go out regardless of whether
        // this slot produces a block; so does the background noise.
        self.schedule_wp_arrivals(slot);
        self.schedule_assurance_round(slot);
        self.maybe_tickets(slot);
        self.schedule_preimages(slot);
        self.schedule_churn(slot);

        // Missed slots happen (~5.8% on the Toaster reference run).
        if self.rng.random::<f64>() < self.params.p.chain.missed_slot_prob {
            return;
        }

        let author = self.author_of(slot);
        let (rec, included) = self.make_block(slot);
        self.blocks.insert(slot, rec);
        self.last_block_slot = Some(slot);

        // --- Author chain: 40 → 42 → 47 → 11 ---
        let p = &self.params.p.chain;
        let slot_t = self.params.slots.slot_start(slot);
        let t_claim = slot_t + self.rng.random_range(ms_range(p.authoring_jitter_ms));
        let t_authored = t_claim + self.rng.random_range(ms_range(p.authoring_dur_ms));
        let t_exec_a = t_authored + self.rng.random_range(5 * MICROS_PER_MS..40 * MICROS_PER_MS);
        let t_best_a = t_exec_a + self.rng.random_range(2 * MICROS_PER_MS..20 * MICROS_PER_MS);

        let authoring = id_slot();
        self.schedule(t_claim, author, Emit::Authoring { slot, self_id: authoring.clone() });
        self.schedule(t_authored, author, Emit::Authored { slot, authoring: authoring.clone() });
        self.schedule(t_exec_a, author, Emit::Executed { parent: authoring });
        self.schedule(t_best_a, author, Emit::BestBlock { slot });

        // --- Gossip wave: a random tree rooted at the author ---
        // Each node picks an already-ready source, receives the announcement
        // one hop later, imports, and becomes a source itself.
        let v = self.act();
        let mut order: Vec<u16> = (0..v).filter(|&n| n != author).collect();
        order.shuffle(&mut self.rng);

        // (node, ready_time = its execution completed)
        let mut ready: Vec<(u16, SimTime)> = vec![(author, t_exec_a)];
        for dst in order {
            let (src, src_ready) = ready[self.rng.random_range(0..ready.len())];
            let p = &self.params.p.chain;
            let t_send = src_ready + self.rng.random_range(ms_range(p.gossip_hop_ms));
            let t_arr = t_send + self.rng.random_range(ms_range(p.net_ms));
            let t_imp = t_arr + self.rng.random_range(MICROS_PER_MS..8 * MICROS_PER_MS);
            let t_ver = t_imp + self.rng.random_range(ms_range(p.verify_ms));
            let t_exec = t_ver + self.rng.random_range(ms_range(p.exec_ms));
            let t_best = t_exec + self.rng.random_range(2 * MICROS_PER_MS..20 * MICROS_PER_MS);

            self.schedule(t_send, src, Emit::Announced { peer: dst, announcer_local: true, slot });
            self.schedule(t_arr, dst, Emit::Announced { peer: src, announcer_local: false, slot });

            let importing = id_slot();
            self.schedule(t_imp, dst, Emit::Importing { slot, self_id: importing.clone() });
            self.schedule(t_ver, dst, Emit::Verified { importing: importing.clone() });
            self.schedule(t_exec, dst, Emit::Executed { parent: importing });
            self.schedule(t_best, dst, Emit::BestBlock { slot });

            ready.push((dst, t_exec));
        }

        // --- Duplicate announcements beyond the tree ---
        // The Toaster reference shows ~5.8 announce edges per node per slot;
        // the tree contributes ~1, the rest are gossip duplicates that don't
        // trigger imports.
        let lambda = self.params.p.chain.announce_dup_lambda;
        let poisson = Poisson::new(lambda).expect("lambda > 0");
        let ready_snapshot = ready.clone();
        for &(src, src_ready) in &ready_snapshot {
            let dups = poisson.sample(&mut self.rng) as usize;
            for _ in 0..dups.min(self.params.v - 1) {
                let dst = loop {
                    let cand = self.rng.random_range(0..v);
                    if cand != src {
                        break cand;
                    }
                };
                let t_send = src_ready + self.rng.random_range(50..800) * MICROS_PER_MS;
                let t_arr = t_send + self.rng.random_range(ms_range(self.params.p.chain.net_ms));
                self.schedule(t_send, src, Emit::Announced {
                    peer: dst,
                    announcer_local: true,
                    slot,
                });
                self.schedule(t_arr, dst, Emit::Announced {
                    peer: src,
                    announcer_local: false,
                    slot,
                });
            }
        }

        // --- Guarantee discards: a report included in this block leaves the
        // pools of the nodes that actually hold its guarantee, shortly after
        // each imports the block ---
        let ready_map: std::collections::HashMap<u16, SimTime> = ready.iter().copied().collect();
        for &wp in &included {
            let holders = self.wps.get(&wp).map(|r| r.holders.clone()).unwrap_or_default();
            for h in holders {
                let Some(&node_ready) = ready_map.get(&h) else { continue };
                let at = node_ready
                    + self.rng.random_range(100 * MICROS_PER_MS..600 * MICROS_PER_MS);
                let reason_pick = self.rng.random::<u8>();
                self.schedule(at, h, Emit::GuaranteeDiscarded { wp, reason_pick });
            }
        }

        // --- Auditors recover the included bundles one tranche later ---
        self.schedule_audit_round(slot, &included);

        // --- Background sync: some nodes pull recent blocks explicitly ---
        self.schedule_block_sync(slot);

        // --- Finality: trail the head, monotonic ---
        // The lag is persistent and steps occasionally, so finality sometimes
        // stalls for a slot and then catches up in a burst.
        if self.rng.random::<f64>() < self.params.p.chain.finality_lag_switch_prob {
            self.finality_lag = self
                .rng
                .random_range(self.params.p.chain.finality_lag.0..=self.params.p.chain.finality_lag.1);
        }
        let target = slot
            .checked_sub(self.finality_lag)
            .and_then(|upto| self.blocks.range(..=upto).next_back().map(|(s, _)| *s));
        if let Some(fin) = target {
            if self.finalized.is_none_or(|cur| fin > cur) {
                self.finalized = Some(fin);
                for &(node, node_ready) in &ready {
                    let at = node_ready
                        + self.rng.random_range(20 * MICROS_PER_MS..200 * MICROS_PER_MS);
                    self.schedule(at, node, Emit::Finalized { slot: fin });
                }
            }
        }

        // Prune records that nothing will reference anymore.
        if let Some(fin) = self.finalized {
            let cutoff = fin.saturating_sub(20);
            self.blocks.retain(|s, _| *s >= cutoff);
            self.wps.retain(|_, rec| rec.slot >= cutoff);
        }
    }

    /// Deterministic authorship lottery (stands in for Safrole). Offline
    /// validators hold no tickets, so only active nodes author.
    fn author_of(&self, slot: u32) -> u16 {
        let h = super::ids::h32(self.seed, "author", slot as u64, 0);
        (u64::from_le_bytes(h[0..8].try_into().expect("8 bytes")) % self.active as u64) as u16
    }

    /// Build the block record for `slot` and drain the guarantee backlog into
    /// it (one report per core per block). Returns the record and the included
    /// WP ids for downstream scheduling (discards).
    fn make_block(&mut self, slot: u32) -> (BlockRec, Vec<u64>) {
        let parent = self
            .last_block_slot
            .map(|s| self.block_hash(s))
            .unwrap_or_else(|| self.genesis_hash());

        // Tickets flow only outside the epoch tail.
        let in_tail = self.params.slots.slot_phase(slot) >= self.params.epoch_tail_start();
        let max_tickets = self.params.proto.max_tickets_per_block as u32;
        let num_tickets = if in_tail {
            0
        } else {
            let poisson = Poisson::new(self.params.p.chain.tickets_per_block_mean).expect("mean > 0");
            (poisson.sample(&mut self.rng) as u32).min(max_tickets)
        };

        let num_preimages = match self.rng.random::<f64>() {
            x if x < 0.85 => 0,
            x if x < 0.97 => 1,
            _ => 2,
        };
        let preimages_size = if num_preimages > 0 {
            num_preimages * self.rng.random_range(500..5_000)
        } else {
            0
        };

        // Include pending guarantees, at most one per core.
        let backlog = std::mem::take(&mut self.guaranteed_backlog);
        let mut included = Vec::new();
        let mut leftover = Vec::new();
        let mut used_cores = std::collections::HashSet::new();
        for wp in backlog {
            let Some(rec) = self.wps.get(&wp) else { continue };
            if used_cores.insert(rec.core) {
                included.push(wp);
            } else {
                leftover.push(wp);
            }
        }
        self.guaranteed_backlog = leftover;

        let mut services = Vec::new();
        let mut avail = Vec::new();
        for &wp in &included {
            let rec = &self.wps[&wp];
            self.pending_per_core[rec.core as usize] =
                self.pending_per_core[rec.core as usize].saturating_sub(1);
            avail.push(rec.core);
            services.push((
                rec.service,
                self.rng.random_range(2_000_000..9_000_000),
                self.rng.random_range(500_000..4_000_000),
                rec.num_items,
            ));
        }
        self.avail_cores.insert(slot, avail);
        let window = self.params.proto.availability_timeout as u32 + 2;
        self.avail_cores.retain(|s, _| *s + window >= slot);

        // Roughly everyone assures; a few are always late.
        let missing = (Poisson::new(1.5).expect("mean > 0").sample(&mut self.rng) as u32)
            .min(self.params.v as u32);
        let num_assurances = self.params.v as u32 - missing;
        let num_guarantees = included.len() as u32;

        let rec = BlockRec {
            parent,
            size: 620
                + num_tickets * 784
                + preimages_size
                + num_guarantees * 810
                + num_assurances * 47
                + self.rng.random_range(0..400),
            num_tickets,
            num_preimages,
            preimages_size,
            num_guarantees,
            num_assurances,
            services,
        };
        (rec, included)
    }

    fn walk_clock_drift(&mut self) {
        let max = self.params.p.chain.clock_drift_max_us;
        for s in &mut self.streams {
            let step = self.rng.random_range(-200..=200);
            s.drift_us = (s.drift_us + step).clamp(-max, max);
        }
    }
}

/// Who dials whom: deterministic tiebreak, both ends agree (polkajam decides
/// by ed25519 key order; index order is our stand-in).
fn open_tiebreak(a: u16, b: u16) -> bool {
    if (a + b) % 2 == 0 {
        a < b
    } else {
        a > b
    }
}

fn ms_range(r: (u64, u64)) -> std::ops::Range<u64> {
    r.0 * MICROS_PER_MS..r.1 * MICROS_PER_MS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tiebreak_is_antisymmetric_and_balanced() {
        let mut opened_by_low = 0;
        for a in 0..10u16 {
            for b in 0..10u16 {
                if a == b {
                    continue;
                }
                assert_ne!(open_tiebreak(a, b), open_tiebreak(b, a));
                if a < b && open_tiebreak(a, b) {
                    opened_by_low += 1;
                }
            }
        }
        // Roughly half the pairs are opened by the lower index.
        assert!(opened_by_low > 10 && opened_by_low < 35, "got {}", opened_by_low);
    }
}
