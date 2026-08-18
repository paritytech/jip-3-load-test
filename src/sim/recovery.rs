//! Segment recovery (guarantor imports, events 160–178) and bundle recovery
//! for auditing (events 140–153).
//!
//! Shapes follow the Toaster reference mix: imports are served overwhelmingly
//! by direct segment requests to the previous guarantors (173/174/176/177/178
//! dominate the whole network's traffic), ~10% of them fail and retry, and a
//! small share escalates to segment-shard requests against assurers with a
//! reconstruction pass. Bundle recovery is ~99.5% direct requests to a
//! guarantor; the shard path with reconstruction is the rare fallback. All
//! `audit_id` fields are `EventId(0)` — the real auditing code never wires
//! them up.

use rand::Rng;
use rand_distr::{Distribution, Poisson};

use super::roles;
use super::time::{SimTime, MICROS_PER_MS, MICROS_PER_SEC};
use super::world::{id_slot, Emit, IdSlot, World};


impl World {
    /// Schedule the import-segment fetch for a WP on its primary guarantor.
    /// Returns the sim time by which imports are complete (feeds
    /// ImportsReceived / refine timing). `sub` is the submission id slot.
    pub(super) fn schedule_import_fetch(
        &mut self,
        wp: u64,
        primary: u16,
        core: u16,
        slot: u32,
        sub: IdSlot,
        after: SimTime,
    ) -> SimTime {
        if self.rng.random::<f64>() >= self.params.p.segments.has_imports {
            return after + self.rng.random_range(2..10) * MICROS_PER_MS;
        }

        // Previous rotation's trio for this core made the exports; only its
        // online members can actually serve segment requests. A fully offline
        // previous trio degrades to any online node (assurers hold shards).
        let rotation = (slot as u64 / self.params.proto.rotation_period as u64).saturating_sub(1);
        let act = self.act();
        let mut prev_trio: Vec<u16> =
            roles::assignment(self.seed, self.params.v, self.params.c, rotation).cores
                [core as usize]
                .into_iter()
                .filter(|&x| x < act && x != primary)
                .collect();
        if prev_trio.is_empty() {
            let fallback = loop {
                let cand = self.rng.random_range(0..act);
                if cand != primary {
                    break cand;
                }
            };
            prev_trio.push(fallback);
        }

        // Rare indirect route resolution.
        if self.rng.random::<f64>() < 0.01 {
            let t = after + self.rng.random_range(1..6) * MICROS_PER_MS;
            self.schedule(t, primary, Emit::SegHashMapped { wp, sub: sub.clone() });
        }

        let batches = Poisson::new(self.params.p.segments.batches_mean)
            .expect("mean > 0")
            .sample(&mut self.rng) as usize;
        // Spread wide enough that the primary's ~3 events per batch (~990 for
        // a mean fetch) stay under its 900/s socket drain — heavy but
        // sustainable, like the real fetch.
        let window_us = self.rng.random_range(1_500..3_500) * MICROS_PER_MS;
        let mut done = after;

        for i in 0..batches {
            let t0 = after + (window_us * i as u64) / batches.max(1) as u64;
            let end = self.direct_segment_batch(wp, primary, prev_trio.clone(), sub.clone(), t0, 0);
            done = done.max(end);
            // Roughly every second batch closes with a verification event.
            if i % 2 == 0 {
                let t = end + self.rng.random_range(1..5) * MICROS_PER_MS;
                self.schedule(t, primary, Emit::SegVerified { sub: sub.clone() });
            }
        }
        done + self.rng.random_range(2..12) * MICROS_PER_MS
    }

    /// One direct segment request; retries once on failure, escalating to the
    /// shard path with some probability. Returns completion time.
    #[allow(clippy::too_many_arguments)]
    fn direct_segment_batch(
        &mut self,
        wp: u64,
        primary: u16,
        prev_trio: Vec<u16>,
        sub: IdSlot,
        t0: SimTime,
        attempt: u8,
    ) -> SimTime {
        let guarantor = prev_trio[self.rng.random_range(0..prev_trio.len())];
        let req = id_slot();
        let t_176 = t0 + self.rng.random_range(2..8) * MICROS_PER_MS;
        self.schedule(t0, primary, Emit::SegReqSend {
            guarantor,
            sub: sub.clone(),
            self_id: req.clone(),
        });
        self.schedule(t_176, primary, Emit::SegReqSent { parent: req.clone() });

        if attempt == 0 && self.rng.random::<f64>() < self.params.p.segments.req_fail {
            let t_175 = t0 + self.rng.random_range(200..800) * MICROS_PER_MS;
            self.schedule(t_175, primary, Emit::SegReqFailed { parent: req });
            return if self.rng.random::<f64>() < self.params.p.segments.escalate {
                self.segment_shard_path(wp, primary, sub, t_175)
            } else {
                self.direct_segment_batch(wp, primary, prev_trio, sub, t_175, 1)
            };
        }

        // Responder side.
        let recv = id_slot();
        let t_174 = t0 + self.rng.random_range(4..20) * MICROS_PER_MS;
        let t_177 = t_174 + self.rng.random_range(1..6) * MICROS_PER_MS;
        let t_178g = t_177 + self.rng.random_range(3..25) * MICROS_PER_MS;
        let num = self.rng.random_range(1..16);
        self.schedule(t_174, guarantor, Emit::SegRecv {
            guarantor: primary,
            self_id: recv.clone(),
        });
        self.schedule(t_177, guarantor, Emit::SegReqReceived { num, parent: recv.clone() });
        self.schedule(t_178g, guarantor, Emit::SegTransferred { parent: recv });

        let t_178p = t_178g + self.rng.random_range(3..20) * MICROS_PER_MS;
        self.schedule(t_178p, primary, Emit::SegTransferred { parent: req });
        t_178p
    }

    /// Shard-path fallback: fetch segment shards from assurers, reconstruct.
    fn segment_shard_path(&mut self, _wp: u64, primary: u16, sub: IdSlot, t0: SimTime) -> SimTime {
        let v = self.act();
        let proofs = self.rng.random::<f64>() < 0.5;
        let mut last = t0;

        for _ in 0..2 {
            let assurer = loop {
                let cand = self.rng.random_range(0..v);
                if cand != primary {
                    break cand;
                }
            };
            let req = id_slot();
            let t_send = t0 + self.rng.random_range(5..60) * MICROS_PER_MS;
            let t_165 = t_send + self.rng.random_range(2..8) * MICROS_PER_MS;
            self.schedule(t_send, primary, Emit::SegShardReqSend {
                assurer,
                proofs,
                sub: sub.clone(),
                self_id: req.clone(),
            });
            self.schedule(t_165, primary, Emit::SegShardReqSent { parent: req.clone() });

            if self.rng.random::<f64>() < self.params.p.segments.shard_fail {
                let t_164 = t_send + self.rng.random_range(150..700) * MICROS_PER_MS;
                self.schedule(t_164, primary, Emit::SegShardReqFailed { parent: req });
                last = last.max(t_164);
                continue;
            }

            let recv = id_slot();
            let t_163 = t_send + self.rng.random_range(4..20) * MICROS_PER_MS;
            let t_166 = t_163 + self.rng.random_range(1..6) * MICROS_PER_MS;
            let t_167a = t_166 + self.rng.random_range(3..20) * MICROS_PER_MS;
            let num = self.rng.random_range(1..16);
            self.schedule(t_163, assurer, Emit::SegShardRecv {
                sender: primary,
                proofs,
                self_id: recv.clone(),
            });
            self.schedule(t_166, assurer, Emit::SegShardReqReceived { num, parent: recv.clone() });
            self.schedule(t_167a, assurer, Emit::SegShardsTransferred { parent: recv });
            let t_167p = t_167a + self.rng.random_range(3..15) * MICROS_PER_MS;
            self.schedule(t_167p, primary, Emit::SegShardsTransferred { parent: req });
            last = last.max(t_167p);
        }

        // Reconstruction closes the group.
        let rec = id_slot();
        let trivial = self.rng.random::<f64>() < 0.5;
        let t_168 = last + self.rng.random_range(2..10) * MICROS_PER_MS;
        let t_170 = t_168 + self.rng.random_range(3..25) * MICROS_PER_MS;
        self.schedule(t_168, primary, Emit::SegReconstructing {
            trivial,
            sub,
            self_id: rec.clone(),
        });
        self.schedule(t_170, primary, Emit::SegReconstructed { parent: rec });
        t_170
    }

    /// Auditors pull bundles of the reports included in this slot's block,
    /// starting one tranche (8 s) later.
    pub(super) fn schedule_audit_round(&mut self, slot: u32, included: &[u64]) {
        let v = self.act();
        let base = self.params.slots.slot_start(slot) + 8 * MICROS_PER_SEC;
        let fetchers_dist = Poisson::new(self.params.p.audit.fetchers_mean).expect("mean > 0");

        for &wp in included {
            let Some(rec) = self.wps.get(&wp) else { continue };
            // The bundle lives with the guarantors that signed the report —
            // they are both the fetch targets and the only nodes with nothing
            // to fetch. (The current rotation's trio is irrelevant here: a
            // report can be included after a rotation boundary.)
            let holders = rec.guarantors.clone();
            if holders.is_empty() {
                continue;
            }
            let candidates: Vec<u16> = (0..v).filter(|c| !holders.contains(c)).collect();
            if candidates.is_empty() {
                continue;
            }

            let fetchers = fetchers_dist.sample(&mut self.rng) as usize;
            for _ in 0..fetchers.min(self.params.v - holders.len()) {
                let auditor = candidates[self.rng.random_range(0..candidates.len())];
                let t0 = base + self.rng.random_range(0..2_000) * MICROS_PER_MS;
                if self.rng.random::<f64>() < self.params.p.audit.shard_path {
                    self.bundle_shard_path(wp, auditor, t0);
                } else {
                    self.direct_bundle_fetch(wp, auditor, &holders, t0, 0);
                }
            }
        }
    }

    fn direct_bundle_fetch(
        &mut self,
        wp: u64,
        auditor: u16,
        holders: &[u16],
        t0: SimTime,
        attempt: u8,
    ) {
        let guarantor = holders[self.rng.random_range(0..holders.len())];
        let req = id_slot();
        let t_151 = t0 + self.rng.random_range(2..8) * MICROS_PER_MS;
        self.schedule(t0, auditor, Emit::BundleReqSend {
            guarantor,
            self_id: req.clone(),
        });
        self.schedule(t_151, auditor, Emit::BundleReqSent { parent: req.clone() });

        if attempt == 0 && self.rng.random::<f64>() < self.params.p.audit.req_fail {
            let t_150 = t0 + self.rng.random_range(200..900) * MICROS_PER_MS;
            self.schedule(t_150, auditor, Emit::BundleReqFailed { parent: req });
            self.direct_bundle_fetch(wp, auditor, holders, t_150, 1);
            return;
        }

        let recv = id_slot();
        let t_149 = t0 + self.rng.random_range(4..20) * MICROS_PER_MS;
        let t_152 = t_149 + self.rng.random_range(1..6) * MICROS_PER_MS;
        let t_153g = t_152 + self.rng.random_range(5..40) * MICROS_PER_MS;
        self.schedule(t_149, guarantor, Emit::BundleRecv {
            auditor,
            self_id: recv.clone(),
        });
        self.schedule(t_152, guarantor, Emit::BundleReqReceived { wp, parent: recv.clone() });
        self.schedule(t_153g, guarantor, Emit::BundleTransferred { parent: recv });
        let t_153a = t_153g + self.rng.random_range(3..25) * MICROS_PER_MS;
        self.schedule(t_153a, auditor, Emit::BundleTransferred { parent: req });
    }

    fn bundle_shard_path(&mut self, wp: u64, auditor: u16, t0: SimTime) {
        let v = self.act();
        let mut last = t0;
        for _ in 0..8 {
            let assurer = loop {
                let cand = self.rng.random_range(0..v);
                if cand != auditor {
                    break cand;
                }
            };
            let req = id_slot();
            let t_send = t0 + self.rng.random_range(0..120) * MICROS_PER_MS;
            let t_143 = t_send + self.rng.random_range(2..8) * MICROS_PER_MS;
            self.schedule(t_send, auditor, Emit::BundleShardReqSend {
                assurer,
                shard: assurer,
                self_id: req.clone(),
            });
            self.schedule(t_143, auditor, Emit::BundleShardReqSent { parent: req.clone() });

            if self.rng.random::<f64>() < 0.03 {
                let t_142 = t_send + self.rng.random_range(150..600) * MICROS_PER_MS;
                self.schedule(t_142, auditor, Emit::BundleShardReqFailed { parent: req });
                continue;
            }

            let recv = id_slot();
            let t_141 = t_send + self.rng.random_range(4..20) * MICROS_PER_MS;
            let t_144 = t_141 + self.rng.random_range(1..6) * MICROS_PER_MS;
            let t_145a = t_144 + self.rng.random_range(3..20) * MICROS_PER_MS;
            self.schedule(t_141, assurer, Emit::BundleShardRecv {
                auditor,
                self_id: recv.clone(),
            });
            self.schedule(t_144, assurer, Emit::BundleShardReqReceived {
                wp,
                shard: assurer,
                parent: recv.clone(),
            });
            self.schedule(t_145a, assurer, Emit::BundleShardTransferred { parent: recv });
            let t_145b = t_145a + self.rng.random_range(3..15) * MICROS_PER_MS;
            self.schedule(t_145b, auditor, Emit::BundleShardTransferred { parent: req });
            last = last.max(t_145b);
        }
        let trivial = self.rng.random::<f64>() < 0.3;
        let t_146 = last + self.rng.random_range(2..10) * MICROS_PER_MS;
        let t_147 = t_146 + self.rng.random_range(5..40) * MICROS_PER_MS;
        self.schedule(t_146, auditor, Emit::BundleReconstructing { trivial });
        self.schedule(t_147, auditor, Emit::BundleReconstructed);
    }
}
