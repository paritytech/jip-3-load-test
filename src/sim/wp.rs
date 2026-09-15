//! Work-package → guarantee → availability pipeline scheduling.
//!
//! Role rules from the polkajam emission model (docs/research/):
//! - a builder submits a WP to exactly ONE guarantor (the "primary"), so
//!   event 90 occurs once per WP network-wide; the primary shares with the
//!   two secondaries of its core (98/100), which emit 91 and sign (103);
//! - only the primary builds and broadcasts the guarantee: 105, then
//!   106/108 (or 107) to every validator except itself (V−1), then 109;
//! - every other validator emits 110 → 112 and eventually discards (113)
//!   once the report lands in a block;
//! - assurers (everyone but the trio) fetch their shard from a random trio
//!   member: 120/123/125 requester-side, 121/124/125 responder-side;
//! - assurances go out per slot 2 s before the boundary: 126 → 128×(V−1)
//!   (some 127) → 129, receivers emit 131.
//!
//! Failure shares follow the Toaster reference run: ~15% guarantee/assurance
//! send failures, ~25% share failures, ~12% early WP failures, ~0.3% shard
//! request failures.

use rand::Rng;

use super::roles;
use super::time::{SimTime, MICROS_PER_MS};
use super::world::{id_slot, Emit, World, WpRec};


impl World {
    /// Sample WP arrivals for one slot and schedule their pipelines.
    pub(super) fn schedule_wp_arrivals(&mut self, slot: u32) {
        use rand_distr::{Distribution, Poisson};
        let rate = self.params.p.wp.per_core_per_slot;
        if rate <= 0.0 {
            return;
        }
        let poisson = Poisson::new(rate).expect("rate > 0");
        let slot_t = self.params.slots.slot_start(slot);
        for core in 0..self.params.c as u16 {
            let arrivals = poisson.sample(&mut self.rng) as usize;
            for _ in 0..arrivals.min(3) {
                let t0 = slot_t + self.rng.random_range(200..4_500) * MICROS_PER_MS;
                self.schedule_wp_pipeline(core, slot, t0);
            }
        }
    }

    /// Schedule the full lifecycle of one work package.
    fn schedule_wp_pipeline(&mut self, core: u16, slot: u32, t0: SimTime) {
        let rotation = slot as u64 / self.params.proto.rotation_period as u64;
        let trio =
            roles::assignment(self.seed, self.params.v, self.params.c, rotation).cores
                [core as usize];
        // The builder needs a reachable guarantor: the primary is an online
        // trio member. A fully offline trio means the WP never surfaces.
        let act = self.act();
        let online_trio: Vec<u16> = trio.iter().copied().filter(|&x| x < act).collect();
        if online_trio.is_empty() {
            return;
        }
        let primary = online_trio[self.rng.random_range(0..online_trio.len())];
        let secondaries: Vec<u16> =
            trio.into_iter().filter(|&x| x != primary).collect();

        let wp = self.wp_counter;
        self.wp_counter += 1;

        let num_items = 1 + u32::from(self.rng.random::<f64>() < 0.3);
        let payload_size = self.rng.random_range(1_000..80_000);
        let extrinsic_size = self.rng.random_range(0..30_000);
        let size = 500 + payload_size / 4 + self.rng.random_range(0..2_000);
        let service = self.services[self.rng.random_range(0..self.services.len())];
        self.wps.insert(
            wp,
            WpRec {
                core,
                service,
                slot,
                size,
                num_items,
                payload_size,
                extrinsic_size,
                num_exports: self.rng.random_range(0..48),
                bundle_size: size + extrinsic_size + self.rng.random_range(500..4_000),
                anchor_slot: self.last_block_slot.unwrap_or(slot.saturating_sub(1)),
                guarantors: Vec::new(), // filled below once signing resolves
                holders: Vec::new(),    // filled once the broadcast resolves
                builder: self.rng.random_range(0..self.builder_peers.len()),
                bundle: self.rng.random::<f64>() < 0.1,
            },
        );

        // --- Primary: submission and validation chain ---
        let submission = id_slot();
        let ms = |r: std::ops::Range<u64>, rng: &mut rand::rngs::StdRng| {
            rng.random_range(r) * MICROS_PER_MS
        };
        let t_94 = t0 + ms(4..15, &mut self.rng);
        let t_96 = t_94 + ms(8..30, &mut self.rng);
        let t_95 = t_96 + ms(10..50, &mut self.rng);
        let t_97 = t_95 + ms(15..60, &mut self.rng);

        self.schedule(t0, primary, Emit::WpSubmission { wp, self_id: submission.clone() });
        self.schedule(t_94, primary, Emit::WpReceived { wp, parent: submission.clone() });
        self.schedule(t_96, primary, Emit::WpExtrinsic { parent: submission.clone() });

        if self.rng.random::<f64>() < self.params.p.wp.early_fail {
            // Dies at authorization/extrinsic stage; nothing propagates.
            let t_fail = t_95 + ms(5..40, &mut self.rng);
            self.schedule(t_fail, primary, Emit::WpFailed {
                parent: submission,
                reason: "work package authorization failed",
            });
            return;
        }

        let auth_ns = self.rng.random_range(400_000..2_500_000);
        let auth_gas = self.rng.random_range(2_000_000..40_000_000);
        self.schedule(t_95, primary, Emit::WpAuthorized {
            parent: submission.clone(),
            gas: auth_gas,
            ns: auth_ns,
        });

        // Bundled submissions carry their imports; otherwise the primary
        // fetches import segments first (the dominant traffic of a loaded
        // network), and ImportsReceived fires once that completes.
        let bundle = self.wps[&wp].bundle;
        let t_97 = if bundle {
            t_97
        } else {
            self.schedule_import_fetch(wp, primary, core, slot, submission.clone(), t_96)
        };
        self.schedule(t_97, primary, Emit::WpImports { parent: submission.clone() });

        // Refine on the primary; ns mirrors the played-out stage latency.
        let refine_ms = self.rng.random_range(100..260);
        let t_101 = t_97 + refine_ms * MICROS_PER_MS;
        let t_102 = t_101 + ms(4..15, &mut self.rng);
        let rec_items = self.wps[&wp].num_items;
        let items: Vec<(u64, u64)> = (0..rec_items)
            .map(|_| {
                (
                    self.rng.random_range(200_000_000..3_000_000_000),
                    refine_ms * 1_000_000 / rec_items as u64,
                )
            })
            .collect();
        self.schedule(t_101, primary, Emit::WpRefined {
            parent: submission.clone(),
            items: items.clone(),
        });
        self.schedule(t_102, primary, Emit::WpReportBuilt { wp, parent: submission.clone() });

        // --- Sharing with secondaries ---
        let mut signed: Vec<u16> = vec![primary];
        let mut last_sig_t = t_102;
        for &sec in &secondaries {
            // No connection to an offline secondary — the primary never even
            // initiates the share (no 98/99), it just proceeds with fewer
            // signers. The reference data confirms this: the observed share
            // failure rate sits BELOW the pure offline share.
            if sec >= act {
                continue;
            }
            let t_98 = t_94 + ms(8..25, &mut self.rng);
            self.schedule(t_98, primary, Emit::Sharing {
                parent: submission.clone(),
                secondary: sec,
            });
            if self.rng.random::<f64>() < self.params.p.wp.share_fail {
                let t_99 = t_98 + ms(20..80, &mut self.rng);
                self.schedule(t_99, primary, Emit::ShareFailed {
                    parent: submission.clone(),
                    secondary: sec,
                });
                continue;
            }
            let t_100 = t_98 + ms(15..50, &mut self.rng);
            self.schedule(t_100, primary, Emit::BundleSent {
                parent: submission.clone(),
                secondary: sec,
            });

            // Secondary's own chain, all referencing its 91.
            let share = id_slot();
            let t_91 = t_98 + ms(10..40, &mut self.rng);
            let t_s94 = t_91 + ms(5..15, &mut self.rng);
            let t_s96 = t_s94 + ms(5..25, &mut self.rng);
            let t_s97 = t_s96 + ms(5..25, &mut self.rng);
            let t_s95 = t_s97 + ms(10..45, &mut self.rng);
            let sec_refine_ms = self.rng.random_range(100..260);
            let t_s101 = t_s95 + sec_refine_ms * MICROS_PER_MS;
            let t_s102 = t_s101 + ms(4..15, &mut self.rng);
            let t_103 = t_s102 + ms(5..20, &mut self.rng);

            self.schedule(t_91, sec, Emit::WpShared { primary, self_id: share.clone() });
            self.schedule(t_s94, sec, Emit::WpReceived { wp, parent: share.clone() });
            self.schedule(t_s96, sec, Emit::WpExtrinsic { parent: share.clone() });
            self.schedule(t_s97, sec, Emit::WpImports { parent: share.clone() });
            let s_auth_ns = self.rng.random_range(400_000..2_500_000);
            let s_auth_gas = self.rng.random_range(2_000_000..40_000_000);
            self.schedule(t_s95, sec, Emit::WpAuthorized {
                parent: share.clone(),
                gas: s_auth_gas,
                ns: s_auth_ns,
            });
            let sec_items: Vec<(u64, u64)> = (0..rec_items)
                .map(|_| {
                    (
                        self.rng.random_range(200_000_000..3_000_000_000),
                        sec_refine_ms * 1_000_000 / rec_items as u64,
                    )
                })
                .collect();
            self.schedule(t_s101, sec, Emit::WpRefined { parent: share.clone(), items: sec_items });
            self.schedule(t_s102, sec, Emit::WpReportBuilt { wp, parent: share.clone() });
            self.schedule(t_103, sec, Emit::SigSent { parent: share });

            // Primary receives the signature.
            let t_104 = t_103 + ms(8..35, &mut self.rng);
            self.schedule(t_104, primary, Emit::SigReceived {
                parent: submission.clone(),
                secondary: sec,
            });
            signed.push(sec);
            last_sig_t = last_sig_t.max(t_104);
        }

        // Threshold: 2 of 3 signatures (primary's own counts as one).
        if signed.len() < 2 {
            let t_fail = last_sig_t + ms(400..1_200, &mut self.rng);
            self.schedule(t_fail, primary, Emit::WpFailed {
                parent: submission,
                reason: "not enough guarantor signatures",
            });
            self.wps.get_mut(&wp).expect("just inserted").guarantors = signed;
            return;
        }
        signed.sort_unstable();
        self.wps.get_mut(&wp).expect("just inserted").guarantors = signed;

        // --- Guarantee build and broadcast (primary only) ---
        let built = id_slot();
        let t_105 = last_sig_t + ms(5..20, &mut self.rng);
        self.schedule(t_105, primary, Emit::GuaranteeBuilt {
            wp,
            parent: submission.clone(),
            self_id: built.clone(),
        });

        // Broadcast to EVERY other validator of the epoch, offline ones
        // included: the real node stamps all SendingGuarantee events within
        // ~25 ms of GuaranteeBuilt (observed on Toaster), acks follow within
        // milliseconds, and sends to offline peers fail immediately.
        let burst_us = self.params.p.wp.guarantee_burst_ms * MICROS_PER_MS;
        let mut holders: Vec<u16> = vec![primary];
        for recipient in 0..self.params.v as u16 {
            if recipient == primary {
                continue;
            }
            let sending = id_slot();
            let t_106 = t_105 + self.rng.random_range(0..burst_us.max(1));
            self.schedule(t_106, primary, Emit::SendingGuarantee {
                built: built.clone(),
                recipient,
                self_id: sending.clone(),
            });
            let offline = recipient >= act;
            if offline {
                let t_107 = t_106 + ms(1..6, &mut self.rng);
                self.schedule(t_107, primary, Emit::GuaranteeSendFailed { sending });
                continue;
            }
            if self.rng.random::<f64>() < self.params.p.wp.guarantee_send_fail {
                // Online but failing: a timeout-shaped failure.
                let t_107 = t_106 + ms(300..3_000, &mut self.rng);
                self.schedule(t_107, primary, Emit::GuaranteeSendFailed { sending });
                continue;
            }
            let ack = self.params.p.wp.guarantee_ack_ms;
            let t_108 = t_106 + ms(ack.0..ack.1.max(ack.0 + 1), &mut self.rng);
            self.schedule(t_108, primary, Emit::GuaranteeSent { sending });

            // Receiver side, one LAN hop away.
            let receiving = id_slot();
            let t_110 = t_106 + self.lan_delay();
            self.schedule(t_110, recipient, Emit::RecvGuarantee {
                primary,
                self_id: receiving.clone(),
            });
            if self.rng.random::<f64>() < self.params.p.wp.guarantee_recv_fail {
                continue; // 111 is rare enough to reserve for later polish
            }
            let t_112 = t_110 + ms(1..5, &mut self.rng);
            self.schedule(t_112, recipient, Emit::GuaranteeReceived {
                wp,
                parent: receiving,
            });
            holders.push(recipient);

            // Assurers (everyone outside the trio) fetch their shard.
            if !trio.contains(&recipient) {
                self.schedule_shard_fetch(wp, recipient, online_trio.clone(), t_112);
            }
        }
        self.wps.get_mut(&wp).expect("just inserted").holders = holders;

        let done = self.params.p.wp.guarantee_done_ms;
        let t_109 = t_105 + ms(done.0..done.1.max(done.0 + 1), &mut self.rng);
        self.schedule(t_109, primary, Emit::GuaranteesDistributed { parent: submission });
    }

    /// One LAN hop for paired send/receive events: mostly ~1 ms with a tail
    /// to ~30 ms (observed on Toaster).
    pub(super) fn lan_delay(&mut self) -> SimTime {
        let (lo, hi) = self.params.p.wp.lan_us;
        let base = self.rng.random_range(lo..hi.max(lo + 1));
        if self.rng.random::<f64>() < 0.15 {
            base + self.rng.random_range(0..26_000)
        } else {
            base
        }
    }

    /// One assurer pulls its shard from a random online trio member.
    fn schedule_shard_fetch(&mut self, wp: u64, assurer: u16, online_trio: Vec<u16>, after: SimTime) {
        let ms = MICROS_PER_MS;
        let guarantor = online_trio[self.rng.random_range(0..online_trio.len())];
        let req = id_slot();
        let t_120 = after + self.rng.random_range(50..900) * ms;
        let t_123 = t_120 + self.rng.random_range(2..10) * ms;
        self.schedule(t_120, assurer, Emit::ShardReqSend {
            wp,
            guarantor,
            shard: assurer,
            self_id: req.clone(),
        });
        self.schedule(t_123, assurer, Emit::ShardReqSent { parent: req.clone() });

        if self.rng.random::<f64>() < self.params.p.wp.shard_req_fail {
            let t_122 = t_120 + self.rng.random_range(200..1_500) * ms;
            self.schedule(t_122, assurer, Emit::ShardReqFailed { parent: req });
            return;
        }

        // Responder side on the guarantor.
        let recv = id_slot();
        let t_121 = t_120 + self.rng.random_range(4..20) * ms;
        let t_124 = t_121 + self.rng.random_range(2..10) * ms;
        let t_125g = t_124 + self.rng.random_range(10..60) * ms;
        self.schedule(t_121, guarantor, Emit::ShardRecv {
            assurer,
            self_id: recv.clone(),
        });
        self.schedule(t_124, guarantor, Emit::ShardReqReceived {
            wp,
            shard: assurer,
            parent: recv.clone(),
        });
        self.schedule(t_125g, guarantor, Emit::ShardsTransferred { parent: recv });

        // Requester completes.
        let t_125a = t_125g + self.rng.random_range(5..40) * ms;
        self.schedule(t_125a, assurer, Emit::ShardsTransferred { parent: req });
    }

    /// Per-slot assurance round: every validator distributes 2 s before the
    /// next slot boundary.
    pub(super) fn schedule_assurance_round(&mut self, slot: u32) {
        let Some(anchor_slot) = self.last_block_slot else { return };

        // Cores made available within the availability window.
        let window_start = slot.saturating_sub(self.params.proto.availability_timeout as u32);
        let mut bits: Vec<u16> = self
            .avail_cores
            .range(window_start..=slot)
            .flat_map(|(_, cores)| cores.iter().copied())
            .collect();
        bits.sort_unstable();
        bits.dedup();

        // Shape measured on live Toaster nodes: DistributingAssurance lands
        // exactly `assurance_lead_ms` before the boundary (±4 ms); every
        // AssuranceSent/Failed is stamped within ~1 ms of it (the node logs the
        // send at initiation, to ALL other validators, offline ones failing
        // instantly); AssuranceDistributed follows 20–600 ms later.
        let boundary = self.params.slots.slot_start(slot + 1);
        let base = boundary.saturating_sub(self.params.p.wp.assurance_lead_ms * MICROS_PER_MS);
        let act = self.act();
        let v_total = self.params.v as u16;
        let burst_us = self.params.p.wp.assurance_burst_us.max(1);
        let done = self.params.p.wp.assurance_done_ms;

        for node in 0..act {
            let distributing = id_slot();
            let t_126 = base + self.rng.random_range(0..4_000);
            self.schedule(t_126, node, Emit::Assuring {
                anchor_slot,
                bits: bits.clone(),
                self_id: distributing.clone(),
            });

            for recipient in 0..v_total {
                if recipient == node {
                    continue;
                }
                let t_send = t_126 + self.rng.random_range(0..burst_us);
                let offline = recipient >= act;
                if offline || self.rng.random::<f64>() < self.params.p.wp.assurance_send_fail {
                    self.schedule(t_send, node, Emit::AssuranceSendFailed {
                        parent: distributing.clone(),
                        recipient,
                    });
                    continue;
                }
                self.schedule(t_send, node, Emit::AssuranceSent {
                    parent: distributing.clone(),
                    recipient,
                });
                let t_recv = t_send + self.lan_delay();
                self.schedule(t_recv, recipient, Emit::AssuranceReceived {
                    sender: node,
                    anchor_slot,
                });
            }
            let t_129 = t_126 + self.rng.random_range(done.0..done.1.max(done.0 + 1)) * MICROS_PER_MS;
            self.schedule(t_129, node, Emit::AssuranceDistributed { parent: distributing });
        }
    }
}
