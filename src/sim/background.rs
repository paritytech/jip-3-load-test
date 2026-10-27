//! Background traffic: per-epoch tickets, preimage gossip, connection churn.
//!
//! Shapes from the Toaster reference: nodes endlessly redial dead peers
//! (~99% of ConnectingOut fails, ~1.2 dials/s per node), ticket transfers
//! run through proxies in the ticketing slot of each epoch with ~9% transfer
//! failures, preimage flows are sparse, and PeerMisbehaved is rare noise.

use rand::Rng;
use rand_distr::{Distribution, Poisson};

use super::ids::h32;
use super::time::{MICROS_PER_MS, MICROS_PER_SEC};
use super::world::{id_slot, Emit, World};


impl World {
    /// Ticket generation and proxy transfers in the ticketing slot.
    pub(super) fn maybe_tickets(&mut self, slot: u32) {
        let e = self.params.slots.epoch_period;
        let buffer = if e > 30 { e / 30 } else { 1 };
        let connect = if e > 60 { e / 60 } else { 1 };
        if self.params.slots.slot_phase(slot) != buffer + connect {
            return;
        }
        let epoch = slot / e + 1;
        let v = self.act();
        let n_tickets = self.params.proto.max_tickets_per_block as u64;
        let base = self.params.slots.slot_start(slot);
        let window = 3 * self.params.slots.slot_period_us;

        for val in 0..v {
            let generating = id_slot();
            let t_80 = base + self.rng.random_range(0..1_500) * MICROS_PER_MS;
            let t_82 = t_80 + self.rng.random_range(50..400) * MICROS_PER_MS;
            let tickets: Vec<u64> = (0..n_tickets)
                .map(|_| {
                    self.ticket_counter += 1;
                    self.ticket_counter
                })
                .collect();
            self.schedule(t_80, val, Emit::GeneratingTickets {
                epoch,
                self_id: generating.clone(),
            });
            self.schedule(t_82, val, Emit::TicketsGenerated {
                parent: generating,
                ids: tickets.clone(),
            });

            for (k, &ticket) in tickets.iter().enumerate() {
                let attempt = (k % self.params.proto.max_tickets_per_block as usize) as u8;
                // Proxy derived from the ticket id, like polkajam's proxy_index.
                let proxy_pick = u64::from_le_bytes(
                    h32(self.seed, "proxy", ticket, 0)[0..8].try_into().expect("8"),
                );
                // The proxy is drawn from the FULL validator set: an offline
                // proxy means the leg fails, like a dead peer on a real net.
                let proxy = (proxy_pick % self.params.v as u64) as u16;
                if proxy == val {
                    continue; // real code diverts to the forward queue; skip
                }
                if proxy >= v {
                    let t_send = t_80 + self.rng.random_range(200..2_000) * MICROS_PER_MS;
                    self.schedule(t_send, val, Emit::TicketTransferFailed {
                        peer: proxy,
                        local: true,
                        from_proxy: false,
                    });
                    continue;
                }

                // Leg 1: author → proxy.
                let t_send = t_80 + self.rng.random_range(200..window / MICROS_PER_MS / 2) * MICROS_PER_MS;
                if self.rng.random::<f64>() < self.params.p.background.ticket_fail {
                    self.schedule(t_send, val, Emit::TicketTransferFailed {
                        peer: proxy,
                        local: true,
                        from_proxy: false,
                    });
                    continue;
                }
                self.schedule(t_send, val, Emit::TicketTransferred {
                    peer: proxy,
                    local: true,
                    from_proxy: false,
                    epoch,
                    attempt,
                    ticket,
                });
                let t_recv = t_send + self.rng.random_range(5..40) * MICROS_PER_MS;
                self.schedule(t_recv, proxy, Emit::TicketTransferred {
                    peer: val,
                    local: false,
                    from_proxy: false,
                    epoch,
                    attempt,
                    ticket,
                });

                // Leg 2: proxy → a sample of active validators.
                let fanout = (self.params.p.background.proxy_fanout * (v - 1) as f64) as usize;
                for _ in 0..fanout {
                    let target = loop {
                        let cand = self.rng.random_range(0..v);
                        if cand != proxy {
                            break cand;
                        }
                    };
                    let t_fwd = t_recv + self.rng.random_range(100..2_500) * MICROS_PER_MS;
                    if self.rng.random::<f64>() < self.params.p.background.ticket_fail {
                        self.schedule(t_fwd, proxy, Emit::TicketTransferFailed {
                            peer: target,
                            local: true,
                            from_proxy: true,
                        });
                        continue;
                    }
                    self.schedule(t_fwd, proxy, Emit::TicketTransferred {
                        peer: target,
                        local: true,
                        from_proxy: true,
                        epoch,
                        attempt,
                        ticket,
                    });
                    let t_tgt = t_fwd + self.rng.random_range(5..40) * MICROS_PER_MS;
                    self.schedule(t_tgt, target, Emit::TicketTransferred {
                        peer: proxy,
                        local: false,
                        from_proxy: true,
                        epoch,
                        attempt,
                        ticket,
                    });
                }
            }
        }
    }

    /// Sparse preimage gossip stories.
    pub(super) fn schedule_preimages(&mut self, slot: u32) {
        if self.rng.random::<f64>() >= self.params.p.background.preimage_per_slot {
            return;
        }
        self.preimage_counter += 1;
        let preimage = self.preimage_counter;
        let v = self.act();
        let announcer = self.rng.random_range(0..v);
        let service = self.services[self.rng.random_range(0..self.services.len())];
        let length = self.rng.random_range(1_000..100_000);
        let base = self.params.slots.slot_start(slot)
            + self.rng.random_range(0..4_000) * MICROS_PER_MS;

        // Announce to a handful of peers; both sides emit 191 (or 190 on a
        // failed edge, announcer side only).
        let fanout = self.rng.random_range(3..8).min(v as usize - 1);
        let mut peers = Vec::new();
        for _ in 0..fanout {
            let peer = loop {
                let cand = self.rng.random_range(0..v);
                if cand != announcer && !peers.contains(&cand) {
                    break cand;
                }
            };
            peers.push(peer);
            let t = base + self.rng.random_range(0..800) * MICROS_PER_MS;
            if self.rng.random::<f64>() < self.params.p.background.announce_fail {
                // The real event carries the peer; reuse Misbehaved-style noise.
                self.schedule(t, announcer, Emit::PreimageAnnounceFailed { peer });
                continue;
            }
            self.schedule(t, announcer, Emit::PreimageAnnounced {
                peer,
                local: true,
                service,
                preimage,
                length,
            });
            let t_recv = t + self.rng.random_range(5..40) * MICROS_PER_MS;
            self.schedule(t_recv, peer, Emit::PreimageAnnounced {
                peer: announcer,
                local: false,
                service,
                preimage,
                length,
            });
        }

        // One or two peers pull the preimage.
        for &peer in peers.iter().take(self.rng.random_range(1..3)) {
            let req = id_slot();
            let t_193 = base + self.rng.random_range(900..2_000) * MICROS_PER_MS;
            let t_196 = t_193 + self.rng.random_range(2..8) * MICROS_PER_MS;
            self.schedule(t_193, peer, Emit::PreimageReqSend {
                recipient: announcer,
                preimage,
                self_id: req.clone(),
            });
            self.schedule(t_196, peer, Emit::PreimageReqSent { parent: req.clone() });

            let recv = id_slot();
            let t_194 = t_193 + self.rng.random_range(4..20) * MICROS_PER_MS;
            let t_197 = t_194 + self.rng.random_range(1..6) * MICROS_PER_MS;
            let t_198a = t_197 + self.rng.random_range(3..25) * MICROS_PER_MS;
            self.schedule(t_194, announcer, Emit::PreimageRecv {
                sender: peer,
                self_id: recv.clone(),
            });
            self.schedule(t_197, announcer, Emit::PreimageReqReceived {
                preimage,
                parent: recv.clone(),
            });
            self.schedule(t_198a, announcer, Emit::PreimageTransferred {
                length,
                parent: recv,
            });
            let t_198b = t_198a + self.rng.random_range(3..20) * MICROS_PER_MS;
            self.schedule(t_198b, peer, Emit::PreimageTransferred { length, parent: req });
        }

        // Later lifecycle: the announcer discards; sometimes peers forget.
        let t_199 = base + self.rng.random_range(5..30) * MICROS_PER_SEC;
        let reason_pick = self.rng.random::<u8>();
        self.schedule(t_199, announcer, Emit::PreimageDiscarded {
            preimage,
            length,
            reason_pick,
        });
        if self.rng.random::<f64>() < 0.1 {
            let peer = peers[self.rng.random_range(0..peers.len())];
            let t_192 = base + self.rng.random_range(5..30) * MICROS_PER_SEC;
            let reason_pick = self.rng.random::<u8>();
            self.schedule(t_192, peer, Emit::PreimageForgotten {
                service,
                preimage,
                length,
                reason_pick,
            });
        }
    }

    /// Connection churn: failed dials to dead peers, rare flaps, rare
    /// misbehavior reports.
    pub(super) fn schedule_churn(&mut self, slot: u32) {
        let v = self.act();
        let base = self.params.slots.slot_start(slot);
        let slot_ms = self.params.slots.slot_period_us / MICROS_PER_MS;
        let dials = Poisson::new(self.params.p.background.dials_per_slot).expect("mean > 0");

        for node in 0..v {
            // Background redials of long-dead peers.
            let n = dials.sample(&mut self.rng) as usize;
            for _ in 0..n.min(30) {
                let dead = self.rng.random_range(0..self.params.p.background.dead_peers);
                let req = id_slot();
                let t0 = base + self.rng.random_range(0..slot_ms) * MICROS_PER_MS;
                self.schedule(t0, node, Emit::DialOut { dead_peer: dead, self_id: req.clone() });
                if self.rng.random::<f64>() < self.params.p.background.dial_ok {
                    let t1 = t0 + self.rng.random_range(20..200) * MICROS_PER_MS;
                    self.schedule(t1, node, Emit::DialOutOk { parent: req });
                } else {
                    let t1 = t0 + self.rng.random_range(1_000..3_000) * MICROS_PER_MS;
                    self.schedule(t1, node, Emit::DialOutFailed { parent: req });
                }
            }

            // Rare protocol violations.
            if self.rng.random::<f64>() < self.params.p.background.misbehave_per_slot {
                let peer = loop {
                    let cand = self.rng.random_range(0..v);
                    if cand != node {
                        break cand;
                    }
                };
                let t = base + self.rng.random_range(0..slot_ms) * MICROS_PER_MS;
                self.schedule(t, node, Emit::Misbehaved { peer });
            }

            // Rare disconnect/reconnect flap with stream lifecycle on both ends.
            if self.rng.random::<f64>() < self.params.p.background.flap_per_slot {
                let peer = loop {
                    let cand = self.rng.random_range(0..v);
                    if cand != node {
                        break cand;
                    }
                };
                let t = base + self.rng.random_range(0..slot_ms) * MICROS_PER_MS;
                self.schedule_flap(node, peer, t);
            }
        }
    }

    /// Background block-sync: nodes explicitly pull recent blocks from peers
    /// (catch-up after hiccups — a steady hum on the reference network, with
    /// a high failure share because laggards ask laggards).
    pub(super) fn schedule_block_sync(&mut self, slot: u32) {
        let Some(head) = self.last_block_slot else { return };
        let v = self.act();
        let rate = self.params.p.background.block_req_per_slot;
        if rate <= 0.0 {
            return;
        }
        let poisson = Poisson::new(rate).expect("rate > 0");
        let base = self.params.slots.slot_start(slot);
        let slot_ms = self.params.slots.slot_period_us / MICROS_PER_MS;
        let p_fail = self.params.p.background.block_req_fail;

        for node in 0..v {
            let n = poisson.sample(&mut self.rng) as usize;
            for _ in 0..n.min(20) {
                let peer = loop {
                    let cand = self.rng.random_range(0..v);
                    if cand != node {
                        break cand;
                    }
                };
                let want = head.saturating_sub(self.rng.random_range(0..4));
                let t0 = base + self.rng.random_range(0..slot_ms) * MICROS_PER_MS;

                // Requester side.
                let req = id_slot();
                let t_66 = t0 + self.rng.random_range(2..8) * MICROS_PER_MS;
                self.schedule(t0, node, Emit::BlockReqSend {
                    peer,
                    slot: want,
                    self_id: req.clone(),
                });
                self.schedule(t_66, node, Emit::BlockReqSent { parent: req.clone() });
                if self.rng.random::<f64>() < p_fail {
                    let t_65 = t0 + self.rng.random_range(300..2_000) * MICROS_PER_MS;
                    self.schedule(t_65, node, Emit::BlockReqFailed { parent: req });
                } else {
                    let t_68 = t0 + self.rng.random_range(25..120) * MICROS_PER_MS;
                    self.schedule(t_68, node, Emit::BlockXfer {
                        slot: want,
                        parent: req,
                        last: true,
                    });
                }

                // Responder side.
                let recv = id_slot();
                let t_64 = t0 + self.rng.random_range(4..20) * MICROS_PER_MS;
                let t_67 = t_64 + self.rng.random_range(1..6) * MICROS_PER_MS;
                self.schedule(t_64, peer, Emit::BlockReqRecv {
                    sender: node,
                    self_id: recv.clone(),
                });
                self.schedule(t_67, peer, Emit::BlockReqReceived {
                    slot: want,
                    parent: recv.clone(),
                });
                if self.rng.random::<f64>() < p_fail {
                    let t_65 = t_67 + self.rng.random_range(300..2_000) * MICROS_PER_MS;
                    self.schedule(t_65, peer, Emit::BlockReqFailed { parent: recv });
                } else {
                    let t_68 = t_67 + self.rng.random_range(10..60) * MICROS_PER_MS;
                    self.schedule(t_68, peer, Emit::BlockXfer {
                        slot: want,
                        parent: recv,
                        last: true,
                    });
                }
            }
        }
    }

    /// One connection flap between a pair: streams close, the connection
    /// drops, the deterministic dialer reconnects, streams reopen.
    fn schedule_flap(&mut self, a: u16, b: u16, t0: super::time::SimTime) {
        // Close both directions' announcement streams (61 references each
        // side's own original 60), then the connection itself drops.
        for (me, other, by_local) in [(a, b, false), (b, a, true)] {
            if let Some(opened) = self.stream_ids.get(&(me, other)).cloned() {
                let t = t0 + self.rng.random_range(0..30) * MICROS_PER_MS;
                self.schedule(t, me, Emit::StreamClosed { opened, by_local });
            }
            let t_27 = t0 + self.rng.random_range(30..80) * MICROS_PER_MS;
            self.schedule(t_27, me, Emit::Disconnected { peer: other, by_local });
        }

        // Reconnect: one side dials (deterministic tiebreak on real nodes;
        // random here), the other accepts, streams reopen on both ends.
        let (dialer, acceptor) = if self.rng.random::<bool>() { (a, b) } else { (b, a) };
        let t_dial = t0 + self.rng.random_range(500..3_000) * MICROS_PER_MS;

        let out = id_slot();
        self.schedule(t_dial, dialer, Emit::DialToPeer { peer: acceptor, self_id: out.clone() });
        let t_ok = t_dial + self.rng.random_range(20..120) * MICROS_PER_MS;
        self.schedule(t_ok, dialer, Emit::DialOutOk { parent: out });

        let inc = id_slot();
        let t_in = t_dial + self.rng.random_range(5..25) * MICROS_PER_MS;
        self.schedule(t_in, acceptor, Emit::ConnIn { from: dialer, self_id: inc.clone() });
        let t_in_ok = t_in + self.rng.random_range(10..80) * MICROS_PER_MS;
        self.schedule(t_in_ok, acceptor, Emit::ConnInOk { peer: dialer, parent: inc });

        for (me, other, opener_local) in [(dialer, acceptor, true), (acceptor, dialer, false)] {
            let self_id = id_slot();
            self.stream_ids.insert((me, other), self_id.clone());
            let t = t_ok + self.rng.random_range(20..150) * MICROS_PER_MS;
            self.schedule(t, me, Emit::StreamOpened { peer: other, opener_local, self_id });
        }
    }
}
