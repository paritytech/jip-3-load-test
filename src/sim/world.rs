//! Discrete-event simulation engine.
//!
//! A single world thread pops scheduled emissions in global sim-time order,
//! materializes them into concrete `TimestampedEvent`s (assigning implicit
//! EventIds per node at that moment) and pushes them into per-node bounded
//! queues consumed by the sender threads.
//!
//! Because the heap is popped in global time order and every child event is
//! scheduled strictly later than its parent on the same node, a parent is
//! always materialized — and its id known — before any child that references
//! it. Parent links are `Rc<Cell<Option<u64>>>` slots filled at
//! materialization time.

use std::cell::Cell;
use std::cmp::Ordering as CmpOrdering;
use std::collections::{BTreeMap, BinaryHeap};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bounded_collections::BoundedVec;
use jam_std_common::telemetry::{
    event, AccumulateCosts, BlockOutline, ConnectionSide, Event, EventId, GuaranteeDiscardReason,
    GuaranteeOutline, TimestampedEvent, WorkItemOutline, WorkPackageOutline, WorkReportOutline,
};
use jam_std_common::{
    ed25519, AvailabilityBitfield, AvailabilityStatement, ErasureRoot, PeerId, Truncated,
};
use jam_types::{
    CoreIndex, FixedVec, HeaderHash, SegmentTreeRoot, ServiceId, Slot, ValIndex, WorkPackageHash,
    WorkReportHash,
};
use rand::rngs::StdRng;
use rand::SeedableRng;

use super::ids::{h32, subseed};
use super::params::SimParams;
use super::stream::{NodeShared, NodeStream, QueuedEvent};
use super::time::{SimTime, MICROS_PER_SEC};
use super::values;

/// How far ahead of the pacing clock the world may generate (live mode).
const LOOKAHEAD_US: u64 = 2 * MICROS_PER_SEC;
/// How far ahead of the slowest sender the world may generate (max-speed mode).
const MAX_AHEAD_US: u64 = 30 * MICROS_PER_SEC;

/// Pacing mode shared with the sender threads.
pub enum PaceMode {
    /// Sim time advances at `speed` × wall time.
    Live { speed: f64 },
    /// As fast as the senders can push (token-bucket limited).
    Max,
}

pub struct Pace {
    pub mode: PaceMode,
    pub wall_anchor: std::time::Instant,
    pub sim_anchor: SimTime,
}

impl Pace {
    /// Current position of the pacing clock on the sim timeline.
    pub fn sim_now(&self) -> SimTime {
        match self.mode {
            PaceMode::Live { speed } => {
                let elapsed = self.wall_anchor.elapsed().as_micros() as f64 * speed;
                self.sim_anchor.saturating_add(elapsed as u64)
            }
            PaceMode::Max => SimTime::MAX,
        }
    }

    /// Wall-clock instant at which the given sim time is "due".
    /// `None` means due immediately (max-speed mode).
    pub fn wall_for(&self, t: SimTime) -> Option<std::time::Instant> {
        match self.mode {
            PaceMode::Live { speed } => {
                let ahead_us = t.saturating_sub(self.sim_anchor) as f64 / speed;
                Some(self.wall_anchor + std::time::Duration::from_micros(ahead_us as u64))
            }
            PaceMode::Max => None,
        }
    }
}

/// A slot for an implicit EventId, filled at materialization time.
pub type IdSlot = Rc<Cell<Option<u64>>>;

pub fn id_slot() -> IdSlot {
    Rc::new(Cell::new(None))
}

fn resolve(slot: &IdSlot) -> EventId {
    // A parent is always materialized before its children (strict time order),
    // so an unfilled slot is a scheduling bug; EventId(0) keeps going anyway,
    // mimicking polkajam's disabled-telemetry placeholder.
    debug_assert!(slot.get().is_some(), "parent id slot not filled");
    EventId(slot.get().unwrap_or(0))
}

/// What to emit; the concrete `Event` is built at materialization time.
pub enum Emit {
    Status,
    SyncStatus { synced: bool },
    BestBlock { slot: u32 },
    Finalized { slot: u32 },
    StreamOpened { peer: u16, opener_local: bool, self_id: IdSlot },
    Announced { peer: u16, announcer_local: bool, slot: u32 },
    /// 63 — background sync request for a recent block.
    BlockReqSend { peer: u16, slot: u32, self_id: IdSlot },
    /// 66
    BlockReqSent { parent: IdSlot },
    /// 65
    BlockReqFailed { parent: IdSlot },
    /// 68
    BlockXfer { slot: u32, parent: IdSlot, last: bool },
    /// 64
    BlockReqRecv { sender: u16, self_id: IdSlot },
    /// 67
    BlockReqReceived { slot: u32, parent: IdSlot },
    Authoring { slot: u32, self_id: IdSlot },
    Authored { slot: u32, authoring: IdSlot },
    Importing { slot: u32, self_id: IdSlot },
    Verified { importing: IdSlot },
    /// BlockExecuted; parent is the Authoring or Importing id.
    Executed { parent: IdSlot },

    // --- Work-package pipeline (primary guarantor) ---
    /// 90; the network-wide single submission event for this WP.
    WpSubmission { wp: u64, self_id: IdSlot },
    /// 94; parent is the submission (90) or share (91) on this node.
    WpReceived { wp: u64, parent: IdSlot },
    /// 96
    WpExtrinsic { parent: IdSlot },
    /// 95
    WpAuthorized { parent: IdSlot, gas: u64, ns: u64 },
    /// 97
    WpImports { parent: IdSlot },
    /// 101; one (gas, ns) per work item.
    WpRefined { parent: IdSlot, items: Vec<(u64, u64)> },
    /// 102
    WpReportBuilt { wp: u64, parent: IdSlot },
    /// 92
    WpFailed { parent: IdSlot, reason: &'static str },
    /// 98
    Sharing { parent: IdSlot, secondary: u16 },
    /// 99
    ShareFailed { parent: IdSlot, secondary: u16 },
    /// 100
    BundleSent { parent: IdSlot, secondary: u16 },
    /// 104
    SigReceived { parent: IdSlot, secondary: u16 },
    /// 105
    GuaranteeBuilt { wp: u64, parent: IdSlot, self_id: IdSlot },
    /// 106
    SendingGuarantee { built: IdSlot, recipient: u16, self_id: IdSlot },
    /// 108
    GuaranteeSent { sending: IdSlot },
    /// 107
    GuaranteeSendFailed { sending: IdSlot },
    /// 109
    GuaranteesDistributed { parent: IdSlot },

    // --- Work-package pipeline (secondary guarantors) ---
    /// 91
    WpShared { primary: u16, self_id: IdSlot },
    /// 103
    SigSent { parent: IdSlot },

    // --- Guarantee reception (other validators) ---
    /// 110
    RecvGuarantee { primary: u16, self_id: IdSlot },
    /// 112
    GuaranteeReceived { wp: u64, parent: IdSlot },
    /// 113; reason_pick selects the discard reason mix.
    GuaranteeDiscarded { wp: u64, reason_pick: u8 },

    // --- Availability: shard distribution ---
    /// 120
    ShardReqSend { wp: u64, guarantor: u16, shard: u16, self_id: IdSlot },
    /// 123
    ShardReqSent { parent: IdSlot },
    /// 122
    ShardReqFailed { parent: IdSlot },
    /// 125 (emitted on both requester and responder, different parents)
    ShardsTransferred { parent: IdSlot },
    /// 121
    ShardRecv { assurer: u16, self_id: IdSlot },
    /// 124
    ShardReqReceived { wp: u64, shard: u16, parent: IdSlot },

    // --- Availability: assurances ---
    /// 126; bits captured at scheduling time.
    Assuring { anchor_slot: u32, bits: Vec<u16>, self_id: IdSlot },
    /// 128
    AssuranceSent { parent: IdSlot, recipient: u16 },
    /// 127
    AssuranceSendFailed { parent: IdSlot, recipient: u16 },
    /// 129
    AssuranceDistributed { parent: IdSlot },
    /// 131
    AssuranceReceived { sender: u16, anchor_slot: u32 },

    // --- Segment recovery (guarantor imports; `sub` is the WP submission) ---
    /// 160
    SegHashMapped { wp: u64, sub: IdSlot },
    /// 173
    SegReqSend { guarantor: u16, sub: IdSlot, self_id: IdSlot },
    /// 176
    SegReqSent { parent: IdSlot },
    /// 175
    SegReqFailed { parent: IdSlot },
    /// 178 (both sides, different parents)
    SegTransferred { parent: IdSlot },
    /// 174
    SegRecv { guarantor: u16, self_id: IdSlot },
    /// 177
    SegReqReceived { num: u16, parent: IdSlot },
    /// 162
    SegShardReqSend { assurer: u16, proofs: bool, sub: IdSlot, self_id: IdSlot },
    /// 165
    SegShardReqSent { parent: IdSlot },
    /// 164
    SegShardReqFailed { parent: IdSlot },
    /// 167 (both sides)
    SegShardsTransferred { parent: IdSlot },
    /// 163
    SegShardRecv { sender: u16, proofs: bool, self_id: IdSlot },
    /// 166
    SegShardReqReceived { num: u16, parent: IdSlot },
    /// 168
    SegReconstructing { trivial: bool, sub: IdSlot, self_id: IdSlot },
    /// 170
    SegReconstructed { parent: IdSlot },
    /// 172
    SegVerified { sub: IdSlot },

    // --- Bundle recovery for auditing (audit_id always 0) ---
    /// 148
    BundleReqSend { guarantor: u16, self_id: IdSlot },
    /// 151
    BundleReqSent { parent: IdSlot },
    /// 150
    BundleReqFailed { parent: IdSlot },
    /// 153 (both sides)
    BundleTransferred { parent: IdSlot },
    /// 149
    BundleRecv { auditor: u16, self_id: IdSlot },
    /// 152
    BundleReqReceived { wp: u64, parent: IdSlot },
    /// 140
    BundleShardReqSend { assurer: u16, shard: u16, self_id: IdSlot },
    /// 143
    BundleShardReqSent { parent: IdSlot },
    /// 142
    BundleShardReqFailed { parent: IdSlot },
    /// 145 (both sides)
    BundleShardTransferred { parent: IdSlot },
    /// 141
    BundleShardRecv { auditor: u16, self_id: IdSlot },
    /// 144
    BundleShardReqReceived { wp: u64, shard: u16, parent: IdSlot },
    /// 146
    BundleReconstructing { trivial: bool },
    /// 147
    BundleReconstructed,

    // --- Tickets (per epoch) ---
    /// 80
    GeneratingTickets { epoch: u32, self_id: IdSlot },
    /// 82
    TicketsGenerated { parent: IdSlot, ids: Vec<u64> },
    /// 84
    TicketTransferred { peer: u16, local: bool, from_proxy: bool, epoch: u32, attempt: u8, ticket: u64 },
    /// 83
    TicketTransferFailed { peer: u16, local: bool, from_proxy: bool },

    // --- Preimages ---
    /// 191
    PreimageAnnounced { peer: u16, local: bool, service: u32, preimage: u64, length: u32 },
    /// 193
    PreimageReqSend { recipient: u16, preimage: u64, self_id: IdSlot },
    /// 196
    PreimageReqSent { parent: IdSlot },
    /// 194
    PreimageRecv { sender: u16, self_id: IdSlot },
    /// 197
    PreimageReqReceived { preimage: u64, parent: IdSlot },
    /// 198 (both sides)
    PreimageTransferred { length: u32, parent: IdSlot },
    /// 192
    PreimageForgotten { service: u32, preimage: u64, length: u32, reason_pick: u8 },
    /// 199
    PreimageDiscarded { preimage: u64, length: u32, reason_pick: u8 },
    /// 190
    PreimageAnnounceFailed { peer: u16 },

    // --- Connection churn ---
    /// 24; dials to dead peers carry synthetic details.
    DialOut { dead_peer: u64, self_id: IdSlot },
    /// 24 towards a live validator (reconnect after a flap).
    DialToPeer { peer: u16, self_id: IdSlot },
    /// 25
    DialOutFailed { parent: IdSlot },
    /// 26
    DialOutOk { parent: IdSlot },
    /// 21
    ConnIn { from: u16, self_id: IdSlot },
    /// 23
    ConnInOk { peer: u16, parent: IdSlot },
    /// 27
    Disconnected { peer: u16, by_local: bool },
    /// 61; references the stream's original 60 on this node.
    StreamClosed { opened: IdSlot, by_local: bool },
    /// 28
    Misbehaved { peer: u16 },
}

enum Action {
    SlotStart(u32),
    StatusTick,
    Emit(Emit),
}

struct Scheduled {
    at: SimTime,
    seq: u64,
    node: u16,
    action: Action,
}

impl PartialEq for Scheduled {
    fn eq(&self, other: &Self) -> bool {
        self.at == other.at && self.seq == other.seq
    }
}
impl Eq for Scheduled {}
impl PartialOrd for Scheduled {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}
impl Ord for Scheduled {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        // Reversed: BinaryHeap is a max-heap, we need earliest-first.
        // seq breaks ties deterministically in schedule order.
        other
            .at
            .cmp(&self.at)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

/// A produced block.
pub struct BlockRec {
    pub parent: [u8; 32],
    pub size: u32,
    pub num_tickets: u32,
    pub num_preimages: u32,
    pub preimages_size: u32,
    pub num_guarantees: u32,
    pub num_assurances: u32,
    /// (service, accumulate gas, accumulate ns, items) of included reports —
    /// feeds BlockExecuted.accumulate_costs.
    pub services: Vec<(u32, u64, u64, u32)>,
}

/// A work package moving through the guarantee pipeline.
pub struct WpRec {
    pub core: u16,
    pub service: u32,
    /// Report slot (the slot the WP arrived / was guaranteed in).
    pub slot: u32,
    pub size: u32,
    pub num_items: u32,
    pub payload_size: u32,
    pub extrinsic_size: u32,
    pub num_exports: u16,
    pub bundle_size: u32,
    /// Anchor block for the outline (last produced slot at arrival).
    pub anchor_slot: u32,
    /// Signed guarantor set, sorted (2 or 3 validators).
    pub guarantors: Vec<u16>,
    /// Nodes holding the guarantee in their pool (successful receivers plus
    /// the primary); these are the ones that later discard it (113).
    pub holders: Vec<u16>,
    pub builder: usize,
    pub bundle: bool,
}

pub struct World {
    pub params: SimParams,
    /// Nodes actually online (a prefix 0..active of the validator set); the
    /// rest are registered but silent, like a real network's offline share.
    pub active: usize,
    pub seed: u64,
    pub rng: StdRng,
    pub start_ts: SimTime,
    /// Exclusive end of generation, or None to run until stopped.
    pub end_ts: Option<SimTime>,

    heap: BinaryHeap<Scheduled>,
    seq: u64,
    pub streams: Vec<NodeStream>,
    pub shared: Vec<Arc<NodeShared>>,
    pub peer_ids: Vec<PeerId>,

    /// Produced blocks by slot (pruned behind finality).
    pub blocks: BTreeMap<u32, BlockRec>,
    /// Slot of the most recent produced block.
    pub last_block_slot: Option<u32>,
    /// Highest finalized slot announced so far.
    pub finalized: Option<u32>,
    /// Current finalization lag (persistent, steps occasionally).
    pub finality_lag: u32,

    /// Work packages in flight (pruned once finalized well behind).
    pub wps: BTreeMap<u64, WpRec>,
    pub wp_counter: u64,
    /// Service ids active on this network.
    pub services: Vec<u32>,
    /// Builder peers submitting work packages (not validators).
    pub builder_peers: Vec<PeerId>,
    /// FullySigned guarantees in the pool per core (feeds Status).
    pub pending_per_core: Vec<u8>,
    /// Guarantees built but not yet included in a block.
    pub guaranteed_backlog: Vec<u64>,
    /// Recently available cores per slot (feeds assurance bitfields);
    /// window of `availability_timeout` slots.
    pub avail_cores: BTreeMap<u32, Vec<u16>>,
    /// Live announcement-stream open-event ids per (node, peer), so a later
    /// StreamClosed(61) can reference its StreamOpened(60).
    pub stream_ids: std::collections::HashMap<(u16, u16), IdSlot>,
    pub ticket_counter: u64,
    pub preimage_counter: u64,

    pace: Arc<Pace>,
    running: Arc<AtomicBool>,
    pub generated: u64,
}

impl World {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        params: SimParams,
        seed: u64,
        start_ts: SimTime,
        duration_secs: u64,
        streams: Vec<NodeStream>,
        shared: Vec<Arc<NodeShared>>,
        pace: Arc<Pace>,
        running: Arc<AtomicBool>,
    ) -> Self {
        let v = params.v;
        let c = params.c;
        let initial_lag = params.p.chain.finality_lag.0;
        let peer_ids = (0..v)
            .map(|i| PeerId(ed25519::Public::from(h32(seed, "peer", i as u64, 0))))
            .collect();
        // CoreVM-style services and builder nodes, counts from the profile.
        let services: Vec<u32> = (0..params.p.wp.services.max(1) as u64)
            .map(|k| (u64::from_le_bytes(h32(seed, "service", k, 0)[0..8].try_into().expect("8")) % 100) as u32)
            .collect();
        let builder_peers = (0..params.p.wp.builders.max(1) as u64)
            .map(|i| PeerId(ed25519::Public::from(h32(seed, "builder", i, 0))))
            .collect();
        let active = params.p.network.active_nodes.unwrap_or(v).clamp(4, v);
        Self {
            params,
            active,
            seed,
            rng: StdRng::seed_from_u64(subseed(seed, "world", 0)),
            start_ts,
            end_ts: (duration_secs > 0).then(|| start_ts + duration_secs * MICROS_PER_SEC),
            heap: BinaryHeap::new(),
            seq: 0,
            streams,
            shared,
            peer_ids,
            blocks: BTreeMap::new(),
            last_block_slot: None,
            finalized: None,
            finality_lag: initial_lag,
            wps: BTreeMap::new(),
            wp_counter: 0,
            services,
            builder_peers,
            pending_per_core: vec![0; c],
            guaranteed_backlog: Vec::new(),
            avail_cores: BTreeMap::new(),
            stream_ids: std::collections::HashMap::new(),
            ticket_counter: 0,
            preimage_counter: 0,
            pace,
            running,
            generated: 0,
        }
    }

    /// Online node count as the u16 used by scheduling loops.
    pub fn act(&self) -> u16 {
        self.active as u16
    }

    pub fn block_hash(&self, slot: u32) -> [u8; 32] {
        h32(self.seed, "block", slot as u64, 0)
    }

    pub fn genesis_hash(&self) -> [u8; 32] {
        h32(self.seed, "genesis", 0, 0)
    }

    pub fn schedule(&mut self, at: SimTime, node: u16, emit: Emit) {
        self.push(at, node, Action::Emit(emit));
    }

    pub fn schedule_slot_start(&mut self, slot: u32) {
        self.push(self.params.slots.slot_start(slot), 0, Action::SlotStart(slot));
    }

    pub fn schedule_status_tick(&mut self, at: SimTime, node: u16) {
        self.push(at, node, Action::StatusTick);
    }

    fn push(&mut self, at: SimTime, node: u16, action: Action) {
        self.seq += 1;
        self.heap.push(Scheduled { at, seq: self.seq, node, action });
    }

    /// Minimum sent-watermark across all senders (max-speed backpressure).
    fn min_watermark(&self) -> SimTime {
        self.shared.iter().map(|s| s.watermark()).min().unwrap_or(0)
    }

    /// Main loop: pop → pace → dispatch.
    pub fn run(&mut self) {
        loop {
            if !self.running.load(Ordering::Relaxed) {
                break;
            }
            let Some(top_at) = self.heap.peek().map(|s| s.at) else {
                break;
            };
            if let Some(end) = self.end_ts {
                if top_at >= end {
                    break;
                }
            }

            // Pacing: don't run ahead of the clock (live) or the senders (max).
            let may_proceed = match self.pace.mode {
                PaceMode::Live { .. } => top_at <= self.pace.sim_now().saturating_add(LOOKAHEAD_US),
                PaceMode::Max => top_at <= self.min_watermark().saturating_add(MAX_AHEAD_US),
            };
            if !may_proceed {
                std::thread::sleep(std::time::Duration::from_millis(match self.pace.mode {
                    PaceMode::Live { .. } => 50,
                    PaceMode::Max => 10,
                }));
                continue;
            }

            let Scheduled { at, node, action, .. } = self.heap.pop().expect("peeked above");
            match action {
                Action::SlotStart(slot) => self.on_slot_start(slot),
                Action::StatusTick => self.on_status_tick(node, at),
                Action::Emit(emit) => self.materialize(node, at, emit),
            }
        }
    }

    /// Assign an id, build the concrete event, and hand it to the node's queue.
    pub fn materialize(&mut self, node: u16, at: SimTime, emit: Emit) {
        let event = self.build_event(node, &emit);

        // State transitions tied to the moment the event actually happens.
        if let Emit::GuaranteeBuilt { wp, .. } = &emit {
            if let Some(rec) = self.wps.get(wp) {
                let core = rec.core as usize;
                self.pending_per_core[core] = self.pending_per_core[core].saturating_add(1);
            }
            self.guaranteed_backlog.push(*wp);
        }

        let stream = &mut self.streams[node as usize];
        let id = stream.assign();
        if let Some(slot) = emit_self_slot(&emit) {
            slot.set(Some(id));
        }
        let timestamp = stream.stamp(at);
        self.generated += 1;
        self.shared[node as usize]
            .queue
            .lock()
            .expect("queue lock poisoned")
            .staging
            .push_back(QueuedEvent::new(id, TimestampedEvent { timestamp, event }));
    }

    fn build_event(&self, _node: u16, emit: &Emit) -> Event {
        match emit {
            Emit::Status => Event::Status(event::Status {
                num_peers: (self.active - 1) as u32,
                num_val_peers: (self.active - 1) as u32,
                num_sync_peers: (self.active - 1) as u32,
                // FullySigned guarantees in the pool per core, like polkajam's
                // guarantee table snapshot.
                num_guarantees: FixedVec::from_fn(|c| {
                    self.pending_per_core.get(c).copied().unwrap_or(0)
                }),
                // Real polkajam hardcodes zeros here (TODO in node/mod.rs) —
                // reproducing that is part of looking authentic.
                num_shards: 0,
                shards_size: 0,
                num_preimages: 0,
                preimages_size: 0,
            }),
            Emit::SyncStatus { synced } => {
                Event::SyncStatusChanged(event::SyncStatusChanged { synced: *synced })
            }
            Emit::BestBlock { slot } => Event::BestBlockChanged(event::BestBlockChanged {
                slot: Slot::from(*slot),
                hash: HeaderHash(self.block_hash(*slot)),
            }),
            Emit::Finalized { slot } => Event::FinalizedBlockChanged(event::FinalizedBlockChanged {
                slot: Slot::from(*slot),
                hash: HeaderHash(self.block_hash(*slot)),
            }),
            Emit::StreamOpened { peer, opener_local, self_id: _ } => {
                Event::BlockAnnouncementStreamOpened(event::BlockAnnouncementStreamOpened {
                    peer: self.peer_ids[*peer as usize],
                    opener: side(*opener_local),
                })
            }
            Emit::Announced { peer, announcer_local, slot } => {
                Event::BlockAnnounced(event::BlockAnnounced {
                    peer: self.peer_ids[*peer as usize],
                    announcer: side(*announcer_local),
                    slot: Slot::from(*slot),
                    hash: HeaderHash(self.block_hash(*slot)),
                })
            }
            Emit::BlockReqSend { peer, slot, self_id: _ } => {
                Event::SendingBlockRequest(event::SendingBlockRequest {
                    recipient: self.peer_ids[*peer as usize],
                    hash: HeaderHash(self.block_hash(*slot)),
                    direction: block_direction(*slot),
                    max_blocks: 1 + (*slot % 4),
                })
            }
            Emit::BlockReqSent { parent } => Event::BlockRequestSent(event::BlockRequestSent {
                request_id: resolve(parent),
            }),
            Emit::BlockReqFailed { parent } => {
                Event::BlockRequestFailed(event::BlockRequestFailed {
                    request_id: resolve(parent),
                    reason: "timed out waiting for blocks".truncated::<128>(),
                })
            }
            Emit::BlockXfer { slot, parent, last } => {
                Event::BlockTransferred(event::BlockTransferred {
                    request_id: resolve(parent),
                    slot: Slot::from(*slot),
                    outline: self.outline(*slot),
                    last: *last,
                })
            }
            Emit::BlockReqRecv { sender, self_id: _ } => {
                Event::ReceivingBlockRequest(event::ReceivingBlockRequest {
                    sender: self.peer_ids[*sender as usize],
                })
            }
            Emit::BlockReqReceived { slot, parent } => {
                Event::BlockRequestReceived(event::BlockRequestReceived {
                    request_id: resolve(parent),
                    hash: HeaderHash(self.block_hash(*slot)),
                    direction: block_direction(*slot),
                    max_blocks: 1 + (*slot % 4),
                })
            }
            Emit::Authoring { slot, .. } => Event::Authoring(event::Authoring {
                slot: Slot::from(*slot),
                parent: HeaderHash(
                    self.blocks
                        .get(slot)
                        .map(|b| b.parent)
                        .unwrap_or_else(|| self.genesis_hash()),
                ),
            }),
            Emit::Authored { slot, authoring } => Event::Authored(event::Authored {
                authoring_id: resolve(authoring),
                outline: self.outline(*slot),
            }),
            Emit::Importing { slot, .. } => Event::Importing(event::Importing {
                slot: Slot::from(*slot),
                outline: self.outline(*slot),
            }),
            Emit::Verified { importing } => Event::BlockVerified(event::BlockVerified {
                importing_id: resolve(importing),
            }),
            Emit::Executed { parent } => Event::BlockExecuted(event::BlockExecuted {
                authoring_or_importing_id: resolve(parent),
                accumulate_costs: self.accumulate_costs_of_head(),
            }),

            // --- WP pipeline: primary ---
            Emit::WpSubmission { wp, self_id: _ } => {
                let rec = &self.wps[wp];
                Event::WorkPackageSubmission(event::WorkPackageSubmission {
                    builder: self.builder_peers[rec.builder],
                    bundle: rec.bundle,
                })
            }
            Emit::WpReceived { wp, parent } => {
                let rec = &self.wps[wp];
                Event::WorkPackageReceived(event::WorkPackageReceived {
                    submission_or_share_id: resolve(parent),
                    core: CoreIndex::from(rec.core),
                    outline: self.wp_outline(*wp),
                })
            }
            Emit::WpExtrinsic { parent } => {
                Event::ExtrinsicDataReceived(event::ExtrinsicDataReceived {
                    submission_or_share_id: resolve(parent),
                })
            }
            Emit::WpAuthorized { parent, gas, ns } => Event::Authorized(event::Authorized {
                submission_or_share_id: resolve(parent),
                cost: values::is_authorized_cost(*gas, *ns),
            }),
            Emit::WpImports { parent } => Event::ImportsReceived(event::ImportsReceived {
                submission_or_share_id: resolve(parent),
            }),
            Emit::WpRefined { parent, items } => Event::Refined(event::Refined {
                submission_or_share_id: resolve(parent),
                costs: BoundedVec::truncate_from(
                    items.iter().map(|&(gas, ns)| values::refine_cost(gas, ns)).collect(),
                ),
            }),
            Emit::WpReportBuilt { wp, parent } => Event::WorkReportBuilt(event::WorkReportBuilt {
                submission_or_share_id: resolve(parent),
                outline: self.report_outline(*wp),
            }),
            Emit::WpFailed { parent, reason } => Event::WorkPackageFailed(event::WorkPackageFailed {
                submission_or_share_id: resolve(parent),
                reason: reason.truncated::<128>(),
            }),
            Emit::Sharing { parent, secondary } => {
                Event::SharingWorkPackage(event::SharingWorkPackage {
                    submission_id: resolve(parent),
                    secondary: self.peer_ids[*secondary as usize],
                })
            }
            Emit::ShareFailed { parent, secondary } => {
                Event::WorkPackageSharingFailed(event::WorkPackageSharingFailed {
                    submission_id: resolve(parent),
                    secondary: self.peer_ids[*secondary as usize],
                    reason: "timed out waiting for bundle stream".truncated::<128>(),
                })
            }
            Emit::BundleSent { parent, secondary } => Event::BundleSent(event::BundleSent {
                submission_id: resolve(parent),
                secondary: self.peer_ids[*secondary as usize],
            }),
            Emit::SigReceived { parent, secondary } => {
                Event::WorkReportSignatureReceived(event::WorkReportSignatureReceived {
                    submission_id: resolve(parent),
                    secondary: self.peer_ids[*secondary as usize],
                })
            }
            Emit::GuaranteeBuilt { wp, parent, self_id: _ } => {
                Event::GuaranteeBuilt(event::GuaranteeBuilt {
                    submission_id: resolve(parent),
                    outline: self.guarantee_outline(*wp),
                })
            }
            Emit::SendingGuarantee { built, recipient, self_id: _ } => {
                Event::SendingGuarantee(event::SendingGuarantee {
                    built_id: resolve(built),
                    recipient: self.peer_ids[*recipient as usize],
                })
            }
            Emit::GuaranteeSent { sending } => Event::GuaranteeSent(event::GuaranteeSent {
                sending_id: resolve(sending),
            }),
            Emit::GuaranteeSendFailed { sending } => {
                Event::GuaranteeSendFailed(event::GuaranteeSendFailed {
                    sending_id: resolve(sending),
                    reason: "timed out sending guarantee".truncated::<128>(),
                })
            }
            Emit::GuaranteesDistributed { parent } => {
                Event::GuaranteesDistributed(event::GuaranteesDistributed {
                    submission_id: resolve(parent),
                })
            }

            // --- WP pipeline: secondary ---
            Emit::WpShared { primary, self_id: _ } => {
                Event::WorkPackageBeingShared(event::WorkPackageBeingShared {
                    primary: self.peer_ids[*primary as usize],
                })
            }
            Emit::SigSent { parent } => {
                Event::WorkReportSignatureSent(event::WorkReportSignatureSent {
                    share_id: resolve(parent),
                })
            }

            // --- Guarantee reception ---
            Emit::RecvGuarantee { primary, self_id: _ } => {
                Event::ReceivingGuarantee(event::ReceivingGuarantee {
                    sender: self.peer_ids[*primary as usize],
                })
            }
            Emit::GuaranteeReceived { wp, parent } => {
                Event::GuaranteeReceived(event::GuaranteeReceived {
                    receiving_id: resolve(parent),
                    outline: self.guarantee_outline(*wp),
                })
            }
            Emit::GuaranteeDiscarded { wp, reason_pick } => {
                Event::GuaranteeDiscarded(event::GuaranteeDiscarded {
                    outline: self.guarantee_outline(*wp),
                    reason: match reason_pick % 20 {
                        0..=15 => GuaranteeDiscardReason::PackageReportedOnChain,
                        16..=18 => GuaranteeDiscardReason::Superseded,
                        _ => GuaranteeDiscardReason::TooMany,
                    },
                })
            }

            // --- Shards ---
            Emit::ShardReqSend { wp, guarantor, shard, self_id: _ } => {
                Event::SendingShardRequest(event::SendingShardRequest {
                    guarantor: self.peer_ids[*guarantor as usize],
                    erasure_root: ErasureRoot(h32(self.seed, "erasure", *wp, 0)),
                    shard: *shard,
                })
            }
            Emit::ShardReqSent { parent } => Event::ShardRequestSent(event::ShardRequestSent {
                request_id: resolve(parent),
            }),
            Emit::ShardReqFailed { parent } => Event::ShardRequestFailed(event::ShardRequestFailed {
                request_id: resolve(parent),
                reason: "connection reset by peer".truncated::<128>(),
            }),
            Emit::ShardsTransferred { parent } => {
                Event::ShardsTransferred(event::ShardsTransferred {
                    request_id: resolve(parent),
                })
            }
            Emit::ShardRecv { assurer, self_id: _ } => {
                Event::ReceivingShardRequest(event::ReceivingShardRequest {
                    assurer: self.peer_ids[*assurer as usize],
                })
            }
            Emit::ShardReqReceived { wp, shard, parent } => {
                Event::ShardRequestReceived(event::ShardRequestReceived {
                    request_id: resolve(parent),
                    erasure_root: ErasureRoot(h32(self.seed, "erasure", *wp, 0)),
                    shard: *shard,
                })
            }

            // --- Assurances ---
            Emit::Assuring { anchor_slot, bits, self_id: _ } => {
                let mut bitfield = AvailabilityBitfield::new();
                for &core in bits {
                    bitfield.set(CoreIndex::from(core));
                }
                Event::DistributingAssurance(event::DistributingAssurance {
                    statement: AvailabilityStatement {
                        anchor: HeaderHash(self.block_hash(*anchor_slot)),
                        bitfield,
                    },
                })
            }
            Emit::AssuranceSent { parent, recipient } => {
                Event::AssuranceSent(event::AssuranceSent {
                    distributing_id: resolve(parent),
                    recipient: self.peer_ids[*recipient as usize],
                })
            }
            Emit::AssuranceSendFailed { parent, recipient } => {
                Event::AssuranceSendFailed(event::AssuranceSendFailed {
                    distributing_id: resolve(parent),
                    recipient: self.peer_ids[*recipient as usize],
                    reason: "timed out sending assurance".truncated::<128>(),
                })
            }
            Emit::AssuranceDistributed { parent } => {
                Event::AssuranceDistributed(event::AssuranceDistributed {
                    distributing_id: resolve(parent),
                })
            }
            Emit::AssuranceReceived { sender, anchor_slot } => {
                Event::AssuranceReceived(event::AssuranceReceived {
                    sender: self.peer_ids[*sender as usize],
                    anchor: HeaderHash(self.block_hash(*anchor_slot)),
                })
            }

            // --- Segment recovery ---
            Emit::SegHashMapped { wp, sub } => {
                Event::WorkPackageHashMapped(event::WorkPackageHashMapped {
                    submission_id: resolve(sub),
                    work_package_hash: WorkPackageHash(h32(self.seed, "wp", *wp, 0)),
                    segments_root: SegmentTreeRoot(h32(self.seed, "exports", *wp, 0)),
                })
            }
            Emit::SegReqSend { guarantor, sub, self_id: _ } => {
                Event::SendingSegmentRequest(event::SendingSegmentRequest {
                    submission_id: resolve(sub),
                    prev_guarantor: self.peer_ids[*guarantor as usize],
                    segments: BoundedVec::new(),
                })
            }
            Emit::SegReqSent { parent } => Event::SegmentRequestSent(event::SegmentRequestSent {
                request_id: resolve(parent),
            }),
            Emit::SegReqFailed { parent } => {
                Event::SegmentRequestFailed(event::SegmentRequestFailed {
                    request_id: resolve(parent),
                    reason: "timed out waiting for segments".truncated::<128>(),
                })
            }
            Emit::SegTransferred { parent } => {
                Event::SegmentsTransferred(event::SegmentsTransferred {
                    request_id: resolve(parent),
                })
            }
            Emit::SegRecv { guarantor, self_id: _ } => {
                Event::ReceivingSegmentRequest(event::ReceivingSegmentRequest {
                    guarantor: self.peer_ids[*guarantor as usize],
                })
            }
            Emit::SegReqReceived { num, parent } => {
                Event::SegmentRequestReceived(event::SegmentRequestReceived {
                    request_id: resolve(parent),
                    num: *num,
                })
            }
            Emit::SegShardReqSend { assurer, proofs, sub, self_id: _ } => {
                Event::SendingSegmentShardRequest(event::SendingSegmentShardRequest {
                    submission_id: resolve(sub),
                    assurer: self.peer_ids[*assurer as usize],
                    proofs: *proofs,
                    shards: BoundedVec::new(),
                })
            }
            Emit::SegShardReqSent { parent } => {
                Event::SegmentShardRequestSent(event::SegmentShardRequestSent {
                    request_id: resolve(parent),
                })
            }
            Emit::SegShardReqFailed { parent } => {
                Event::SegmentShardRequestFailed(event::SegmentShardRequestFailed {
                    request_id: resolve(parent),
                    reason: "connection reset by peer".truncated::<128>(),
                })
            }
            Emit::SegShardsTransferred { parent } => {
                Event::SegmentShardsTransferred(event::SegmentShardsTransferred {
                    request_id: resolve(parent),
                })
            }
            Emit::SegShardRecv { sender, proofs, self_id: _ } => {
                Event::ReceivingSegmentShardRequest(event::ReceivingSegmentShardRequest {
                    sender: self.peer_ids[*sender as usize],
                    proofs: *proofs,
                })
            }
            Emit::SegShardReqReceived { num, parent } => {
                Event::SegmentShardRequestReceived(event::SegmentShardRequestReceived {
                    request_id: resolve(parent),
                    num: *num,
                })
            }
            Emit::SegReconstructing { trivial, sub, self_id: _ } => {
                Event::ReconstructingSegments(event::ReconstructingSegments {
                    submission_id: resolve(sub),
                    segments: BoundedVec::new(),
                    kind: reconstruction(*trivial),
                })
            }
            Emit::SegReconstructed { parent } => {
                Event::SegmentsReconstructed(event::SegmentsReconstructed {
                    reconstructing_id: resolve(parent),
                })
            }
            Emit::SegVerified { sub } => Event::SegmentsVerified(event::SegmentsVerified {
                submission_id: resolve(sub),
                segments: BoundedVec::new(),
            }),

            // --- Bundle recovery (audit_id is always EventId(0), like the
            // real node: auditing proper emits no telemetry) ---
            Emit::BundleReqSend { guarantor, self_id: _ } => {
                Event::SendingBundleRequest(event::SendingBundleRequest {
                    audit_id: EventId(0),
                    guarantor: self.peer_ids[*guarantor as usize],
                })
            }
            Emit::BundleReqSent { parent } => Event::BundleRequestSent(event::BundleRequestSent {
                request_id: resolve(parent),
            }),
            Emit::BundleReqFailed { parent } => {
                Event::BundleRequestFailed(event::BundleRequestFailed {
                    request_id: resolve(parent),
                    reason: "timed out waiting for bundle".truncated::<128>(),
                })
            }
            Emit::BundleTransferred { parent } => {
                Event::BundleTransferred(event::BundleTransferred {
                    request_id: resolve(parent),
                })
            }
            Emit::BundleRecv { auditor, self_id: _ } => {
                Event::ReceivingBundleRequest(event::ReceivingBundleRequest {
                    auditor: self.peer_ids[*auditor as usize],
                })
            }
            Emit::BundleReqReceived { wp, parent } => {
                Event::BundleRequestReceived(event::BundleRequestReceived {
                    request_id: resolve(parent),
                    erasure_root: ErasureRoot(h32(self.seed, "erasure", *wp, 0)),
                })
            }
            Emit::BundleShardReqSend { assurer, shard, self_id: _ } => {
                Event::SendingBundleShardRequest(event::SendingBundleShardRequest {
                    audit_id: EventId(0),
                    assurer: self.peer_ids[*assurer as usize],
                    shard: *shard,
                })
            }
            Emit::BundleShardReqSent { parent } => {
                Event::BundleShardRequestSent(event::BundleShardRequestSent {
                    request_id: resolve(parent),
                })
            }
            Emit::BundleShardReqFailed { parent } => {
                Event::BundleShardRequestFailed(event::BundleShardRequestFailed {
                    request_id: resolve(parent),
                    reason: "connection reset by peer".truncated::<128>(),
                })
            }
            Emit::BundleShardTransferred { parent } => {
                Event::BundleShardTransferred(event::BundleShardTransferred {
                    request_id: resolve(parent),
                })
            }
            Emit::BundleShardRecv { auditor, self_id: _ } => {
                Event::ReceivingBundleShardRequest(event::ReceivingBundleShardRequest {
                    auditor: self.peer_ids[*auditor as usize],
                })
            }
            Emit::BundleShardReqReceived { wp, shard, parent } => {
                Event::BundleShardRequestReceived(event::BundleShardRequestReceived {
                    request_id: resolve(parent),
                    erasure_root: ErasureRoot(h32(self.seed, "erasure", *wp, 0)),
                    shard: *shard,
                })
            }
            Emit::BundleReconstructing { trivial } => {
                Event::ReconstructingBundle(event::ReconstructingBundle {
                    audit_id: EventId(0),
                    kind: reconstruction(*trivial),
                })
            }
            Emit::BundleReconstructed => Event::BundleReconstructed(event::BundleReconstructed {
                audit_id: EventId(0),
            }),

            // --- Tickets ---
            Emit::GeneratingTickets { epoch, self_id: _ } => {
                Event::GeneratingTickets(event::GeneratingTickets { epoch: *epoch })
            }
            Emit::TicketsGenerated { parent, ids } => {
                Event::TicketsGenerated(event::TicketsGenerated {
                    generating_id: resolve(parent),
                    ids: BoundedVec::truncate_from(
                        ids.iter()
                            .map(|&t| jam_std_common::TicketId(h32(self.seed, "ticket", t, 0)))
                            .collect(),
                    ),
                })
            }
            Emit::TicketTransferred { peer, local, from_proxy, epoch, attempt, ticket } => {
                Event::TicketTransferred(event::TicketTransferred {
                    peer: self.peer_ids[*peer as usize],
                    sender: side(*local),
                    from_proxy: *from_proxy,
                    epoch: *epoch,
                    attempt: jam_types::TicketAttempt::from(*attempt),
                    id: jam_std_common::TicketId(h32(self.seed, "ticket", *ticket, 0)),
                })
            }
            Emit::TicketTransferFailed { peer, local, from_proxy } => {
                Event::TicketTransferFailed(event::TicketTransferFailed {
                    peer: self.peer_ids[*peer as usize],
                    sender: side(*local),
                    from_proxy: *from_proxy,
                    reason: "timed out sending ticket".truncated::<128>(),
                })
            }

            // --- Preimages ---
            Emit::PreimageAnnounced { peer, local, service, preimage, length } => {
                Event::PreimageAnnounced(event::PreimageAnnounced {
                    peer: self.peer_ids[*peer as usize],
                    announcer: side(*local),
                    service: ServiceId::from(*service),
                    hash: h32(self.seed, "preimage", *preimage, 0),
                    length: *length,
                })
            }
            Emit::PreimageReqSend { recipient, preimage, self_id: _ } => {
                Event::SendingPreimageRequest(event::SendingPreimageRequest {
                    recipient: self.peer_ids[*recipient as usize],
                    hash: h32(self.seed, "preimage", *preimage, 0),
                })
            }
            Emit::PreimageReqSent { parent } => {
                Event::PreimageRequestSent(event::PreimageRequestSent {
                    request_id: resolve(parent),
                })
            }
            Emit::PreimageRecv { sender, self_id: _ } => {
                Event::ReceivingPreimageRequest(event::ReceivingPreimageRequest {
                    sender: self.peer_ids[*sender as usize],
                })
            }
            Emit::PreimageReqReceived { preimage, parent } => {
                Event::PreimageRequestReceived(event::PreimageRequestReceived {
                    request_id: resolve(parent),
                    hash: h32(self.seed, "preimage", *preimage, 0),
                })
            }
            Emit::PreimageTransferred { length, parent } => {
                Event::PreimageTransferred(event::PreimageTransferred {
                    request_id: resolve(parent),
                    length: *length,
                })
            }
            Emit::PreimageForgotten { service, preimage, length, reason_pick } => {
                use jam_std_common::telemetry::AnnouncedPreimageForgetReason as R;
                Event::AnnouncedPreimageForgotten(event::AnnouncedPreimageForgotten {
                    service: ServiceId::from(*service),
                    hash: h32(self.seed, "preimage", *preimage, 0),
                    length: *length,
                    reason: match reason_pick % 10 {
                        0..=6 => R::ProvidedOnChain,
                        7..=8 => R::NotRequestedOnChain,
                        _ => R::TooMany,
                    },
                })
            }
            Emit::PreimageDiscarded { preimage, length, reason_pick } => {
                use jam_std_common::telemetry::PreimageDiscardReason as R;
                Event::PreimageDiscarded(event::PreimageDiscarded {
                    hash: h32(self.seed, "preimage", *preimage, 0),
                    length: *length,
                    reason: match reason_pick % 10 {
                        0..=6 => R::ProvidedOnChain,
                        7..=8 => R::NotRequestedOnChain,
                        _ => R::TooMany,
                    },
                })
            }
            Emit::PreimageAnnounceFailed { peer } => {
                Event::PreimageAnnouncementFailed(event::PreimageAnnouncementFailed {
                    peer: self.peer_ids[*peer as usize],
                    announcer: ConnectionSide::Local,
                    reason: "peer stream closed".truncated::<128>(),
                })
            }

            // --- Connection churn ---
            Emit::DialOut { dead_peer, self_id: _ } => {
                Event::ConnectingOut(event::ConnectingOut { to: self.dead_peer(*dead_peer) })
            }
            Emit::DialToPeer { peer, self_id: _ } => {
                Event::ConnectingOut(event::ConnectingOut {
                    to: jam_std_common::PeerDetails {
                        id: self.peer_ids[*peer as usize],
                        addr: self.peer_addr(*peer),
                    },
                })
            }
            Emit::DialOutFailed { parent } => Event::ConnectOutFailed(event::ConnectOutFailed {
                connecting_id: resolve(parent),
                reason: "connection refused".truncated::<128>(),
            }),
            Emit::DialOutOk { parent } => Event::ConnectedOut(event::ConnectedOut {
                connecting_id: resolve(parent),
            }),
            Emit::ConnIn { from, self_id: _ } => Event::ConnectingIn(event::ConnectingIn {
                from: self.peer_addr(*from),
            }),
            Emit::ConnInOk { peer, parent } => Event::ConnectedIn(event::ConnectedIn {
                connecting_id: resolve(parent),
                peer_id: self.peer_ids[*peer as usize],
            }),
            Emit::Disconnected { peer, by_local } => Event::Disconnected(event::Disconnected {
                peer: self.peer_ids[*peer as usize],
                terminator: Some(side(*by_local)),
                reason: "connection reset by peer".truncated::<128>(),
            }),
            Emit::StreamClosed { opened, by_local } => {
                Event::BlockAnnouncementStreamClosed(event::BlockAnnouncementStreamClosed {
                    opened_id: resolve(opened),
                    closer: side(*by_local),
                    reason: "peer disconnected".truncated::<128>(),
                })
            }
            Emit::Misbehaved { peer } => Event::PeerMisbehaved(event::PeerMisbehaved {
                peer: self.peer_ids[*peer as usize],
                reason: "unexpected message on stream".truncated::<128>(),
            }),
        }
    }

    /// Synthetic details of a long-dead peer that nodes keep redialing.
    fn dead_peer(&self, k: u64) -> jam_std_common::PeerDetails {
        let bytes = h32(self.seed, "dead", k, 0);
        let mut ip = [0u8; 16];
        ip[10] = 0xff;
        ip[11] = 0xff;
        ip[12] = 10;
        ip[13] = 20;
        ip[14] = 200 + (k % 8) as u8;
        ip[15] = bytes[2];
        jam_std_common::PeerDetails {
            id: PeerId(ed25519::Public::from(bytes)),
            addr: jam_std_common::PeerAddr { ip, port: 40_000 + (k as u16 % 1000) },
        }
    }

    fn peer_addr(&self, idx: u16) -> jam_std_common::PeerAddr {
        let mut ip = [0u8; 16];
        ip[10] = 0xff;
        ip[11] = 0xff;
        ip[12] = 10;
        ip[13] = 20;
        ip[14] = (idx >> 8) as u8;
        ip[15] = (idx & 0xff) as u8;
        jam_std_common::PeerAddr { ip, port: 40_000 + idx }
    }

    /// WorkPackageOutline shared by every node that sees this WP.
    fn wp_outline(&self, wp: u64) -> WorkPackageOutline {
        let rec = &self.wps[&wp];
        let per_item_payload = rec.payload_size / rec.num_items.max(1);
        let items = (0..rec.num_items)
            .map(|i| WorkItemOutline {
                service: ServiceId::from(rec.service),
                payload_size: per_item_payload,
                refine_gas_limit: 500_000_000 + (wp % 7) * 300_000_000,
                accumulate_gas_limit: 2_000_000 + (wp % 5) * 1_500_000,
                extrinsic_size: rec.extrinsic_size / rec.num_items.max(1),
                imports: BoundedVec::new(),
                num_exports: rec.num_exports / rec.num_items.max(1) as u16
                    + u16::from(i == 0) * (rec.num_exports % rec.num_items.max(1) as u16),
            })
            .collect::<Vec<_>>();
        WorkPackageOutline {
            size: rec.size,
            hash: WorkPackageHash(h32(self.seed, "wp", wp, 0)),
            anchor: HeaderHash(self.block_hash(rec.anchor_slot)),
            lookup_anchor_slot: Slot::from(rec.anchor_slot),
            prerequisites: BoundedVec::new(),
            items: BoundedVec::truncate_from(items),
        }
    }

    /// WorkReportOutline shared by the guarantor trio and reused by assurers.
    fn report_outline(&self, wp: u64) -> WorkReportOutline {
        let rec = &self.wps[&wp];
        WorkReportOutline {
            hash: WorkReportHash(h32(self.seed, "report", wp, 0)),
            bundle_size: rec.bundle_size,
            erasure_root: ErasureRoot(h32(self.seed, "erasure", wp, 0)),
            exports_root: SegmentTreeRoot(h32(self.seed, "exports", wp, 0)),
        }
    }

    /// GuaranteeOutline consistent between GuaranteeBuilt and GuaranteeReceived.
    fn guarantee_outline(&self, wp: u64) -> GuaranteeOutline {
        let rec = &self.wps[&wp];
        GuaranteeOutline {
            report_hash: WorkReportHash(h32(self.seed, "report", wp, 0)),
            slot: Slot::from(rec.slot),
            guarantors: BoundedVec::truncate_from(
                rec.guarantors.iter().map(|&v| ValIndex::from(v)).collect(),
            ),
        }
    }

    /// Accumulate costs of the most recent block (used by BlockExecuted).
    fn accumulate_costs_of_head(&self) -> AccumulateCosts {
        let Some(slot) = self.last_block_slot else {
            return BoundedVec::new();
        };
        let Some(rec) = self.blocks.get(&slot) else {
            return BoundedVec::new();
        };
        BoundedVec::truncate_from(
            rec.services
                .iter()
                .map(|&(svc, gas, ns, items)| {
                    (ServiceId::from(svc), values::accumulate_cost(gas, ns, items))
                })
                .collect(),
        )
    }

    fn outline(&self, slot: u32) -> BlockOutline {
        let rec = self.blocks.get(&slot);
        BlockOutline {
            size: rec.map(|b| b.size).unwrap_or(620),
            hash: HeaderHash(self.block_hash(slot)),
            num_tickets: rec.map(|b| b.num_tickets).unwrap_or(0),
            num_preimages: rec.map(|b| b.num_preimages).unwrap_or(0),
            preimages_size: rec.map(|b| b.preimages_size).unwrap_or(0),
            num_guarantees: rec.map(|b| b.num_guarantees).unwrap_or(0),
            num_assurances: rec.map(|b| b.num_assurances).unwrap_or(0),
            num_dispute_verdicts: 0,
        }
    }
}

fn block_direction(slot: u32) -> jam_std_common::telemetry::BlockRequestDirection {
    if slot % 2 == 0 {
        jam_std_common::telemetry::BlockRequestDirection::AscendingExclusive
    } else {
        jam_std_common::telemetry::BlockRequestDirection::DescendingInclusive
    }
}

fn side(local: bool) -> ConnectionSide {
    if local {
        ConnectionSide::Local
    } else {
        ConnectionSide::Remote
    }
}

fn emit_self_slot(emit: &Emit) -> Option<&IdSlot> {
    match emit {
        Emit::Authoring { self_id, .. }
        | Emit::Importing { self_id, .. }
        | Emit::StreamOpened { self_id, .. }
        | Emit::WpSubmission { self_id, .. }
        | Emit::WpShared { self_id, .. }
        | Emit::GuaranteeBuilt { self_id, .. }
        | Emit::SendingGuarantee { self_id, .. }
        | Emit::RecvGuarantee { self_id, .. }
        | Emit::ShardReqSend { self_id, .. }
        | Emit::ShardRecv { self_id, .. }
        | Emit::Assuring { self_id, .. }
        | Emit::SegReqSend { self_id, .. }
        | Emit::SegRecv { self_id, .. }
        | Emit::SegShardReqSend { self_id, .. }
        | Emit::SegShardRecv { self_id, .. }
        | Emit::SegReconstructing { self_id, .. }
        | Emit::BundleReqSend { self_id, .. }
        | Emit::BundleRecv { self_id, .. }
        | Emit::BundleShardReqSend { self_id, .. }
        | Emit::BundleShardRecv { self_id, .. }
        | Emit::GeneratingTickets { self_id, .. }
        | Emit::PreimageReqSend { self_id, .. }
        | Emit::PreimageRecv { self_id, .. }
        | Emit::DialOut { self_id, .. }
        | Emit::DialToPeer { self_id, .. }
        | Emit::ConnIn { self_id, .. }
        | Emit::BlockReqSend { self_id, .. }
        | Emit::BlockReqRecv { self_id, .. } => Some(self_id),
        _ => None,
    }
}

/// ReconstructionKind from a coin flip captured at scheduling time.
fn reconstruction(trivial: bool) -> jam_std_common::telemetry::ReconstructionKind {
    if trivial {
        jam_std_common::telemetry::ReconstructionKind::Trivial
    } else {
        jam_std_common::telemetry::ReconstructionKind::NonTrivial
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sim::params::{ParamsKind, SimParams};
    use crate::sim::stream::NodeStream;
    use codec::Encode;

    /// Run a short world to completion and return every node's event stream.
    /// Queues are large enough (24 sim-seconds of tiny traffic per node) that
    /// no Dropped merging occurs.
    fn run_world(seed: u64) -> Vec<Vec<QueuedEvent>> {
        let params = SimParams::new(ParamsKind::Tiny, crate::sim::profile::Profile::default());
        params.proto.apply().expect("apply protocol parameters");
        let v = params.v;
        let start_ts = 1_000_000_000_000; // fixed, deterministic
        let streams: Vec<NodeStream> = (0..v).map(|_| NodeStream::new(0)).collect();
        let shared: Vec<Arc<NodeShared>> =
            (0..v).map(|_| Arc::new(NodeShared::new(start_ts))).collect();
        let pace = Arc::new(Pace {
            mode: PaceMode::Max,
            wall_anchor: std::time::Instant::now(),
            sim_anchor: start_ts,
        });
        let running = Arc::new(AtomicBool::new(true));
        let mut world = World::new(
            params,
            seed,
            start_ts,
            24,
            streams,
            shared.clone(),
            pace,
            running,
        );
        world.bootstrap();
        world.run();

        shared
            .iter()
            .map(|s| {
                let mut q = s.queue.lock().expect("lock");
                // No sender is attached in this test, so everything the world
                // produced sits in staging in generation order.
                let events: Vec<QueuedEvent> = q.staging.drain(..).collect();
                assert_eq!(q.live.len(), 0, "nothing must reach the live queue without a sender");
                assert_eq!(q.live.evicted, 0, "test volume must not overflow the queue");
                events
            })
            .collect()
    }

    fn encode_streams(streams: &[Vec<QueuedEvent>]) -> Vec<Vec<u8>> {
        streams
            .iter()
            .map(|events| {
                let mut bytes = Vec::new();
                for e in events {
                    bytes.extend(e.id.to_le_bytes());
                    bytes.extend(e.event.encode());
                }
                bytes
            })
            .collect()
    }

    /// All implicit-id references carried by an event.
    fn refs_of(ev: &Event) -> Vec<u64> {
        match ev {
            Event::ConnectInFailed(e) => vec![e.connecting_id.0],
            Event::ConnectedIn(e) => vec![e.connecting_id.0],
            Event::ConnectOutFailed(e) => vec![e.connecting_id.0],
            Event::ConnectedOut(e) => vec![e.connecting_id.0],
            Event::AuthoringFailed(e) => vec![e.authoring_id.0],
            Event::Authored(e) => vec![e.authoring_id.0],
            Event::BlockVerificationFailed(e) => vec![e.importing_id.0],
            Event::BlockVerified(e) => vec![e.importing_id.0],
            Event::BlockExecutionFailed(e) => vec![e.authoring_or_importing_id.0],
            Event::BlockExecuted(e) => vec![e.authoring_or_importing_id.0],
            Event::BlockAnnouncementStreamClosed(e) => vec![e.opened_id.0],
            Event::BlockRequestFailed(e) => vec![e.request_id.0],
            Event::BlockRequestSent(e) => vec![e.request_id.0],
            Event::BlockRequestReceived(e) => vec![e.request_id.0],
            Event::BlockTransferred(e) => vec![e.request_id.0],
            Event::WorkPackageFailed(e) => vec![e.submission_or_share_id.0],
            Event::DuplicateWorkPackage(e) => vec![e.submission_or_share_id.0],
            Event::WorkPackageReceived(e) => vec![e.submission_or_share_id.0],
            Event::Authorized(e) => vec![e.submission_or_share_id.0],
            Event::ExtrinsicDataReceived(e) => vec![e.submission_or_share_id.0],
            Event::ImportsReceived(e) => vec![e.submission_or_share_id.0],
            Event::SharingWorkPackage(e) => vec![e.submission_id.0],
            Event::WorkPackageSharingFailed(e) => vec![e.submission_id.0],
            Event::BundleSent(e) => vec![e.submission_id.0],
            Event::Refined(e) => vec![e.submission_or_share_id.0],
            Event::WorkReportBuilt(e) => vec![e.submission_or_share_id.0],
            Event::WorkReportSignatureSent(e) => vec![e.share_id.0],
            Event::WorkReportSignatureReceived(e) => vec![e.submission_id.0],
            Event::GuaranteeBuilt(e) => vec![e.submission_id.0],
            Event::SendingGuarantee(e) => vec![e.built_id.0],
            Event::GuaranteeSendFailed(e) => vec![e.sending_id.0],
            Event::GuaranteeSent(e) => vec![e.sending_id.0],
            Event::GuaranteesDistributed(e) => vec![e.submission_id.0],
            Event::GuaranteeReceiveFailed(e) => vec![e.receiving_id.0],
            Event::GuaranteeReceived(e) => vec![e.receiving_id.0],
            Event::ShardRequestFailed(e) => vec![e.request_id.0],
            Event::ShardRequestSent(e) => vec![e.request_id.0],
            Event::ShardRequestReceived(e) => vec![e.request_id.0],
            Event::ShardsTransferred(e) => vec![e.request_id.0],
            Event::AssuranceSendFailed(e) => vec![e.distributing_id.0],
            Event::AssuranceSent(e) => vec![e.distributing_id.0],
            Event::AssuranceDistributed(e) => vec![e.distributing_id.0],
            Event::WorkPackageHashMapped(e) => vec![e.submission_id.0],
            Event::SendingSegmentRequest(e) => vec![e.submission_id.0],
            Event::SegmentRequestFailed(e) => vec![e.request_id.0],
            Event::SegmentRequestSent(e) => vec![e.request_id.0],
            Event::SegmentRequestReceived(e) => vec![e.request_id.0],
            Event::SegmentsTransferred(e) => vec![e.request_id.0],
            Event::SendingSegmentShardRequest(e) => vec![e.submission_id.0],
            Event::SegmentShardRequestFailed(e) => vec![e.request_id.0],
            Event::SegmentShardRequestSent(e) => vec![e.request_id.0],
            Event::SegmentShardRequestReceived(e) => vec![e.request_id.0],
            Event::SegmentShardsTransferred(e) => vec![e.request_id.0],
            Event::ReconstructingSegments(e) => vec![e.submission_id.0],
            Event::SegmentsReconstructed(e) => vec![e.reconstructing_id.0],
            Event::SegmentsVerified(e) => vec![e.submission_id.0],
            Event::BundleRequestFailed(e) => vec![e.request_id.0],
            Event::BundleRequestSent(e) => vec![e.request_id.0],
            Event::BundleRequestReceived(e) => vec![e.request_id.0],
            Event::BundleTransferred(e) => vec![e.request_id.0],
            Event::BundleShardRequestFailed(e) => vec![e.request_id.0],
            Event::BundleShardRequestSent(e) => vec![e.request_id.0],
            Event::BundleShardRequestReceived(e) => vec![e.request_id.0],
            Event::BundleShardTransferred(e) => vec![e.request_id.0],
            Event::TicketsGenerated(e) => vec![e.generating_id.0],
            Event::PreimageRequestFailed(e) => vec![e.request_id.0],
            Event::PreimageRequestSent(e) => vec![e.request_id.0],
            Event::PreimageRequestReceived(e) => vec![e.request_id.0],
            Event::PreimageTransferred(e) => vec![e.request_id.0],
            _ => vec![],
        }
    }

    #[test]
    fn same_seed_reproduces_identical_streams() {
        assert_eq!(encode_streams(&run_world(7)), encode_streams(&run_world(7)));
    }

    #[test]
    fn different_seed_changes_streams() {
        assert_ne!(encode_streams(&run_world(7)), encode_streams(&run_world(8)));
    }

    /// The measured shape of a real node's assurance round (Toaster,
    /// 2026-09-09): every AssuranceSent/Failed is stamped within the burst
    /// window of its DistributingAssurance, and the round covers every other
    /// validator of the epoch exactly once.
    #[test]
    fn assurance_round_is_an_instant_burst_to_all_validators() {
        let streams = run_world(5);
        let burst_us = crate::sim::profile::Profile::default().wp.assurance_burst_us;
        let v = streams.len();
        let mut rounds = 0;
        for events in &streams {
            let mut round_start: Option<u64> = None;
            let mut sends = 0usize;
            let mut close_round = |sends: &mut usize, rounds: &mut i32| {
                assert_eq!(*sends, v - 1, "a round must address every other validator");
                *rounds += 1;
                *sends = 0;
            };
            for qe in events {
                match &qe.event.event {
                    Event::DistributingAssurance(_) => {
                        if round_start.is_some() {
                            close_round(&mut sends, &mut rounds);
                        }
                        round_start = Some(qe.event.timestamp.0);
                    }
                    Event::AssuranceSent(_) | Event::AssuranceSendFailed(_) => {
                        let start = round_start.expect("send before DistributingAssurance");
                        let dt = qe.event.timestamp.0 - start;
                        assert!(dt <= burst_us, "AssuranceSent {} µs after Distributing (burst {})", dt, burst_us);
                        sends += 1;
                    }
                    _ => {}
                }
            }
            if round_start.is_some() {
                close_round(&mut sends, &mut rounds);
            }
        }
        assert!(rounds >= 3, "expected several complete rounds, got {}", rounds);
    }

    /// The invariant tart's enricher depends on: every reference resolves to an
    /// id already assigned earlier on the SAME node, and ids are dense.
    #[test]
    fn reference_chains_resolve_within_node() {
        let streams = run_world(11);
        let mut total_refs = 0usize;
        for (node, events) in streams.iter().enumerate() {
            let mut seen = std::collections::HashSet::new();
            for (pos, qe) in events.iter().enumerate() {
                assert_eq!(
                    qe.id, pos as u64,
                    "node {}: ids must be dense when nothing was merged",
                    node
                );
                for r in refs_of(&qe.event.event) {
                    total_refs += 1;
                    assert!(
                        seen.contains(&r),
                        "node {}: event {} references id {} which was not seen yet",
                        node,
                        qe.id,
                        r
                    );
                }
                seen.insert(qe.id);
            }
        }
        assert!(total_refs > 100, "expected a meaningful number of references, got {}", total_refs);
    }
}
