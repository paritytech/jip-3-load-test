//! Per-node event stream state shared between the world thread (producer) and
//! the sender threads (consumers).
//!
//! Reproduces polkajam's transport semantics exactly
//! (`polkajam/crates/node/src/telemetry.rs`, as of PR #1218, 2026-09-02):
//!
//! - implicit EventIds: a per-node counter assigns ids at generation time; ids
//!   survive reconnects (the counter belongs to the node, not the connection);
//! - events are pre-encoded (length-prefixed SCALE) into the outbound queue,
//!   which is bounded in BYTES (16 MiB). Before each push the oldest whole
//!   messages are evicted until a maximum-size event would still fit;
//! - evictions are accounted in a single pending `Dropped { num }` whose
//!   timestamp is the oldest evicted event's and `last_timestamp` the newest's.
//!   It is sent ahead of the surviving events on the next flush, so the
//!   server's implicit numbering stays aligned;
//! - after a reconnect, the server restarts implicit numbering from 0, so the
//!   sender first emits `Dropped { num = <id of the first queued event> }` to
//!   re-align the server's counter — this keeps earlier parent references
//!   valid, exactly like the real node.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use codec::{Encode, MaxEncodedLen};
use jam_std_common::telemetry::{event, Event, Timestamp, TimestampedEvent};

use super::time::SimTime;

/// polkajam's MAX_REMOTE_EVENT_QUEUE_BYTES.
pub const MAX_QUEUE_BYTES: usize = 16 * 1024 * 1024;

/// Bytes framing each queued message (little-endian u32 length).
const MSG_LEN_PREFIX_LEN: usize = std::mem::size_of::<u32>();

/// The largest a framed event can be — the room the queue keeps free.
pub fn max_msg_len() -> usize {
    MSG_LEN_PREFIX_LEN + TimestampedEvent::max_encoded_len()
}

/// An event with its pre-assigned implicit id and its wire encoding.
pub struct QueuedEvent {
    /// Implicit EventId assigned at generation time.
    pub id: u64,
    /// Kept for tests and diagnostics; the wire form is `bytes`.
    pub event: TimestampedEvent,
    /// Framed message: `u32 LE length` + SCALE payload.
    pub bytes: Vec<u8>,
}

impl QueuedEvent {
    pub fn new(id: u64, event: TimestampedEvent) -> Self {
        let payload = event.encode();
        let mut bytes = Vec::with_capacity(MSG_LEN_PREFIX_LEN + payload.len());
        bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&payload);
        Self { id, event, bytes }
    }
}

/// Byte-bounded outbound queue with polkajam's eviction semantics.
pub struct NodeQueue {
    deque: VecDeque<QueuedEvent>,
    bytes: usize,
    max_bytes: usize,
    /// Accounting for events evicted to keep the queue within its size:
    /// (timestamp of the oldest evicted, Dropped covering all of them).
    /// Sent to the server ahead of the queued events on the next flush.
    pub dropped: Option<(Timestamp, event::Dropped)>,
    /// Total events evicted (statistics).
    pub evicted: u64,
}

impl Default for NodeQueue {
    fn default() -> Self {
        Self::with_max_bytes(MAX_QUEUE_BYTES)
    }
}

impl NodeQueue {
    pub fn with_max_bytes(max_bytes: usize) -> Self {
        debug_assert!(max_bytes >= max_msg_len(), "the queue has to fit a maximum-size event");
        Self { deque: VecDeque::new(), bytes: 0, max_bytes, dropped: None, evicted: 0 }
    }

    /// Append an event, first evicting the oldest ones so that a maximum-size
    /// event would still fit (mirrors `push_remote_event`).
    pub fn push(&mut self, ev: QueuedEvent) {
        self.evict_oldest(self.max_bytes.saturating_sub(max_msg_len()));
        self.bytes += ev.bytes.len();
        self.deque.push_back(ev);
    }

    fn evict_oldest(&mut self, target: usize) {
        while self.bytes > target {
            let Some(old) = self.deque.pop_front() else { break };
            self.bytes -= old.bytes.len();
            self.evicted += 1;
            let ts = old.event.timestamp;
            match &mut self.dropped {
                Some((_, d)) => {
                    d.last_timestamp = ts;
                    d.num += 1;
                }
                None => self.dropped = Some((ts, event::Dropped { last_timestamp: ts, num: 1 })),
            }
        }
    }

    /// Take the pending eviction `Dropped` as a wire-ready event, if any.
    pub fn take_dropped(&mut self) -> Option<TimestampedEvent> {
        self.dropped
            .take()
            .map(|(timestamp, d)| TimestampedEvent { timestamp, event: Event::Dropped(d) })
    }

    pub fn pop(&mut self) -> Option<QueuedEvent> {
        let ev = self.deque.pop_front()?;
        self.bytes -= ev.bytes.len();
        Some(ev)
    }

    /// Timestamp of the head event, if any (for pacing without popping).
    pub fn head_ts(&self) -> Option<Timestamp> {
        self.deque.front().map(|e| e.event.timestamp)
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.deque.len()
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

/// Two-stage outbound path of one node.
///
/// The world generates ahead of the pacing clock (lookahead), but polkajam's
/// bounded queue only ever sees events that have already "happened". So the
/// world pushes into an unbounded `staging` buffer, and the sender transfers
/// entries into the bounded `live` queue as their sim time arrives. Eviction
/// then fires exactly when a real node's would: when the socket cannot drain
/// events that already exist.
#[derive(Default)]
pub struct NodeChannel {
    pub staging: VecDeque<QueuedEvent>,
    pub live: NodeQueue,
}

/// State shared between the world thread and one sender thread.
pub struct NodeShared {
    pub queue: Mutex<NodeChannel>,
    /// Sim timestamp of the last event handed to the socket (backfill pacing).
    pub sent_watermark: AtomicU64,
    pub sent_count: AtomicU64,
}

impl NodeShared {
    pub fn new(start_ts: SimTime) -> Self {
        Self {
            queue: Mutex::new(NodeChannel::default()),
            sent_watermark: AtomicU64::new(start_ts),
            sent_count: AtomicU64::new(0),
        }
    }

    pub fn watermark(&self) -> SimTime {
        self.sent_watermark.load(Ordering::Relaxed)
    }
}

/// World-side per-node generation state: id assignment and clock drift.
pub struct NodeStream {
    counter: u64,
    /// Current clock drift, µs (bounded random walk, updated per slot).
    pub drift_us: i64,
}

impl NodeStream {
    pub fn new(initial_drift_us: i64) -> Self {
        Self { counter: 0, drift_us: initial_drift_us }
    }

    /// Assign the next implicit EventId.
    pub fn assign(&mut self) -> u64 {
        let id = self.counter;
        self.counter += 1;
        id
    }

    /// Stamp a sim time with this node's clock drift.
    pub fn stamp(&self, t: SimTime) -> Timestamp {
        Timestamp(t.saturating_add_signed(self.drift_us))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(id: u64, ts: u64) -> QueuedEvent {
        QueuedEvent::new(
            id,
            TimestampedEvent {
                timestamp: Timestamp(ts),
                event: Event::SyncStatusChanged(event::SyncStatusChanged { synced: true }),
            },
        )
    }

    /// Replay the queue with polkajam's `next_id` rule: the pending Dropped
    /// (if any) advances the counter by `num`, every entry by 1. The first
    /// surviving id must equal the number of evicted events (all ids start at 0).
    fn server_ids_consistent(q: &NodeQueue) {
        let mut expected = q.dropped.as_ref().map(|(_, d)| d.num).unwrap_or(0);
        for entry in &q.deque {
            assert_eq!(entry.id, expected, "server id drifted from assigned id");
            expected += 1;
        }
    }

    /// polkajam evicts until `bytes > target` is false and THEN pushes, so the
    /// queue holds `target` worth of events plus the one just pushed.
    #[test]
    fn eviction_accounts_into_one_dropped() {
        let one = ev(0, 0).bytes.len();
        let max_bytes = max_msg_len() + 4 * one; // target = 4 small events
        let mut q = NodeQueue::with_max_bytes(max_bytes);
        for i in 0..7u64 {
            q.push(ev(i, 1000 + i));
        }
        assert_eq!(q.len(), 5, "target plus the event just pushed");
        assert_eq!(q.evicted, 2);
        let (ts, d) = q.dropped.as_ref().expect("evictions were accounted");
        assert_eq!(ts.0, 1000, "Dropped timestamp is the oldest evicted");
        assert_eq!(d.last_timestamp.0, 1001, "last_timestamp is the newest evicted");
        assert_eq!(d.num, 2);
        assert!(q.bytes() <= max_bytes, "never over the hard limit");
        server_ids_consistent(&q);
    }

    #[test]
    fn take_dropped_yields_wire_event_and_clears() {
        let one = ev(0, 0).bytes.len();
        let mut q = NodeQueue::with_max_bytes(max_msg_len() + one);
        q.push(ev(0, 10));
        q.push(ev(1, 11));
        q.push(ev(2, 12)); // now over target: the oldest goes
        let dropped = q.take_dropped().expect("one eviction pending");
        assert_eq!(dropped.timestamp.0, 10);
        match dropped.event {
            Event::Dropped(d) => assert_eq!((d.num, d.last_timestamp.0), (1, 10)),
            _ => panic!("not a Dropped"),
        }
        assert!(q.dropped.is_none());
        assert_eq!(q.pop().expect("survivor").id, 1);
        assert_eq!(q.pop().expect("survivor").id, 2);
    }

    #[test]
    fn no_eviction_below_capacity() {
        let mut q = NodeQueue::default();
        for i in 0..5_000u64 {
            q.push(ev(i, i));
        }
        assert_eq!(q.len(), 5_000);
        assert_eq!(q.evicted, 0);
        assert!(q.dropped.is_none());
        server_ids_consistent(&q);
    }

    #[test]
    fn framing_is_length_prefixed_scale() {
        let e = ev(7, 42);
        let len = u32::from_le_bytes(e.bytes[0..4].try_into().expect("4")) as usize;
        assert_eq!(len + 4, e.bytes.len());
        assert_eq!(&e.bytes[4..], &e.event.encode()[..]);
    }

    #[test]
    fn stream_assigns_sequential_ids_with_drift() {
        let mut s = NodeStream::new(-1500);
        assert_eq!(s.assign(), 0);
        assert_eq!(s.assign(), 1);
        assert_eq!(s.stamp(10_000).0, 8_500);
        assert_eq!(s.stamp(1_000).0, 0);
    }
}
