//! Per-node event stream state shared between the world thread (producer) and
//! the sender threads (consumers).
//!
//! Reproduces polkajam's transport semantics exactly
//! (`polkajam/crates/node/src/telemetry.rs`):
//!
//! - implicit EventIds: a per-node counter assigns ids at generation time; ids
//!   survive reconnects (the counter belongs to the node, not the connection);
//! - the outbound queue holds at most 200 events; on overflow the two oldest
//!   entries are merged into one `Dropped { num }` at the head (if the head
//!   already is a `Dropped`, it eats one more event and `num += 1`);
//! - after a reconnect, the server restarts implicit numbering from 0, so the
//!   sender first emits `Dropped { num = <id of the first queued event> }` to
//!   re-align the server's counter — this keeps earlier parent references
//!   valid, exactly like the real node.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use jam_std_common::telemetry::{event, Event, Timestamp, TimestampedEvent};

use super::time::SimTime;

/// polkajam's MAX_REMOTE_EVENT_QUEUE_LEN.
pub const MAX_QUEUE_LEN: usize = 200;

/// An event with its pre-assigned implicit id.
pub struct QueuedEvent {
    /// Implicit EventId assigned at generation time. For a merged `Dropped`
    /// entry this is the id of the first event it covers.
    pub id: u64,
    pub event: TimestampedEvent,
}

/// Bounded outbound queue with polkajam's overflow semantics.
#[derive(Default)]
pub struct NodeQueue {
    deque: VecDeque<QueuedEvent>,
    /// Total events merged away into `Dropped` entries (statistics).
    pub merged_away: u64,
}

impl NodeQueue {
    pub fn push(&mut self, ev: QueuedEvent) {
        if self.deque.len() >= MAX_QUEUE_LEN {
            self.merge_head();
        }
        self.deque.push_back(ev);
    }

    /// Merge the two oldest entries into one `Dropped` at the head.
    fn merge_head(&mut self) {
        let Some(first) = self.deque.pop_front() else { return };
        match first.event.event {
            Event::Dropped(ref d) => {
                // Head already collapsed: eat one more event.
                let Some(eaten) = self.deque.pop_front() else {
                    self.deque.push_front(first);
                    return;
                };
                let num = d.num + 1;
                self.merged_away += 1;
                self.deque.push_front(QueuedEvent {
                    id: first.id,
                    event: TimestampedEvent {
                        timestamp: first.event.timestamp,
                        event: Event::Dropped(event::Dropped {
                            last_timestamp: eaten.event.timestamp,
                            num,
                        }),
                    },
                });
            }
            _ => {
                let Some(second) = self.deque.pop_front() else {
                    self.deque.push_front(first);
                    return;
                };
                self.merged_away += 2;
                self.deque.push_front(QueuedEvent {
                    id: first.id,
                    event: TimestampedEvent {
                        timestamp: first.event.timestamp,
                        event: Event::Dropped(event::Dropped {
                            last_timestamp: second.event.timestamp,
                            num: 2,
                        }),
                    },
                });
            }
        }
    }

    pub fn pop(&mut self) -> Option<QueuedEvent> {
        self.deque.pop_front()
    }

    /// Timestamp of the head event, if any (for pacing without popping).
    pub fn head_ts(&self) -> Option<Timestamp> {
        self.deque.front().map(|e| e.event.timestamp)
    }

    pub fn len(&self) -> usize {
        self.deque.len()
    }
}

/// Two-stage outbound path of one node.
///
/// The world generates ahead of the pacing clock (lookahead), but polkajam's
/// bounded queue only ever sees events that have already "happened". So the
/// world pushes into an unbounded `staging` buffer, and the sender transfers
/// entries into the bounded `live` queue as their sim time arrives. The
/// Dropped-merge semantics of `live` then fire exactly when a real node's
/// would: when the socket cannot drain events that already exist.
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
        QueuedEvent {
            id,
            event: TimestampedEvent {
                timestamp: Timestamp(ts),
                event: Event::SyncStatusChanged(event::SyncStatusChanged { synced: true }),
            },
        }
    }

    fn head_dropped(q: &NodeQueue) -> (u64, u64) {
        let front = q.deque.front().expect("non-empty");
        match front.event.event {
            Event::Dropped(ref d) => (front.id, d.num),
            _ => panic!("head is not Dropped"),
        }
    }

    /// Server-side id accounting: replay the queue with polkajam's next_id rule
    /// and check that each entry's pre-assigned id matches.
    fn server_ids_consistent(q: &NodeQueue) {
        let mut expected = q.deque.front().map(|e| e.id).unwrap_or(0);
        for entry in &q.deque {
            assert_eq!(entry.id, expected, "server id drifted from assigned id");
            expected += match entry.event.event {
                Event::Dropped(ref d) => d.num,
                _ => 1,
            };
        }
    }

    #[test]
    fn overflow_merges_two_oldest_into_dropped() {
        let mut q = NodeQueue::default();
        for i in 0..MAX_QUEUE_LEN as u64 {
            q.push(ev(i, 1000 + i));
        }
        assert_eq!(q.len(), MAX_QUEUE_LEN);

        q.push(ev(200, 1200));
        assert_eq!(q.len(), MAX_QUEUE_LEN); // 200 - 2 + 1(dropped) + 1(new)
        let (id, num) = head_dropped(&q);
        assert_eq!((id, num), (0, 2));
        assert_eq!(q.merged_away, 2);
        server_ids_consistent(&q);
    }

    #[test]
    fn repeated_overflow_grows_head_dropped() {
        let mut q = NodeQueue::default();
        for i in 0..MAX_QUEUE_LEN as u64 {
            q.push(ev(i, 1000 + i));
        }
        q.push(ev(200, 1200)); // merge 0,1 -> Dropped(num=2)
        q.push(ev(201, 1201)); // head Dropped eats 2 -> num=3
        q.push(ev(202, 1202)); // eats 3 -> num=4

        let (id, num) = head_dropped(&q);
        assert_eq!((id, num), (0, 4));
        assert_eq!(q.merged_away, 4);
        // Next entry after the Dropped must be id 4.
        assert_eq!(q.deque[1].id, 4);
        server_ids_consistent(&q);
    }

    #[test]
    fn no_merge_below_capacity() {
        let mut q = NodeQueue::default();
        for i in 0..100u64 {
            q.push(ev(i, i));
        }
        assert_eq!(q.len(), 100);
        assert_eq!(q.merged_away, 0);
        server_ids_consistent(&q);
    }

    #[test]
    fn stream_assigns_sequential_ids_with_drift() {
        let mut s = NodeStream::new(-1500);
        assert_eq!(s.assign(), 0);
        assert_eq!(s.assign(), 1);
        assert_eq!(s.stamp(10_000).0, 8_500);
        // Drift never underflows below zero.
        assert_eq!(s.stamp(1_000).0, 0);
    }
}
