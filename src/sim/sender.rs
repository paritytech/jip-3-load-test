//! Per-node transport worker.
//!
//! Consumes the node's queue, paces events to the wall clock (live mode) or
//! pushes them as fast as the socket takes them (max-speed mode; an optional
//! token bucket exists for targets running tart's per-node limiter), and
//! writes the pre-encoded JIP-3 messages. Evictions from the byte-bounded
//! queue are reported ahead of surviving events, and on reconnect the gap
//! `Dropped { num = <id of the next event> }` is emitted first — both exactly
//! like polkajam's transport, so implicit-id references stay valid.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use jam_std_common::telemetry::{event, Event, NodeInfo, Timestamp, TimestampedEvent};

use crate::protocol::{connect_and_init, interruptible_sleep, reconnect_backoff, send_message};

use super::stream::NodeShared;
use super::time::SimTime;
use super::world::{Pace, PaceMode};

/// Write an already-framed message (the queue pre-encodes events, like polkajam).
async fn write_framed(stream: &mut tokio::net::TcpStream, bytes: &[u8]) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;
    stream.write_all(bytes).await?;
    stream.flush().await?;
    Ok(())
}

pub struct SenderCfg {
    pub node_idx: u16,
    pub server: String,
    pub node_info: NodeInfo,
    /// Sim time the node came up (timestamp of the reconnect gap Dropped).
    pub node_created_at: SimTime,
    pub connect_delay: std::time::Duration,
    pub reconnect_max_backoff: std::time::Duration,
    /// Per-node send cap, events/s; 0 = unlimited (a real node has no such
    /// cap — set ≤1000 only against a tart with its per-node limiter on).
    pub max_send_rate: f64,
    pub quiet: bool,
}

/// Optional token bucket in front of the socket.
struct Bucket {
    rate: f64,
    burst: f64,
    tokens: f64,
    last: std::time::Instant,
}

impl Bucket {
    fn new(rate: f64) -> Self {
        let burst = (rate / 6.0).max(1.0);
        Self { rate, burst, tokens: burst, last: std::time::Instant::now() }
    }

    fn try_take(&mut self) -> bool {
        if self.rate <= 0.0 {
            return true;
        }
        let now = std::time::Instant::now();
        self.tokens = (self.tokens + now.duration_since(self.last).as_secs_f64() * self.rate)
            .min(self.burst);
        self.last = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

pub fn spawn(
    shared: Arc<NodeShared>,
    pace: Arc<Pace>,
    running: Arc<AtomicBool>,
    world_done: Arc<AtomicBool>,
    cfg: SenderCfg,
) -> std::thread::JoinHandle<anyhow::Result<()>> {
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| anyhow::anyhow!("failed to create tokio runtime: {}", e))?;
        rt.block_on(run_sender(shared, pace, running, world_done, cfg))
    })
}

async fn run_sender(
    shared: Arc<NodeShared>,
    pace: Arc<Pace>,
    running: Arc<AtomicBool>,
    world_done: Arc<AtomicBool>,
    cfg: SenderCfg,
) -> anyhow::Result<()> {
    let mut rng = rand::rng();
    let mut bucket = Bucket::new(cfg.max_send_rate);
    let mut reconnect_attempt: u32 = 0;

    if !cfg.connect_delay.is_zero() && !interruptible_sleep(cfg.connect_delay, &running).await {
        return Ok(());
    }

    'reconnect: loop {
        if !running.load(Ordering::Relaxed) {
            break;
        }

        let mut stream = match connect_and_init(&cfg.server, &cfg.node_info).await {
            Ok(s) => {
                if !cfg.quiet {
                    log::info!("sim node {} connected to {}", cfg.node_idx, cfg.server);
                }
                reconnect_attempt = 0;
                s
            }
            Err(e) => {
                reconnect_attempt += 1;
                let backoff =
                    reconnect_backoff(reconnect_attempt, cfg.reconnect_max_backoff, &mut rng);
                log::warn!(
                    "sim node {} connect failed: {} — retrying in {:.1}s",
                    cfg.node_idx,
                    e,
                    backoff.as_secs_f64()
                );
                if !interruptible_sleep(backoff, &running).await {
                    break 'reconnect;
                }
                continue 'reconnect;
            }
        };

        // The server restarts implicit numbering at 0 per connection; if our
        // next event's id is beyond that, close the gap first.
        let mut gap_pending = true;

        loop {
            if !running.load(Ordering::Relaxed) {
                break 'reconnect;
            }

            // Transfer events whose sim time has arrived from staging into the
            // byte-bounded live queue (where polkajam's eviction semantics
            // apply), then take the head for sending. The lock is held only
            // for the transfer/pop, never across sleeps.
            enum Step {
                Send(Option<TimestampedEvent>, super::stream::QueuedEvent),
                Empty,
                NotDue(std::time::Instant),
                Throttled,
            }

            let step = {
                let now = std::time::Instant::now();
                let mut q = shared.queue.lock().expect("queue lock poisoned");
                match pace.mode {
                    PaceMode::Live { .. } => {
                        // An event enters the real node's queue the moment it
                        // happens — even if the socket is stalled (that is
                        // what makes organic evictions possible).
                        while let Some(head) = q.staging.front() {
                            let due = pace
                                .wall_for(head.event.timestamp.0)
                                .expect("live pace always yields a deadline");
                            if due <= now {
                                let e = q.staging.pop_front().expect("front checked");
                                q.live.push(e);
                            } else {
                                break;
                            }
                        }
                    }
                    PaceMode::Max => {
                        // No real time in backfill: the bounded queue is just
                        // a transfer window, never a drop point.
                        while q.live.bytes() < super::stream::MAX_QUEUE_BYTES / 4 {
                            match q.staging.pop_front() {
                                Some(e) => q.live.push(e),
                                None => break,
                            }
                        }
                    }
                }

                if q.live.head_ts().is_some() {
                    if bucket.try_take() {
                        // Evictions are reported ahead of the surviving
                        // events, like the real node's flush prologue.
                        let dropped = q.live.take_dropped();
                        Step::Send(dropped, q.live.pop().expect("head checked above"))
                    } else {
                        Step::Throttled
                    }
                } else if let Some(head) = q.staging.front() {
                    match pace.wall_for(head.event.timestamp.0) {
                        Some(due) => Step::NotDue(due),
                        None => Step::Empty, // unreachable: Max drains staging
                    }
                } else {
                    Step::Empty
                }
            };

            match step {
                Step::Empty => {
                    if world_done.load(Ordering::Relaxed) {
                        break 'reconnect; // drained and no more will come
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
                Step::NotDue(due) => {
                    let wait = due
                        .saturating_duration_since(std::time::Instant::now())
                        .min(std::time::Duration::from_millis(50));
                    tokio::time::sleep(wait.max(std::time::Duration::from_millis(1))).await;
                }
                Step::Throttled => {
                    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                }
                Step::Send(evicted, entry) => {
                    // A send failure loses this event, like the real node: the
                    // server realigns via the gap Dropped on the next connect,
                    // so implicit-id accounting stays correct.
                    if gap_pending {
                        gap_pending = false;
                        if entry.id > 0 {
                            let gap = TimestampedEvent {
                                timestamp: Timestamp(cfg.node_created_at),
                                event: Event::Dropped(event::Dropped {
                                    last_timestamp: entry.event.timestamp,
                                    num: entry.id,
                                }),
                            };
                            if let Err(e) = send_message(&mut stream, &gap).await {
                                log::warn!(
                                    "sim node {} send failed ({}), event {} lost — reconnecting",
                                    cfg.node_idx,
                                    e,
                                    entry.id
                                );
                                continue 'reconnect;
                            }
                        }
                    }
                    if let Some(dropped) = evicted {
                        if let Err(e) = send_message(&mut stream, &dropped).await {
                            log::warn!(
                                "sim node {} send failed ({}), event {} lost — reconnecting",
                                cfg.node_idx,
                                e,
                                entry.id
                            );
                            continue 'reconnect;
                        }
                    }
                    match write_framed(&mut stream, &entry.bytes).await {
                        Ok(()) => {
                            shared
                                .sent_watermark
                                .store(entry.event.timestamp.0, Ordering::Relaxed);
                            shared.sent_count.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => {
                            log::warn!(
                                "sim node {} send failed ({}), event {} lost — reconnecting",
                                cfg.node_idx,
                                e,
                                entry.id
                            );
                            continue 'reconnect;
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
