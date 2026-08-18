//! Legacy rate-based load generator ("spammer").
//!
//! Each worker thread simulates one node with an independent per-event-type
//! rate schedule. This mode is meant for stress testing the telemetry server;
//! it makes no attempt at cross-node coherence (see the `sim` mode for that).

pub mod events;
mod generate;
mod latency;
mod registry;

use std::collections::HashMap;

use crate::protocol::{connect_and_init, interruptible_sleep, reconnect_backoff, send_message};
use generate::generate_event;
use latency::LatencyConfig;
use registry::ValidatorRegistry;

/// Options for the `spam` subcommand.
#[derive(clap::Args, Debug)]
#[command(after_help = "Use --list-events to see all available events and tiers")]
pub struct SpamArgs {
    /// Number of worker threads (each simulates one node with its own connection)
    #[arg(long, default_value = "1")]
    threads: usize,

    /// Target telemetry server address
    #[arg(long, default_value = "127.0.0.1:9000")]
    server: String,

    /// Starting node ID for unique node generation
    #[arg(long, default_value = "0")]
    start_id: u64,

    /// Duration in seconds to run (0 = forever)
    #[arg(long, default_value = "0")]
    duration: u64,

    /// Suppress per-event logging
    #[arg(long, short)]
    quiet: bool,

    /// Set frequency for events (events/second)
    ///
    /// Format: SPEC,SPEC,... where SPEC is:
    ///   ID:RATE     - single event (e.g., 10:0.5)
    ///   tierN:RATE  - all events in tier N (e.g., tier1:0.5)
    ///   all:RATE    - all events
    ///
    /// Later specs override earlier ones.
    #[arg(long, value_name = "SPEC")]
    rate: Option<String>,

    /// Use preset rate configuration
    ///
    /// Available presets:
    ///   minimal    - Status only (tier1:0.5)
    ///   realistic  - Matches JIP-3 spec timing (default)
    ///   network    - Connection events (tier1 + tier2)
    ///   blocks     - Block events (tier1 + tier3)
    ///   guarantees - Work package pipeline (status + work-package + guaranteeing)
    ///   stress     - 10x realistic rates for load testing
    #[arg(long, default_value = "realistic")]
    preset: String,

    /// Show all tiers, events, and default rates
    #[arg(long)]
    list_events: bool,

    /// Show all presets with their event rates
    #[arg(long)]
    list_presets: bool,

    /// Enable pair-wise event generation with real validator peer IDs
    /// and cross-thread receive-side event scheduling
    #[arg(long)]
    pair_wise: bool,

    /// Number of validators in the simulated network (defaults to --threads)
    #[arg(long)]
    validators: Option<usize>,

    /// Number of random recipients per directed event firing (sampled mode)
    #[arg(long, default_value = "10")]
    pairs_per_node: usize,

    /// Default delivery latency in milliseconds for receive-side events
    #[arg(long, default_value = "6000")]
    latency_ms: u64,

    /// Ramp-up window in seconds over which node connections are opened.
    ///
    /// If omitted, a window is chosen automatically so connections open at a
    /// fixed ~200/s (~5ms apart) regardless of --threads. Pass an explicit
    /// value to override, or 0 to open all connections immediately.
    #[arg(long)]
    ramp_up: Option<f64>,

    /// Disable automatic reconnection.
    ///
    /// By default a node whose connection drops (or which cannot connect yet)
    /// retries with exponential backoff, so the load survives a server restart.
    /// With this flag a dropped/failed connection ends that node instead.
    #[arg(long)]
    no_reconnect: bool,

    /// Maximum backoff between reconnection attempts, in seconds.
    ///
    /// Reconnects use exponential backoff with full jitter (uniform in
    /// [0, current_cap]) so a server restart doesn't trigger a thundering herd.
    #[arg(long, default_value = "10.0")]
    reconnect_max_backoff: f64,
}

/// An event scheduled for delayed delivery on a specific thread.
pub(crate) struct ScheduledEvent {
    delivery_time: std::time::Instant,
    event: jam_std_common::telemetry::TimestampedEvent,
}

impl PartialEq for ScheduledEvent {
    fn eq(&self, other: &Self) -> bool {
        self.delivery_time == other.delivery_time
    }
}
impl Eq for ScheduledEvent {}

impl PartialOrd for ScheduledEvent {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScheduledEvent {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse ordering: earliest delivery_time is "greatest" for BinaryHeap min-heap
        other.delivery_time.cmp(&self.delivery_time)
    }
}

/// Context for pair-wise event generation, passed to generate_event when --pair-wise is active.
pub(crate) struct PairContext<'a> {
    registry: &'a ValidatorRegistry,
    senders: &'a [crossbeam_channel::Sender<ScheduledEvent>],
    latency: &'a LatencyConfig,
    my_index: usize,
    pairs_per_node: usize,
}

/// Schedule a receive-side event on another thread's inbox.
/// Silently drops if target_index is outside the simulated range.
fn schedule_receive(
    ctx: &PairContext<'_>,
    target_index: usize,
    recv_event_id: u8,
    event: jam_std_common::telemetry::TimestampedEvent,
) {
    if target_index >= ctx.senders.len() {
        return;
    }
    let _ = ctx.senders[target_index].send(ScheduledEvent {
        delivery_time: std::time::Instant::now() + ctx.latency.latency_for(recv_event_id),
        event,
    });
}

/// Receive-side event IDs that are generated by pair-wise scheduling.
/// When --pair-wise is active, these are suppressed from the rate-based scheduler.
const PAIR_RECEIVE_EVENTS: &[u8] = &[
    110, // ReceivingGuarantee (from 106)
    131, // AssuranceReceived (from 128)
    121, // ReceivingShardRequest (from 120)
    141, // ReceivingBundleShardRequest (from 140)
    149, // ReceivingBundleRequest (from 148)
    163, // ReceivingSegmentShardRequest (from 162)
    174, // ReceivingSegmentRequest (from 173)
    194, // ReceivingPreimageRequest (from 193)
    64,  // ReceivingBlockRequest (from 63)
    91,  // WorkPackageBeingShared (from 98)
];

/// Default connection-establishment rate (connections/second) used when
/// --ramp-up is not given.
const DEFAULT_CONNECT_RATE_PER_SEC: f64 = 200.0;

pub fn run(args: SpamArgs) -> anyhow::Result<()> {
    // Handle --list-events
    if args.list_events {
        println!("{}", events::tiers_help());
        println!("\nPresets:");
        for (name, desc) in events::PRESETS {
            println!("  {:<12} - {}", name, desc);
        }
        return Ok(());
    }

    // Handle --list-presets
    if args.list_presets {
        print!("{}", events::list_presets());
        return Ok(());
    }

    // Parse rates
    let rates = if let Some(rate_spec) = &args.rate {
        events::parse_rates(rate_spec)?
    } else {
        events::preset_rates(&args.preset)?
    };

    // Filter to only enabled events (rate > 0)
    let mut enabled_rates: HashMap<u8, f64> = rates
        .into_iter()
        .filter(|(_, rate)| *rate > 0.0)
        .collect();

    // Pair-wise infrastructure
    let pair_wise = args.pair_wise;
    let num_validators = if pair_wise {
        args.validators.unwrap_or(args.threads)
    } else {
        0
    };

    // When pair-wise, suppress receive-side events from rate scheduler
    // (they'll be scheduled by send-side events on other threads)
    if pair_wise {
        for &recv_id in PAIR_RECEIVE_EVENTS {
            enabled_rates.remove(&recv_id);
        }
    }

    if enabled_rates.is_empty() {
        anyhow::bail!("No events enabled. Use --rate or --preset to enable events.");
    }

    let registry = if pair_wise {
        Some(std::sync::Arc::new(ValidatorRegistry::new(args.start_id, num_validators)))
    } else {
        None
    };

    let latency_config = if pair_wise {
        Some(std::sync::Arc::new(LatencyConfig::new(args.latency_ms)))
    } else {
        None
    };

    // Create crossbeam channels: one per thread
    let (all_senders, mut all_receivers) = if pair_wise {
        let mut senders = Vec::with_capacity(args.threads);
        let mut receivers: Vec<Option<crossbeam_channel::Receiver<ScheduledEvent>>> =
            Vec::with_capacity(args.threads);
        for _ in 0..args.threads {
            let (tx, rx) = crossbeam_channel::unbounded::<ScheduledEvent>();
            senders.push(tx);
            receivers.push(Some(rx));
        }
        (Some(senders), receivers)
    } else {
        (None, Vec::new())
    };

    let pairs_per_node = args.pairs_per_node;

    if let Some(v) = args.ramp_up {
        if v < 0.0 {
            anyhow::bail!("--ramp-up must be non-negative");
        }
    }
    let ramp_up_secs = args
        .ramp_up
        .unwrap_or(args.threads as f64 / DEFAULT_CONNECT_RATE_PER_SEC);
    let ramp_up_interval = if ramp_up_secs > 0.0 && args.threads > 0 {
        std::time::Duration::from_secs_f64(ramp_up_secs / args.threads as f64)
    } else {
        std::time::Duration::ZERO
    };

    log::info!(
        "Starting {} thread(s) connecting to {}",
        args.threads,
        args.server
    );
    log::info!("Node IDs: {} to {}", args.start_id, args.start_id + args.threads as u64 - 1);
    log::info!("Enabled events: {:?}", enabled_rates.keys().collect::<Vec<_>>());

    if pair_wise {
        log::info!(
            "Pair-wise mode: {} validators, {} pairs/node, {}ms latency",
            num_validators, pairs_per_node, args.latency_ms
        );
    }

    if !ramp_up_interval.is_zero() {
        log::info!(
            "Ramp-up: {} connection(s) over {:.2}s (~{:.0} conn/s, {:.0}ms apart){}",
            args.threads,
            ramp_up_secs,
            args.threads as f64 / ramp_up_secs,
            ramp_up_interval.as_secs_f64() * 1000.0,
            if args.ramp_up.is_none() { " [auto]" } else { "" }
        );
    } else {
        log::info!("Ramp-up: disabled");
    }

    if args.no_reconnect {
        log::info!("Reconnect: disabled (a dropped connection ends the node)");
    } else {
        if args.reconnect_max_backoff <= 0.0 || !args.reconnect_max_backoff.is_finite() {
            anyhow::bail!("--reconnect-max-backoff must be a positive, finite number of seconds");
        }
        log::info!(
            "Reconnect: enabled (exponential backoff up to {:.1}s, jittered)",
            args.reconnect_max_backoff
        );
    }
    let reconnect_max_backoff = std::time::Duration::from_secs_f64(args.reconnect_max_backoff);

    if args.duration > 0 {
        log::info!("Duration: {} seconds", args.duration);
    } else {
        log::info!("Duration: unlimited (Ctrl+C to stop)");
    }

    // Set up Ctrl+C handler BEFORE spawning threads
    let running = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let r = running.clone();
    ctrlc::set_handler(move || {
        log::info!("Received Ctrl+C, shutting down...");
        r.store(false, std::sync::atomic::Ordering::SeqCst);
    })?;

    // Spawn worker threads
    let mut handles: Vec<(u64, _)> = Vec::with_capacity(args.threads);

    #[allow(clippy::needless_range_loop)]
    for thread_id in 0..args.threads {
        let node_id = args.start_id + thread_id as u64;
        let connect_delay = ramp_up_interval.mul_f64(thread_id as f64);
        let server = args.server.clone();
        let rates = enabled_rates.clone();
        let duration = args.duration;
        let quiet = args.quiet;
        let no_reconnect = args.no_reconnect;
        let running = running.clone();
        let registry = registry.clone();
        let latency_config = latency_config.clone();
        let senders = all_senders.clone();
        let receiver = if pair_wise {
            all_receivers[thread_id].take()
        } else {
            None
        };

        let handle = std::thread::spawn(move || -> anyhow::Result<()> {
            // Each thread gets its own single-threaded tokio runtime
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| anyhow::anyhow!("Failed to create tokio runtime: {}", e))?;

            rt.block_on(async move {
                run_worker(
                    node_id,
                    &server,
                    rates,
                    duration,
                    quiet,
                    &running,
                    registry,
                    senders,
                    receiver,
                    latency_config,
                    pairs_per_node,
                    connect_delay,
                    no_reconnect,
                    reconnect_max_backoff,
                )
                .await
            })
        });

        handles.push((node_id, handle));
    }

    // Wait for all threads
    for (node_id, handle) in handles {
        match handle.join() {
            Ok(Ok(())) => log::debug!("Thread for Node {} finished", node_id),
            Ok(Err(e)) => log::error!("Thread for Node {} failed: {}", node_id, e),
            Err(_) => log::error!("Thread for Node {} panicked", node_id),
        }
    }

    log::info!("All threads finished");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_worker(
    node_id: u64,
    server: &str,
    rates: HashMap<u8, f64>,
    duration: u64,
    quiet: bool,
    running: &std::sync::atomic::AtomicBool,
    // Pair-wise parameters (None when --pair-wise is not used):
    registry: Option<std::sync::Arc<ValidatorRegistry>>,
    senders: Option<Vec<crossbeam_channel::Sender<ScheduledEvent>>>,
    receiver: Option<crossbeam_channel::Receiver<ScheduledEvent>>,
    latency_config: Option<std::sync::Arc<LatencyConfig>>,
    pairs_per_node: usize,
    // Ramp-up delay: how long this worker waits before opening its connection.
    connect_delay: std::time::Duration,
    no_reconnect: bool,
    reconnect_max_backoff: std::time::Duration,
) -> anyhow::Result<()> {
    use jam_std_common::telemetry::NodeInfo;
    use jam_std_common::{ed25519, PeerAddr, PeerDetails, PeerId, Truncated};
    use jam_types::{HeaderHash, ProtocolParameters};

    // Generate deterministic fake IP: 10.{byte2}.{byte1}.{byte0}
    let fake_ip = format!(
        "10.{}.{}.{}",
        (node_id >> 16) & 0xff,
        (node_id >> 8) & 0xff,
        node_id & 0xff
    );
    let fake_port = 30333 + (node_id % 1000) as u16;

    // Build NodeInfo once; it is deterministic and reused across reconnects.
    let node_info = {
        // Deterministic peer ID from node_id
        let mut peer_id_bytes = [0u8; 32];
        peer_id_bytes[0..8].copy_from_slice(&node_id.to_le_bytes());
        let peer_id = PeerId(ed25519::Public::from(peer_id_bytes));

        // IPv4-mapped IPv6: ::ffff:10.x.y.z
        let mut ip = [0u8; 16];
        ip[10] = 0xff;
        ip[11] = 0xff;
        ip[12] = 10;
        ip[13] = ((node_id >> 16) & 0xff) as u8;
        ip[14] = ((node_id >> 8) & 0xff) as u8;
        ip[15] = (node_id & 0xff) as u8;

        let details = PeerDetails {
            id: peer_id,
            addr: PeerAddr { ip, port: fake_port },
        };

        // Use tiny params for testing
        let params = ProtocolParameters::tiny();
        // Apply params so FixedVec<_, CoreCount> encodes with correct size
        params.apply().expect("Failed to apply protocol parameters");

        NodeInfo {
            params,
            genesis: HeaderHash([0u8; 32]),
            details,
            flags: 0,
            impl_name: format!("Spammer-{}", node_id).truncated::<32>(),
            impl_version: "0.1.0".truncated::<32>(),
            gp_version: jam_types::GP_VERSION.truncated::<16>(),
            note: format!("Stress test node {}", node_id).truncated::<512>(),
        }
    };

    // Ramp-up: wait our staggered slot before the FIRST connection so the
    // server's listen() backlog isn't hit by every node at once. Interruptible
    // so Ctrl+C aborts a long ramp-up promptly.
    if !connect_delay.is_zero() && !interruptible_sleep(connect_delay, running).await {
        log::info!("Node {} aborted during ramp-up wait", node_id);
        return Ok(());
    }

    // Event-generation state that persists across reconnects. `sequence` and the
    // per-event-type counters keep advancing, so the server-side monotonic
    // checks stay meaningful (a reconnect shows up as a gap, not a reset).
    let start = std::time::Instant::now();
    let mut sequence: u64 = 0;
    let mut event_counts: HashMap<u8, u64> = HashMap::new();
    // Pair-wise: delay queue for events received from other threads
    let mut delay_queue: std::collections::BinaryHeap<ScheduledEvent> =
        std::collections::BinaryHeap::new();
    let my_index = registry.as_ref().and_then(|r| r.index_of_node(node_id));

    use rand::Rng;
    let mut rng = rand::rng();
    let mut reconnect_attempt: u32 = 0;

    // Reconnect loop: (re)establish the connection, run the event loop until it
    // drops, then back off and try again (unless --no-reconnect). This keeps the
    // simulated load alive across Tart restarts.
    'reconnect: loop {
        if !running.load(std::sync::atomic::Ordering::Relaxed) {
            break 'reconnect;
        }
        if duration > 0 && start.elapsed().as_secs() >= duration {
            break 'reconnect;
        }

        log::info!(
            "Node {} ({}:{}) connecting to {}{}",
            node_id,
            fake_ip,
            fake_port,
            server,
            if reconnect_attempt > 0 {
                format!(" (attempt {})", reconnect_attempt + 1)
            } else {
                String::new()
            }
        );

        let mut stream = match connect_and_init(server, &node_info).await {
            Ok(stream) => {
                if reconnect_attempt > 0 {
                    log::info!(
                        "Node {} reconnected after {} failed attempt(s)",
                        node_id, reconnect_attempt
                    );
                }
                reconnect_attempt = 0;
                stream
            }
            Err(e) => {
                if no_reconnect {
                    return Err(e);
                }
                reconnect_attempt += 1;
                let backoff = reconnect_backoff(reconnect_attempt, reconnect_max_backoff, &mut rng);
                log::warn!(
                    "Node {} connect failed: {} — retrying in {:.1}s",
                    node_id,
                    e,
                    backoff.as_secs_f64()
                );
                if !interruptible_sleep(backoff, running).await {
                    break 'reconnect;
                }
                continue 'reconnect;
            }
        };

        // Rebuild the fire schedule from now and drop any stale inbound events,
        // so downtime doesn't unleash a catch-up burst on reconnect. A random
        // sub-interval offset keeps nodes from firing in lockstep.
        let now = std::time::Instant::now();
        let mut next_fire: HashMap<u8, std::time::Instant> = rates
            .iter()
            .map(|(&id, &rate)| {
                let interval = std::time::Duration::from_secs_f64(1.0 / rate);
                let offset = std::time::Duration::from_secs_f64(
                    rng.random::<f64>() * interval.as_secs_f64(),
                );
                (id, now + offset)
            })
            .collect();
        delay_queue.clear();

        // Event loop for this connection. Breaks with Some(err) when a send
        // fails (connection dropped) so we reconnect; None on a clean stop
        // (duration elapsed / Ctrl+C) so the worker exits.
        let outcome: Option<anyhow::Error> = 'evloop: loop {
            // Check duration
            if duration > 0 && start.elapsed().as_secs() >= duration {
                break 'evloop None;
            }

            // Check Ctrl+C
            if !running.load(std::sync::atomic::Ordering::Relaxed) {
                break 'evloop None;
            }

            let now = std::time::Instant::now();

            // Step 1: Drain inbox from crossbeam channel into delay queue
            if let Some(ref rx) = receiver {
                while let Ok(evt) = rx.try_recv() {
                    delay_queue.push(evt);
                }
            }

            // Step 2: Emit ready events from delay queue
            while delay_queue.peek().is_some_and(|e| e.delivery_time <= now) {
                if let Some(scheduled) = delay_queue.pop() {
                    if let Err(e) = send_message(&mut stream, &scheduled.event).await {
                        break 'evloop Some(e);
                    }
                }
            }

            // Find next event to fire
            let (&next_id, &fire_time) = next_fire
                .iter()
                .min_by_key(|(_, time)| *time)
                .expect("rates is non-empty");

            // Compute sleep target: min of next rate-fire and next delayed delivery
            let sleep_target = if let Some(top) = delay_queue.peek() {
                fire_time.min(top.delivery_time)
            } else {
                fire_time
            };

            // Sleep if needed, capped at 50ms so inbox is polled at ~20Hz
            if sleep_target > now {
                let max_sleep = std::time::Duration::from_millis(50);
                let actual_sleep = (sleep_target - now).min(max_sleep);
                tokio::time::sleep(actual_sleep).await;
                continue; // re-check inbox after sleeping
            }

            // Only fire rate-based event if its fire_time has arrived
            if fire_time > now {
                continue;
            }

            // Generate and send event
            let event_count = *event_counts.get(&next_id).unwrap_or(&0);

            // Build PairContext if pair-wise is active
            let event = if let (Some(ref reg), Some(ref sndrs), Some(ref lat), Some(idx)) =
                (&registry, &senders, &latency_config, my_index)
            {
                let mut ctx = PairContext {
                    registry: reg,
                    senders: sndrs,
                    latency: lat,
                    my_index: idx,
                    pairs_per_node,
                };
                generate_event(next_id, node_id, sequence, event_count, Some(&mut ctx), &mut rng)?
            } else {
                generate_event(next_id, node_id, sequence, event_count, None, &mut rng)?
            };

            if let Err(e) = send_message(&mut stream, &event).await {
                break 'evloop Some(e);
            }

            if !quiet {
                log::debug!(
                    "Node {} sent event {} (seq {}, count {})",
                    node_id, next_id, sequence, event_count
                );
            }

            sequence += 1;
            *event_counts.entry(next_id).or_insert(0) += 1;

            // Schedule next fire for this event
            let rate = rates[&next_id];
            let interval = std::time::Duration::from_secs_f64(1.0 / rate);
            next_fire.insert(next_id, std::time::Instant::now() + interval);
        };

        match outcome {
            None => break 'reconnect, // clean stop: duration elapsed or Ctrl+C
            Some(e) => {
                if no_reconnect {
                    return Err(e);
                }
                reconnect_attempt += 1;
                let backoff = reconnect_backoff(reconnect_attempt, reconnect_max_backoff, &mut rng);
                log::warn!(
                    "Node {} connection lost after {} events: {} — reconnecting in {:.1}s",
                    node_id,
                    sequence,
                    e,
                    backoff.as_secs_f64()
                );
                if !interruptible_sleep(backoff, running).await {
                    break 'reconnect;
                }
            }
        }
    }

    log::info!(
        "Node {} finished: {} events in {:?}",
        node_id,
        sequence,
        start.elapsed()
    );

    Ok(())
}
