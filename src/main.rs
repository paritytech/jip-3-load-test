//! JIP-3 Telemetry Spammer
//!
//! Stress testing tool for JAM telemetry servers.

mod events;
mod latency;
mod registry;

use clap::Parser;
use std::collections::HashMap;

use latency::LatencyConfig;
use registry::ValidatorRegistry;

/// JIP-3 Telemetry Spammer - Stress test JAM telemetry servers
#[derive(Parser, Debug)]
#[command(name = "jip-3-spammer")]
#[command(version, about, long_about = None)]
#[command(after_help = "Use --list-events to see all available events and tiers")]
struct Args {
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
struct ScheduledEvent {
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
struct PairContext<'a> {
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

/// Base delay for reconnection backoff. The cap doubles each attempt from here
/// up to --reconnect-max-backoff; the actual wait is jittered within [0, cap].
const RECONNECT_BASE_BACKOFF_MS: u64 = 100;

fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();

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
            gp_version: "0.7.1".truncated::<16>(),
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

/// Sleep for `dur` while polling the shutdown flag, so Ctrl+C aborts promptly.
/// Returns `true` if the full duration elapsed, `false` if interrupted.
async fn interruptible_sleep(
    dur: std::time::Duration,
    running: &std::sync::atomic::AtomicBool,
) -> bool {
    let deadline = std::time::Instant::now() + dur;
    loop {
        if !running.load(std::sync::atomic::Ordering::Relaxed) {
            return false;
        }
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return true;
        }
        tokio::time::sleep(remaining.min(std::time::Duration::from_millis(100))).await;
    }
}

/// Open a TCP connection and send the JIP-3 handshake (PROTOCOL_VERSION, NodeInfo).
async fn connect_and_init(
    server: &str,
    node_info: &jam_std_common::telemetry::NodeInfo,
) -> anyhow::Result<tokio::net::TcpStream> {
    let mut stream = tokio::net::TcpStream::connect(server).await?;
    let init_msg = (jam_std_common::telemetry::PROTOCOL_VERSION, node_info);
    send_message(&mut stream, &init_msg).await?;
    Ok(stream)
}

/// Reconnection backoff: exponential from `RECONNECT_BASE_BACKOFF_MS`, capped at
/// `max_backoff`, with full jitter (uniform in `[0, cap]`). `attempt` starts at
/// 1 for the first retry. Full jitter spreads a fleet's reconnects so a server
/// restart doesn't produce a synchronized thundering herd.
fn reconnect_backoff(
    attempt: u32,
    max_backoff: std::time::Duration,
    rng: &mut impl rand::Rng,
) -> std::time::Duration {
    let base = std::time::Duration::from_millis(RECONNECT_BASE_BACKOFF_MS);
    // base * 2^(attempt-1), saturating; shift capped so it can't overflow.
    let shift = attempt.saturating_sub(1).min(20);
    let cap = base.saturating_mul(1u32 << shift).min(max_backoff);
    cap.mul_f64(rng.random::<f64>())
}

/// Send a length-prefixed SCALE-encoded message.
async fn send_message<T: codec::Encode>(
    stream: &mut tokio::net::TcpStream,
    msg: &T,
) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;

    let encoded = msg.encode();
    let len = (encoded.len() as u32).to_le_bytes();

    stream.write_all(&len).await?;
    stream.write_all(&encoded).await?;
    stream.flush().await?;

    Ok(())
}

/// Generate a telemetry event by ID.
/// `event_count` is the per-event-type counter (used for sequential values like slots).
/// `pair_ctx` enables pair-wise generation with real peer IDs and cross-thread scheduling.
/// `rng` is used for sampling random recipients in pair-wise mode.
fn generate_event(
    event_id: u8,
    node_id: u64,
    sequence: u64,
    event_count: u64,
    mut pair_ctx: Option<&mut PairContext<'_>>,
    rng: &mut impl rand::Rng,
) -> anyhow::Result<jam_std_common::telemetry::TimestampedEvent> {
    use bounded_collections::BoundedVec;
    use jam_std_common::telemetry::{
        event, AnnouncedPreimageForgetReason, BlockOutline, BlockRequestDirection, ConnectionSide,
        Event, EventId, GuaranteeDiscardReason, GuaranteeOutline, IsAuthorizedCost,
        PreimageDiscardReason, ReconstructionKind, Timestamp, TimestampedEvent,
        WorkPackageOutline, WorkReportOutline,
    };
    use jam_std_common::{
        ed25519, AvailabilityBitfield, ErasureRoot, PeerAddr, PeerDetails, PeerId, TicketId,
        Truncated,
    };
    use jam_types::{
        CoreIndex, FixedVec, HeaderHash, SegmentTreeRoot, ServiceId, Slot, TicketAttempt,
        WorkPackageHash, WorkReportHash,
    };

    let timestamp = Timestamp::now();

    // Helper to generate deterministic hash from node_id and sequence
    // Use 0x80 | prefix to ensure non-ASCII first byte (displays as hex, not string)
    let make_hash = |prefix: u8| -> [u8; 32] {
        let mut hash = [0u8; 32];
        hash[0] = 0x80 | prefix;
        hash[1..9].copy_from_slice(&node_id.to_le_bytes());
        hash[9..17].copy_from_slice(&sequence.to_le_bytes());
        hash
    };

    // Helper to generate deterministic PeerId
    let make_peer_id = |seed: u64| -> PeerId {
        let mut bytes = [0u8; 32];
        bytes[0..8].copy_from_slice(&seed.to_le_bytes());
        bytes[8..16].copy_from_slice(&node_id.to_le_bytes());
        PeerId(ed25519::Public::from(bytes))
    };

    // Helper to generate deterministic PeerAddr
    let make_peer_addr = |seed: u64| -> PeerAddr {
        let mut ip = [0u8; 16];
        ip[10] = 0xff;
        ip[11] = 0xff;
        ip[12] = 10;
        ip[13] = ((seed >> 16) & 0xff) as u8;
        ip[14] = ((seed >> 8) & 0xff) as u8;
        ip[15] = (seed & 0xff) as u8;
        PeerAddr {
            ip,
            port: 30333 + (seed % 1000) as u16,
        }
    };

    // Helper to generate deterministic PeerDetails
    let _make_peer_details = |seed: u64| -> PeerDetails {
        PeerDetails {
            id: make_peer_id(seed),
            addr: make_peer_addr(seed),
        }
    };

    // Helper to generate deterministic EventId (use sequence as base)
    let make_event_id = |offset: u64| -> EventId {
        EventId(sequence.saturating_sub(offset).max(1))
    };

    // Helper to generate deterministic reason string
    let make_reason = |msg: &str| -> jam_std_common::telemetry::Reason {
        format!("{} (node={}, seq={})", msg, node_id, sequence).truncated::<128>()
    };

    // Helper for BlockOutline
    let make_block_outline = || -> BlockOutline {
        BlockOutline {
            size: 1000 + (sequence % 5000) as u32,
            hash: HeaderHash(make_hash(0x42)),
            num_tickets: (sequence % 10) as u32,
            num_preimages: (sequence % 5) as u32,
            preimages_size: (sequence % 1000) as u32,
            num_guarantees: (sequence % 8) as u32,
            num_assurances: (sequence % 20) as u32,
            num_dispute_verdicts: 0,
        }
    };

    // Helper for WorkPackageOutline
    let make_work_package_outline = || -> WorkPackageOutline {
        WorkPackageOutline {
            size: 500 + (sequence % 2000) as u32,
            hash: WorkPackageHash(make_hash(0x57)),
            anchor: HeaderHash(make_hash(0x58)),
            lookup_anchor_slot: Slot::from(sequence.saturating_sub(10) as u32),
            prerequisites: BoundedVec::new(),
            items: BoundedVec::new(),
        }
    };

    // Helper for WorkReportOutline
    let make_work_report_outline = || -> WorkReportOutline {
        WorkReportOutline {
            hash: WorkReportHash(make_hash(0x52)),
            bundle_size: 1000 + (sequence % 10000) as u32,
            erasure_root: ErasureRoot(make_hash(0x53)),
            exports_root: SegmentTreeRoot(make_hash(0x54)),
        }
    };

    // Helper for GuaranteeOutline
    let make_guarantee_outline = || -> GuaranteeOutline {
        GuaranteeOutline {
            report_hash: WorkReportHash(make_hash(0x47)),
            slot: Slot::from(sequence as u32),
            guarantors: BoundedVec::new(),
        }
    };

    // Macro-like helper: pick a real peer from registry (pair-wise) or fake (legacy)
    macro_rules! peer_or_fake {
        ($seed:expr) => {
            if let Some(ref mut ctx) = pair_ctx {
                let t = ctx.registry.pick_others(ctx.my_index, 1, rng);
                ctx.registry.peer_id(t[0])
            } else {
                make_peer_id($seed)
            }
        };
    }

    let event = match event_id {
        // ============== Tier 1: Status/chain events ==============
        10 => Event::Status(event::Status {
            num_peers: 100,
            num_val_peers: 50,
            num_sync_peers: 50,
            num_guarantees: FixedVec::new(0),
            // VERIFICATION FIELDS:
            num_shards: (node_id as u32).wrapping_mul(1_000_000).wrapping_add(sequence as u32),
            shards_size: sequence,
            num_preimages: node_id as u32,
            preimages_size: 0,
        }),

        11 => Event::BestBlockChanged(event::BestBlockChanged {
            slot: Slot::from(event_count as u32),
            hash: HeaderHash(make_hash(0x11)),
        }),

        12 => Event::FinalizedBlockChanged(event::FinalizedBlockChanged {
            slot: Slot::from(event_count as u32),
            hash: HeaderHash(make_hash(0x12)),
        }),

        13 => Event::SyncStatusChanged(event::SyncStatusChanged {
            synced: sequence % 2 == 0,
        }),

        // ============== Tier 2: Connection events ==============
        20 => Event::ConnectionRefused(event::ConnectionRefused {
            from: make_peer_addr(sequence),
        }),

        21 => Event::ConnectingIn(event::ConnectingIn {
            from: make_peer_addr(sequence),
        }),

        22 => Event::ConnectInFailed(event::ConnectInFailed {
            connecting_id: make_event_id(1),
            reason: make_reason("Connection timeout"),
        }),

        23 => Event::ConnectedIn(event::ConnectedIn {
            connecting_id: make_event_id(1),
            peer_id: peer_or_fake!(sequence),
        }),

        24 => Event::ConnectingOut(event::ConnectingOut {
            to: PeerDetails {
                id: peer_or_fake!(sequence),
                addr: make_peer_addr(sequence),
            },
        }),

        25 => Event::ConnectOutFailed(event::ConnectOutFailed {
            connecting_id: make_event_id(1),
            reason: make_reason("Connection refused"),
        }),

        26 => Event::ConnectedOut(event::ConnectedOut {
            connecting_id: make_event_id(1),
        }),

        27 => Event::Disconnected(event::Disconnected {
            peer: peer_or_fake!(sequence),
            terminator: Some(if sequence % 2 == 0 {
                ConnectionSide::Local
            } else {
                ConnectionSide::Remote
            }),
            reason: make_reason("Graceful disconnect"),
        }),

        28 => Event::PeerMisbehaved(event::PeerMisbehaved {
            peer: peer_or_fake!(sequence),
            reason: make_reason("Invalid message"),
        }),

        // ============== Tier 3: Block authoring/importing events ==============
        40 => Event::Authoring(event::Authoring {
            slot: Slot::from(sequence as u32),
            parent: HeaderHash(make_hash(0x40)),
        }),

        41 => Event::AuthoringFailed(event::AuthoringFailed {
            authoring_id: make_event_id(1),
            reason: make_reason("No tickets available"),
        }),

        42 => Event::Authored(event::Authored {
            authoring_id: make_event_id(1),
            outline: make_block_outline(),
        }),

        43 => Event::Importing(event::Importing {
            slot: Slot::from(sequence as u32),
            outline: make_block_outline(),
        }),

        44 => Event::BlockVerificationFailed(event::BlockVerificationFailed {
            importing_id: make_event_id(1),
            reason: make_reason("Invalid signature"),
        }),

        45 => Event::BlockVerified(event::BlockVerified {
            importing_id: make_event_id(1),
        }),

        46 => Event::BlockExecutionFailed(event::BlockExecutionFailed {
            authoring_or_importing_id: make_event_id(1),
            reason: make_reason("Execution error"),
        }),

        47 => Event::BlockExecuted(event::BlockExecuted {
            authoring_or_importing_id: make_event_id(1),
            accumulate_costs: BoundedVec::new(),
        }),

        // Block sync events
        60 => Event::BlockAnnouncementStreamOpened(event::BlockAnnouncementStreamOpened {
            peer: peer_or_fake!(sequence),
            opener: if sequence % 2 == 0 {
                ConnectionSide::Local
            } else {
                ConnectionSide::Remote
            },
        }),

        61 => Event::BlockAnnouncementStreamClosed(event::BlockAnnouncementStreamClosed {
            peer: peer_or_fake!(sequence),
            closer: ConnectionSide::Remote,
            reason: make_reason("Stream closed"),
        }),

        62 => Event::BlockAnnounced(event::BlockAnnounced {
            peer: peer_or_fake!(sequence),
            announcer: ConnectionSide::Remote,
            slot: Slot::from(sequence as u32),
            hash: HeaderHash(make_hash(0x62)),
        }),

        63 => {
            if let Some(ref mut ctx) = pair_ctx {
                let targets = ctx.registry.pick_others(ctx.my_index, ctx.pairs_per_node, rng);
                let my_peer = ctx.registry.peer_id(ctx.my_index);
                for &t in &targets {
                    schedule_receive(ctx, t, 64, TimestampedEvent {
                        timestamp,
                        event: Event::ReceivingBlockRequest(event::ReceivingBlockRequest {
                            sender: my_peer,
                        }),
                    });
                }
                let first = targets.first().copied().unwrap_or(0);
                Event::SendingBlockRequest(event::SendingBlockRequest {
                    recipient: ctx.registry.peer_id(first),
                    hash: HeaderHash(make_hash(0x63)),
                    direction: if sequence % 2 == 0 {
                        BlockRequestDirection::AscendingExclusive
                    } else {
                        BlockRequestDirection::DescendingInclusive
                    },
                    max_blocks: 10,
                })
            } else {
                Event::SendingBlockRequest(event::SendingBlockRequest {
                    recipient: make_peer_id(sequence),
                    hash: HeaderHash(make_hash(0x63)),
                    direction: if sequence % 2 == 0 {
                        BlockRequestDirection::AscendingExclusive
                    } else {
                        BlockRequestDirection::DescendingInclusive
                    },
                    max_blocks: 10,
                })
            }
        },

        64 => Event::ReceivingBlockRequest(event::ReceivingBlockRequest {
            sender: make_peer_id(sequence),
        }),

        65 => Event::BlockRequestFailed(event::BlockRequestFailed {
            request_id: make_event_id(1),
            reason: make_reason("Request timeout"),
        }),

        66 => Event::BlockRequestSent(event::BlockRequestSent {
            request_id: make_event_id(1),
        }),

        67 => Event::BlockRequestReceived(event::BlockRequestReceived {
            request_id: make_event_id(1),
            hash: HeaderHash(make_hash(0x67)),
            direction: BlockRequestDirection::DescendingInclusive,
            max_blocks: 10,
        }),

        68 => Event::BlockTransferred(event::BlockTransferred {
            request_id: make_event_id(2),
            slot: Slot::from(sequence as u32),
            outline: make_block_outline(),
            last: sequence % 3 == 0,
        }),

        // ============== Tier 4: Ticket events ==============
        80 => Event::GeneratingTickets(event::GeneratingTickets {
            epoch: (sequence / 600) as u32,
        }),

        81 => Event::TicketGenerationFailed(event::TicketGenerationFailed {
            generating_id: make_event_id(1),
            reason: make_reason("VRF failure"),
        }),

        82 => Event::TicketsGenerated(event::TicketsGenerated {
            generating_id: make_event_id(1),
            ids: BoundedVec::new(),
        }),

        83 => Event::TicketTransferFailed(event::TicketTransferFailed {
            peer: peer_or_fake!(sequence),
            sender: ConnectionSide::Local,
            from_proxy: sequence % 2 == 0,
            reason: make_reason("Transfer failed"),
        }),

        84 => Event::TicketTransferred(event::TicketTransferred {
            peer: peer_or_fake!(sequence),
            sender: ConnectionSide::Local,
            from_proxy: sequence % 2 == 0,
            epoch: (sequence / 600) as u32,
            attempt: TicketAttempt::from((sequence % 16) as u8),
            id: TicketId(make_hash(0x84)),
        }),

        // ============== Tier 5: Work package and guarantee events ==============
        90 => Event::WorkPackageSubmission(event::WorkPackageSubmission {
            builder: peer_or_fake!(sequence),
            bundle: sequence % 2 == 0,
        }),

        91 => Event::WorkPackageBeingShared(event::WorkPackageBeingShared {
            primary: peer_or_fake!(sequence),
        }),

        92 => Event::WorkPackageFailed(event::WorkPackageFailed {
            submission_or_share_id: make_event_id(1),
            reason: make_reason("Work package invalid"),
        }),

        93 => Event::DuplicateWorkPackage(event::DuplicateWorkPackage {
            submission_or_share_id: make_event_id(1),
            core: CoreIndex::from((sequence % 341) as u16),
            hash: WorkPackageHash(make_hash(0x93)),
        }),

        94 => Event::WorkPackageReceived(event::WorkPackageReceived {
            submission_or_share_id: make_event_id(1),
            core: CoreIndex::from((sequence % 341) as u16),
            outline: make_work_package_outline(),
        }),

        95 => Event::Authorized(event::Authorized {
            submission_or_share_id: make_event_id(1),
            cost: IsAuthorizedCost::default(),
        }),

        96 => Event::ExtrinsicDataReceived(event::ExtrinsicDataReceived {
            submission_or_share_id: make_event_id(1),
        }),

        97 => Event::ImportsReceived(event::ImportsReceived {
            submission_or_share_id: make_event_id(1),
        }),

        98 => {
            if let Some(ref mut ctx) = pair_ctx {
                let targets = ctx.registry.pick_others(ctx.my_index, ctx.pairs_per_node, rng);
                let my_peer = ctx.registry.peer_id(ctx.my_index);
                for &t in &targets {
                    schedule_receive(ctx, t, 91, TimestampedEvent {
                        timestamp,
                        event: Event::WorkPackageBeingShared(event::WorkPackageBeingShared {
                            primary: my_peer,
                        }),
                    });
                }
                let first = targets.first().copied().unwrap_or(0);
                Event::SharingWorkPackage(event::SharingWorkPackage {
                    submission_id: make_event_id(2),
                    secondary: ctx.registry.peer_id(first),
                })
            } else {
                Event::SharingWorkPackage(event::SharingWorkPackage {
                    submission_id: make_event_id(2),
                    secondary: make_peer_id(sequence + 1),
                })
            }
        },

        99 => Event::WorkPackageSharingFailed(event::WorkPackageSharingFailed {
            submission_id: make_event_id(2),
            secondary: peer_or_fake!(sequence + 1),
            reason: make_reason("Sharing failed"),
        }),

        100 => Event::BundleSent(event::BundleSent {
            submission_id: make_event_id(2),
            secondary: peer_or_fake!(sequence + 1),
        }),

        101 => Event::Refined(event::Refined {
            submission_or_share_id: make_event_id(2),
            costs: BoundedVec::new(),
        }),

        102 => Event::WorkReportBuilt(event::WorkReportBuilt {
            submission_or_share_id: make_event_id(2),
            outline: make_work_report_outline(),
        }),

        103 => Event::WorkReportSignatureSent(event::WorkReportSignatureSent {
            share_id: make_event_id(2),
        }),

        104 => Event::WorkReportSignatureReceived(event::WorkReportSignatureReceived {
            submission_id: make_event_id(2),
            secondary: peer_or_fake!(sequence + 1),
        }),

        105 => Event::GuaranteeBuilt(event::GuaranteeBuilt {
            submission_id: make_event_id(3),
            outline: make_guarantee_outline(),
        }),

        106 => {
            if let Some(ref mut ctx) = pair_ctx {
                let targets = ctx.registry.pick_others(ctx.my_index, ctx.pairs_per_node, rng);
                let my_peer = ctx.registry.peer_id(ctx.my_index);
                for &t in &targets {
                    schedule_receive(ctx, t, 110, TimestampedEvent {
                        timestamp,
                        event: Event::ReceivingGuarantee(event::ReceivingGuarantee {
                            sender: my_peer,
                        }),
                    });
                }
                let first = targets.first().copied().unwrap_or(0);
                Event::SendingGuarantee(event::SendingGuarantee {
                    built_id: make_event_id(1),
                    recipient: ctx.registry.peer_id(first),
                })
            } else {
                Event::SendingGuarantee(event::SendingGuarantee {
                    built_id: make_event_id(1),
                    recipient: make_peer_id(sequence + 1),
                })
            }
        },

        107 => Event::GuaranteeSendFailed(event::GuaranteeSendFailed {
            sending_id: make_event_id(1),
            reason: make_reason("Send failed"),
        }),

        108 => Event::GuaranteeSent(event::GuaranteeSent {
            sending_id: make_event_id(1),
        }),

        109 => Event::GuaranteesDistributed(event::GuaranteesDistributed {
            submission_id: make_event_id(5),
        }),

        110 => Event::ReceivingGuarantee(event::ReceivingGuarantee {
            sender: make_peer_id(sequence),
        }),

        111 => Event::GuaranteeReceiveFailed(event::GuaranteeReceiveFailed {
            receiving_id: make_event_id(1),
            reason: make_reason("Invalid guarantee"),
        }),

        112 => Event::GuaranteeReceived(event::GuaranteeReceived {
            receiving_id: make_event_id(1),
            outline: make_guarantee_outline(),
        }),

        113 => Event::GuaranteeDiscarded(event::GuaranteeDiscarded {
            outline: make_guarantee_outline(),
            reason: if sequence % 3 == 0 {
                GuaranteeDiscardReason::Superseded
            } else if sequence % 3 == 1 {
                GuaranteeDiscardReason::TooMany
            } else {
                GuaranteeDiscardReason::PackageReportedOnChain
            },
        }),

        // ============== Tier 6: Availability, auditing, segments, preimages ==============
        // Availability (120-131)
        120 => {
            if let Some(ref mut ctx) = pair_ctx {
                let targets = ctx.registry.pick_others(ctx.my_index, ctx.pairs_per_node, rng);
                let my_peer = ctx.registry.peer_id(ctx.my_index);
                for &t in &targets {
                    schedule_receive(ctx, t, 121, TimestampedEvent {
                        timestamp,
                        event: Event::ReceivingShardRequest(event::ReceivingShardRequest {
                            assurer: my_peer,
                        }),
                    });
                }
                let first = targets.first().copied().unwrap_or(0);
                Event::SendingShardRequest(event::SendingShardRequest {
                    guarantor: ctx.registry.peer_id(first),
                    erasure_root: ErasureRoot(make_hash(0x78)),
                    shard: (sequence % 1023) as u16,
                })
            } else {
                Event::SendingShardRequest(event::SendingShardRequest {
                    guarantor: make_peer_id(sequence),
                    erasure_root: ErasureRoot(make_hash(0x78)),
                    shard: (sequence % 1023) as u16,
                })
            }
        },

        121 => Event::ReceivingShardRequest(event::ReceivingShardRequest {
            assurer: make_peer_id(sequence),
        }),

        122 => Event::ShardRequestFailed(event::ShardRequestFailed {
            request_id: make_event_id(1),
            reason: make_reason("Shard request failed"),
        }),

        123 => Event::ShardRequestSent(event::ShardRequestSent {
            request_id: make_event_id(1),
        }),

        124 => Event::ShardRequestReceived(event::ShardRequestReceived {
            request_id: make_event_id(1),
            erasure_root: ErasureRoot(make_hash(0x7C)),
            shard: (sequence % 1023) as u16,
        }),

        125 => Event::ShardsTransferred(event::ShardsTransferred {
            request_id: make_event_id(2),
        }),

        126 => {
            use jam_std_common::AvailabilityStatement;
            Event::DistributingAssurance(event::DistributingAssurance {
                statement: AvailabilityStatement {
                    anchor: HeaderHash(make_hash(0x7E)),
                    bitfield: AvailabilityBitfield::new(),
                },
            })
        }

        127 => Event::AssuranceSendFailed(event::AssuranceSendFailed {
            distributing_id: make_event_id(1),
            recipient: peer_or_fake!(sequence + 1),
            reason: make_reason("Assurance send failed"),
        }),

        128 => {
            if let Some(ref mut ctx) = pair_ctx {
                let targets = ctx.registry.pick_others(ctx.my_index, ctx.pairs_per_node, rng);
                let my_peer = ctx.registry.peer_id(ctx.my_index);
                for &t in &targets {
                    schedule_receive(ctx, t, 131, TimestampedEvent {
                        timestamp,
                        event: Event::AssuranceReceived(event::AssuranceReceived {
                            sender: my_peer,
                            anchor: HeaderHash(make_hash(0x83)),
                        }),
                    });
                }
                let first = targets.first().copied().unwrap_or(0);
                Event::AssuranceSent(event::AssuranceSent {
                    distributing_id: make_event_id(1),
                    recipient: ctx.registry.peer_id(first),
                })
            } else {
                Event::AssuranceSent(event::AssuranceSent {
                    distributing_id: make_event_id(1),
                    recipient: make_peer_id(sequence + 1),
                })
            }
        },

        129 => Event::AssuranceDistributed(event::AssuranceDistributed {
            distributing_id: make_event_id(2),
        }),

        130 => Event::AssuranceReceiveFailed(event::AssuranceReceiveFailed {
            sender: peer_or_fake!(sequence),
            reason: make_reason("Invalid assurance"),
        }),

        131 => Event::AssuranceReceived(event::AssuranceReceived {
            sender: make_peer_id(sequence),
            anchor: HeaderHash(make_hash(0x83)),
        }),

        // Auditing (140-153)
        140 => {
            if let Some(ref mut ctx) = pair_ctx {
                let targets = ctx.registry.pick_others(ctx.my_index, ctx.pairs_per_node, rng);
                let my_peer = ctx.registry.peer_id(ctx.my_index);
                for &t in &targets {
                    schedule_receive(ctx, t, 141, TimestampedEvent {
                        timestamp,
                        event: Event::ReceivingBundleShardRequest(event::ReceivingBundleShardRequest {
                            auditor: my_peer,
                        }),
                    });
                }
                let first = targets.first().copied().unwrap_or(0);
                Event::SendingBundleShardRequest(event::SendingBundleShardRequest {
                    audit_id: make_event_id(5),
                    assurer: ctx.registry.peer_id(first),
                    shard: (sequence % 1023) as u16,
                })
            } else {
                Event::SendingBundleShardRequest(event::SendingBundleShardRequest {
                    audit_id: make_event_id(5),
                    assurer: make_peer_id(sequence),
                    shard: (sequence % 1023) as u16,
                })
            }
        },

        141 => Event::ReceivingBundleShardRequest(event::ReceivingBundleShardRequest {
            auditor: make_peer_id(sequence),
        }),

        142 => Event::BundleShardRequestFailed(event::BundleShardRequestFailed {
            request_id: make_event_id(1),
            reason: make_reason("Bundle shard request failed"),
        }),

        143 => Event::BundleShardRequestSent(event::BundleShardRequestSent {
            request_id: make_event_id(1),
        }),

        144 => Event::BundleShardRequestReceived(event::BundleShardRequestReceived {
            request_id: make_event_id(1),
            erasure_root: ErasureRoot(make_hash(0x90)),
            shard: (sequence % 1023) as u16,
        }),

        145 => Event::BundleShardTransferred(event::BundleShardTransferred {
            request_id: make_event_id(2),
        }),

        146 => Event::ReconstructingBundle(event::ReconstructingBundle {
            audit_id: make_event_id(5),
            kind: if sequence % 2 == 0 {
                ReconstructionKind::Trivial
            } else {
                ReconstructionKind::NonTrivial
            },
        }),

        147 => Event::BundleReconstructed(event::BundleReconstructed {
            audit_id: make_event_id(5),
        }),

        148 => {
            if let Some(ref mut ctx) = pair_ctx {
                let targets = ctx.registry.pick_others(ctx.my_index, ctx.pairs_per_node, rng);
                let my_peer = ctx.registry.peer_id(ctx.my_index);
                for &t in &targets {
                    schedule_receive(ctx, t, 149, TimestampedEvent {
                        timestamp,
                        event: Event::ReceivingBundleRequest(event::ReceivingBundleRequest {
                            auditor: my_peer,
                        }),
                    });
                }
                let first = targets.first().copied().unwrap_or(0);
                Event::SendingBundleRequest(event::SendingBundleRequest {
                    audit_id: make_event_id(5),
                    guarantor: ctx.registry.peer_id(first),
                })
            } else {
                Event::SendingBundleRequest(event::SendingBundleRequest {
                    audit_id: make_event_id(5),
                    guarantor: make_peer_id(sequence),
                })
            }
        },

        149 => Event::ReceivingBundleRequest(event::ReceivingBundleRequest {
            auditor: make_peer_id(sequence),
        }),

        150 => Event::BundleRequestFailed(event::BundleRequestFailed {
            request_id: make_event_id(1),
            reason: make_reason("Bundle request failed"),
        }),

        151 => Event::BundleRequestSent(event::BundleRequestSent {
            request_id: make_event_id(1),
        }),

        152 => Event::BundleRequestReceived(event::BundleRequestReceived {
            request_id: make_event_id(1),
            erasure_root: ErasureRoot(make_hash(0x98)),
        }),

        153 => Event::BundleTransferred(event::BundleTransferred {
            request_id: make_event_id(2),
        }),

        // Segments (160-178)
        160 => Event::WorkPackageHashMapped(event::WorkPackageHashMapped {
            submission_id: make_event_id(5),
            work_package_hash: WorkPackageHash(make_hash(0xA0)),
            segments_root: SegmentTreeRoot(make_hash(0xA1)),
        }),

        161 => Event::SegmentsRootMapped(event::SegmentsRootMapped {
            submission_id: make_event_id(5),
            segments_root: SegmentTreeRoot(make_hash(0xA1)),
            erasure_root: ErasureRoot(make_hash(0xA2)),
        }),

        162 => {
            if let Some(ref mut ctx) = pair_ctx {
                let targets = ctx.registry.pick_others(ctx.my_index, ctx.pairs_per_node, rng);
                let my_peer = ctx.registry.peer_id(ctx.my_index);
                for &t in &targets {
                    schedule_receive(ctx, t, 163, TimestampedEvent {
                        timestamp,
                        event: Event::ReceivingSegmentShardRequest(event::ReceivingSegmentShardRequest {
                            sender: my_peer,
                            proofs: sequence % 2 == 0,
                        }),
                    });
                }
                let first = targets.first().copied().unwrap_or(0);
                Event::SendingSegmentShardRequest(event::SendingSegmentShardRequest {
                    submission_id: make_event_id(5),
                    assurer: ctx.registry.peer_id(first),
                    proofs: sequence % 2 == 0,
                    shards: BoundedVec::new(),
                })
            } else {
                Event::SendingSegmentShardRequest(event::SendingSegmentShardRequest {
                    submission_id: make_event_id(5),
                    assurer: make_peer_id(sequence),
                    proofs: sequence % 2 == 0,
                    shards: BoundedVec::new(),
                })
            }
        },

        163 => Event::ReceivingSegmentShardRequest(event::ReceivingSegmentShardRequest {
            sender: make_peer_id(sequence),
            proofs: sequence % 2 == 0,
        }),

        164 => Event::SegmentShardRequestFailed(event::SegmentShardRequestFailed {
            request_id: make_event_id(1),
            reason: make_reason("Segment shard request failed"),
        }),

        165 => Event::SegmentShardRequestSent(event::SegmentShardRequestSent {
            request_id: make_event_id(1),
        }),

        166 => Event::SegmentShardRequestReceived(event::SegmentShardRequestReceived {
            request_id: make_event_id(1),
            num: (sequence % 100) as u16,
        }),

        167 => Event::SegmentShardsTransferred(event::SegmentShardsTransferred {
            request_id: make_event_id(2),
        }),

        168 => Event::ReconstructingSegments(event::ReconstructingSegments {
            submission_id: make_event_id(5),
            segments: BoundedVec::new(),
            kind: if sequence % 2 == 0 {
                ReconstructionKind::Trivial
            } else {
                ReconstructionKind::NonTrivial
            },
        }),

        169 => Event::SegmentReconstructionFailed(event::SegmentReconstructionFailed {
            reconstructing_id: make_event_id(1),
            reason: make_reason("Segment reconstruction failed"),
        }),

        170 => Event::SegmentsReconstructed(event::SegmentsReconstructed {
            reconstructing_id: make_event_id(1),
        }),

        171 => Event::SegmentVerificationFailed(event::SegmentVerificationFailed {
            submission_id: make_event_id(5),
            segments: BoundedVec::new(),
            reason: make_reason("Segment verification failed"),
        }),

        172 => Event::SegmentsVerified(event::SegmentsVerified {
            submission_id: make_event_id(5),
            segments: BoundedVec::new(),
        }),

        173 => {
            if let Some(ref mut ctx) = pair_ctx {
                let targets = ctx.registry.pick_others(ctx.my_index, ctx.pairs_per_node, rng);
                let my_peer = ctx.registry.peer_id(ctx.my_index);
                for &t in &targets {
                    schedule_receive(ctx, t, 174, TimestampedEvent {
                        timestamp,
                        event: Event::ReceivingSegmentRequest(event::ReceivingSegmentRequest {
                            guarantor: my_peer,
                        }),
                    });
                }
                let first = targets.first().copied().unwrap_or(0);
                Event::SendingSegmentRequest(event::SendingSegmentRequest {
                    submission_id: make_event_id(5),
                    prev_guarantor: ctx.registry.peer_id(first),
                    segments: BoundedVec::new(),
                })
            } else {
                Event::SendingSegmentRequest(event::SendingSegmentRequest {
                    submission_id: make_event_id(5),
                    prev_guarantor: make_peer_id(sequence),
                    segments: BoundedVec::new(),
                })
            }
        },

        174 => Event::ReceivingSegmentRequest(event::ReceivingSegmentRequest {
            guarantor: make_peer_id(sequence),
        }),

        175 => Event::SegmentRequestFailed(event::SegmentRequestFailed {
            request_id: make_event_id(1),
            reason: make_reason("Segment request failed"),
        }),

        176 => Event::SegmentRequestSent(event::SegmentRequestSent {
            request_id: make_event_id(1),
        }),

        177 => Event::SegmentRequestReceived(event::SegmentRequestReceived {
            request_id: make_event_id(1),
            num: (sequence % 100) as u16,
        }),

        178 => Event::SegmentsTransferred(event::SegmentsTransferred {
            request_id: make_event_id(2),
        }),

        // Preimages (190-199)
        190 => Event::PreimageAnnouncementFailed(event::PreimageAnnouncementFailed {
            peer: peer_or_fake!(sequence),
            announcer: ConnectionSide::Local,
            reason: make_reason("Announcement failed"),
        }),

        191 => Event::PreimageAnnounced(event::PreimageAnnounced {
            peer: peer_or_fake!(sequence),
            announcer: ConnectionSide::Remote,
            service: ServiceId::from(sequence as u32),
            hash: make_hash(0xBF),
            length: (1000 + sequence % 10000) as u32,
        }),

        192 => Event::AnnouncedPreimageForgotten(event::AnnouncedPreimageForgotten {
            service: ServiceId::from(sequence as u32),
            hash: make_hash(0xC0),
            length: (1000 + sequence % 10000) as u32,
            reason: if sequence % 3 == 0 {
                AnnouncedPreimageForgetReason::ProvidedOnChain
            } else if sequence % 3 == 1 {
                AnnouncedPreimageForgetReason::NotRequestedOnChain
            } else {
                AnnouncedPreimageForgetReason::TooMany
            },
        }),

        193 => {
            if let Some(ref mut ctx) = pair_ctx {
                let targets = ctx.registry.pick_others(ctx.my_index, ctx.pairs_per_node, rng);
                let my_peer = ctx.registry.peer_id(ctx.my_index);
                for &t in &targets {
                    schedule_receive(ctx, t, 194, TimestampedEvent {
                        timestamp,
                        event: Event::ReceivingPreimageRequest(event::ReceivingPreimageRequest {
                            sender: my_peer,
                        }),
                    });
                }
                let first = targets.first().copied().unwrap_or(0);
                Event::SendingPreimageRequest(event::SendingPreimageRequest {
                    recipient: ctx.registry.peer_id(first),
                    hash: make_hash(0xC1),
                })
            } else {
                Event::SendingPreimageRequest(event::SendingPreimageRequest {
                    recipient: make_peer_id(sequence),
                    hash: make_hash(0xC1),
                })
            }
        },

        194 => Event::ReceivingPreimageRequest(event::ReceivingPreimageRequest {
            sender: make_peer_id(sequence),
        }),

        195 => Event::PreimageRequestFailed(event::PreimageRequestFailed {
            request_id: make_event_id(1),
            reason: make_reason("Preimage request failed"),
        }),

        196 => Event::PreimageRequestSent(event::PreimageRequestSent {
            request_id: make_event_id(1),
        }),

        197 => Event::PreimageRequestReceived(event::PreimageRequestReceived {
            request_id: make_event_id(1),
            hash: make_hash(0xC5),
        }),

        198 => Event::PreimageTransferred(event::PreimageTransferred {
            request_id: make_event_id(2),
            length: (1000 + sequence % 10000) as u32,
        }),

        199 => Event::PreimageDiscarded(event::PreimageDiscarded {
            hash: make_hash(0xC7),
            length: (1000 + sequence % 10000) as u32,
            reason: if sequence % 3 == 0 {
                PreimageDiscardReason::ProvidedOnChain
            } else if sequence % 3 == 1 {
                PreimageDiscardReason::NotRequestedOnChain
            } else {
                PreimageDiscardReason::TooMany
            },
        }),

        _ => anyhow::bail!("Unknown event ID {}", event_id),
    };

    Ok(TimestampedEvent { timestamp, event })
}
