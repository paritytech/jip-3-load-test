//! Protocol-level network simulator producing dashboard-realistic JIP-3
//! traffic. See docs/DESIGN.md for the architecture and
//! docs/research/polkajam-emission-model.md for the behavioral ground truth.

mod background;
mod chain;
mod ids;
pub mod params;
pub mod profile;
mod recovery;
mod roles;
mod sender;
mod stream;
mod time;
mod values;
mod world;
mod wp;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use rand::Rng;
use rand::SeedableRng;

use params::{ParamsKind, SimParams};
use stream::{NodeShared, NodeStream};
use world::{Pace, PaceMode, World};

/// Options for the `sim` subcommand.
#[derive(clap::Args, Debug)]
pub struct SimArgs {
    /// Target telemetry server address
    #[arg(long, default_value = "127.0.0.1:9000")]
    server: String,

    /// Protocol parameter set; node count equals its validator count
    #[arg(long, value_enum, default_value_t = ParamsKind::Mini)]
    params: ParamsKind,

    /// World seed: the same seed reproduces the identical network
    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// Duration of generated traffic in sim seconds (0 = until Ctrl+C)
    #[arg(long, default_value_t = 0)]
    duration: u64,

    /// Timeline start: "now" or an RFC3339 timestamp for backfill
    /// (e.g. 2026-08-10T00:00:00Z)
    #[arg(long, default_value = "now")]
    start: String,

    /// Timeline speed: a multiplier ("1", "2.5") or "max" (as fast as the
    /// per-node rate limit allows; use with --start in the past)
    #[arg(long, default_value = "1")]
    speed: String,

    /// Maximum backoff between reconnection attempts, in seconds
    #[arg(long, default_value_t = 10.0)]
    reconnect_max_backoff: f64,

    /// Traffic profile (TOML); defaults reproduce the Toaster reference
    #[arg(long)]
    profile: Option<std::path::PathBuf>,

    /// Work-package load multiplier applied on top of the profile: 1.0
    /// reproduces full Toaster CoreVM load (~0.83 WP/core/slot), 0.25 keeps
    /// a small TimescaleDB comfortable
    #[arg(long, default_value_t = 0.25)]
    load_scale: f64,

    /// Suppress per-node connection logging
    #[arg(long, short)]
    quiet: bool,
}

pub fn run(args: SimArgs) -> anyhow::Result<()> {
    if args.load_scale < 0.0 || !args.load_scale.is_finite() {
        anyhow::bail!("--load-scale must be a non-negative number");
    }
    let mut prof = match &args.profile {
        Some(path) => profile::Profile::load(path)?,
        None => profile::Profile::default(),
    };
    prof.wp.per_core_per_slot *= args.load_scale;
    let sim_params = SimParams::new(args.params, prof);
    sim_params
        .proto
        .apply()
        .map_err(|e| anyhow::anyhow!("failed to apply protocol parameters: {:?}", e))?;
    let v = sim_params.v;

    // --- Timeline ---
    let start_ts = if args.start == "now" {
        time::wall_now()
    } else {
        time::parse_rfc3339(&args.start)?
    };
    let pace = Arc::new(Pace {
        mode: match args.speed.as_str() {
            "max" => PaceMode::Max,
            s => {
                let speed: f64 = s
                    .parse()
                    .map_err(|_| anyhow::anyhow!("--speed must be a number or \"max\""))?;
                if speed <= 0.0 || !speed.is_finite() {
                    anyhow::bail!("--speed must be positive");
                }
                PaceMode::Live { speed }
            }
        },
        wall_anchor: std::time::Instant::now(),
        sim_anchor: start_ts,
    });

    log::info!(
        "sim: {:?} (V={}, C={}), seed {}, start {}, speed {}{}",
        args.params,
        v,
        sim_params.c,
        args.seed,
        args.start,
        args.speed,
        if args.duration > 0 {
            format!(", duration {}s", args.duration)
        } else {
            String::new()
        }
    );

    // --- Per-node state ---
    let mut init_rng = rand::rngs::StdRng::seed_from_u64(ids::subseed(args.seed, "drift", 0));
    let streams: Vec<NodeStream> = (0..v)
        .map(|_| NodeStream::new(init_rng.random_range(-3_000..=3_000)))
        .collect();
    let shared: Vec<Arc<NodeShared>> = (0..v).map(|_| Arc::new(NodeShared::new(start_ts))).collect();

    // --- Control flags ---
    let running = Arc::new(AtomicBool::new(true));
    let world_done = Arc::new(AtomicBool::new(false));
    let r = running.clone();
    ctrlc::set_handler(move || {
        log::info!("received Ctrl+C, shutting down...");
        r.store(false, Ordering::SeqCst);
    })?;

    // --- Senders (only online nodes hold connections) ---
    let active = sim_params.p.network.active_nodes.unwrap_or(v).clamp(4, v);
    if active < v {
        log::info!("active nodes: {} of {} (the rest stay silent)", active, v);
    }
    let reconnect_max_backoff = std::time::Duration::from_secs_f64(args.reconnect_max_backoff);
    let ramp_interval = std::time::Duration::from_millis(5); // ~200 connections/s
    let mut handles = Vec::with_capacity(active);
    for n in 0..active as u16 {
        let cfg = sender::SenderCfg {
            node_idx: n,
            server: args.server.clone(),
            node_info: node_info(args.seed, n, &sim_params),
            node_created_at: start_ts,
            connect_delay: ramp_interval * n as u32,
            reconnect_max_backoff,
            quiet: args.quiet,
        };
        handles.push(sender::spawn(
            shared[n as usize].clone(),
            pace.clone(),
            running.clone(),
            world_done.clone(),
            cfg,
        ));
    }

    // --- World ---
    let mut world = World::new(
        sim_params,
        args.seed,
        start_ts,
        args.duration,
        streams,
        shared.clone(),
        pace,
        running.clone(),
    );
    world.bootstrap();
    world.run();
    let generated = world.generated;
    world_done.store(true, Ordering::SeqCst);
    log::info!("world finished: {} events generated, draining senders...", generated);

    for h in handles {
        match h.join() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => log::error!("sender failed: {}", e),
            Err(_) => log::error!("sender panicked"),
        }
    }

    let sent: u64 = shared.iter().map(|s| s.sent_count.load(Ordering::Relaxed)).sum();
    let merged: u64 = shared
        .iter()
        .map(|s| s.queue.lock().expect("queue lock poisoned").live.merged_away)
        .sum();
    log::info!(
        "sim finished: generated {}, sent {}, merged into Dropped {}",
        generated,
        sent,
        merged
    );
    if merged > 0 {
        log::warn!(
            "{} events were merged into Dropped entries — the senders could not keep up; \
             lower the load or check the server",
            merged
        );
    }

    Ok(())
}

/// Build a PolkaJam-look-alike NodeInfo for node `idx`.
fn node_info(seed: u64, idx: u16, params: &SimParams) -> jam_std_common::telemetry::NodeInfo {
    use jam_std_common::telemetry::NodeInfo;
    use jam_std_common::{ed25519, PeerAddr, PeerDetails, PeerId, Truncated};
    use jam_types::HeaderHash;

    let peer_id = PeerId(ed25519::Public::from(ids::h32(seed, "peer", idx as u64, 0)));

    // IPv4-mapped IPv6, 10.20.x.y like a lab network.
    let mut ip = [0u8; 16];
    ip[10] = 0xff;
    ip[11] = 0xff;
    ip[12] = 10;
    ip[13] = 20;
    ip[14] = (idx >> 8) as u8;
    ip[15] = (idx & 0xff) as u8;
    let port = 40_000 + idx;

    NodeInfo {
        params: params.proto.clone(),
        genesis: HeaderHash(ids::h32(seed, "genesis", 0, 0)),
        details: PeerDetails { id: peer_id, addr: PeerAddr { ip, port } },
        // NODE_USES_PVM_RECOMPILER — real Toaster nodes run the compiler backend.
        flags: 1,
        impl_name: "PolkaJam".truncated::<32>(),
        impl_version: "0.1.28".truncated::<32>(),
        gp_version: jam_types::GP_VERSION.truncated::<16>(),
        note: "".truncated::<512>(),
    }
}
