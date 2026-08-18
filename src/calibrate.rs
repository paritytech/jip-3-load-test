//! `calibrate` — fit a traffic profile from a live tart's aggregates.
//!
//! Reads per-type event counts over a window plus the network shape from the
//! handshake parameters, solves the pipeline arithmetic backwards into
//! profile knobs, and writes a TOML profile (including the observed mix as
//! `reference_mix` for `verify`).

use crate::sim::profile::Profile;
use crate::tartapi;

#[derive(clap::Args, Debug)]
pub struct CalibrateArgs {
    /// Base URL of the tart API (e.g. http://192.168.20.84:8080)
    #[arg(long)]
    tart: String,

    /// Window start, RFC3339 (e.g. 2026-08-01T00:00:00Z)
    #[arg(long)]
    start: String,

    /// Window end, RFC3339
    #[arg(long)]
    end: String,

    /// Output profile path
    #[arg(long, short, default_value = "profiles/calibrated.toml")]
    out: std::path::PathBuf,
}

fn ratio(num: u64, den: u64) -> f64 {
    if den == 0 {
        0.0
    } else {
        num as f64 / den as f64
    }
}

pub fn run(args: CalibrateArgs) -> anyhow::Result<()> {
    log::info!("calibrate: fetching network shape from {}", args.tart);
    let (v, c, epoch_period, slot_secs, tickets_n) = tartapi::fetch_network_shape(&args.tart)?;
    log::info!("network: V={}, C={}, epoch={}, slot={}s", v, c, epoch_period, slot_secs);

    log::info!("calibrate: fetching event counts {} .. {}", args.start, args.end);
    // Short windows must use the 1m aggregate: the hourly one only covers
    // fully elapsed hours inside the window.
    let span_secs = tartapi::unix_of(&args.end)?.saturating_sub(tartapi::unix_of(&args.start)?);
    let interval = if span_secs < 3 * 3600 { "1m" } else { "1h" };
    let counts = tartapi::fetch_type_counts(&args.tart, &args.start, &args.end, interval)?;
    let total: u64 = counts.values().sum();
    if total == 0 {
        anyhow::bail!("no events in the window — nothing to calibrate from");
    }
    let n = |t: u8| -> u64 { counts.get(&t).copied().unwrap_or(0) };

    let window_secs = tartapi::unix_of(&args.end)?
        .checked_sub(tartapi::unix_of(&args.start)?)
        .ok_or_else(|| anyhow::anyhow!("--end must be after --start"))?;
    let slots = (window_secs / slot_secs as u64).max(1);
    let blocks = n(42).max(1);

    // Nodes actually emitting during the window (a Toaster run typically has
    // fewer live machines than val_count).
    let v_active = (ratio(n(47), blocks)).round().max(1.0);
    log::info!(
        "window: {} slots, {} blocks ({} authored/slot), ~{} active nodes of {}",
        slots,
        blocks,
        ratio(blocks, slots),
        v_active,
        v
    );

    let mut p = Profile::default();

    // --- network ---
    p.network.active_nodes = Some(v_active as usize);

    // --- chain ---
    p.chain.missed_slot_prob = (1.0 - ratio(blocks, slots)).clamp(0.0, 0.5);
    p.chain.announce_dup_lambda =
        (ratio(n(62), 2 * blocks) / v_active - 1.0).clamp(0.0, 20.0);

    // --- wp funnel ---
    p.wp.per_core_per_slot = ratio(n(90), slots) / c as f64;
    // Shares are only ever attempted towards reachable secondaries (both on
    // the real node and in the simulator), so the observed rate is the rate.
    p.wp.share_fail = ratio(n(99), n(98)).clamp(0.0, 0.9);
    // c105/c90 = (1 - early) * (1 - share²)  →  early
    let guaranteed = ratio(n(105), n(90));
    let sig_ok = 1.0 - p.wp.share_fail * p.wp.share_fail;
    p.wp.early_fail = if sig_ok > 0.0 {
        (1.0 - guaranteed / sig_ok).clamp(0.0, 0.9)
    } else {
        Profile::default().wp.early_fail
    };
    p.wp.guarantee_send_fail = ratio(n(107), n(107) + n(108)).clamp(0.0, 0.9);
    p.wp.assurance_send_fail = ratio(n(127), n(127) + n(128)).clamp(0.0, 0.9);
    p.wp.shard_req_fail = ratio(n(122), n(122) + n(123)).clamp(0.0, 0.9);
    p.wp.guarantee_recv_fail = ratio(n(111), n(110)).clamp(0.0, 0.5);

    // --- segments ---
    p.segments.req_fail = ratio(n(175), n(173)).clamp(0.0, 0.9);
    p.segments.escalate = if n(175) > 0 {
        (ratio(n(162), 2 * n(175))).clamp(0.0, 1.0)
    } else {
        Profile::default().segments.escalate
    };
    p.segments.shard_fail = ratio(n(164), n(162)).clamp(0.0, 0.9);
    // Direct batches per importing WP; has_imports is not observable from
    // counts alone, so the default share is assumed.
    let has_imports = Profile::default().segments.has_imports;
    p.segments.batches_mean = (ratio(n(173), n(90)) / has_imports).max(0.0);

    // --- audit ---
    p.audit.fetchers_mean = ratio(n(148) + n(140) / 8, n(105)).max(0.0);
    p.audit.shard_path = ratio(n(140) / 8, n(148).max(1)).clamp(0.0, 0.5);
    p.audit.req_fail = ratio(n(150), n(150) + n(151)).clamp(0.0, 0.9);

    // --- background ---
    let node_slots = slots as f64 * v_active;
    p.background.dials_per_slot = ratio(n(24), 1) / node_slots;
    p.background.dial_ok = ratio(n(26), n(24)).clamp(0.0, 1.0);
    p.background.ticket_fail = ratio(n(83), n(83) + n(84)).clamp(0.0, 0.9);
    let epochs = (slots as f64 / epoch_period as f64).max(f64::MIN_POSITIVE);
    // legs per ticket = 2 + 2·fanout·(V−1); observed legs = c84/(V·N·epochs)
    let legs = ratio(n(84), 1) / (v_active * tickets_n as f64 * epochs);
    p.background.proxy_fanout =
        (((legs - 2.0) / 2.0).max(0.0) / (v as f64 - 1.0)).clamp(0.0, 1.0);
    p.background.preimage_per_slot = ratio(n(199), slots).clamp(0.0, 1.0);
    p.background.announce_fail = ratio(n(190), n(190) + n(191)).clamp(0.0, 0.9);
    p.background.misbehave_per_slot = (ratio(n(28), 1) / node_slots).clamp(0.0, 1.0);
    p.background.flap_per_slot = (ratio(n(27), 2) / node_slots).clamp(0.0, 1.0);
    p.background.block_req_per_slot = ratio(n(63), 1) / node_slots;
    p.background.block_req_fail = ratio(n(65), n(63) + n(64)).clamp(0.0, 0.95);

    // --- reference mix ---
    for (&t, &cnt) in &counts {
        // Type 0 (Dropped) is transport-generated on the reference network;
        // the simulator's healthy default doesn't produce it — keep it out of
        // the comparison basis.
        if t == 0 {
            continue;
        }
        p.reference_mix
            .insert(t.to_string(), cnt as f64 / total as f64);
    }

    let toml_text = toml::to_string_pretty(&p)?;
    let header = format!(
        "# Traffic profile calibrated from {}\n# window: {} .. {} ({} slots, {} events)\n# network: V={}, C={}, epoch={}, ~{} active nodes\n# Heuristics: has_imports assumed {}, tickets_per_block/finality left at defaults.\n\n",
        args.tart, args.start, args.end, slots, total, v, c, epoch_period, v_active, has_imports
    );
    if let Some(dir) = args.out.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(&args.out, header + &toml_text)?;
    log::info!("profile written to {}", args.out.display());
    println!("{}", args.out.display());
    Ok(())
}
