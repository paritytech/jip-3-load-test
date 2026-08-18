//! `verify` — check that traffic observed by a tart looks like the profile.
//!
//! Pulls per-type counts over a window and validates: structural pairings
//! (send=sent, both-sides transfers, funnel arithmetic), failure shares
//! against the profile, and the overall event-type mix against the profile's
//! `reference_mix`. Exits non-zero if any check fails.

use crate::sim::profile::Profile;
use crate::tartapi;

#[derive(clap::Args, Debug)]
pub struct VerifyArgs {
    /// Base URL of the tart API (e.g. http://127.0.0.1:8080)
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    tart: String,

    /// Look back this many minutes from now (ignored if --start given)
    #[arg(long, default_value_t = 10)]
    last_minutes: u64,

    /// End the default window this many minutes before now: tart's per-type
    /// aggregates converge several minutes behind ingest (ignored if --end
    /// given)
    #[arg(long, default_value_t = 5)]
    settle_minutes: u64,

    /// Window start, RFC3339 (overrides --last-minutes)
    #[arg(long)]
    start: Option<String>,

    /// Window end, RFC3339 (default: now)
    #[arg(long)]
    end: Option<String>,

    /// Profile with expected shares and reference mix
    #[arg(long)]
    profile: Option<std::path::PathBuf>,

    /// Relative tolerance for pairing checks
    #[arg(long, default_value_t = 0.08)]
    pair_tolerance: f64,

    /// Absolute tolerance for failure-share checks
    #[arg(long, default_value_t = 0.05)]
    share_tolerance: f64,

    /// Relative tolerance for reference-mix shares
    #[arg(long, default_value_t = 0.35)]
    mix_tolerance: f64,

    /// Minimum reference share for a type to participate in the mix check
    #[arg(long, default_value_t = 0.002)]
    mix_floor: f64,
}

struct Report {
    checks: u32,
    failures: u32,
}

impl Report {
    fn check(&mut self, name: &str, pass: bool, detail: String) {
        self.checks += 1;
        if pass {
            println!("  PASS  {:<44} {}", name, detail);
        } else {
            self.failures += 1;
            println!("  FAIL  {:<44} {}", name, detail);
        }
    }
}

pub fn run(args: VerifyArgs) -> anyhow::Result<()> {
    let profile = match &args.profile {
        Some(path) => Profile::load(path)?,
        None => Profile::default(),
    };

    let now_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let settled = now_unix - args.settle_minutes * 60;
    let end = args.end.clone().unwrap_or_else(|| tartapi::rfc3339_of(settled));
    let start = args
        .start
        .clone()
        .unwrap_or_else(|| tartapi::rfc3339_of(settled - args.last_minutes * 60));

    println!("verify: {} window {} .. {}", args.tart, start, end);
    let counts = tartapi::fetch_type_counts(&args.tart, &start, &end, "1m")?;
    let total: u64 = counts.values().sum();
    if total == 0 {
        anyhow::bail!("no events in the verification window");
    }
    let n = |t: u8| -> u64 { counts.get(&t).copied().unwrap_or(0) };
    println!("  {} events across {} types\n", total, counts.len());

    let mut r = Report { checks: 0, failures: 0 };
    let tol = args.pair_tolerance;

    let rel_close = |a: u64, b: u64| -> (bool, String) {
        let (fa, fb) = (a as f64, b as f64);
        let denom = fa.max(fb).max(1.0);
        let delta = (fa - fb).abs() / denom;
        (delta <= tol, format!("{} vs {} (Δ {:.1}%)", a, b, delta * 100.0))
    };

    // --- Structural pairings ---
    println!("pairings:");
    let pairs: &[(&str, u8, u8)] = &[
        ("Authoring = Authored", 40, 42),
        ("Importing = BlockVerified", 43, 45),
        ("SendingShardReq = ReceivingShardReq", 120, 121),
        ("ShardReqSent = ShardReqReceived", 123, 124),
        ("AssuranceSent = AssuranceReceived", 128, 131),
        ("SendingSegReq = SegReqSent", 173, 176),
        ("ReceivingSegReq = SegReqReceived", 174, 177),
        ("SendingSegShardReq = SegShardReqSent", 162, 165),
        ("SendingBundleReq = BundleReqSent", 148, 151),
        ("ReceivingBundleReq = BundleReqReceived", 149, 152),
        ("SendingBlockReq = BlockReqSent", 63, 66),
        ("ReceivingBlockReq = BlockReqReceived", 64, 67),
        ("ReceivingGuarantee = GuaranteeReceived", 110, 112),
        ("Reconstructing = Reconstructed (segments)", 168, 170),
    ];
    for (name, a, b) in pairs {
        let (ok, detail) = rel_close(n(*a), n(*b));
        r.check(name, ok, detail);
    }

    // Both-sides transfers ≈ 2 × receiver-side requests.
    for (name, transfer, recv) in
        [("SegTransferred = 2×recv", 178u8, 174u8), ("BundleTransferred = 2×recv", 153, 149)]
    {
        let (ok, detail) = rel_close(n(transfer), 2 * n(recv));
        r.check(name, ok, detail);
    }

    // Funnel arithmetic.
    let (ok, detail) = rel_close(n(47), n(42) + n(43));
    r.check("BlockExecuted = Authored + Importing", ok, detail);
    let (ok, detail) = rel_close(n(106), n(107) + n(108));
    r.check("SendingGuarantee = failed + sent", ok, detail);
    let (ok, detail) = rel_close(n(98), n(99) + n(100));
    r.check("Sharing = failed + BundleSent", ok, detail);

    // --- Failure shares against the profile ---
    println!("\nfailure shares (± {:.0}%):", args.share_tolerance * 100.0);
    let share = |num: u64, den: u64| -> f64 {
        if den == 0 {
            0.0
        } else {
            num as f64 / den as f64
        }
    };
    let shares: &[(&str, f64, f64)] = &[
        ("guarantee_send_fail", share(n(107), n(107) + n(108)), profile.wp.guarantee_send_fail),
        ("assurance_send_fail", share(n(127), n(127) + n(128)), profile.wp.assurance_send_fail),
        ("segment_req_fail", share(n(175), n(173)), profile.segments.req_fail),
        ("share_fail", share(n(99), n(98)), profile.wp.share_fail),
        ("ticket_fail", share(n(83), n(83) + n(84)), profile.background.ticket_fail),
        ("dial_fail", share(n(25), n(24)), 1.0 - profile.background.dial_ok),
    ];
    for (name, observed, expected) in shares {
        let applicable = match *name {
            "ticket_fail" => n(83) + n(84) > 50,
            _ => true,
        };
        if !applicable {
            continue;
        }
        let ok = (observed - expected).abs() <= args.share_tolerance;
        r.check(name, ok, format!("observed {:.3}, expected {:.3}", observed, expected));
    }

    // --- Mix against the reference ---
    if profile.reference_mix.is_empty() {
        println!("\nmix: profile has no reference_mix — skipped");
    } else {
        println!(
            "\nmix vs reference (types ≥ {:.1}%, ± {:.0}% rel):",
            args.mix_floor * 100.0,
            args.mix_tolerance * 100.0
        );
        for (key, &ref_share) in &profile.reference_mix {
            if ref_share < args.mix_floor {
                continue;
            }
            let t: u8 = match key.parse() {
                Ok(t) => t,
                Err(_) => continue,
            };
            let observed = n(t) as f64 / total as f64;
            let delta = (observed - ref_share).abs() / ref_share;
            let ok = delta <= args.mix_tolerance;
            r.check(
                &format!("mix[{}]", t),
                ok,
                format!("observed {:.3}%, reference {:.3}%", observed * 100.0, ref_share * 100.0),
            );
        }
    }

    println!("\n{} checks, {} failures", r.checks, r.failures);
    if r.failures > 0 {
        anyhow::bail!("{} verification check(s) failed", r.failures);
    }
    Ok(())
}
