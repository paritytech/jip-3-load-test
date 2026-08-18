//! JIP-3 traffic tooling for JAM telemetry servers.
//!
//! Subcommands:
//! - `spam`: legacy rate-based stress generator (independent per-event timers)
//! - `sim` (planned): protocol-level network simulator producing realistic traffic
//! - `calibrate` (planned): fit a traffic profile from a live tart instance
//! - `verify` (planned): check generated traffic against profile invariants

mod calibrate;
mod protocol;
mod sim;
mod spam;
mod tartapi;
mod verify;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "jip-3-spammer")]
#[command(version, about, long_about = None)]
enum Command {
    /// Rate-based stress generator (legacy spammer behavior)
    Spam(spam::SpamArgs),
    /// Protocol-level network simulator (dashboard-realistic traffic)
    Sim(sim::SimArgs),
    /// Fit a traffic profile from a live tart's aggregates
    Calibrate(calibrate::CalibrateArgs),
    /// Check traffic observed by a tart against a profile
    Verify(verify::VerifyArgs),
}

fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    match Command::parse() {
        Command::Spam(args) => spam::run(args),
        Command::Sim(args) => sim::run(args),
        Command::Calibrate(args) => calibrate::run(args),
        Command::Verify(args) => verify::run(args),
    }
}
