# Usage

One binary, four subcommands:

| Subcommand | Purpose |
|---|---|
| `sim` | protocol-level network simulator — dashboard-realistic JIP-3 traffic |
| `calibrate` | fit a traffic profile from a live tart's aggregates |
| `verify` | check traffic observed by a tart against a profile (CI-friendly) |
| `spam` | legacy rate-based stress generator (independent per-event timers) |

## Building

Requires a polkajam checkout next to this repository: `jam-std-common` and
`jam-types` are path dependencies on `../polkajam/crates/*`, because the
telemetry protocol spoken by current tart (v1, jam-std-common 0.1.28) is not
published to crates.io — the last registry release still speaks protocol v0,
which tart rejects at handshake.

```bash
cargo build --release
```

## Simulating a network: `sim`

```bash
# Live Toaster-scale network (78 nodes) at a gentle load, until Ctrl+C:
jip-3-spammer sim --params mini

# Faithful Toaster reproduction: calibrated profile, 63 active nodes, full load:
jip-3-spammer sim --params mini --profile profiles/toaster.toml --load-scale 1

# Small deterministic network for development:
jip-3-spammer sim --params tiny --seed 7 --duration 60

# Backfill 24 hours of history at maximum speed:
jip-3-spammer sim --params mini --profile profiles/toaster.toml \
    --start 2026-08-11T00:00:00Z --duration 86400 --speed max
```

Key options:

- `--params tiny|mini|full` — protocol parameter set (V=6/78/1023, C=V/3).
  The simulated node count equals the validator count; the profile's
  `network.active_nodes` may keep part of the set offline.
- `--profile <toml>` — behavioral knobs (see below). Without it, defaults
  reproduce the Toaster reference.
- `--load-scale <f>` — multiplies the profile's work-package intensity.
  `0.25` (default) keeps a laptop-class TimescaleDB comfortable at mini
  scale (~2.5k events/s); `1.0` reproduces full CoreVM load (~9k events/s).
  Availability/assurance traffic is protocol-driven and does not scale with
  this knob.
- `--seed <u64>` — full determinism: the same seed, profile, params and
  duration reproduce byte-identical per-node event streams.
- `--start <rfc3339>` + `--speed max` — backfill: timestamps are embedded in
  events and tart stores them as the event time, so history can be written
  at wire speed (add `--max-send-rate 900` if the target tart runs its
  per-node limiter). `--speed 2.5` style multipliers also work for
  accelerated live runs.
- `--duration <secs>` — length of the generated timeline in **sim** seconds
  (0 = until Ctrl+C).
- `--max-send-rate <n>` — optional per-node send cap in events/s (0 =
  unlimited, like a real node). Set to ~900 only when the target tart runs
  with its per-node rate limiter enabled; the Toaster deployment does not.

At shutdown the simulator prints the accounting line:

```
sim finished: generated N, sent M, evicted into Dropped K
```

`generated = sent + evicted − <Dropped events>`. With the 16 MiB per-node
queue (polkajam PR #1218) evictions only happen when a socket stalls for a
long time — a non-zero `evicted` means the target server could not keep up,
not a burst. (Pre-#1218 nodes with the 200-entry queue dropped ~0.2% at full
load; the August 2026 Toaster reference carries that signature.)

## Fitting a profile: `calibrate`

```bash
jip-3-spammer calibrate --tart http://192.168.20.84:8080 \
    --start 2026-08-01T23:00:00Z --end 2026-08-03T08:37:00Z \
    -o profiles/toaster.toml
```

Reads per-type event counts and the network shape (validator/core counts from
the nodes' handshake parameters) over the window, inverts the pipeline
arithmetic into profile knobs — WP intensity, failure shares, gossip density,
audit fan-out, churn rates, active-node count — and writes a TOML profile
with the observed event mix stored as `[reference_mix]`.

Practical notes:

- Use the window when telemetry actually ran; idle hours inside the window
  read as missed slots and skew `missed_slot_prob`.
- Windows shorter than 3 h are fetched at 1-minute aggregate resolution
  automatically; longer windows use hourly buckets.
- Heuristics that counts cannot determine (`segments.has_imports`,
  `tickets_per_block_mean`, finality lag) keep their defaults — the header
  comment of the generated file lists them.

## Checking realism: `verify`

```bash
# Against a live feed, last 10 settled minutes, default expectations:
jip-3-spammer verify --tart http://127.0.0.1:8080

# A specific window against a calibrated profile:
jip-3-spammer verify --tart http://127.0.0.1:8080 \
    --start 2026-08-13T19:27:00Z --end 2026-08-13T19:33:00Z \
    --profile profiles/toaster-selfcheck.toml
```

Three layers of checks, exit code 1 if any fails:

1. **Structural pairings** — send = sent, receiving = received, both-sides
   transfers = 2× requests, funnel sums (`SendingGuarantee = failed + sent`,
   `BlockExecuted = Authored + Importing`, …).
2. **Failure shares** against the profile (guarantee/assurance/segment/share/
   dial failure rates), absolute tolerance `--share-tolerance`. Broadcast
   failure shares on the wire are `offline share + residual`: pass `--params`
   matching the run so the offline share (`network.active_nodes` vs the
   validator count) is sized correctly.
3. **Event mix** against the profile's `[reference_mix]`, relative tolerance
   `--mix-tolerance` for types above `--mix-floor`.

**Aggregate lag.** Tart's per-type aggregates converge several minutes behind
ingest (different subsystems flush on independent schedules). The default
window therefore ends `--settle-minutes` (5) before now; when verifying a
just-finished run with explicit `--start/--end`, wait ~5 minutes after the
run before calling `verify`.

**Choosing the reference.** `profiles/toaster-selfcheck.toml` (calibrated
from the simulator's own output) is the regression baseline — 65/65 checks
green. Against the real Toaster profile the mix check reports known
deviations: the reference network's own mix is skewed by node-side drops of
overloaded senders, which a loss-free simulation intentionally does not
reproduce (see ARCHITECTURE.md, Known limitations).

## Profiles

TOML overlay over built-in defaults; unknown keys are rejected. Sections:

- `[network]` — `active_nodes` (offline validators stay registered but
  silent; offline trio members skip shares, offline proxies fail ticket legs,
  only online nodes author).
- `[chain]` — missed slots, latency ranges (`[min_ms, max_ms]` pairs),
  gossip density, finality lag bounds, clock drift, tickets per block.
- `[wp]` — WP intensity per core per slot, pipeline failure shares (the
  guarantee/assurance send-failure knobs are *residual* rates among online
  recipients — offline recipients always fail), service and builder counts,
  and the measured timing shape of broadcasts (assurance lead 2 s, ~1 ms
  send burst, 20–600 ms round completion; guarantee burst 25 ms, acks 1–6 ms,
  LAN hop ~1 ms with a tail to 30 ms).
- `[segments]` — import-fetch share, batches per WP, failure/escalation
  shares.
- `[audit]` — auditors per report, bundle-shard path share, failure share.
- `[background]` — dead-peer dials, ticket transfer behavior, preimage rate,
  misbehavior/flap rates, block-sync rate and failure share.
- `[reference_mix]` — event-type → share-of-total, consumed by `verify`.

A partial file overrides only what it names:

```toml
[wp]
per_core_per_slot = 0.4

[background]
dials_per_slot = 2.0
```

## Recipes

**Standing Grafana feed** (survives tart restarts thanks to reconnect logic;
does not survive host reboots):

```bash
nohup ./target/release/jip-3-spammer sim --params mini \
    --profile profiles/toaster.toml --quiet >/tmp/jip3-feed.log 2>&1 &
```

**Regression check of the simulator** (run → settle → snapshot → verify):

```bash
T0=$(date -u +%Y-%m-%dT%H:%M:%SZ)
jip-3-spammer sim --params mini --profile profiles/toaster.toml \
    --load-scale 1 --duration 300 --quiet
T1=$(date -u +%Y-%m-%dT%H:%M:%SZ)
sleep 300   # let tart's aggregates settle
jip-3-spammer calibrate --tart http://127.0.0.1:8080 --start $T0 --end $T1 \
    -o /tmp/selfcheck.toml
jip-3-spammer verify --tart http://127.0.0.1:8080 --start $T0 --end $T1 \
    --profile /tmp/selfcheck.toml
```

**Filling dashboards with a week of history:**

```bash
jip-3-spammer sim --params mini --profile profiles/toaster.toml \
    --start 2026-08-05T00:00:00Z --duration 604800 --speed max
```

Note that raw events in tart live ~1 hour; history lands in the aggregate
tables, which is what the dashboards read.

## Operational notes

- **Stopping.** Ctrl+C stops promptly. If a run ever stops responding to
  SIGTERM, use SIGKILL (`pkill -9 -x jip-3-spammer`) — the graceful path
  depends on the world loop checking its stop flag.
- **Node identity.** Node ids/PeerIds derive from the seed; a re-run with the
  same seed re-registers the same nodes on tart (useful: dashboards keep
  continuity; change the seed to get a fresh fleet).
- **The legacy `spam` subcommand** is unchanged from the original tool
  (independent per-event-type timers, `--list-events` / `--list-presets`) and
  remains the right hammer for raw ingest stress tests where realism is not
  the goal.
