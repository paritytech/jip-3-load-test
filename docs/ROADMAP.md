# Roadmap

Phases 0–4 of [DESIGN.md](DESIGN.md) are complete: the simulator covers the
full JIP-3 event space with verified structural realism, a profile calibrated
from the real JAM Toaster run, and an automated `calibrate`/`verify` loop.
Everything below is future work, roughly ordered by value.

## 1. Scenario layer (design phase 5)

A timeline of modifiers over a base profile, turning the steady-state
simulator into a story-teller for demos, alert tuning and operator training:

- **Node outages and recoveries** — take a subset of nodes offline mid-run
  and bring them back; reconnects already emit the gap `Dropped` correctly,
  so downtime renders exactly like a real node's (this also exercises
  `watch-dropped.py` and the Dropped panels).
- **Overload / drop-skew reproduction** — deliberately narrow the segment
  fetch window or raise WP intensity so primaries overflow their 200-event
  queues at the reference network's observed rate. This is the missing piece
  for type-for-type mix parity with the real Toaster, whose own mix is
  distorted by node-side drops (~38% excess of receiver-side segment events).
- **Network degradation** — time-boxed multipliers on latency ranges and
  failure shares (guarantee/assurance/segment sends), visible as convergence
  percentile spikes and failure-rate steps on the dashboards.
- **Load ramps and bursts** — diurnal curves or step changes in WP intensity.
- Suggested shape: a `[scenario]` TOML with `at = <sim-offset>` entries
  patching profile fields, applied by the world on slot boundaries.

## 2. Mock JAM RPC for the on-chain dashboards

The three `TART On-Chain *` dashboards (41 panels) are fed exclusively by
tart's WebSocket subscription to a JAM node (`subscribe_statistics`,
`subscribe_finalized_block`). A small WS server speaking just those two
methods, driven by the same world state (blocks, guarantees, validator
activity are all already simulated), would light up the remaining 3 of 10
dashboards. Point tart's `JAM_RPC_URL` at it. Note polkajam 0.1.28's
`ValActivityRecord` layout when encoding statistics (a known tart decode
mismatch exists — see the jamtart notes).

## 3. Full-scale (V=1023) hardening

The world model and profiles are parameter-driven and `full` is already a
CLI option, but production scale is untested end-to-end:

- 1023 sender threads work but are heavy; consider multiplexing several
  nodes per tokio runtime.
- Assurance traffic grows quadratically (V·(V−1) per slot ≈ 174k events/slot
  at V=1023) — the per-node token bucket is fine, but the target tart and its
  TimescaleDB need sizing (the reference full-load figure would be ~1M
  events/s cluster-wide; likely calls for `--load-scale` discipline or a
  sharded ingest).
- The gossip tree, rotation math and ticket flows need no changes.

## 4. Fidelity backlog (small, independent items)

- **Forks and reorgs** — occasional competing blocks with `BestBlockChanged`
  switching branches; tart's convergence trackers would show reorg blips.
  The chain model currently produces a linear chain with missed slots only.
- **Dispute verdicts** — `BlockOutline.num_dispute_verdicts` is always 0;
  a rare dispute scenario would exercise it.
- **Populated outlines** — `WorkItemOutline.imports` and segment-id vectors
  are carried empty today (nothing in tart consumes them); fill with
  plausible `ImportSpec`s if a future tart starts reading them.
- **Builder connections** — builders exist as PeerIds submitting WPs but
  don't hold telemetry connections of their own; real builder nodes may
  appear in the fleet once builders emit JIP-3 in production networks.
- **Per-node personality** — per-node latency/failure multipliers (slow
  disks, bad links) instead of a uniform fleet; would diversify the per-node
  dashboards.
- **Event 61 in steady state** — `BlockAnnouncementStreamClosed` currently
  appears only during flaps; real networks also close streams on peer-set
  changes at epoch boundaries.
- **`GuaranteeReceiveFailed` (111)** — modeled at ~0 today; wire the rare
  failure path in the broadcast receive.

## 5. Tooling and packaging

- **Registry dependencies** — switch `jam-std-common`/`jam-types` back to
  crates.io versions once a release ≥0.1.28 (protocol v1, new event-61
  layout) is published; drop the polkajam-checkout build requirement.
- **`verify --json`** — machine-readable report for CI annotations.
- **Profile diffing** — `calibrate` twice, diff the knobs: a quick health
  comparison between two networks (or two runs of one network).
- **Prometheus endpoint on the simulator** — expose generated/sent/merged
  counters live instead of end-of-run logs, so long feeds can be monitored
  from the same Grafana.
- **Production-network profile** — when the real JAM network launches, run
  `calibrate` against its tart and ship `profiles/mainnet.toml`; the
  generator was built for exactly this hand-off.

## Non-goals

- Byte-faithful reproduction of polkajam internals that telemetry cannot
  observe (real erasure coding, real VRF lotteries, PVM execution).
- Replay of captured raw event streams — tart retains raw events for ~1 hour,
  and aggregates are the durable truth; calibration-from-aggregates is the
  chosen approach.
- Load-testing tart's limits — that is the legacy `spam` subcommand's job.
