# Realistic JIP-3 Traffic Simulator — Design

Status: approved 2026-08-12. Phases 0–4 planned, phase 5 optional.

## Goal

Generate JIP-3 telemetry traffic that is indistinguishable — on tart's Grafana
dashboards and in reasonable DB queries — from a real polkajam network
(JAM Toaster today, the production JAM network later). The legacy rate-based
spammer stays available for pure stress testing; the simulator is a separate
`sim` mode in the same binary.

## Why protocol simulation instead of per-event rates

Tart computes nearly all analytics in-memory at ingest and serves 10 dashboards
from pre-aggregated tables via `/api/grafana/*`. Realism is therefore defined by
hard invariants that independent per-event-type timers cannot satisfy:

1. **Implicit-EventId reference chains.** The enricher extracts
   `core`/`wp_hash`/`service` exclusively from `WorkPackageReceived(94)` and
   propagates them along `submission_id`/`built_id`/`sending_id`/`request_id`
   references *within one node's connection*. IDs are implicit (a per-connection
   counter; `Dropped{num}` advances it by `num`). One broken link silently
   empties the Cores dashboard, the WP funnel, bottleneck and guarantor-profiling
   panels, and half of Services.
2. **Slot synchronicity.** Exactly one `Authored(42)` per slot network-wide;
   importers' `Importing(43)`/`BestBlockChanged(11)` strictly later than the
   author's events (convergence percentiles go negative otherwise); finalization
   lags 2–3 slots; 6-second cadence.
3. **Cross-node value consistency.** Block hash identical at author and
   importers; `WorkReportOutline.erasure_root` shared by guarantors and reused in
   assurers' shard requests; `AssuranceReceived.sender` equals the distributing
   node's PeerId; paired send/receive events carry each other's PeerIds.
4. **Real-node quirks** (their absence makes traffic "too correct"):
   `Status.num_shards = shards_size = 0` (hardcoded TODO in polkajam),
   `audit_id = 0` always, event types 0/161/171 never emitted by a node,
   `Status` on a free-running 2 s timer whose phase drifts against slots,
   a steady background of failed outbound dials (~99% of `ConnectingOut` on
   Toaster fails — nodes keep redialing dead peers).
5. **Real volume mix** (reference: 1.73 B events from the 2026-08-01..03 Toaster
   run, aggregates still queryable from toaster-tart): segments dominate
   (`SegmentsTransferred` 380 M), then assurances, guarantees, shard requests;
   characteristic failure shares (`SegmentRequestFailed` ~7.7%,
   `GuaranteeSendFailed` ~15%); ~220 events/s/node on average.
6. **Transport framing.** Handshake `(PROTOCOL_VERSION = 1, NodeInfo)` per TCP
   connection; `params.core_count` must equal the length of
   `Status.num_guarantees` or the decoder rejects the event; nodes hold their
   sockets open (active-nodes is counted from live sockets, not from the DB);
   per-node ingest is rate-limited to 1000 events/s (silent drops above).

Raw events are retained for only one hour, so replaying captured traffic is not
an option; a discrete-event simulation calibrated against surviving aggregates
is.

Known limitation: the three `TART On-Chain *` dashboards (41 panels) are fed
solely by tart's JAM RPC subscription and cannot be driven by telemetry. The
simulator covers 7 of 10 dashboards; a mock JAM RPC endpoint is a possible
future extension.

## Architecture

```
┌────────────────────────────────────────────────────────────┐
│ 1. World Simulator — one thread, discrete-event, seeded    │
│    SlotClock (6 s slots, epochs, rotations, 8 s tranches)  │
│    ChainSim: authorship lottery, hashes, finality, announce│
│    RoleScheduler: per-core guarantor trios, assurers       │
│    Pipelines: WP→guarantee, DA (shards+assurance), segment │
│      recovery, bundle recovery, tickets, preimages, churn  │
│    Failure injection with calibrated probabilities         │
│    ⇒ (node_idx, event, parent_handle) in sim-time order    │
├────────────────────────────────────────────────────────────┤
│ 2. Node Stream Materializer — per node                     │
│    implicit EventId counter, parent_handle → id resolution │
│    bounded queue of 200 with polkajam's exact Dropped      │
│      merge semantics; Dropped{num=counter} on reconnect    │
│      (keeps enricher chains intact, like the real node)    │
│    per-node clock drift (bounded walk within ±250 ms)      │
├────────────────────────────────────────────────────────────┤
│ 3. Transport — reused from the spammer                     │
│    thread-per-node, tokio current_thread, reconnect with   │
│    jittered backoff, ramp-up, handshake (v1, NodeInfo with │
│    mini()/PolkaJam/0.1.28/GP 0.7.2)                        │
│    live pacing to wall clock OR backfill (--start/--speed, │
│    token bucket under tart's 1000/s/node limiter)          │
├────────────────────────────────────────────────────────────┤
│ 4. Profiles — TOML: network params, intensities, latencies,│
│    failure shares, size/gas/ns distributions               │
│    `calibrate`: fit a profile from a live tart's aggregates│
│    `verify`: run → compare REST aggregates against profile │
└────────────────────────────────────────────────────────────┘
```

Design properties:

- **Determinism.** All randomness from one seeded RNG; a given (seed, profile)
  reproduces the identical event stream. Golden tests compare per-type per-slot
  counts against analytic expectations.
- **Timestamps are authoritative.** Tart stores the event-embedded timestamp
  (JCE microseconds), not arrival time, so the generator controls the timeline:
  live mode paces events to the wall clock; backfill mode emits historical
  timestamps at full speed (respecting the per-node rate limiter).
- **Reference integrity across reconnects.** The per-node id counter outlives
  the TCP connection; after reconnect the first message is
  `Dropped{num = counter}`, exactly like polkajam's transport, so earlier
  parent references stay valid on the server side.
- **Organic Dropped.** In a healthy steady state the bounded queue never
  overflows and no `Dropped` is sent (a single one turns a tart panel yellow).
  Overload scenarios produce them mechanically, with real semantics.

### Module layout (target)

```
src/
  main.rs        CLI dispatch (clap subcommands)
  protocol.rs    framing, handshake, shared transport helpers
  spam/          legacy rate-based generator (unchanged behavior)
  sim/
    clock.rs     sim clock, slot/epoch/rotation math
    world.rs     DES engine (BinaryHeap of timed actions)
    chain.rs     blocks, lottery, finality, announcements
    roles.rs     guarantor rotation, authorship assignment
    wp.rs        work-package → guarantee pipeline
    da.rs        shard distribution + assurances
    segments.rs  segment recovery flows
    audit.rs     bundle recovery (audit_id = 0)
    tickets.rs   per-epoch ticket generation/transfer
    preimages.rs preimage gossip
    churn.rs     connection events incl. failed-dial background
    values.rs    hash/size/gas/ns distributions
    stream.rs    per-node materializer (EventId, Dropped, drift)
    profile.rs   TOML traffic profile
    calibrate.rs profile fitting from tart REST aggregates
    verify.rs    invariant/mix verification against a tart
profiles/
  toaster.toml   calibrated from the 2026-08 Toaster run
docs/
  research/      polkajam emission model + tart dashboard model (ground truth)
```

### CLI

```
jip-3-spammer spam ...                        # legacy, unchanged flags
jip-3-spammer sim --profile profiles/toaster.toml \
    [--server 127.0.0.1:9000] [--nodes N] [--seed S] \
    [--load-scale 0.25] [--start <rfc3339>|now] [--speed 1|max] [--duration]
jip-3-spammer calibrate --tart http://host:8080 --window 24h -o profile.toml
jip-3-spammer verify --tart http://host:8080 --profile profile.toml
```

Defaults (decided 2026-08-12): profile `mini`/Toaster (V=78, C=26, epoch 600,
rotation 10) with `--load-scale 0.25` (~4k events/s total) so the local
TimescaleDB is comfortable; `--load-scale 1.0` reproduces full Toaster volume
(~14–17k events/s).

## Dependency note

`jam-std-common`/`jam-types` are path dependencies on the local polkajam
checkout (0.1.28): the last published registry version (0.1.26) still speaks
protocol v0 and the old event-61 layout, which current jamtart rejects at
handshake. Pin back to a registry release once 0.1.28+ is published.

## Roadmap

- **Phase 0 — migration (done 2026-08-12).** Path deps, protocol v1, new
  event-61 layout, subcommand CLI (`spam` preserved). Verified: `spam` against
  a local tart, 35250/35250 events ingested, zero decode errors.
- **Phase 1 — core + chain (done 2026-08-12).** DES engine, materializer,
  transport wiring, live pacing + backfill. Slots, lottery, authoring/import
  chains, best/finalized, Status, announcements. Verified at mini scale
  (78 nodes, 19 slots): exactly one author per slot, author-first in 19/19
  slots (convergence spread 46 ms – 1.4 s), 17 distinct authors, enricher
  resolves the `slot` column for 42/45/47 through implicit-id chains, event mix
  matches expectations to within timer granularity; backfill fills 10 sim-min
  in 1.7 s with zero rate-limiting; identical streams for identical seeds.
- **Phase 2 — WP pipeline + DA (done 2026-08-12).** Rotations, guarantor
  trios, 90–113 with primary/secondary asymmetry, shards 120–125, assurances
  126–131 at slot−2 s; guarantees flow into block outlines, Status.num_guarantees
  and BlockExecuted.accumulate_costs. Verified against tart's derived tables:
  wp_tracking reconstructs the funnel (24 submissions → 16 guarantees →
  16 distributed, matching generation exactly), guarantee_convergence rows carry
  core+wp_hash, da_node_stats pairs every shard request/response,
  assurance receiver_count hits the analytic expectation (V−1)·(1−fail);
  a stream-walking unit test asserts every implicit-id reference resolves
  within its node.
- **Phase 3 — volume tail (done 2026-08-13).** Segments 160–178 (dominant
  volume: direct requests to the previous rotation's guarantors with ~9%
  retries and rare shard-path escalation with reconstruction), bundle recovery
  140–153 with `audit_id = 0` fetched from the signing guarantors, tickets
  80–84 with the proxy pattern, preimages 190–199, connection churn 20–28
  (~99% failed dials to dead peers, rare flaps closing/reopening streams with
  valid 61→60 references). Verified at tiny and mini scale: pairings exact
  (send=sent, both-sides transfers = 2×, fail shares on target), segments
  ≈75% of volume like the reference, enricher propagates core/wp_hash into
  segment chains via submission_id, zero merges at default load. Found and
  fixed en route: an unbounded candidate-pick loop that could spin the world
  across rotation boundaries (audit fetch now targets the signing guarantors,
  picks from a materialized candidate list).
- **Phase 4 — profiles, calibrate, verify (done 2026-08-13).** Every
  behavioral knob moved into a TOML `Profile` (defaults = Toaster reference);
  `calibrate` fits a profile from any tart's aggregates — run against the real
  Toaster it reproduced the hand-derived values to three digits (missed 5.89%,
  0.832 WP/core/slot) and corrected several estimates (gossip density 6.65,
  segment escalation 21%, preimages 10× rarer); `verify` checks structural
  pairings, failure shares and the event mix, with `--settle-minutes`
  compensating tart's per-subsystem aggregate lag. Added en route, driven by
  mix mismatches against the real reference: the block-sync flow 63–68
  (~4 req/node/slot, 45% failure — a steady catch-up hum), the
  `network.active_nodes` knob (Toaster ran ~63 of 78 validators; offline trio
  members skip shares, offline proxies fail ticket legs, only active nodes
  author or receive), and holder-precise guarantee discards.

  A finding worth keeping: the real Toaster's own event mix is internally
  distorted by node-side queue overflows (its ReceivingSegmentRequest exceeds
  SendingSegmentRequest by 38% — overloaded senders dropped their own
  telemetry), so a loss-free simulation cannot and should not match it
  type-for-type. Structural pairings and failure shares match the reference;
  the regression baseline for `verify` is a self-calibrated profile
  (`profiles/toaster-selfcheck.toml`); reproducing the reference's drop-skewed
  mix exactly is a phase-5 scenario (overload the primaries).
- **Phase 5 (optional) — scenarios.** Timeline modifiers: node outages
  (reconnect with `Dropped`), degradations, load bursts with organic queue
  overflow; possibly a mock JAM RPC for the on-chain dashboards.

## Ground-truth references

- `docs/research/polkajam-emission-model.md` — full emission map: EventId
  mechanics, causal chains per subsystem, timing table (tiny/mini/full),
  cross-node consistency rules, structure signatures. Derived from the polkajam
  sources at 0.1.28.
- `docs/research/tart-dashboards-model.md` — ingest path, enricher chains,
  aggregate tables, per-dashboard data requirements, 20-invariant checklist.
- Reference traffic mix: toaster-tart REST aggregates
  (`/api/grafana/timeseries?...&group_by=event_type`), 2026-08-01..03 run:
  1.73 B events, 65 active nodes, ~220 events/s/node.
