# Architecture

This document describes how the `sim` traffic generator works as built. For
the decision history and phase-by-phase verification results, see
[DESIGN.md](DESIGN.md); for the behavioral ground truth it implements, see
[research/polkajam-emission-model.md](research/polkajam-emission-model.md) and
[research/tart-dashboards-model.md](research/tart-dashboards-model.md).

## The problem it solves

JIP-3 telemetry consumers (tart + Grafana) do not visualize events in
isolation. Tart reconstructs *structure*: it resolves implicit per-connection
event ids into reference chains, correlates work packages across nodes by
hash, measures cross-node convergence of slots and guarantees, and pairs
send/receive flows. Traffic that fires events at realistic *rates* but without
this structure renders as empty or broken dashboards.

The simulator therefore does not model event rates at all. It simulates the
protocol: a chain with an authorship lottery, guarantor rotations, the work
package pipeline, availability distribution, segment recovery, audits, and
network background noise. Realistic rates *emerge* from the protocol model,
and every structural property that tart checks holds by construction.

## Layer overview

```
┌────────────────────────────────────────────────────────────┐
│ 1. World simulator          one thread, discrete-event     │
│    sim/world.rs   DES engine, materialization, event heap  │
│    sim/chain.rs   slots, lottery, gossip wave, finality    │
│    sim/wp.rs      WP → guarantee pipeline, shards,         │
│                   assurances                               │
│    sim/recovery.rs segments (import fetch), bundle audit   │
│    sim/background.rs tickets, preimages, churn, block sync │
│    sim/roles.rs   guarantor trios per core per rotation    │
├────────────────────────────────────────────────────────────┤
│ 2. Per-node stream state    sim/stream.rs                  │
│    implicit EventId counter · staging buffer ·             │
│    bounded live queue (200) with Dropped-merge semantics   │
├────────────────────────────────────────────────────────────┤
│ 3. Transport                sim/sender.rs, protocol.rs     │
│    one thread + tokio runtime per online node ·            │
│    JIP-3 framing and handshake · pacing · token bucket ·   │
│    reconnect with gap-Dropped                              │
├────────────────────────────────────────────────────────────┤
│ 4. Profile & tooling        sim/profile.rs, calibrate.rs,  │
│    verify.rs, tartapi.rs                                   │
└────────────────────────────────────────────────────────────┘
```

### 1. World simulator

A single thread owns a `BinaryHeap` of scheduled actions ordered by simulated
time (µs since the JAM Common Era — the same unit as the JIP-3 `Timestamp`,
so materialized events carry sim time directly). Three action kinds exist:
`SlotStart` (drives everything periodic), `StatusTick` (per node, free-running
2 s timer whose phase drifts against slots, like the real node), and `Emit`
(one concrete future event).

On each `SlotStart` the world decides whether the slot produces a block
(missed-slot probability), picks the author by a deterministic lottery over
*online* validators, and schedules complete causal chains as `Emit` actions
with sampled latencies:

- the author's `Authoring → Authored → BlockExecuted → BestBlockChanged`;
- a gossip wave over a random tree rooted at the author — every other online
  node receives an announcement, imports (`Importing → BlockVerified →
  BlockExecuted → BestBlockChanged`) strictly later than the author, and
  becomes an announcement source itself; duplicate announce edges are added to
  match the observed gossip density;
- work-package arrivals per core (Poisson), each expanding into the full
  pipeline below;
- the per-slot assurance round 2 s before the next boundary;
- finality advancement trailing the head by a persistent 2–3 slot lag that
  occasionally steps, so finalization stalls and catches up in bursts;
- background noise: dead-peer dials, block-sync requests, preimage stories,
  ticket rounds in the epoch's ticketing slot, rare flaps and misbehavior.

**Work-package pipeline** (`sim/wp.rs`) follows the polkajam role rules
exactly: a builder submits to one online guarantor (the *primary* — event 90
happens once per WP network-wide); the primary shares with its core's
secondaries (skipping offline ones entirely — no 98/99, matching the
reference data); secondaries run their own receive/refine chains and sign;
with ≥2 signatures the primary builds the guarantee and broadcasts it to
every other online validator (some sends fail); receivers outside the trio
then fetch their availability shard from an online trio member. Guarantees
flow into the next block's outline, into `Status.num_guarantees` per core, and
into `BlockExecuted.accumulate_costs`; on inclusion, exactly the nodes that
hold the guarantee discard it (113).

**Segment recovery** (`sim/recovery.rs`) is the volume king, as on the real
network: for non-bundled WPs the primary fetches import segments from the
*previous* rotation's online guarantors in hundreds of direct request batches
(173/174/176/177/178), with a calibrated failure share retrying and a small
share escalating to segment-shard requests against assurers plus a
reconstruction pass. Bundle recovery for audits (140–153) hits the signing
guarantors directly, with `audit_id = 0` always — the real auditing code
never wires it up, and reproducing such quirks is part of looking authentic.

**Determinism.** All randomness flows from one seeded RNG; hashes, peer ids
and ticket ids are derived from the seed via a SplitMix64 mixer (`sim/ids.rs`).
The heap breaks ties by insertion order. The same (seed, profile, params,
duration) produces byte-identical per-node streams — unit tests assert this.

### 2. Materialization and the two-stage queue

When an `Emit` action pops, the world *materializes* it: assigns the node's
next implicit EventId, resolves parent references, stamps the timestamp
(sim time plus the node's ±5 ms clock-drift walk), and pushes the finished
`TimestampedEvent` into the node's **staging** buffer (unbounded).

Reference resolution uses `Rc<Cell<Option<u64>>>` id slots shared between a
parent's `Emit` and its children's. Because the heap pops in global time
order and every child is scheduled strictly after its parent on the same
node, the parent's id is always assigned before any child resolves it. This
is the property tart's enricher depends on: `core`/`wp_hash`/`slot` propagate
through `submission_id`/`built_id`/`request_id`/… chains that must resolve
within one node's connection. A stream-walking unit test asserts every
reference points to an already-assigned id on the same node.

The sender transfers events from staging into the bounded **live queue**
(200 entries — polkajam's `MAX_REMOTE_EVENT_QUEUE_LEN`) only when their sim
time has arrived. The live queue implements polkajam's exact overflow
semantics: the two oldest entries merge into one `Dropped { num }` at the
head; a head that is already `Dropped` eats one more event and increments
`num`. Because staging absorbs the world's generation lookahead, the live
queue overflows only when the socket genuinely cannot drain events that have
already "happened" — so `Dropped` events are organic, appearing exactly when
a real node's would (overloaded primaries during segment-fetch bursts produce
a steady ~0.2% at full load, mirroring the reference network's behavior).

### 3. Transport

One OS thread with a single-threaded tokio runtime per **online** node
(profile `network.active_nodes` may keep part of the validator set silent,
like the ~63-of-78 reference run). Each sender:

- performs the JIP-3 handshake `(PROTOCOL_VERSION = 1, NodeInfo)` with a
  PolkaJam-look-alike identity (real `ProtocolParameters`, `impl_name
  "PolkaJam"`, GP version from jam-types, flags = PVM recompiler);
- paces sends: in live mode an event is sent when the wall clock reaches its
  timestamp (scaled by `--speed`); in `--speed max` (backfill) events flow as
  fast as a token bucket allows (900/s per node, safely under tart's 1000/s
  limiter);
- on reconnect first emits the gap `Dropped { num = <id of the next event> }`
  exactly like polkajam's transport, so the server's implicit-id counting —
  and therefore every earlier parent reference — stays valid across
  connection loss. Events lost inside a broken connection are covered by the
  same mechanism.

Timestamps are authoritative: tart stores the event-embedded timestamp, not
arrival time, which is what makes historical backfill possible.

### 4. Profiles, calibrate, verify

Every behavioral knob — failure shares, latency ranges, WP intensity, segment
batch counts, churn rates, active-node count — lives in a `Profile`
(TOML-serializable, defaults = the Toaster reference). `calibrate` fits a
profile from any tart's `/api/grafana/timeseries` aggregates by inverting the
pipeline arithmetic (funnel ratios → failure shares, counts per slot →
intensities), and stores the observed event-type mix as `reference_mix`.
`verify` fetches a window from a tart and checks three layers: structural
pairings (send = sent, both-sides transfers, funnel sums), failure shares
against the profile, and the mix against `reference_mix`; it exits non-zero
on any failure, making it CI-friendly.

## Scale and performance envelope

| Configuration | Cluster rate | Notes |
|---|---|---|
| tiny (V=6), full load | ~400 events/s | development loop |
| mini (V=78), `--load-scale 0.25` | ~2.5k events/s | comfortable standing feed |
| mini, calibrated Toaster profile, full load | ~9k events/s | validated for hours, 0 rate-limited |
| backfill (`--speed max`) | ~5–6k events/s/node burst | 10 sim-minutes ≈ 1.7 s wall |

Multi-hour soak runs at mini scale delivered hundreds of millions of events
with zero unaccounted losses (generated = sent + organically-merged).

## Known limitations

- **On-chain dashboards.** The three `TART On-Chain *` dashboards are fed by
  tart's JAM RPC subscription, not telemetry; the simulator covers 7 of 10
  dashboards. A mock RPC endpoint is a possible extension (see ROADMAP).
- **The real Toaster mix is not a fixed point.** The reference network's own
  event mix is internally distorted by node-side queue overflows (its
  `ReceivingSegmentRequest` exceeds `SendingSegmentRequest` by 38% because
  overloaded senders dropped their own telemetry). A loss-free simulation
  intentionally does not reproduce that skew; `verify` against the real
  reference passes all structural and share checks, while type-for-type mix
  parity would require reproducing the reference's overload (a scenario-layer
  feature).
- **Outline payload depth.** `WorkItemOutline.imports` and segment id vectors
  are carried empty; tart's current dashboards don't consume them. Populate
  if a future tart starts to.
- **Path dependencies.** `jam-std-common`/`jam-types` come from a local
  polkajam checkout (protocol v1 / event-61 layout are not on crates.io yet).
