# Toaster tart under stress — live observations, 2026-09-09 ~17:00 UTC

Source: tart at 192.168.20.34:8080 (Toaster), REST aggregates and /metrics.
Network: 78 validators (mini params), 155 registered PolkaJam 0.1.28 nodes,
103 live connections, 77 active validators.

## Throughput envelope of this tart deployment

| Metric | Value |
|---|---|
| Ingest during observation | ~158k events/s, batch-writer buffer 0%, 0 current drops |
| Heaviest day (2026-08-31) | 42.0 B events; **550k events/s average over 21 h** |
| Peak minute (2026-08-31 10:48) | **923k events/s** |
| Cumulative since start | 317 B received, 2.77 B dropped server-side (0.87%) |
| Rate-limited events | 0 — this instance runs with the per-node limiter disabled (`--no-rate-limit`) |

Conclusion: full-JAM-scale volume (~0.5M events/s for 1023 validators, see
ROADMAP §3) is already inside this deployment's demonstrated envelope; the
server-side drop share at that load was <1%.

## The stress pattern

Chain healthy: 241 blocks in 250 slots (3.6% missed), 0.99 WP/core/slot
(+19% vs the August run), assurances normal (1.37 M each side in 25 min),
dead-peer dials 0.64/node/slot (10× fewer than in August — healthier mesh).

Anomalies (started 16:00 UTC, still ongoing):
- One validator (192.168.20.20:40017) in a block-request storm: 12.5 M
  SendingBlockRequest → BlockRequestSent → BlockRequestFailed in 20 minutes,
  every request failing, **32.5k events/s sustained** (30 s buckets: min 15.5k,
  p90 34.3k). 98.5% of that node's telemetry.
- Network-wide preimage announcement flood: PreimageAnnounced = 31% of all
  traffic (60.5 M in 25 min).
- Median node runs at ~966 events/s (p90 1107, max 1163) — above tart's
  nominal 1000/s per-node constant, which is why the limiter must be off.

## Consequences for the simulator

- `profiles/toaster-stress-2026-09-09.toml` captures this window as network
  averages; reproducing it faithfully needs per-node personality (one runaway
  node) and a preimage-flood knob beyond the current `preimage_per_slot`
  (saturated at 1.0) — both listed in ROADMAP §4.
- The real node's per-node rate here is a sustained flood, not bursts — the
  simulator's token bucket (900/s) would cap a faithful reproduction of the
  storm node; make it a profile knob before attempting.

## Broadcast timing shape (raw events, median validator, 25 rounds)

Assurance round (per slot):
- `DistributingAssurance` at **4000 ms** into the slot (= boundary − 2 s), jitter 0–4 ms.
- All 77 `AssuranceSent`/`AssuranceSendFailed` within **0–1 ms** of it (1900 of
  1900 sampled sends in the 0 ms bin) — stamped at send initiation, to every
  other validator of the epoch. The one failure per round is the one offline
  validator: failures to offline peers are immediate.
- `AssuranceDistributed` at +19…+603 ms (median +446).
- Receiver side: 74 arrivals per round, median +1 ms after the network-wide
  send moment, last +30 ms. One node's assurances arrive at 1707 ms into the
  slot every round — a validator with a ~2.3 s clock/slot skew.

Guarantee broadcast (per guaranteed WP, 8 rounds sampled):
- All 77 `SendingGuarantee` within **0–25 ms** of `GuaranteeBuilt` (median 5–9 ms).
- `GuaranteeSent` within 1–6 ms after each send; whole broadcast acked in ≤29 ms.
- `GuaranteesDistributed` at +137…+601 ms.
- 3 failures of 77: the offline validator plus ~2.6% residual.

Simulator changes made from these numbers (2026-09-09): burst-shaped
assurance and guarantee rounds addressing all V−1 validators with instant
offline failures; residual failure knobs; timing knobs in `[wp]`;
`--max-send-rate` replaces the fixed 900/s sender bucket (default unlimited).

## Node-side queue change (polkajam PR #1218, merged 2026-09-02)

The telemetry queue is now bounded in bytes (16 MiB, events pre-encoded)
instead of 200 entries; evictions accumulate into a single `Dropped` sent in
the next flush's prologue. A V=1023 assurance burst (~70 KB) fits with two
orders of magnitude to spare, so the earlier prediction of per-slot node-side
`Dropped` at full scale no longer holds for nodes running this code. The
Toaster fleet observed here (0.1.28 builds from before/after the PR — check
`implementation_version` when it changes) dropped nothing server-side during
the observation window.

