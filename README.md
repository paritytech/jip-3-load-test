# JIP-3 Telemetry Spammer

Stress testing tool for JAM telemetry servers. Simulates multiple nodes sending telemetry events according to the JIP-3 specification.

## Building

```bash
cargo build --release
```

## CLI Options

```
jip-3-spammer [OPTIONS]

Options:
    --threads <N>        Number of worker threads (default: 1)
    --server <HOST:PORT> Target server (default: 127.0.0.1:9000)
    --start-id <N>       Starting node ID (default: 0)
    --duration <SECS>    Run duration, 0 = forever (default: 0)
    -q, --quiet          Suppress per-event logging
    --rate <SPEC>        Set frequency for events (see Rate Control)
    --preset <NAME>      Use preset rate configuration (default: realistic)
    --ramp-up <SECS>     Spread connections over SECS window (default: auto ~200/s)
    --no-reconnect       Fail-fast instead of reconnecting on a dropped connection
    --reconnect-max-backoff <SECS>  Cap on reconnect backoff (default: 10)
    --list-events        Show all tiers, events, and default rates
```

## Usage Scenarios

### 1. Basic Test - Single Node

Test connectivity with a single node sending status events:

```bash
./target/release/jip-3-spammer --preset minimal --duration 30
```

### 2. Realistic Simulation

Simulate a node with realistic event timing (matches JIP-3 spec):

```bash
./target/release/jip-3-spammer --preset realistic --duration 60
```

### 3. Multi-Node Test

Simulate 10 nodes with unique IDs:

```bash
./target/release/jip-3-spammer --threads 10 --preset realistic --duration 60
```

### 4. Network Events Focus

Test connection-related events (tier1 + tier2):

```bash
./target/release/jip-3-spammer --threads 5 --preset network --duration 60
```

### 5. Block Events Focus

Test block authoring/importing events (tier1 + tier3):

```bash
./target/release/jip-3-spammer --threads 5 --preset blocks --duration 60
```

### 6. Work Package Pipeline

Test guarantee/work package events (tier1 + tier5):

```bash
./target/release/jip-3-spammer --threads 5 --preset guarantees --duration 60
```

### 7. Full Coverage Test

Test all event tiers at low rate:

```bash
./target/release/jip-3-spammer --threads 10 --preset full --duration 120
```

### 8. Stress Test

High-frequency load testing:

```bash
# Increase file descriptor limit first (required for many threads)
ulimit -n 4096

./target/release/jip-3-spammer --threads 100 --preset stress --duration 60
```

### 9. Custom Event Rates

Fine-grained control over specific events:

```bash
# Status every 2 seconds, BestBlockChanged every 6 seconds
./target/release/jip-3-spammer --rate "10:0.5,11:0.167" --duration 60

# All tier1 events at 1/s, but Status at 0.5/s
./target/release/jip-3-spammer --rate "tier1:1.0,10:0.5" --duration 60

# All events at 10/s (extreme load)
./target/release/jip-3-spammer --rate "all:10" --duration 30
```

### 10. Scale Test - 1024 Nodes

For testing with 1024 nodes, run multiple processes:

```bash
# 10 processes x 100 threads each = 1000 nodes
for i in $(seq 0 9); do
  START_ID=$((i * 100))
  ./target/release/jip-3-spammer \
    --threads 100 \
    --start-id $START_ID \
    --server 10.0.0.1:9000 \
    --preset realistic \
    --duration 300 &
done
wait
```

Or for full 1024 nodes:

```bash
# Process 1: nodes 0-511
./target/release/jip-3-spammer --threads 512 --start-id 0 --preset realistic &

# Process 2: nodes 512-1023
./target/release/jip-3-spammer --threads 512 --start-id 512 --preset realistic &

wait
```

> Note: ramp-up is per-process. When launching several processes, the aggregate
> connection rate is the sum across processes, so size `--ramp-up` accordingly.

### 11. Quiet Mode (Production Load Test)

Suppress per-event logging for cleaner output during long tests:

```bash
./target/release/jip-3-spammer --threads 100 --preset stress --duration 300 --quiet
```

### 12. Remote Server Test

Test against a remote telemetry server:

```bash
./target/release/jip-3-spammer \
  --server telemetry.example.com:9000 \
  --threads 50 \
  --preset realistic \
  --duration 600
```

## Rate Control

### Rate Specification Format

```
--rate "SPEC,SPEC,..."
```

Where SPEC can be:
- `ID:RATE` - Single event (e.g., `10:0.5` = Status at 0.5/s)
- `tierN:RATE` - All events in tier N (e.g., `tier1:0.5`)
- `all:RATE` - All events

Later specs override earlier ones.

### Available Presets

| Preset | Description | Events |
|--------|-------------|--------|
| `minimal` | Status only | tier1 at 0.5/s |
| `realistic` | Matches JIP-3 spec timing | tier1 at spec rates |
| `network` | Connection events | tier1 + tier2 |
| `blocks` | Block events | tier1 + tier3 |
| `guarantees` | Work package pipeline | tier1 + tier5 |
| `full` | All tiers at low rate | tier1-6 |
| `stress` | High frequency load test | tier1 at 10/s, tier5-6 at 50/s |

## Event Tiers

Run `--list-events` to see all events:

| Tier | Description | Event IDs |
|------|-------------|-----------|
| tier1 | Status/chain events | 10-13 |
| tier2 | Connection events | 20-28 |
| tier3 | Block authoring/importing/sync | 40-47, 60-68 |
| tier4 | Ticket events | 80-84 |
| tier5 | Work package/guarantee events | 90-113 |
| tier6 | Availability/auditing/segments/preimages | 120-199 |

## Verification

The spammer embeds verification data in Status events:

| Field | Value | Purpose |
|-------|-------|---------|
| `num_preimages` | node_id | Identifies source node |
| `shards_size` | sequence | Monotonic counter per node |
| `num_shards` | node_id * 1,000,000 + sequence | Checksum |

Server-side verification:
1. Extract `node_id` from `num_preimages`
2. Track `sequence` from `shards_size` - detect gaps
3. Verify `num_shards == node_id * 1,000,000 + sequence`

## Architecture

Each worker thread gets:
- Its own OS thread via `std::thread::spawn`
- Isolated single-threaded tokio runtime (`new_current_thread()`)
- Dedicated TCP connection to server
- Unique node ID = `start_id + thread_index`

This design avoids io_uring congestion that can occur with shared runtimes under high load.

## Wire Protocol

Per JIP-3:
1. Messages are length-prefixed: `[4-byte LE u32 length][SCALE-encoded content]`
2. First message: `(PROTOCOL_VERSION, NodeInfo)` tuple
3. Subsequent messages: `TimestampedEvent` structs

## Troubleshooting

### "Too many open files" error

Each thread needs file descriptors for TCP sockets and the tokio runtime. Increase the limit:

```bash
# Check current limit
ulimit -n

# Increase for current session
ulimit -n 4096

# Or for many threads
ulimit -n 65536
```

For permanent changes, edit `/etc/security/limits.conf`:
```
* soft nofile 65536
* hard nofile 65536
```

### Running 1000+ nodes

For large-scale tests, use multiple processes instead of many threads per process:

```bash
# 10 processes x 100 threads = 1000 nodes
ulimit -n 4096
for i in $(seq 0 9); do
  ./target/release/jip-3-spammer --threads 100 --start-id $((i * 100)) &
done
wait
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Built for the [JAM (Join-Accumulate Machine)](https://graypaper.com/) protocol
- Specification: JIP-3 Telemetry
