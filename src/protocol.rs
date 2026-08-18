//! JIP-3 wire protocol and shared transport helpers.
//!
//! Per JIP-3, a telemetry connection is a plain TCP stream carrying
//! length-prefixed messages: `[4-byte LE u32 length][SCALE-encoded content]`.
//! The first message is the handshake tuple `(PROTOCOL_VERSION, NodeInfo)`;
//! every subsequent message is a `TimestampedEvent`.

use jam_std_common::telemetry::NodeInfo;

/// Open a TCP connection and send the JIP-3 handshake (PROTOCOL_VERSION, NodeInfo).
pub async fn connect_and_init(
    server: &str,
    node_info: &NodeInfo,
) -> anyhow::Result<tokio::net::TcpStream> {
    let mut stream = tokio::net::TcpStream::connect(server).await?;
    let init_msg = (jam_std_common::telemetry::PROTOCOL_VERSION, node_info);
    send_message(&mut stream, &init_msg).await?;
    Ok(stream)
}

/// Send a length-prefixed SCALE-encoded message.
pub async fn send_message<T: codec::Encode>(
    stream: &mut tokio::net::TcpStream,
    msg: &T,
) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;

    let encoded = msg.encode();
    let len = (encoded.len() as u32).to_le_bytes();

    stream.write_all(&len).await?;
    stream.write_all(&encoded).await?;
    stream.flush().await?;

    Ok(())
}

/// Sleep for `dur` while polling the shutdown flag, so Ctrl+C aborts promptly.
/// Returns `true` if the full duration elapsed, `false` if interrupted.
pub async fn interruptible_sleep(
    dur: std::time::Duration,
    running: &std::sync::atomic::AtomicBool,
) -> bool {
    let deadline = std::time::Instant::now() + dur;
    loop {
        if !running.load(std::sync::atomic::Ordering::Relaxed) {
            return false;
        }
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return true;
        }
        tokio::time::sleep(remaining.min(std::time::Duration::from_millis(100))).await;
    }
}

/// Base delay for reconnection backoff. The cap doubles each attempt from here
/// up to the configured maximum; the actual wait is jittered within [0, cap].
pub const RECONNECT_BASE_BACKOFF_MS: u64 = 100;

/// Reconnection backoff: exponential from `RECONNECT_BASE_BACKOFF_MS`, capped at
/// `max_backoff`, with full jitter (uniform in `[0, cap]`). `attempt` starts at
/// 1 for the first retry. Full jitter spreads a fleet's reconnects so a server
/// restart doesn't produce a synchronized thundering herd.
pub fn reconnect_backoff(
    attempt: u32,
    max_backoff: std::time::Duration,
    rng: &mut impl rand::Rng,
) -> std::time::Duration {
    let base = std::time::Duration::from_millis(RECONNECT_BASE_BACKOFF_MS);
    // base * 2^(attempt-1), saturating; shift capped so it can't overflow.
    let shift = attempt.saturating_sub(1).min(20);
    let cap = base.saturating_mul(1u32 << shift).min(max_backoff);
    cap.mul_f64(rng.random::<f64>())
}
