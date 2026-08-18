//! Minimal client for tart's Grafana REST API, shared by `calibrate` and
//! `verify`.

use std::collections::BTreeMap;

/// Sum event counts per type over a window via
/// `/api/grafana/timeseries?...&group_by=event_type`.
///
/// Interval selects the backing aggregate: "1m" is the reliable choice for
/// recent short windows (the 1h continuous aggregates refresh per-subsystem
/// on independent schedules and can lag minutes behind), "1h" is right for
/// long historical windows aligned to whole hours.
pub fn fetch_type_counts(
    base: &str,
    start: &str,
    end: &str,
    interval: &str,
) -> anyhow::Result<BTreeMap<u8, u64>> {
    let url = format!(
        "{}/api/grafana/timeseries?start={}&end={}&interval={}&group_by=event_type",
        base.trim_end_matches('/'),
        start,
        end,
        interval
    );
    let rows: serde_json::Value = ureq::get(&url)
        .timeout(std::time::Duration::from_secs(60))
        .call()
        .map_err(|e| anyhow::anyhow!("GET {} failed: {}", url, e))?
        .into_json()?;
    let mut counts = BTreeMap::new();
    let arr = rows
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("unexpected timeseries response shape"))?;
    for row in arr {
        let t = row["event_type"]
            .as_u64()
            .ok_or_else(|| anyhow::anyhow!("row without event_type: {}", row))?;
        let c = row["count"].as_u64().unwrap_or(0);
        *counts.entry(t as u8).or_insert(0) += c;
    }
    Ok(counts)
}

/// Network shape from the newest registered node's handshake parameters:
/// (val_count, core_count, epoch_period, slot_period_sec, tickets_attempts).
pub fn fetch_network_shape(base: &str) -> anyhow::Result<(usize, usize, u32, u32, u32)> {
    let url = format!("{}/api/grafana/nodes", base.trim_end_matches('/'));
    let rows: serde_json::Value = ureq::get(&url)
        .timeout(std::time::Duration::from_secs(30))
        .call()
        .map_err(|e| anyhow::anyhow!("GET {} failed: {}", url, e))?
        .into_json()?;
    let arr = rows
        .as_array()
        .filter(|a| !a.is_empty())
        .ok_or_else(|| anyhow::anyhow!("no nodes registered on {}", base))?;
    // Prefer the most recently seen node's parameters.
    let params = arr
        .iter()
        .filter_map(|n| {
            let p = &n["node_info"]["params"];
            p.is_object().then_some(p)
        })
        .last()
        .ok_or_else(|| anyhow::anyhow!("nodes response carries no params"))?;
    let get = |k: &str| -> anyhow::Result<u64> {
        params[k]
            .as_u64()
            .ok_or_else(|| anyhow::anyhow!("node parameters missing '{}'", k))
    };
    Ok((
        get("val_count")? as usize,
        get("core_count")? as usize,
        get("epoch_period")? as u32,
        get("slot_period_sec")? as u32,
        get("tickets_attempts_number")? as u32,
    ))
}

/// Parse an RFC3339 timestamp into unix seconds.
pub fn unix_of(s: &str) -> anyhow::Result<u64> {
    let t = humantime::parse_rfc3339(s)
        .map_err(|e| anyhow::anyhow!("invalid RFC3339 timestamp '{}': {}", s, e))?;
    Ok(t.duration_since(std::time::UNIX_EPOCH)?.as_secs())
}

/// Format unix seconds as RFC3339 (UTC, second precision).
pub fn rfc3339_of(unix: u64) -> String {
    humantime::format_rfc3339_seconds(
        std::time::UNIX_EPOCH + std::time::Duration::from_secs(unix),
    )
    .to_string()
}
