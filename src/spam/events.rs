//! Event definitions and generation for telemetry spammer.
//!
//! This module is THE source of truth for:
//! - Event IDs and names
//! - Tier groupings
//! - Default rates (based on event-rates.md full-load simulation)
//! - Help text generation

use std::collections::HashMap;

/// Definition of a single telemetry event type.
#[derive(Debug, Clone, Copy)]
pub struct EventDef {
    pub id: u8,
    pub name: &'static str,
    /// Default rate in events per second. 0.0 means disabled by default.
    /// These rates are based on per-node full-load simulation from event-rates.md
    pub default_rate: f64,
}

/// Definition of a tier (group of related events).
#[derive(Debug, Clone)]
pub struct TierDef {
    pub name: &'static str,
    pub description: &'static str,
    pub events: &'static [EventDef],
}

/// All tier definitions - THE source of truth.
/// Tier names match JIP-3 spec section headers.
/// Default rates are from event-rates.md (per-node, full-load):
/// - per-slot rates: divide by 6 to get per-second
/// - per-epoch rates: divide by 3600 to get per-second
/// - per-tranche rates: divide by 8 to get per-second
pub const TIERS: &[TierDef] = &[
    // Status/chain events (10-13)
    TierDef {
        name: "status",
        description: "Status/chain events",
        events: &[
            // 3/slot = 0.5/s
            EventDef { id: 10, name: "Status", default_rate: 0.5 },
            // 1/slot = 0.167/s
            EventDef { id: 11, name: "BestBlockChanged", default_rate: 0.167 },
            // 1/slot = 0.167/s
            EventDef { id: 12, name: "FinalizedBlockChanged", default_rate: 0.167 },
            // ~0 (rare in steady state)
            EventDef { id: 13, name: "SyncStatusChanged", default_rate: 0.0 },
        ],
    },
    // Connection events (20-28)
    TierDef {
        name: "networking",
        description: "Connection events",
        events: &[
            // Variable - depends on network stability, not load
            EventDef { id: 20, name: "ConnectionRefused", default_rate: 0.0 },
            EventDef { id: 21, name: "ConnectingIn", default_rate: 0.1 },
            EventDef { id: 22, name: "ConnectInFailed", default_rate: 0.0 },
            EventDef { id: 23, name: "ConnectedIn", default_rate: 0.1 },
            EventDef { id: 24, name: "ConnectingOut", default_rate: 0.1 },
            EventDef { id: 25, name: "ConnectOutFailed", default_rate: 0.0 },
            EventDef { id: 26, name: "ConnectedOut", default_rate: 0.1 },
            EventDef { id: 27, name: "Disconnected", default_rate: 0.05 },
            EventDef { id: 28, name: "PeerMisbehaved", default_rate: 0.0 },
        ],
    },
    // Block authoring/importing and sync events (40-47, 60-68)
    TierDef {
        name: "blocks",
        description: "Block authoring/importing and distribution events",
        events: &[
            // Authoring: 0.001/slot (1/V chance) = 0.000167/s
            EventDef { id: 40, name: "Authoring", default_rate: 0.000167 },
            EventDef { id: 41, name: "AuthoringFailed", default_rate: 0.0 },
            EventDef { id: 42, name: "Authored", default_rate: 0.000167 },
            // Importing: 1/slot = 0.167/s
            EventDef { id: 43, name: "Importing", default_rate: 0.167 },
            EventDef { id: 44, name: "BlockVerificationFailed", default_rate: 0.0 },
            EventDef { id: 45, name: "BlockVerified", default_rate: 0.167 },
            EventDef { id: 46, name: "BlockExecutionFailed", default_rate: 0.0 },
            EventDef { id: 47, name: "BlockExecuted", default_rate: 0.167 },
            // Stream events: ~0 (once per peer connection)
            EventDef { id: 60, name: "BlockAnnouncementStreamOpened", default_rate: 0.0 },
            EventDef { id: 61, name: "BlockAnnouncementStreamClosed", default_rate: 0.0 },
            // Block sync: 1/slot = 0.167/s
            EventDef { id: 62, name: "BlockAnnounced", default_rate: 0.167 },
            EventDef { id: 63, name: "SendingBlockRequest", default_rate: 0.167 },
            EventDef { id: 64, name: "ReceivingBlockRequest", default_rate: 0.167 },
            EventDef { id: 65, name: "BlockRequestFailed", default_rate: 0.0 },
            EventDef { id: 66, name: "BlockRequestSent", default_rate: 0.167 },
            EventDef { id: 67, name: "BlockRequestReceived", default_rate: 0.167 },
            // BlockTransferred: ~2/slot (both sides emit) = 0.333/s
            EventDef { id: 68, name: "BlockTransferred", default_rate: 0.333 },
        ],
    },
    // Ticket events (80-84)
    TierDef {
        name: "tickets",
        description: "Safrole ticket events",
        events: &[
            // 1/epoch = 0.000278/s (epoch = 3600s)
            EventDef { id: 80, name: "GeneratingTickets", default_rate: 0.000278 },
            EventDef { id: 81, name: "TicketGenerationFailed", default_rate: 0.0 },
            EventDef { id: 82, name: "TicketsGenerated", default_rate: 0.000278 },
            EventDef { id: 83, name: "TicketTransferFailed", default_rate: 0.0 },
            // 4092/epoch = 1.137/s
            EventDef { id: 84, name: "TicketTransferred", default_rate: 1.137 },
        ],
    },
    // Work package events (90-104)
    TierDef {
        name: "work-package",
        description: "Work package submission, sharing, and refinement events",
        events: &[
            // ~1/slot = 0.167/s
            EventDef { id: 90, name: "WorkPackageSubmission", default_rate: 0.167 },
            // ~2/slot = 0.333/s
            EventDef { id: 91, name: "WorkPackageBeingShared", default_rate: 0.333 },
            EventDef { id: 92, name: "WorkPackageFailed", default_rate: 0.0 },
            EventDef { id: 93, name: "DuplicateWorkPackage", default_rate: 0.0 },
            // ~3/slot = 0.5/s (all guarantors)
            EventDef { id: 94, name: "WorkPackageReceived", default_rate: 0.5 },
            EventDef { id: 95, name: "Authorized", default_rate: 0.5 },
            EventDef { id: 96, name: "ExtrinsicDataReceived", default_rate: 0.5 },
            EventDef { id: 97, name: "ImportsReceived", default_rate: 0.5 },
            // ~0.67/slot = 0.112/s (primary only)
            EventDef { id: 98, name: "SharingWorkPackage", default_rate: 0.112 },
            EventDef { id: 99, name: "WorkPackageSharingFailed", default_rate: 0.0 },
            EventDef { id: 100, name: "BundleSent", default_rate: 0.112 },
            // ~3/slot = 0.5/s (all guarantors)
            EventDef { id: 101, name: "Refined", default_rate: 0.5 },
            EventDef { id: 102, name: "WorkReportBuilt", default_rate: 0.5 },
            // ~0.67/slot = 0.112/s
            EventDef { id: 103, name: "WorkReportSignatureSent", default_rate: 0.112 },
            EventDef { id: 104, name: "WorkReportSignatureReceived", default_rate: 0.112 },
        ],
    },
    // Guarantee events (105-113)
    TierDef {
        name: "guaranteeing",
        description: "Guarantee building and distribution events",
        events: &[
            // ~0.33/slot = 0.055/s
            EventDef { id: 105, name: "GuaranteeBuilt", default_rate: 0.055 },
            // ~340/slot = 56.67/s (C × (V-G))
            EventDef { id: 106, name: "SendingGuarantee", default_rate: 56.67 },
            EventDef { id: 107, name: "GuaranteeSendFailed", default_rate: 0.0 },
            EventDef { id: 108, name: "GuaranteeSent", default_rate: 56.67 },
            // ~0.33/slot = 0.055/s
            EventDef { id: 109, name: "GuaranteesDistributed", default_rate: 0.055 },
            // 341/slot = 56.83/s (C per receiver)
            EventDef { id: 110, name: "ReceivingGuarantee", default_rate: 56.83 },
            EventDef { id: 111, name: "GuaranteeReceiveFailed", default_rate: 0.0 },
            EventDef { id: 112, name: "GuaranteeReceived", default_rate: 56.83 },
            EventDef { id: 113, name: "GuaranteeDiscarded", default_rate: 0.0 },
        ],
    },
    // Availability distribution events (120-131)
    TierDef {
        name: "availability",
        description: "Availability shard and assurance distribution",
        events: &[
            // 341/slot = 56.83/s (C × V requests)
            EventDef { id: 120, name: "SendingShardRequest", default_rate: 56.83 },
            EventDef { id: 121, name: "ReceivingShardRequest", default_rate: 56.83 },
            EventDef { id: 122, name: "ShardRequestFailed", default_rate: 0.0 },
            EventDef { id: 123, name: "ShardRequestSent", default_rate: 56.83 },
            EventDef { id: 124, name: "ShardRequestReceived", default_rate: 56.83 },
            EventDef { id: 125, name: "ShardsTransferred", default_rate: 56.83 },
            // 1/slot = 0.167/s
            EventDef { id: 126, name: "DistributingAssurance", default_rate: 0.167 },
            EventDef { id: 127, name: "AssuranceSendFailed", default_rate: 0.0 },
            // 1022/slot = 170.33/s (V × (V-1))
            EventDef { id: 128, name: "AssuranceSent", default_rate: 170.33 },
            // 1/slot = 0.167/s
            EventDef { id: 129, name: "AssuranceDistributed", default_rate: 0.167 },
            EventDef { id: 130, name: "AssuranceReceiveFailed", default_rate: 0.0 },
            // 1022/slot = 170.33/s
            EventDef { id: 131, name: "AssuranceReceived", default_rate: 170.33 },
        ],
    },
    // Bundle recovery / Auditing events (140-153)
    TierDef {
        name: "auditing",
        description: "Bundle recovery for auditing",
        events: &[
            // ~3420/tranche = 427.5/s (10 cores × 342 shards)
            EventDef { id: 140, name: "SendingBundleShardRequest", default_rate: 427.5 },
            EventDef { id: 141, name: "ReceivingBundleShardRequest", default_rate: 427.5 },
            EventDef { id: 142, name: "BundleShardRequestFailed", default_rate: 0.0 },
            EventDef { id: 143, name: "BundleShardRequestSent", default_rate: 427.5 },
            EventDef { id: 144, name: "BundleShardRequestReceived", default_rate: 427.5 },
            EventDef { id: 145, name: "BundleShardTransferred", default_rate: 427.5 },
            // ~10/tranche = 1.25/s
            EventDef { id: 146, name: "ReconstructingBundle", default_rate: 1.25 },
            EventDef { id: 147, name: "BundleReconstructed", default_rate: 1.25 },
            // Full bundle request (rare fallback)
            EventDef { id: 148, name: "SendingBundleRequest", default_rate: 0.0 },
            EventDef { id: 149, name: "ReceivingBundleRequest", default_rate: 0.0 },
            EventDef { id: 150, name: "BundleRequestFailed", default_rate: 0.0 },
            EventDef { id: 151, name: "BundleRequestSent", default_rate: 0.0 },
            EventDef { id: 152, name: "BundleRequestReceived", default_rate: 0.0 },
            EventDef { id: 153, name: "BundleTransferred", default_rate: 0.0 },
        ],
    },
    // Segment recovery events (160-178)
    TierDef {
        name: "segments",
        description: "Segment recovery for work package imports",
        events: &[
            // Variable, workload-dependent - using estimated typical rates
            EventDef { id: 160, name: "WorkPackageHashMapped", default_rate: 0.1 },
            EventDef { id: 161, name: "SegmentsRootMapped", default_rate: 0.1 },
            EventDef { id: 162, name: "SendingSegmentShardRequest", default_rate: 0.1 },
            EventDef { id: 163, name: "ReceivingSegmentShardRequest", default_rate: 0.1 },
            EventDef { id: 164, name: "SegmentShardRequestFailed", default_rate: 0.0 },
            EventDef { id: 165, name: "SegmentShardRequestSent", default_rate: 0.1 },
            EventDef { id: 166, name: "SegmentShardRequestReceived", default_rate: 0.1 },
            EventDef { id: 167, name: "SegmentShardsTransferred", default_rate: 0.1 },
            EventDef { id: 168, name: "ReconstructingSegments", default_rate: 0.05 },
            EventDef { id: 169, name: "SegmentReconstructionFailed", default_rate: 0.0 },
            EventDef { id: 170, name: "SegmentsReconstructed", default_rate: 0.05 },
            EventDef { id: 171, name: "SegmentVerificationFailed", default_rate: 0.0 },
            EventDef { id: 172, name: "SegmentsVerified", default_rate: 0.05 },
            EventDef { id: 173, name: "SendingSegmentRequest", default_rate: 0.05 },
            EventDef { id: 174, name: "ReceivingSegmentRequest", default_rate: 0.05 },
            EventDef { id: 175, name: "SegmentRequestFailed", default_rate: 0.0 },
            EventDef { id: 176, name: "SegmentRequestSent", default_rate: 0.05 },
            EventDef { id: 177, name: "SegmentRequestReceived", default_rate: 0.05 },
            EventDef { id: 178, name: "SegmentsTransferred", default_rate: 0.05 },
        ],
    },
    // Preimage distribution events (190-199)
    TierDef {
        name: "preimages",
        description: "Preimage distribution",
        events: &[
            // Variable, service-dependent - using estimated typical rates
            EventDef { id: 190, name: "PreimageAnnouncementFailed", default_rate: 0.0 },
            EventDef { id: 191, name: "PreimageAnnounced", default_rate: 0.05 },
            EventDef { id: 192, name: "AnnouncedPreimageForgotten", default_rate: 0.01 },
            EventDef { id: 193, name: "SendingPreimageRequest", default_rate: 0.05 },
            EventDef { id: 194, name: "ReceivingPreimageRequest", default_rate: 0.05 },
            EventDef { id: 195, name: "PreimageRequestFailed", default_rate: 0.0 },
            EventDef { id: 196, name: "PreimageRequestSent", default_rate: 0.05 },
            EventDef { id: 197, name: "PreimageRequestReceived", default_rate: 0.05 },
            EventDef { id: 198, name: "PreimageTransferred", default_rate: 0.05 },
            EventDef { id: 199, name: "PreimageDiscarded", default_rate: 0.01 },
        ],
    },
];

/// Find a tier by name.
pub fn find_tier(name: &str) -> Option<&'static TierDef> {
    TIERS.iter().find(|t| t.name == name)
}

/// Find an event by ID across all tiers.
pub fn find_event(id: u8) -> Option<&'static EventDef> {
    TIERS.iter().flat_map(|t| t.events.iter()).find(|e| e.id == id)
}

/// Get all event IDs in a tier.
#[allow(dead_code)]
pub fn tier_event_ids(tier: &TierDef) -> Vec<u8> {
    tier.events.iter().map(|e| e.id).collect()
}

/// Get default rates for all events (events with default_rate > 0).
/// These are the full-load realistic rates from event-rates.md.
pub fn default_rates() -> HashMap<u8, f64> {
    let mut rates = HashMap::new();
    for tier in TIERS {
        for ev in tier.events {
            if ev.default_rate > 0.0 {
                rates.insert(ev.id, ev.default_rate);
            }
        }
    }
    rates
}

/// Get old/low default rates (pre-event-rates.md values).
/// Used for the "realistic-low" preset.
pub fn old_default_rates() -> HashMap<u8, f64> {
    let old_rates: &[(u8, f64)] = &[
        // Status
        (10, 0.5), (11, 0.167), (12, 0.167), (13, 0.01),
        // Networking
        (21, 0.1), (23, 0.1), (24, 0.1), (26, 0.1), (27, 0.05),
        // Blocks
        (40, 0.167), (42, 0.167), (43, 0.167), (45, 0.167), (47, 0.167),
        (60, 0.01), (61, 0.01), (62, 0.167),
        (63, 0.1), (64, 0.1), (66, 0.1), (67, 0.1), (68, 0.1),
        // Tickets
        (80, 0.001), (82, 0.001), (84, 0.01),
        // Guaranteeing
        (90, 0.1), (91, 0.1), (94, 0.1), (95, 0.1), (96, 0.1), (97, 0.1),
        (98, 0.1), (100, 0.1), (101, 0.1), (102, 0.1),
        (103, 0.1), (104, 0.1), (105, 0.1),
        (106, 0.1), (108, 0.1), (109, 0.1), (110, 0.1), (112, 0.1),
        // Availability
        (120, 0.1), (121, 0.1), (123, 0.1), (124, 0.1), (125, 0.1),
        (126, 0.1), (128, 0.1), (129, 0.1), (131, 0.1),
        // Auditing
        (140, 0.05), (141, 0.05), (143, 0.05), (144, 0.05), (145, 0.05),
        (146, 0.01), (147, 0.01), (148, 0.01), (149, 0.01), (151, 0.01), (152, 0.01), (153, 0.01),
        // Segments
        (160, 0.1), (161, 0.1), (162, 0.1), (163, 0.1), (165, 0.1), (166, 0.1), (167, 0.1),
        (168, 0.05), (170, 0.05), (172, 0.05),
        (173, 0.05), (174, 0.05), (176, 0.05), (177, 0.05), (178, 0.05),
        // Preimages
        (191, 0.05), (192, 0.01), (193, 0.05), (194, 0.05), (196, 0.05), (197, 0.05), (198, 0.05), (199, 0.01),
    ];
    old_rates.iter().cloned().collect()
}

/// Generate help text for all tiers and events.
pub fn tiers_help() -> String {
    let mut help = String::from("Event tiers and default rates:\n");
    for tier in TIERS {
        help.push_str(&format!("\n  {} - {}\n", tier.name, tier.description));
        for ev in tier.events {
            if ev.default_rate > 0.0 {
                help.push_str(&format!(
                    "    {:3} {:<35} (default: {:.3}/s)\n",
                    ev.id, ev.name, ev.default_rate
                ));
            } else {
                help.push_str(&format!(
                    "    {:3} {:<35} (disabled)\n",
                    ev.id, ev.name
                ));
            }
        }
    }
    help
}

/// Parse rate specification string into a map of event_id -> rate.
///
/// Format: "SPEC,SPEC,..." where SPEC is one of:
/// - "ID:RATE" - single event (e.g., "10:0.5")
/// - "TIER_NAME:RATE" - all events in tier (e.g., "status:0.5", "auditing:0.0")
/// - "all:RATE" - all events
///
/// Later specs override earlier ones.
pub fn parse_rates(spec: &str) -> anyhow::Result<HashMap<u8, f64>> {
    let mut rates = HashMap::new();

    for part in spec.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        let (key, rate_str) = part
            .split_once(':')
            .ok_or_else(|| anyhow::anyhow!("Invalid rate spec '{}': expected 'ID:RATE' or 'TIER_NAME:RATE'", part))?;

        let rate: f64 = rate_str
            .trim()
            .parse()
            .map_err(|_| anyhow::anyhow!("Invalid rate '{}' in spec '{}'", rate_str, part))?;

        let key = key.trim();

        if key == "all" {
            // All events
            for tier in TIERS {
                for ev in tier.events {
                    rates.insert(ev.id, rate);
                }
            }
        } else if let Some(tier) = find_tier(key) {
            // Tier
            for ev in tier.events {
                rates.insert(ev.id, rate);
            }
        } else {
            // Single event ID
            let id: u8 = key
                .parse()
                .map_err(|_| anyhow::anyhow!("Invalid event ID or tier name '{}' in spec '{}'", key, part))?;

            if find_event(id).is_none() {
                anyhow::bail!("Unknown event ID {} in spec '{}'", id, part);
            }

            rates.insert(id, rate);
        }
    }

    Ok(rates)
}

/// All available preset names and descriptions.
pub const PRESETS: &[(&str, &str)] = &[
    ("minimal", "Status events only (10-13)"),
    ("realistic-low", "Old realistic rates (lower, pre-simulation values)"),
    ("realistic", "Full-load rates from event-rates.md (no auditing)"),
    ("realistic-with-audits", "Full-load rates including auditing events"),
    ("network", "Connection events (status + networking)"),
    ("blocks", "Block events (status + blocks)"),
    ("guarantees", "Work package pipeline (status + work-package + guaranteeing)"),
    ("stress", "10x realistic rates for load testing"),
];

/// Get preset rates by name.
pub fn preset_rates(name: &str) -> anyhow::Result<HashMap<u8, f64>> {
    match name {
        "minimal" => parse_rates("status:0.5"),
        "realistic-low" => Ok(old_default_rates()),
        "realistic" => {
            // Full-load rates but with auditing disabled
            let mut rates = default_rates();
            // Disable auditing tier (140-153)
            for id in 140..=153 {
                rates.remove(&id);
            }
            Ok(rates)
        }
        "realistic-with-audits" => Ok(default_rates()),
        "network" => parse_rates("status:0.5,networking:0.1"),
        "blocks" => parse_rates("status:0.5,blocks:0.167"),
        "guarantees" => parse_rates("status:0.5,work-package:0.1,guaranteeing:0.1"),
        "stress" => Ok(default_rates().into_iter().map(|(id, rate)| (id, rate * 10.0)).collect()),
        _ => {
            let preset_names: Vec<_> = PRESETS.iter().map(|(n, _)| *n).collect();
            anyhow::bail!("Unknown preset '{}'. Available: {}", name, preset_names.join(", "))
        }
    }
}

/// Generate help text listing all presets with their event rates.
pub fn list_presets() -> String {
    let mut output = String::from("Available presets:\n");

    for (name, description) in PRESETS {
        // Get rates for this preset
        let rates = preset_rates(name).expect("preset should be valid");

        // Calculate total rate
        let total_rate: f64 = rates.values().filter(|&&r| r > 0.0).sum();

        output.push_str(&format!("\n{}  - {}  [total: {:.1}/s]\n", name, description, total_rate));

        // Collect and sort by event ID
        let mut entries: Vec<_> = rates.iter().filter(|(_, &r)| r > 0.0).collect();
        entries.sort_by_key(|(&id, _)| id);

        if entries.is_empty() {
            output.push_str("  (no events)\n");
        } else {
            for (&id, &rate) in &entries {
                let name = find_event(id).map(|e| e.name).unwrap_or("?");
                output.push_str(&format!("  {:3} {:<35} {:.3}/s\n", id, name, rate));
            }
        }
    }

    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_parsing_single_event() {
        let rates = parse_rates("10:0.5").expect("parse failed");
        assert_eq!(rates.get(&10), Some(&0.5));
    }

    #[test]
    fn test_rate_parsing_multiple_events() {
        let rates = parse_rates("10:0.5,11:0.167").expect("parse failed");
        assert_eq!(rates.get(&10), Some(&0.5));
        assert_eq!(rates.get(&11), Some(&0.167));
    }

    #[test]
    fn test_rate_parsing_tier() {
        let rates = parse_rates("status:1.0").expect("parse failed");
        // status = events 10-13
        assert_eq!(rates.get(&10), Some(&1.0));
        assert_eq!(rates.get(&11), Some(&1.0));
        assert_eq!(rates.get(&12), Some(&1.0));
        assert_eq!(rates.get(&13), Some(&1.0));
    }

    #[test]
    fn test_rate_parsing_tier_and_event_override() {
        // Event override should take precedence (later specs win)
        let rates = parse_rates("status:1.0,10:0.5").expect("parse failed");
        assert_eq!(rates.get(&10), Some(&0.5)); // overridden
        assert_eq!(rates.get(&11), Some(&1.0)); // from tier
    }

    #[test]
    fn test_tier_event_ids() {
        let status_ids: Vec<u8> = TIERS[0].events.iter().map(|e| e.id).collect();
        assert_eq!(status_ids, vec![10, 11, 12, 13]);
    }

    #[test]
    fn test_preset_realistic() {
        let rates = preset_rates("realistic").expect("preset failed");
        // Should have status events
        assert!(rates.get(&10).is_some());
        assert!(rates.get(&11).is_some());
        // Should NOT have auditing events
        assert!(rates.get(&140).is_none());
        assert!(rates.get(&145).is_none());
    }

    #[test]
    fn test_preset_realistic_with_audits() {
        let rates = preset_rates("realistic-with-audits").expect("preset failed");
        // Should have auditing events
        assert!(rates.get(&140).is_some());
        assert!(rates.get(&145).is_some());
    }

    #[test]
    fn test_preset_realistic_low() {
        let rates = preset_rates("realistic-low").expect("preset failed");
        // Should have old/lower rates
        assert!(rates.get(&10).is_some());
        // Authoring rate should be higher in old rates (0.167 vs 0.000167)
        assert!(rates.get(&40).unwrap() > &0.1);
    }

    #[test]
    fn test_all_events_have_unique_ids() {
        let mut seen = std::collections::HashSet::new();
        for tier in TIERS {
            for ev in tier.events {
                assert!(
                    seen.insert(ev.id),
                    "Duplicate event ID {} ({})",
                    ev.id,
                    ev.name
                );
            }
        }
    }

    #[test]
    fn test_auditing_tier_exists() {
        let tier = find_tier("auditing");
        assert!(tier.is_some());
        let tier = tier.unwrap();
        // Should contain events 140-153
        let ids: Vec<u8> = tier.events.iter().map(|e| e.id).collect();
        assert!(ids.contains(&140));
        assert!(ids.contains(&153));
    }

    #[test]
    fn test_all_tiers_exist() {
        let expected_tiers = ["status", "networking", "blocks", "tickets", "work-package",
                             "guaranteeing", "availability", "auditing", "segments", "preimages"];
        for name in expected_tiers {
            assert!(find_tier(name).is_some(), "Tier '{}' should exist", name);
        }
    }
}
