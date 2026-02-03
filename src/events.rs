//! Event definitions and generation for telemetry spammer.
//!
//! This module is THE source of truth for:
//! - Event IDs and names
//! - Tier groupings
//! - Default rates
//! - Help text generation

use std::collections::HashMap;

/// Definition of a single telemetry event type.
#[derive(Debug, Clone, Copy)]
pub struct EventDef {
    pub id: u8,
    pub name: &'static str,
    /// Default rate in events per second. 0.0 means disabled by default.
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
pub const TIERS: &[TierDef] = &[
    // Tier 1: Status/chain events
    TierDef {
        name: "tier1",
        description: "Status/chain events",
        events: &[
            EventDef { id: 10, name: "Status", default_rate: 0.5 },
            EventDef { id: 11, name: "BestBlockChanged", default_rate: 0.167 },
            EventDef { id: 12, name: "FinalizedBlockChanged", default_rate: 0.167 },
            EventDef { id: 13, name: "SyncStatusChanged", default_rate: 0.01 },
        ],
    },
    // Tier 2: Connection events
    TierDef {
        name: "tier2",
        description: "Connection events",
        events: &[
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
    // Tier 3: Block authoring/importing and sync events
    TierDef {
        name: "tier3",
        description: "Block authoring/importing and sync events",
        events: &[
            EventDef { id: 40, name: "Authoring", default_rate: 0.167 },
            EventDef { id: 41, name: "AuthoringFailed", default_rate: 0.0 },
            EventDef { id: 42, name: "Authored", default_rate: 0.167 },
            EventDef { id: 43, name: "Importing", default_rate: 0.167 },
            EventDef { id: 44, name: "BlockVerificationFailed", default_rate: 0.0 },
            EventDef { id: 45, name: "BlockVerified", default_rate: 0.167 },
            EventDef { id: 46, name: "BlockExecutionFailed", default_rate: 0.0 },
            EventDef { id: 47, name: "BlockExecuted", default_rate: 0.167 },
            EventDef { id: 60, name: "BlockAnnouncementStreamOpened", default_rate: 0.01 },
            EventDef { id: 61, name: "BlockAnnouncementStreamClosed", default_rate: 0.01 },
            EventDef { id: 62, name: "BlockAnnounced", default_rate: 0.167 },
            EventDef { id: 63, name: "SendingBlockRequest", default_rate: 0.1 },
            EventDef { id: 64, name: "ReceivingBlockRequest", default_rate: 0.1 },
            EventDef { id: 65, name: "BlockRequestFailed", default_rate: 0.0 },
            EventDef { id: 66, name: "BlockRequestSent", default_rate: 0.1 },
            EventDef { id: 67, name: "BlockRequestReceived", default_rate: 0.1 },
            EventDef { id: 68, name: "BlockTransferred", default_rate: 0.1 },
        ],
    },
    // Tier 4: Ticket events
    TierDef {
        name: "tier4",
        description: "Ticket events",
        events: &[
            EventDef { id: 80, name: "GeneratingTickets", default_rate: 0.001 },
            EventDef { id: 81, name: "TicketGenerationFailed", default_rate: 0.0 },
            EventDef { id: 82, name: "TicketsGenerated", default_rate: 0.001 },
            EventDef { id: 83, name: "TicketTransferFailed", default_rate: 0.0 },
            EventDef { id: 84, name: "TicketTransferred", default_rate: 0.01 },
        ],
    },
    // Tier 5: Work package and guarantee events
    TierDef {
        name: "tier5",
        description: "Work package and guarantee events",
        events: &[
            EventDef { id: 90, name: "WorkPackageSubmission", default_rate: 0.1 },
            EventDef { id: 91, name: "WorkPackageBeingShared", default_rate: 0.1 },
            EventDef { id: 92, name: "WorkPackageFailed", default_rate: 0.0 },
            EventDef { id: 93, name: "DuplicateWorkPackage", default_rate: 0.0 },
            EventDef { id: 94, name: "WorkPackageReceived", default_rate: 0.1 },
            EventDef { id: 95, name: "Authorized", default_rate: 0.1 },
            EventDef { id: 96, name: "ExtrinsicDataReceived", default_rate: 0.1 },
            EventDef { id: 97, name: "ImportsReceived", default_rate: 0.1 },
            EventDef { id: 98, name: "SharingWorkPackage", default_rate: 0.1 },
            EventDef { id: 99, name: "WorkPackageSharingFailed", default_rate: 0.0 },
            EventDef { id: 100, name: "BundleSent", default_rate: 0.1 },
            EventDef { id: 101, name: "Refined", default_rate: 0.1 },
            EventDef { id: 102, name: "WorkReportBuilt", default_rate: 0.1 },
            EventDef { id: 103, name: "WorkReportSignatureSent", default_rate: 0.1 },
            EventDef { id: 104, name: "WorkReportSignatureReceived", default_rate: 0.1 },
            EventDef { id: 105, name: "GuaranteeBuilt", default_rate: 0.1 },
            EventDef { id: 106, name: "SendingGuarantee", default_rate: 0.1 },
            EventDef { id: 107, name: "GuaranteeSendFailed", default_rate: 0.0 },
            EventDef { id: 108, name: "GuaranteeSent", default_rate: 0.1 },
            EventDef { id: 109, name: "GuaranteesDistributed", default_rate: 0.1 },
            EventDef { id: 110, name: "ReceivingGuarantee", default_rate: 0.1 },
            EventDef { id: 111, name: "GuaranteeReceiveFailed", default_rate: 0.0 },
            EventDef { id: 112, name: "GuaranteeReceived", default_rate: 0.1 },
            EventDef { id: 113, name: "GuaranteeDiscarded", default_rate: 0.0 },
        ],
    },
    // Tier 6: Availability, auditing, segments, preimages
    TierDef {
        name: "tier6",
        description: "Availability, auditing, segments, preimages",
        events: &[
            // Availability (120-131)
            EventDef { id: 120, name: "SendingShardRequest", default_rate: 0.1 },
            EventDef { id: 121, name: "ReceivingShardRequest", default_rate: 0.1 },
            EventDef { id: 122, name: "ShardRequestFailed", default_rate: 0.0 },
            EventDef { id: 123, name: "ShardRequestSent", default_rate: 0.1 },
            EventDef { id: 124, name: "ShardRequestReceived", default_rate: 0.1 },
            EventDef { id: 125, name: "ShardsTransferred", default_rate: 0.1 },
            EventDef { id: 126, name: "DistributingAssurance", default_rate: 0.1 },
            EventDef { id: 127, name: "AssuranceSendFailed", default_rate: 0.0 },
            EventDef { id: 128, name: "AssuranceSent", default_rate: 0.1 },
            EventDef { id: 129, name: "AssuranceDistributed", default_rate: 0.1 },
            EventDef { id: 130, name: "AssuranceReceiveFailed", default_rate: 0.0 },
            EventDef { id: 131, name: "AssuranceReceived", default_rate: 0.1 },
            // Auditing (140-153)
            EventDef { id: 140, name: "SendingBundleShardRequest", default_rate: 0.05 },
            EventDef { id: 141, name: "ReceivingBundleShardRequest", default_rate: 0.05 },
            EventDef { id: 142, name: "BundleShardRequestFailed", default_rate: 0.0 },
            EventDef { id: 143, name: "BundleShardRequestSent", default_rate: 0.05 },
            EventDef { id: 144, name: "BundleShardRequestReceived", default_rate: 0.05 },
            EventDef { id: 145, name: "BundleShardTransferred", default_rate: 0.05 },
            EventDef { id: 146, name: "ReconstructingBundle", default_rate: 0.01 },
            EventDef { id: 147, name: "BundleReconstructed", default_rate: 0.01 },
            EventDef { id: 148, name: "SendingBundleRequest", default_rate: 0.01 },
            EventDef { id: 149, name: "ReceivingBundleRequest", default_rate: 0.01 },
            EventDef { id: 150, name: "BundleRequestFailed", default_rate: 0.0 },
            EventDef { id: 151, name: "BundleRequestSent", default_rate: 0.01 },
            EventDef { id: 152, name: "BundleRequestReceived", default_rate: 0.01 },
            EventDef { id: 153, name: "BundleTransferred", default_rate: 0.01 },
            // Segments (160-178)
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
            // Preimages (190-199)
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
/// - "tierN:RATE" - all events in tier N (e.g., "tier1:0.5")
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
            .ok_or_else(|| anyhow::anyhow!("Invalid rate spec '{}': expected 'ID:RATE' or 'tierN:RATE'", part))?;

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

/// Get preset rates by name.
pub fn preset_rates(name: &str) -> anyhow::Result<HashMap<u8, f64>> {
    match name {
        "minimal" => parse_rates("10:0.5"),
        "realistic" => parse_rates("10:0.5,11:0.167,12:0.167,13:0.01"),
        "network" => parse_rates("tier1:0.5,tier2:0.1"),
        "blocks" => parse_rates("tier1:0.5,tier3:0.167"),
        "guarantees" => parse_rates("tier1:0.5,tier5:0.1"),
        "full" => parse_rates("tier1:0.5,tier2:0.1,tier3:0.1,tier4:0.01,tier5:0.1,tier6:0.1"),
        "stress" => parse_rates("tier1:10,tier5:50,tier6:50"),
        _ => anyhow::bail!("Unknown preset '{}'. Available: minimal, realistic, network, blocks, guarantees, full, stress", name),
    }
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
        let rates = parse_rates("tier1:1.0").expect("parse failed");
        // tier1 = events 10-13
        assert_eq!(rates.get(&10), Some(&1.0));
        assert_eq!(rates.get(&11), Some(&1.0));
        assert_eq!(rates.get(&12), Some(&1.0));
        assert_eq!(rates.get(&13), Some(&1.0));
    }

    #[test]
    fn test_rate_parsing_tier_and_event_override() {
        // Event override should take precedence (later specs win)
        let rates = parse_rates("tier1:1.0,10:0.5").expect("parse failed");
        assert_eq!(rates.get(&10), Some(&0.5)); // overridden
        assert_eq!(rates.get(&11), Some(&1.0)); // from tier
    }

    #[test]
    fn test_tier_event_ids() {
        let tier1_ids: Vec<u8> = TIERS[0].events.iter().map(|e| e.id).collect();
        assert_eq!(tier1_ids, vec![10, 11, 12, 13]);
    }

    #[test]
    fn test_preset_realistic() {
        let rates = preset_rates("realistic").expect("preset failed");
        assert!(rates.get(&10).is_some());
        assert!(rates.get(&11).is_some());
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
}
