//! Telemetry event generation for the rate-based spammer.

use super::{schedule_receive, PairContext};

/// Generate a telemetry event by ID.
/// `event_count` is the per-event-type counter (used for sequential values like slots).
/// `pair_ctx` enables pair-wise generation with real peer IDs and cross-thread scheduling.
/// `rng` is used for sampling random recipients in pair-wise mode.
pub(crate) fn generate_event(
    event_id: u8,
    node_id: u64,
    sequence: u64,
    event_count: u64,
    mut pair_ctx: Option<&mut PairContext<'_>>,
    rng: &mut impl rand::Rng,
) -> anyhow::Result<jam_std_common::telemetry::TimestampedEvent> {
    use bounded_collections::BoundedVec;
    use jam_std_common::telemetry::{
        event, AnnouncedPreimageForgetReason, BlockOutline, BlockRequestDirection, ConnectionSide,
        Event, EventId, GuaranteeDiscardReason, GuaranteeOutline, IsAuthorizedCost,
        PreimageDiscardReason, ReconstructionKind, Timestamp, TimestampedEvent,
        WorkPackageOutline, WorkReportOutline,
    };
    use jam_std_common::{
        ed25519, AvailabilityBitfield, ErasureRoot, PeerAddr, PeerDetails, PeerId, TicketId,
        Truncated,
    };
    use jam_types::{
        CoreIndex, FixedVec, HeaderHash, SegmentTreeRoot, ServiceId, Slot, TicketAttempt,
        WorkPackageHash, WorkReportHash,
    };

    let timestamp = Timestamp::now();

    // Helper to generate deterministic hash from node_id and sequence
    // Use 0x80 | prefix to ensure non-ASCII first byte (displays as hex, not string)
    let make_hash = |prefix: u8| -> [u8; 32] {
        let mut hash = [0u8; 32];
        hash[0] = 0x80 | prefix;
        hash[1..9].copy_from_slice(&node_id.to_le_bytes());
        hash[9..17].copy_from_slice(&sequence.to_le_bytes());
        hash
    };

    // Helper to generate deterministic PeerId
    let make_peer_id = |seed: u64| -> PeerId {
        let mut bytes = [0u8; 32];
        bytes[0..8].copy_from_slice(&seed.to_le_bytes());
        bytes[8..16].copy_from_slice(&node_id.to_le_bytes());
        PeerId(ed25519::Public::from(bytes))
    };

    // Helper to generate deterministic PeerAddr
    let make_peer_addr = |seed: u64| -> PeerAddr {
        let mut ip = [0u8; 16];
        ip[10] = 0xff;
        ip[11] = 0xff;
        ip[12] = 10;
        ip[13] = ((seed >> 16) & 0xff) as u8;
        ip[14] = ((seed >> 8) & 0xff) as u8;
        ip[15] = (seed & 0xff) as u8;
        PeerAddr {
            ip,
            port: 30333 + (seed % 1000) as u16,
        }
    };

    // Helper to generate deterministic PeerDetails
    let _make_peer_details = |seed: u64| -> PeerDetails {
        PeerDetails {
            id: make_peer_id(seed),
            addr: make_peer_addr(seed),
        }
    };

    // Helper to generate deterministic EventId (use sequence as base)
    let make_event_id = |offset: u64| -> EventId {
        EventId(sequence.saturating_sub(offset).max(1))
    };

    // Helper to generate deterministic reason string
    let make_reason = |msg: &str| -> jam_std_common::telemetry::Reason {
        format!("{} (node={}, seq={})", msg, node_id, sequence).truncated::<128>()
    };

    // Helper for BlockOutline
    let make_block_outline = || -> BlockOutline {
        BlockOutline {
            size: 1000 + (sequence % 5000) as u32,
            hash: HeaderHash(make_hash(0x42)),
            num_tickets: (sequence % 10) as u32,
            num_preimages: (sequence % 5) as u32,
            preimages_size: (sequence % 1000) as u32,
            num_guarantees: (sequence % 8) as u32,
            num_assurances: (sequence % 20) as u32,
            num_dispute_verdicts: 0,
        }
    };

    // Helper for WorkPackageOutline
    let make_work_package_outline = || -> WorkPackageOutline {
        WorkPackageOutline {
            size: 500 + (sequence % 2000) as u32,
            hash: WorkPackageHash(make_hash(0x57)),
            anchor: HeaderHash(make_hash(0x58)),
            lookup_anchor_slot: Slot::from(sequence.saturating_sub(10) as u32),
            prerequisites: BoundedVec::new(),
            items: BoundedVec::new(),
        }
    };

    // Helper for WorkReportOutline
    let make_work_report_outline = || -> WorkReportOutline {
        WorkReportOutline {
            hash: WorkReportHash(make_hash(0x52)),
            bundle_size: 1000 + (sequence % 10000) as u32,
            erasure_root: ErasureRoot(make_hash(0x53)),
            exports_root: SegmentTreeRoot(make_hash(0x54)),
        }
    };

    // Helper for GuaranteeOutline
    let make_guarantee_outline = || -> GuaranteeOutline {
        GuaranteeOutline {
            report_hash: WorkReportHash(make_hash(0x47)),
            slot: Slot::from(sequence as u32),
            guarantors: BoundedVec::new(),
        }
    };

    // Macro-like helper: pick a real peer from registry (pair-wise) or fake (legacy)
    macro_rules! peer_or_fake {
        ($seed:expr) => {
            if let Some(ref mut ctx) = pair_ctx {
                let t = ctx.registry.pick_others(ctx.my_index, 1, rng);
                ctx.registry.peer_id(t[0])
            } else {
                make_peer_id($seed)
            }
        };
    }

    let event = match event_id {
        // ============== Tier 1: Status/chain events ==============
        10 => Event::Status(event::Status {
            num_peers: 100,
            num_val_peers: 50,
            num_sync_peers: 50,
            num_guarantees: FixedVec::new(0),
            // VERIFICATION FIELDS:
            num_shards: (node_id as u32).wrapping_mul(1_000_000).wrapping_add(sequence as u32),
            shards_size: sequence,
            num_preimages: node_id as u32,
            preimages_size: 0,
        }),

        11 => Event::BestBlockChanged(event::BestBlockChanged {
            slot: Slot::from(event_count as u32),
            hash: HeaderHash(make_hash(0x11)),
        }),

        12 => Event::FinalizedBlockChanged(event::FinalizedBlockChanged {
            slot: Slot::from(event_count as u32),
            hash: HeaderHash(make_hash(0x12)),
        }),

        13 => Event::SyncStatusChanged(event::SyncStatusChanged {
            synced: sequence % 2 == 0,
        }),

        // ============== Tier 2: Connection events ==============
        20 => Event::ConnectionRefused(event::ConnectionRefused {
            from: make_peer_addr(sequence),
        }),

        21 => Event::ConnectingIn(event::ConnectingIn {
            from: make_peer_addr(sequence),
        }),

        22 => Event::ConnectInFailed(event::ConnectInFailed {
            connecting_id: make_event_id(1),
            reason: make_reason("Connection timeout"),
        }),

        23 => Event::ConnectedIn(event::ConnectedIn {
            connecting_id: make_event_id(1),
            peer_id: peer_or_fake!(sequence),
        }),

        24 => Event::ConnectingOut(event::ConnectingOut {
            to: PeerDetails {
                id: peer_or_fake!(sequence),
                addr: make_peer_addr(sequence),
            },
        }),

        25 => Event::ConnectOutFailed(event::ConnectOutFailed {
            connecting_id: make_event_id(1),
            reason: make_reason("Connection refused"),
        }),

        26 => Event::ConnectedOut(event::ConnectedOut {
            connecting_id: make_event_id(1),
        }),

        27 => Event::Disconnected(event::Disconnected {
            peer: peer_or_fake!(sequence),
            terminator: Some(if sequence % 2 == 0 {
                ConnectionSide::Local
            } else {
                ConnectionSide::Remote
            }),
            reason: make_reason("Graceful disconnect"),
        }),

        28 => Event::PeerMisbehaved(event::PeerMisbehaved {
            peer: peer_or_fake!(sequence),
            reason: make_reason("Invalid message"),
        }),

        // ============== Tier 3: Block authoring/importing events ==============
        40 => Event::Authoring(event::Authoring {
            slot: Slot::from(sequence as u32),
            parent: HeaderHash(make_hash(0x40)),
        }),

        41 => Event::AuthoringFailed(event::AuthoringFailed {
            authoring_id: make_event_id(1),
            reason: make_reason("No tickets available"),
        }),

        42 => Event::Authored(event::Authored {
            authoring_id: make_event_id(1),
            outline: make_block_outline(),
        }),

        43 => Event::Importing(event::Importing {
            slot: Slot::from(sequence as u32),
            outline: make_block_outline(),
        }),

        44 => Event::BlockVerificationFailed(event::BlockVerificationFailed {
            importing_id: make_event_id(1),
            reason: make_reason("Invalid signature"),
        }),

        45 => Event::BlockVerified(event::BlockVerified {
            importing_id: make_event_id(1),
        }),

        46 => Event::BlockExecutionFailed(event::BlockExecutionFailed {
            authoring_or_importing_id: make_event_id(1),
            reason: make_reason("Execution error"),
        }),

        47 => Event::BlockExecuted(event::BlockExecuted {
            authoring_or_importing_id: make_event_id(1),
            accumulate_costs: BoundedVec::new(),
        }),

        // Block sync events
        60 => Event::BlockAnnouncementStreamOpened(event::BlockAnnouncementStreamOpened {
            peer: peer_or_fake!(sequence),
            opener: if sequence % 2 == 0 {
                ConnectionSide::Local
            } else {
                ConnectionSide::Remote
            },
        }),

        // NOTE: layout changed in jam-std-common 0.1.28 — the closed event now
        // references the corresponding BlockAnnouncementStreamOpened by EventId
        // instead of carrying the peer.
        61 => Event::BlockAnnouncementStreamClosed(event::BlockAnnouncementStreamClosed {
            opened_id: make_event_id(1),
            closer: ConnectionSide::Remote,
            reason: make_reason("Stream closed"),
        }),

        62 => Event::BlockAnnounced(event::BlockAnnounced {
            peer: peer_or_fake!(sequence),
            announcer: ConnectionSide::Remote,
            slot: Slot::from(sequence as u32),
            hash: HeaderHash(make_hash(0x62)),
        }),

        63 => {
            if let Some(ref mut ctx) = pair_ctx {
                let targets = ctx.registry.pick_others(ctx.my_index, ctx.pairs_per_node, rng);
                let my_peer = ctx.registry.peer_id(ctx.my_index);
                for &t in &targets {
                    schedule_receive(ctx, t, 64, TimestampedEvent {
                        timestamp,
                        event: Event::ReceivingBlockRequest(event::ReceivingBlockRequest {
                            sender: my_peer,
                        }),
                    });
                }
                let first = targets.first().copied().unwrap_or(0);
                Event::SendingBlockRequest(event::SendingBlockRequest {
                    recipient: ctx.registry.peer_id(first),
                    hash: HeaderHash(make_hash(0x63)),
                    direction: if sequence % 2 == 0 {
                        BlockRequestDirection::AscendingExclusive
                    } else {
                        BlockRequestDirection::DescendingInclusive
                    },
                    max_blocks: 10,
                })
            } else {
                Event::SendingBlockRequest(event::SendingBlockRequest {
                    recipient: make_peer_id(sequence),
                    hash: HeaderHash(make_hash(0x63)),
                    direction: if sequence % 2 == 0 {
                        BlockRequestDirection::AscendingExclusive
                    } else {
                        BlockRequestDirection::DescendingInclusive
                    },
                    max_blocks: 10,
                })
            }
        },

        64 => Event::ReceivingBlockRequest(event::ReceivingBlockRequest {
            sender: make_peer_id(sequence),
        }),

        65 => Event::BlockRequestFailed(event::BlockRequestFailed {
            request_id: make_event_id(1),
            reason: make_reason("Request timeout"),
        }),

        66 => Event::BlockRequestSent(event::BlockRequestSent {
            request_id: make_event_id(1),
        }),

        67 => Event::BlockRequestReceived(event::BlockRequestReceived {
            request_id: make_event_id(1),
            hash: HeaderHash(make_hash(0x67)),
            direction: BlockRequestDirection::DescendingInclusive,
            max_blocks: 10,
        }),

        68 => Event::BlockTransferred(event::BlockTransferred {
            request_id: make_event_id(2),
            slot: Slot::from(sequence as u32),
            outline: make_block_outline(),
            last: sequence % 3 == 0,
        }),

        // ============== Tier 4: Ticket events ==============
        80 => Event::GeneratingTickets(event::GeneratingTickets {
            epoch: (sequence / 600) as u32,
        }),

        81 => Event::TicketGenerationFailed(event::TicketGenerationFailed {
            generating_id: make_event_id(1),
            reason: make_reason("VRF failure"),
        }),

        82 => Event::TicketsGenerated(event::TicketsGenerated {
            generating_id: make_event_id(1),
            ids: BoundedVec::new(),
        }),

        83 => Event::TicketTransferFailed(event::TicketTransferFailed {
            peer: peer_or_fake!(sequence),
            sender: ConnectionSide::Local,
            from_proxy: sequence % 2 == 0,
            reason: make_reason("Transfer failed"),
        }),

        84 => Event::TicketTransferred(event::TicketTransferred {
            peer: peer_or_fake!(sequence),
            sender: ConnectionSide::Local,
            from_proxy: sequence % 2 == 0,
            epoch: (sequence / 600) as u32,
            attempt: TicketAttempt::from((sequence % 16) as u8),
            id: TicketId(make_hash(0x84)),
        }),

        // ============== Tier 5: Work package and guarantee events ==============
        90 => Event::WorkPackageSubmission(event::WorkPackageSubmission {
            builder: peer_or_fake!(sequence),
            bundle: sequence % 2 == 0,
        }),

        91 => Event::WorkPackageBeingShared(event::WorkPackageBeingShared {
            primary: peer_or_fake!(sequence),
        }),

        92 => Event::WorkPackageFailed(event::WorkPackageFailed {
            submission_or_share_id: make_event_id(1),
            reason: make_reason("Work package invalid"),
        }),

        93 => Event::DuplicateWorkPackage(event::DuplicateWorkPackage {
            submission_or_share_id: make_event_id(1),
            core: CoreIndex::from((sequence % 341) as u16),
            hash: WorkPackageHash(make_hash(0x93)),
        }),

        94 => Event::WorkPackageReceived(event::WorkPackageReceived {
            submission_or_share_id: make_event_id(1),
            core: CoreIndex::from((sequence % 341) as u16),
            outline: make_work_package_outline(),
        }),

        95 => Event::Authorized(event::Authorized {
            submission_or_share_id: make_event_id(1),
            cost: IsAuthorizedCost::default(),
        }),

        96 => Event::ExtrinsicDataReceived(event::ExtrinsicDataReceived {
            submission_or_share_id: make_event_id(1),
        }),

        97 => Event::ImportsReceived(event::ImportsReceived {
            submission_or_share_id: make_event_id(1),
        }),

        98 => {
            if let Some(ref mut ctx) = pair_ctx {
                let targets = ctx.registry.pick_others(ctx.my_index, ctx.pairs_per_node, rng);
                let my_peer = ctx.registry.peer_id(ctx.my_index);
                for &t in &targets {
                    schedule_receive(ctx, t, 91, TimestampedEvent {
                        timestamp,
                        event: Event::WorkPackageBeingShared(event::WorkPackageBeingShared {
                            primary: my_peer,
                        }),
                    });
                }
                let first = targets.first().copied().unwrap_or(0);
                Event::SharingWorkPackage(event::SharingWorkPackage {
                    submission_id: make_event_id(2),
                    secondary: ctx.registry.peer_id(first),
                })
            } else {
                Event::SharingWorkPackage(event::SharingWorkPackage {
                    submission_id: make_event_id(2),
                    secondary: make_peer_id(sequence + 1),
                })
            }
        },

        99 => Event::WorkPackageSharingFailed(event::WorkPackageSharingFailed {
            submission_id: make_event_id(2),
            secondary: peer_or_fake!(sequence + 1),
            reason: make_reason("Sharing failed"),
        }),

        100 => Event::BundleSent(event::BundleSent {
            submission_id: make_event_id(2),
            secondary: peer_or_fake!(sequence + 1),
        }),

        101 => Event::Refined(event::Refined {
            submission_or_share_id: make_event_id(2),
            costs: BoundedVec::new(),
        }),

        102 => Event::WorkReportBuilt(event::WorkReportBuilt {
            submission_or_share_id: make_event_id(2),
            outline: make_work_report_outline(),
        }),

        103 => Event::WorkReportSignatureSent(event::WorkReportSignatureSent {
            share_id: make_event_id(2),
        }),

        104 => Event::WorkReportSignatureReceived(event::WorkReportSignatureReceived {
            submission_id: make_event_id(2),
            secondary: peer_or_fake!(sequence + 1),
        }),

        105 => Event::GuaranteeBuilt(event::GuaranteeBuilt {
            submission_id: make_event_id(3),
            outline: make_guarantee_outline(),
        }),

        106 => {
            if let Some(ref mut ctx) = pair_ctx {
                let targets = ctx.registry.pick_others(ctx.my_index, ctx.pairs_per_node, rng);
                let my_peer = ctx.registry.peer_id(ctx.my_index);
                for &t in &targets {
                    schedule_receive(ctx, t, 110, TimestampedEvent {
                        timestamp,
                        event: Event::ReceivingGuarantee(event::ReceivingGuarantee {
                            sender: my_peer,
                        }),
                    });
                }
                let first = targets.first().copied().unwrap_or(0);
                Event::SendingGuarantee(event::SendingGuarantee {
                    built_id: make_event_id(1),
                    recipient: ctx.registry.peer_id(first),
                })
            } else {
                Event::SendingGuarantee(event::SendingGuarantee {
                    built_id: make_event_id(1),
                    recipient: make_peer_id(sequence + 1),
                })
            }
        },

        107 => Event::GuaranteeSendFailed(event::GuaranteeSendFailed {
            sending_id: make_event_id(1),
            reason: make_reason("Send failed"),
        }),

        108 => Event::GuaranteeSent(event::GuaranteeSent {
            sending_id: make_event_id(1),
        }),

        109 => Event::GuaranteesDistributed(event::GuaranteesDistributed {
            submission_id: make_event_id(5),
        }),

        110 => Event::ReceivingGuarantee(event::ReceivingGuarantee {
            sender: make_peer_id(sequence),
        }),

        111 => Event::GuaranteeReceiveFailed(event::GuaranteeReceiveFailed {
            receiving_id: make_event_id(1),
            reason: make_reason("Invalid guarantee"),
        }),

        112 => Event::GuaranteeReceived(event::GuaranteeReceived {
            receiving_id: make_event_id(1),
            outline: make_guarantee_outline(),
        }),

        113 => Event::GuaranteeDiscarded(event::GuaranteeDiscarded {
            outline: make_guarantee_outline(),
            reason: if sequence % 3 == 0 {
                GuaranteeDiscardReason::Superseded
            } else if sequence % 3 == 1 {
                GuaranteeDiscardReason::TooMany
            } else {
                GuaranteeDiscardReason::PackageReportedOnChain
            },
        }),

        // ============== Tier 6: Availability, auditing, segments, preimages ==============
        // Availability (120-131)
        120 => {
            if let Some(ref mut ctx) = pair_ctx {
                let targets = ctx.registry.pick_others(ctx.my_index, ctx.pairs_per_node, rng);
                let my_peer = ctx.registry.peer_id(ctx.my_index);
                for &t in &targets {
                    schedule_receive(ctx, t, 121, TimestampedEvent {
                        timestamp,
                        event: Event::ReceivingShardRequest(event::ReceivingShardRequest {
                            assurer: my_peer,
                        }),
                    });
                }
                let first = targets.first().copied().unwrap_or(0);
                Event::SendingShardRequest(event::SendingShardRequest {
                    guarantor: ctx.registry.peer_id(first),
                    erasure_root: ErasureRoot(make_hash(0x78)),
                    shard: (sequence % 1023) as u16,
                })
            } else {
                Event::SendingShardRequest(event::SendingShardRequest {
                    guarantor: make_peer_id(sequence),
                    erasure_root: ErasureRoot(make_hash(0x78)),
                    shard: (sequence % 1023) as u16,
                })
            }
        },

        121 => Event::ReceivingShardRequest(event::ReceivingShardRequest {
            assurer: make_peer_id(sequence),
        }),

        122 => Event::ShardRequestFailed(event::ShardRequestFailed {
            request_id: make_event_id(1),
            reason: make_reason("Shard request failed"),
        }),

        123 => Event::ShardRequestSent(event::ShardRequestSent {
            request_id: make_event_id(1),
        }),

        124 => Event::ShardRequestReceived(event::ShardRequestReceived {
            request_id: make_event_id(1),
            erasure_root: ErasureRoot(make_hash(0x7C)),
            shard: (sequence % 1023) as u16,
        }),

        125 => Event::ShardsTransferred(event::ShardsTransferred {
            request_id: make_event_id(2),
        }),

        126 => {
            use jam_std_common::AvailabilityStatement;
            Event::DistributingAssurance(event::DistributingAssurance {
                statement: AvailabilityStatement {
                    anchor: HeaderHash(make_hash(0x7E)),
                    bitfield: AvailabilityBitfield::new(),
                },
            })
        }

        127 => Event::AssuranceSendFailed(event::AssuranceSendFailed {
            distributing_id: make_event_id(1),
            recipient: peer_or_fake!(sequence + 1),
            reason: make_reason("Assurance send failed"),
        }),

        128 => {
            if let Some(ref mut ctx) = pair_ctx {
                let targets = ctx.registry.pick_others(ctx.my_index, ctx.pairs_per_node, rng);
                let my_peer = ctx.registry.peer_id(ctx.my_index);
                for &t in &targets {
                    schedule_receive(ctx, t, 131, TimestampedEvent {
                        timestamp,
                        event: Event::AssuranceReceived(event::AssuranceReceived {
                            sender: my_peer,
                            anchor: HeaderHash(make_hash(0x83)),
                        }),
                    });
                }
                let first = targets.first().copied().unwrap_or(0);
                Event::AssuranceSent(event::AssuranceSent {
                    distributing_id: make_event_id(1),
                    recipient: ctx.registry.peer_id(first),
                })
            } else {
                Event::AssuranceSent(event::AssuranceSent {
                    distributing_id: make_event_id(1),
                    recipient: make_peer_id(sequence + 1),
                })
            }
        },

        129 => Event::AssuranceDistributed(event::AssuranceDistributed {
            distributing_id: make_event_id(2),
        }),

        130 => Event::AssuranceReceiveFailed(event::AssuranceReceiveFailed {
            sender: peer_or_fake!(sequence),
            reason: make_reason("Invalid assurance"),
        }),

        131 => Event::AssuranceReceived(event::AssuranceReceived {
            sender: make_peer_id(sequence),
            anchor: HeaderHash(make_hash(0x83)),
        }),

        // Auditing (140-153)
        140 => {
            if let Some(ref mut ctx) = pair_ctx {
                let targets = ctx.registry.pick_others(ctx.my_index, ctx.pairs_per_node, rng);
                let my_peer = ctx.registry.peer_id(ctx.my_index);
                for &t in &targets {
                    schedule_receive(ctx, t, 141, TimestampedEvent {
                        timestamp,
                        event: Event::ReceivingBundleShardRequest(event::ReceivingBundleShardRequest {
                            auditor: my_peer,
                        }),
                    });
                }
                let first = targets.first().copied().unwrap_or(0);
                Event::SendingBundleShardRequest(event::SendingBundleShardRequest {
                    audit_id: make_event_id(5),
                    assurer: ctx.registry.peer_id(first),
                    shard: (sequence % 1023) as u16,
                })
            } else {
                Event::SendingBundleShardRequest(event::SendingBundleShardRequest {
                    audit_id: make_event_id(5),
                    assurer: make_peer_id(sequence),
                    shard: (sequence % 1023) as u16,
                })
            }
        },

        141 => Event::ReceivingBundleShardRequest(event::ReceivingBundleShardRequest {
            auditor: make_peer_id(sequence),
        }),

        142 => Event::BundleShardRequestFailed(event::BundleShardRequestFailed {
            request_id: make_event_id(1),
            reason: make_reason("Bundle shard request failed"),
        }),

        143 => Event::BundleShardRequestSent(event::BundleShardRequestSent {
            request_id: make_event_id(1),
        }),

        144 => Event::BundleShardRequestReceived(event::BundleShardRequestReceived {
            request_id: make_event_id(1),
            erasure_root: ErasureRoot(make_hash(0x90)),
            shard: (sequence % 1023) as u16,
        }),

        145 => Event::BundleShardTransferred(event::BundleShardTransferred {
            request_id: make_event_id(2),
        }),

        146 => Event::ReconstructingBundle(event::ReconstructingBundle {
            audit_id: make_event_id(5),
            kind: if sequence % 2 == 0 {
                ReconstructionKind::Trivial
            } else {
                ReconstructionKind::NonTrivial
            },
        }),

        147 => Event::BundleReconstructed(event::BundleReconstructed {
            audit_id: make_event_id(5),
        }),

        148 => {
            if let Some(ref mut ctx) = pair_ctx {
                let targets = ctx.registry.pick_others(ctx.my_index, ctx.pairs_per_node, rng);
                let my_peer = ctx.registry.peer_id(ctx.my_index);
                for &t in &targets {
                    schedule_receive(ctx, t, 149, TimestampedEvent {
                        timestamp,
                        event: Event::ReceivingBundleRequest(event::ReceivingBundleRequest {
                            auditor: my_peer,
                        }),
                    });
                }
                let first = targets.first().copied().unwrap_or(0);
                Event::SendingBundleRequest(event::SendingBundleRequest {
                    audit_id: make_event_id(5),
                    guarantor: ctx.registry.peer_id(first),
                })
            } else {
                Event::SendingBundleRequest(event::SendingBundleRequest {
                    audit_id: make_event_id(5),
                    guarantor: make_peer_id(sequence),
                })
            }
        },

        149 => Event::ReceivingBundleRequest(event::ReceivingBundleRequest {
            auditor: make_peer_id(sequence),
        }),

        150 => Event::BundleRequestFailed(event::BundleRequestFailed {
            request_id: make_event_id(1),
            reason: make_reason("Bundle request failed"),
        }),

        151 => Event::BundleRequestSent(event::BundleRequestSent {
            request_id: make_event_id(1),
        }),

        152 => Event::BundleRequestReceived(event::BundleRequestReceived {
            request_id: make_event_id(1),
            erasure_root: ErasureRoot(make_hash(0x98)),
        }),

        153 => Event::BundleTransferred(event::BundleTransferred {
            request_id: make_event_id(2),
        }),

        // Segments (160-178)
        160 => Event::WorkPackageHashMapped(event::WorkPackageHashMapped {
            submission_id: make_event_id(5),
            work_package_hash: WorkPackageHash(make_hash(0xA0)),
            segments_root: SegmentTreeRoot(make_hash(0xA1)),
        }),

        161 => Event::SegmentsRootMapped(event::SegmentsRootMapped {
            submission_id: make_event_id(5),
            segments_root: SegmentTreeRoot(make_hash(0xA1)),
            erasure_root: ErasureRoot(make_hash(0xA2)),
        }),

        162 => {
            if let Some(ref mut ctx) = pair_ctx {
                let targets = ctx.registry.pick_others(ctx.my_index, ctx.pairs_per_node, rng);
                let my_peer = ctx.registry.peer_id(ctx.my_index);
                for &t in &targets {
                    schedule_receive(ctx, t, 163, TimestampedEvent {
                        timestamp,
                        event: Event::ReceivingSegmentShardRequest(event::ReceivingSegmentShardRequest {
                            sender: my_peer,
                            proofs: sequence % 2 == 0,
                        }),
                    });
                }
                let first = targets.first().copied().unwrap_or(0);
                Event::SendingSegmentShardRequest(event::SendingSegmentShardRequest {
                    submission_id: make_event_id(5),
                    assurer: ctx.registry.peer_id(first),
                    proofs: sequence % 2 == 0,
                    shards: BoundedVec::new(),
                })
            } else {
                Event::SendingSegmentShardRequest(event::SendingSegmentShardRequest {
                    submission_id: make_event_id(5),
                    assurer: make_peer_id(sequence),
                    proofs: sequence % 2 == 0,
                    shards: BoundedVec::new(),
                })
            }
        },

        163 => Event::ReceivingSegmentShardRequest(event::ReceivingSegmentShardRequest {
            sender: make_peer_id(sequence),
            proofs: sequence % 2 == 0,
        }),

        164 => Event::SegmentShardRequestFailed(event::SegmentShardRequestFailed {
            request_id: make_event_id(1),
            reason: make_reason("Segment shard request failed"),
        }),

        165 => Event::SegmentShardRequestSent(event::SegmentShardRequestSent {
            request_id: make_event_id(1),
        }),

        166 => Event::SegmentShardRequestReceived(event::SegmentShardRequestReceived {
            request_id: make_event_id(1),
            num: (sequence % 100) as u16,
        }),

        167 => Event::SegmentShardsTransferred(event::SegmentShardsTransferred {
            request_id: make_event_id(2),
        }),

        168 => Event::ReconstructingSegments(event::ReconstructingSegments {
            submission_id: make_event_id(5),
            segments: BoundedVec::new(),
            kind: if sequence % 2 == 0 {
                ReconstructionKind::Trivial
            } else {
                ReconstructionKind::NonTrivial
            },
        }),

        169 => Event::SegmentReconstructionFailed(event::SegmentReconstructionFailed {
            reconstructing_id: make_event_id(1),
            reason: make_reason("Segment reconstruction failed"),
        }),

        170 => Event::SegmentsReconstructed(event::SegmentsReconstructed {
            reconstructing_id: make_event_id(1),
        }),

        171 => Event::SegmentVerificationFailed(event::SegmentVerificationFailed {
            submission_id: make_event_id(5),
            segments: BoundedVec::new(),
            reason: make_reason("Segment verification failed"),
        }),

        172 => Event::SegmentsVerified(event::SegmentsVerified {
            submission_id: make_event_id(5),
            segments: BoundedVec::new(),
        }),

        173 => {
            if let Some(ref mut ctx) = pair_ctx {
                let targets = ctx.registry.pick_others(ctx.my_index, ctx.pairs_per_node, rng);
                let my_peer = ctx.registry.peer_id(ctx.my_index);
                for &t in &targets {
                    schedule_receive(ctx, t, 174, TimestampedEvent {
                        timestamp,
                        event: Event::ReceivingSegmentRequest(event::ReceivingSegmentRequest {
                            guarantor: my_peer,
                        }),
                    });
                }
                let first = targets.first().copied().unwrap_or(0);
                Event::SendingSegmentRequest(event::SendingSegmentRequest {
                    submission_id: make_event_id(5),
                    prev_guarantor: ctx.registry.peer_id(first),
                    segments: BoundedVec::new(),
                })
            } else {
                Event::SendingSegmentRequest(event::SendingSegmentRequest {
                    submission_id: make_event_id(5),
                    prev_guarantor: make_peer_id(sequence),
                    segments: BoundedVec::new(),
                })
            }
        },

        174 => Event::ReceivingSegmentRequest(event::ReceivingSegmentRequest {
            guarantor: make_peer_id(sequence),
        }),

        175 => Event::SegmentRequestFailed(event::SegmentRequestFailed {
            request_id: make_event_id(1),
            reason: make_reason("Segment request failed"),
        }),

        176 => Event::SegmentRequestSent(event::SegmentRequestSent {
            request_id: make_event_id(1),
        }),

        177 => Event::SegmentRequestReceived(event::SegmentRequestReceived {
            request_id: make_event_id(1),
            num: (sequence % 100) as u16,
        }),

        178 => Event::SegmentsTransferred(event::SegmentsTransferred {
            request_id: make_event_id(2),
        }),

        // Preimages (190-199)
        190 => Event::PreimageAnnouncementFailed(event::PreimageAnnouncementFailed {
            peer: peer_or_fake!(sequence),
            announcer: ConnectionSide::Local,
            reason: make_reason("Announcement failed"),
        }),

        191 => Event::PreimageAnnounced(event::PreimageAnnounced {
            peer: peer_or_fake!(sequence),
            announcer: ConnectionSide::Remote,
            service: ServiceId::from(sequence as u32),
            hash: make_hash(0xBF),
            length: (1000 + sequence % 10000) as u32,
        }),

        192 => Event::AnnouncedPreimageForgotten(event::AnnouncedPreimageForgotten {
            service: ServiceId::from(sequence as u32),
            hash: make_hash(0xC0),
            length: (1000 + sequence % 10000) as u32,
            reason: if sequence % 3 == 0 {
                AnnouncedPreimageForgetReason::ProvidedOnChain
            } else if sequence % 3 == 1 {
                AnnouncedPreimageForgetReason::NotRequestedOnChain
            } else {
                AnnouncedPreimageForgetReason::TooMany
            },
        }),

        193 => {
            if let Some(ref mut ctx) = pair_ctx {
                let targets = ctx.registry.pick_others(ctx.my_index, ctx.pairs_per_node, rng);
                let my_peer = ctx.registry.peer_id(ctx.my_index);
                for &t in &targets {
                    schedule_receive(ctx, t, 194, TimestampedEvent {
                        timestamp,
                        event: Event::ReceivingPreimageRequest(event::ReceivingPreimageRequest {
                            sender: my_peer,
                        }),
                    });
                }
                let first = targets.first().copied().unwrap_or(0);
                Event::SendingPreimageRequest(event::SendingPreimageRequest {
                    recipient: ctx.registry.peer_id(first),
                    hash: make_hash(0xC1),
                })
            } else {
                Event::SendingPreimageRequest(event::SendingPreimageRequest {
                    recipient: make_peer_id(sequence),
                    hash: make_hash(0xC1),
                })
            }
        },

        194 => Event::ReceivingPreimageRequest(event::ReceivingPreimageRequest {
            sender: make_peer_id(sequence),
        }),

        195 => Event::PreimageRequestFailed(event::PreimageRequestFailed {
            request_id: make_event_id(1),
            reason: make_reason("Preimage request failed"),
        }),

        196 => Event::PreimageRequestSent(event::PreimageRequestSent {
            request_id: make_event_id(1),
        }),

        197 => Event::PreimageRequestReceived(event::PreimageRequestReceived {
            request_id: make_event_id(1),
            hash: make_hash(0xC5),
        }),

        198 => Event::PreimageTransferred(event::PreimageTransferred {
            request_id: make_event_id(2),
            length: (1000 + sequence % 10000) as u32,
        }),

        199 => Event::PreimageDiscarded(event::PreimageDiscarded {
            hash: make_hash(0xC7),
            length: (1000 + sequence % 10000) as u32,
            reason: if sequence % 3 == 0 {
                PreimageDiscardReason::ProvidedOnChain
            } else if sequence % 3 == 1 {
                PreimageDiscardReason::NotRequestedOnChain
            } else {
                PreimageDiscardReason::TooMany
            },
        }),

        _ => anyhow::bail!("Unknown event ID {}", event_id),
    };

    Ok(TimestampedEvent { timestamp, event })
}
