# Модель эмиссии JIP-3-телеметрии в polkajam

Анализ репозитория `/home/claude/jam/polkajam` (read-only). Цель — точная модель того, как настоящая нода генерирует JIP-3 телеметрию, для написания правдоподобного синтетического генератора.

## 0. Какой крейт является источником истины

Актуальный источник — **локальный** крейт `/home/claude/jam/polkajam/crates/jam-std-common/src/telemetry.rs` (версия 0.1.28, path-зависимость в `/home/claude/jam/polkajam/Cargo.toml:91`), а **не** `~/.cargo/registry/.../jam-std-common-0.1.26`. Отличий два, но оба ломающие:

* `PROTOCOL_VERSION: u8 = 1` (в 0.1.26 было `0`) — `crates/jam-std-common/src/telemetry.rs:19`
* событие **61 `BlockAnnouncementStreamClosed`** имеет поле `opened_id: EventId` вместо `peer: PeerId` — `crates/jam-std-common/src/telemetry.rs:619-624`

Всё остальное (дискриминаторы, поля структур) идентично. Также в локальном `crates/jam-types/src/simple.rs`: `GP_VERSION = "0.7.2"` (было 0.7.1), `max_input = 13_791_360` (было 13_794_305), добавлен вариант `ProtocolParameters::mini()`.

---

## 1. Механика EventId и ссылок

### 1.1 Присвоение ID

ID **неявный**: он не передаётся по проводу, а вычисляется получателем как счётчик событий на соединение. Правило (`crates/jam-std-common/src/telemetry.rs:54-60`):

* первое событие в соединении получает ID 0;
* событие, следующее за событием `E`, получает `ID(E) + N`, где `N = num` если `E` — это `Dropped`, иначе `1`.

```rust
// crates/jam-std-common/src/telemetry.rs:1176-1182
pub fn next_id(&self, id: EventId) -> EventId {
    let inc = match self { Self::Dropped(dropped) => dropped.num, _ => 1 };
    EventId(id.0 + inc)
}
```

`Dropped{num: N}` «занимает» N идентификаторов — так нумерация остаётся согласованной с тем, что нода реально сгенерировала.

### 1.2 Формат провода

`crates/node/src/telemetry.rs:175-189, 213-255`. По TCP: сначала одно сообщение `(PROTOCOL_VERSION: u8, NodeInfo)`, далее поток `TimestampedEvent { timestamp: Timestamp, event: Event }`. Каждое сообщение предваряется **u32 little-endian длиной** тела; тело — SCALE. `Timestamp` — микросекунды от JAM Common Era (`JAM_COMMON_ERA = 1_735_732_800`, 2025-01-01 12:00 UTC), тип u64.

### 1.3 Событие Dropped (тип 0)

Не эмитится макросом `event!` (в `event_internal` стоит `debug_assert!(!matches!(event, Event::Dropped(_)))`). Его порождает **только транспорт телеметрии**, в двух местах:

1. **Переполнение очереди** — `crates/node/src/telemetry.rs:96-109`. Очередь `REMOTE_EVENT_QUEUE` ограничена `MAX_REMOTE_EVENT_QUEUE_LEN = 200` (`:19`). При переполнении снимаются два самых старых события и заменяются одним `Dropped { timestamp: <ts первого>, last_timestamp: <ts второго>, num: 2 }`; если первым уже был `Dropped`, то `num = dropped.num + 1`. `Dropped` всегда стоит в голове очереди и «съедает» её начало.
2. **Переподключение** — `crates/node/src/telemetry.rs:226-248`. Если после реконнекта локальный `ids.start != 0`, отправляется `Dropped { timestamp: created_at, last_timestamp: <ts первого отправляемого>, num: ids.start.0 }` — закрывает разрыв между нулём (с которого сервер начнёт нумерацию) и фактическим счётчиком.

Семантика `num` — **число пропущенных событий**, а не число «дропов». `timestamp` самого `Dropped` — время первого пропущенного, `last_timestamp` — время последнего пропущенного.

Практическое следствие: `Dropped` реалистичен только как реакция на всплеск (>200 событий между флашами) или в самом начале соединения. Транспорт флашит после каждой пачки (`telemetry.rs:252-253`), поэтому при живом сервере дропы редки.

### 1.4 Поля-ссылки (`*_id`)

Все `*_id` — это `EventId` **родительского события в том же соединении**. Возвращаемое значение макроса `event!` и есть ID:

```rust
// crates/node/src/telemetry.rs:122-136 — макрос возвращает EventId
let authoring_id = event!(Authoring { slot, parent });
```

Важно: когда телеметрия выключена (`consumers() == 0`) или включён только лог, макрос возвращает `EventId(0)` (`crates/node/src/telemetry.rs:118, 133`). Отсюда — `EventId(0)` в дампе может быть как честной ссылкой на первое событие, так и заглушкой.

Именованные ссылки: `connecting_id`, `authoring_id`, `importing_id`, `authoring_or_importing_id`, `request_id`, `generating_id`, `submission_id`, `share_id`, `submission_or_share_id`, `built_id`, `sending_id`, `receiving_id`, `distributing_id`, `reconstructing_id`, `opened_id`, `audit_id`.

### 1.5 Отдельно про `audit_id`

События 140 (`SendingBundleShardRequest`), 146 (`ReconstructingBundle`), 147 (`BundleReconstructed`), 148 (`SendingBundleRequest`) содержат `audit_id`, который **всегда равен `EventId(0)`**. Единственный продакшен-вызов передаёт литерал:

```rust
// crates/node/src/auditing/controller.rs:175-182
// TODO @dave: Telemetry events for auditing
let (bundle, shard_count) = match recovery
    .recover_bundle(&report, request.assurance_epoch, jam_std_common::telemetry::EventId(0))
```

Аудит как таковой (объявления траншей, вердикты, диспуты) телеметрию **не эмитит вовсе** — только восстановление бандла. Генератор обязан ставить `audit_id = 0`.

---

## 2. Тайминг и параметры протокола

`ProtocolParameters` — `crates/jam-types/src/simple.rs:246-317`, конструкторы `full()` (:320), `mini()` (:357), `tiny()` (:367). Выбор варианта — **по числу генезис-валидаторов в chain-spec** (`crates/node/src/chainspec.rs:393-401`): 6 → tiny, 78 → mini, 1023 → full. Применение — `ProtocolParameters::apply()` в глобальные атомики; чтение — свободные функции `val_count()`, `epoch_period()` и т.д.

### 2.1 Таблица параметров

| Поле (GP) | tiny | mini | full | Смысл |
|---|---|---|---|---|
| `val_count` (V) | **6** | **78** | **1023** | валидаторов |
| `core_count` (C) | **2** | **26** | **341** | кор; всегда `V/3` |
| `VALS_PER_CORE` | 3 | 3 | 3 | константа (`simple.rs:193`) |
| `slot_period_sec` (P) | **6** | 6 | **6** | длительность слота, с |
| `epoch_period` (E) | **12** | **600** | **600** | слотов в эпохе (72 с / 60 мин) |
| `epoch_tail_start` (Y) | **10** | 500 | 500 | конец лотереи тикетов |
| `rotation_period` (R) | **4** | **10** | **10** | слотов в ротации гуарантов |
| `tickets_attempts_number` (N) | **3** | **16** | **2** | тикетов на валидатора за эпоху |
| `max_tickets_per_block` (K) | **3** | 16 | **16** | тикетов в экстринсике блока |
| `basic_piece_len` (We) | **4** | **54** | **684** | байт в базовом куске |
| `segment_piece_count` (Wp) | **1026** | **76** | **6** | кусков в сегменте |
| SEGMENT_LEN (произв.) | 4104 | 4104 | 4104 | всегда 4104 |
| `recovery_threshold` (произв.) | **2** | **27** | **342** | `basic_piece_len / POINT_LEN`, POINT_LEN=2 |
| `availability_timeout` (U) | 5 | 5 | 5 | слотов |
| `max_lookup_anchor_age` (L) | **24** | 14400 | **14400** | слотов |
| `min_turnaround_period` (D) | **32** | 19200 | **19200** | слотов |
| `recent_block_count` (H) | 8 | 8 | 8 | блоков в кэше |
| `max_work_items` (I) | 16 | 16 | 16 | work items в WP |
| `max_dependencies` (J) | 8 | 8 | 8 | |
| `max_extrinsics` (T) | 128 | 128 | 128 | |
| `max_imports` (Wm) | 3072 | 3072 | 3072 | `MaxImportSegments = 2×max_imports = 6144` |
| `max_exports` (Wx) | 3072 | 3072 | 3072 | |
| `max_input` (Wb) | 13 791 360 | " | 13 791 360 | |
| `block_gas_limit` (Gt) | **20 000 000** | 3.5e9 | **3 500 000 000** | |
| `max_refine_gas` (Gr) | **1 000 000 000** | 5e9 | **5 000 000 000** | |
| `max_is_authorized_gas` (Gi) | 50 000 000 | " | 50 000 000 | |
| `max_accumulate_gas` (Ga) | 10 000 000 | " | 10 000 000 | |
| `max_report_elective_data` (Wr) | 49 152 | " | 49 152 | 48 KiB |
| `auth_window` (O) / `auth_queue_len` (Q) | 8 / 80 | 8 / 80 | 8 / 80 | |
| `GP_VERSION` | — | — | **"0.7.2"** | `crates/jam-types/src/simple.rs:9` |

Базовые типы (`crates/jam-types/src/simple.rs:47, 520-535`): `Slot = u32`, `ValIndex = u16`, `CoreIndex = u16`, `ServiceId = u32`, `TicketAttempt = u8`, `UnsignedGas = u64`, `ShardIndex = u16` (в telemetry.rs:81).

Валидация параметров (`simple.rs:415-441`): `val_count == core_count * 3`, `rotation_period` делит `epoch_period`, `epoch_tail_start < epoch_period`, `basic_piece_len` чётный и делит 4104.

### 2.2 Периодические события

Все тайминги отсчитываются от JAM Common Era, класс `PeriodicEvent` (`crates/node/src/util/periodic_event.rs`):

| Событие | Смещение / период | tiny | full |
|---|---|---|---|
| `authoring()` (:104) | `After(0)`, период 1 слот | каждые 6 с на границе слота | каждые 6 с |
| `ticketing(offset)` (:109) | `After(epoch_buffer_slots)`, период E | epoch-слот **1** | epoch-слот **20** |
| …затем `sleep(connect_buffer_slots)` (:378) | | +1 слот → **слот 2** | +10 слотов → **слот 30** |
| `start_guaranteeing()` (:118) | `Before(1 слот)`, период R | за 6 с до границы ротации (каждые 24 с) | каждые 60 с |
| `stop_guaranteeing_prev()` (:127) | `Before(0)`, период R | ровно на границе ротации | " |
| `start_building()` (:135) | `Before(6 с − 250 мс)`, период R | за 5.75 с до ротации | " |
| `net_update()` (:148) | `Before(E − E/30)`, период E | epoch-слот **1** | epoch-слот **20** (=120 с) |
| `permit_judgments()` (:167) | `Before(2 слота)`, период E | epoch-слот **10** | epoch-слот **598** |
| `start_announcing_preimages()` (:192) | `Before(1 слот)`, период E | epoch-слот **11** | epoch-слот **599** |
| `send_assurances()` (:184) | `Before(2 с)`, период 1 слот | за 2 с до каждой границы слота | " |
| `start_tranche(initiator_slot)` (:177) | `After(slots(initiator_slot))`, период **8 с** | транш k в T(slot)+8k с | " |

Константы: `DELAY_BETWEEN_TRANCHES = 8 s` (`periodic_event.rs:11`), `SEND_ASSURANCE_BEFORE = 2 s` (:15), `MAX_CLOCK_DRIFT = 250 ms` (:18).

`epoch_buffer_slots()` = `E/30` если `E>30`, иначе 1; `connect_buffer_slots()` = `E/60` если `E>60`, иначе 1 (`crates/node/src/chain/authoring/ticket_communication.rs:68-86`).

**Status шлётся не по слотам, а по таймеру: `STATUS_UPDATE_PERIOD = 2 s`** (`crates/node/src/node/mod.rs:56, 189`), с `MissedTickBehavior::Delay`. То есть 3 Status на 6-секундный слот, но фаза со слотами не синхронизирована и дрейфует.

### 2.3 Аудит и сетевые таймауты

Аудит: `MAX_TRANCHE_OFFSET = 8` (`crates/node/src/auditing/mod.rs:54`), `initial_tranche_cores() = 10` в продакшен-сборке (`crates/node/src/auditing/block_tracker.rs:59-64`) — в tiny с C=2 это значит «все коры выбираются в транше 0». Отбор отчётов в последующие транши — `ProductionReportSelector::select_report_for_tranche` (`block_tracker.rs:41-56`): включаем отчёт, если `entropy[0] * val_count < count_no_shows * 256 * F`.

Таймауты: `WORK_PACKAGE_SUBMISSION_TIMEOUT = 3 s`, `RECEIVE_WORK_PACKAGE_TIMEOUT = GET_ONE_SIGNATURE_TIMEOUT = 6 s`, `RECEIVE_GUARANTEE_TIMEOUT = 499 ms` (`crates/node/src/chain/guarantors/protocol.rs:108-137`), `SEND_GUARANTEE_TIMEOUT = 3000 ms` (`guarantee_broadcaster.rs:26`), `TICKET_SEND/RECV_TIMEOUT = 5 s` (`ticket_communication.rs:46-48`), `TIMEOUT_START_NEW_REQUESTS = 1 s` (`availability/recovery/state.rs:30`), `MAX_BROADCAST_OUTGOING_STREAMS = 10`, `MAX_CONCURRENT_SIGNATURE_MERGERS = 1` (`guarantor.rs:383-387`).

---

## 3. Каузальные цепочки

Роли: **A** — автор слота, **I** — импортёр, **G1** — primary-гуарант, **G2** — secondary-гуарант, **Val** — любой валидатор, **Asr** — ассурер, **Aud** — аудитор, **Any** — любая нода.

### 3.1 Авторинг (только автор слота)

```
Authoring(40) → Authored(42) → BlockExecuted(47, authoring_or_importing_id=authoring_id)
             ↘ AuthoringFailed(41)          ↘ BlockExecutionFailed(46)
```

`crates/node/src/chain/authoring/mod.rs:542-553, 584-597`. `Authoring` эмитится **только после** успешного `try_claim_slot` (:502), то есть один раз за слот на всю сеть. Автор **не** эмитит `Importing`/`BlockVerified` — он идёт по ветке `StateAction::ApplyChanges` (`chain/mod.rs:621`).

### 3.2 Импорт (все остальные ноды)

```
Importing(43) → BlockVerified(45) → BlockExecuted(47, authoring_or_importing_id=importing_id)
             ↘ BlockVerificationFailed(44)  ↘ BlockExecutionFailed(46)
```

`crates/node/src/chain/mod.rs:572-611`. Независимо: `BestBlockChanged(11)` (`chain/leaves.rs:21, 41` — только при реальной смене лучшего блока, плюс один раз при старте) и `FinalizedBlockChanged(12)` (`chain/mod.rs:466, 780`). `SyncStatusChanged(13)` — при старте `synced:false` (`chain/mod.rs:443`) и при каждой смене (`:534`).

### 3.3 Анонсы блоков и синк

```
BlockAnnouncementStreamOpened(60) ⟶ [BlockAnnounced(62) × N] ⟶ BlockAnnouncementStreamClosed(61, opened_id)
```

`crates/node/src/sync/mod.rs:396-419`. `BlockAnnounced` — по одному на направление и на пира: `announcer=Remote` при приёме (:268), `announcer=Local` при отправке (:338). Есть LRU `known_blocks` (`PEER_KNOWN_BLOCK_LIMIT`), поэтому один блок анонсируется пиру не более раза. Событие 61 не эмитится при разрыве соединения (`StreamErr::Disconnected`/`Misbehaviour` → `closer = None`).

Запрос блоков, инициатор (`sync/mod.rs:602-652`):

```
SendingBlockRequest(63) → BlockRequestSent(66) → BlockTransferred(68) × K (последний с last=true)
                                               ↘ BlockRequestFailed(65)
```

Приёмная сторона (`sync/request_handler.rs:147-186`):

```
ReceivingBlockRequest(64) → BlockRequestReceived(67) → BlockTransferred(68) × K → [BlockRequestFailed(65)]
```

### 3.4 Тикеты (по эпохам)

```
GeneratingTickets(80, epoch=cur/E+1) → TicketsGenerated(82, ids: N штук)
                                     ↘ TicketGenerationFailed(81)
далее на каждый тикет:
  TicketTransferred(84, sender=Local,  from_proxy=false)  [автор → прокси]
  TicketTransferred(84, sender=Remote, from_proxy=false)  [на прокси]
  TicketTransferred(84, sender=Local,  from_proxy=true)   [прокси → активные валидаторы]
  TicketTransferred(84, sender=Remote, from_proxy=true)   [на потенциальном авторе]
```

`crates/node/src/chain/authoring/ticket_communication.rs:404-420` (генерация), `:133-140` (Local), `:190-197` (Remote). Эмитят **только валидаторы следующей эпохи** (`waiting_validators`, :393-399); один раз за эпоху, в epoch-слоте ~2 (tiny) / ~30 (full). Прокси выбирается детерминированно от `TicketId` — `proxy_index(id)` (:437); если прокси это сам автор, тикет уходит в `forward_queue` и рассылается всем активным валидаторам (:461-498).

### 3.5 Work-package → guarantee (главная цепочка)

Билдер шлёт WP **ровно одному** гуаранту: `submit_to_guarantors` возвращает `Ok` после первой успешной отправки (`crates/node/src/chain/guarantors/work_package_submitter.rs:240-265`). Этот гуарант — «primary». Сам билдер JIP-3 событий не эмитит.

**Primary-гуарант** (`node/stream_handler.rs:126, 138`; `chain/guarantors/*`):

```
WorkPackageSubmission(90, builder, bundle=false|true)
 → WorkPackageReceived(94, core, WorkPackageOutline)      [stream_receiver.rs:177 / :225]
 → ExtrinsicDataReceived(96)                              [stream_receiver.rs:141]
 → [если bundle=true: ImportsReceived(97) прямо тут       [stream_receiver.rs:270]]
 → Authorized(95, IsAuthorizedCost)                       [validator.rs:64]
 → [если bundle=false — сбор импортов:
      WorkPackageHashMapped(160) × (indirect-роуты)       [fetch_imports.rs:114]
      … события 162-178 (см. 3.7) …
      SegmentsVerified(172)                               [fetch_imports.rs:68]
      ImportsReceived(97)                                 [fetch_imports.rs:87]]
 → ‖ параллельно:
     ветка refine:   Refined(101, RefineCosts) → WorkReportBuilt(102, WorkReportOutline)
     ветка sharing:  SharingWorkPackage(98, secondary) ×2 → BundleSent(100, secondary) ×2
                      → WorkReportSignatureReceived(104, secondary) ×2
 → GuaranteeBuilt(105, GuaranteeOutline)                  [signatures_merger.rs:89]
 → SendingGuarantee(106, built_id, recipient) × R → GuaranteeSent(108, sending_id) × R
                                                  ↘ GuaranteeSendFailed(107)
 → GuaranteesDistributed(109, submission_id)              [guarantee_broadcaster.rs:105]
```

Отказы в любой точке → `WorkPackageFailed(92, submission_or_share_id)`; дубликат → `DuplicateWorkPackage(93)` (`guarantor.rs:126, 134`).

**Secondary-гуаранты (2 шт.)** (`node/stream_handler.rs:149`; `stream_receiver.rs:289-395`):

```
WorkPackageBeingShared(91, primary)
 → WorkPackageReceived(94) → ExtrinsicDataReceived(96) → ImportsReceived(97)  [все в :306-319]
 → Authorized(95) → Refined(101) → WorkReportBuilt(102)
 → WorkReportSignatureSent(103, share_id)                                     [:357]
```

Secondary **не** эмитит `GuaranteeBuilt`/`SendingGuarantee`: `GuaranteeBuilt(105)` из `stream_receiver.rs:387` и рассылка от всех троих закрыты фичей `send-fully-signed-work-report-spec-back`, которая **не входит в default** (`crates/node/Cargo.toml:97, 104`). При сборке по умолчанию `fix_guarantors` (`guarantee_broadcaster.rs:203-211`) схлопывает список гуарантов до одного, и рассылает только primary — всем валидаторам эпохи, кроме гуарантов, то есть **R = V − 1** (в default-сборке `NUM_GUARANTORS=1`, поэтому исключается только сам primary).

`GUARANTEE_MIN_SIGNATURES = (3×2).div_ceil(3) = 2` (`crates/jam-std-common/src/simple.rs:64`) — primary отдаёт guarantee, набрав ≥2 подписи из 3.

**Прочие валидаторы (V−3)** (`node/stream_handler.rs:160`; `stream_receiver.rs:397-436`):

```
ReceivingGuarantee(110, sender) → GuaranteeReceived(112, receiving_id, GuaranteeOutline)
                                ↘ GuaranteeReceiveFailed(111)
                                → [позже] GuaranteeDiscarded(113, outline, reason)
```

`GuaranteeDiscarded` также из пула: `chain/guarantors/table.rs:138` (маппинг `DiscardReason` → `GuaranteeDiscardReason`: RefinedRecently→PackageReportedOnChain, BadAnchor→CannotReportOnChain, Superseded→Superseded, TooMany→TooMany, Other→Other) и `:379` (дубликат с ≤ подписями → Superseded).

### 3.6 Доступность: шарды и ассуранс

Ассурер тянет свой шард у гуаранта кора; гуарант выбирается **случайно** из тройки, при ошибке берётся следующий (`availability/distribution/background.rs:171-222`):

```
[инициатор]  SendingShardRequest(120, guarantor, erasure_root, shard) → ShardsTransferred(125)
                                                                      ↘ ShardRequestFailed(122)
[гуарант]    ReceivingShardRequest(121, assurer) → ShardRequestReceived(124, erasure_root, shard)
                                                 → ShardsTransferred(125)
```

`availability/net/mod.rs:1160-1171`, `:273`, `:341`; приёмная сторона открывается в `node/stream_handler.rs:56`. Событие `ShardRequestSent(123)` — `net/mod.rs:925`.

Ассуранс — раз в слот, за 2 с до границы (`availability/net/mod.rs:1354-1375`):

```
DistributingAssurance(126, AvailabilityStatement{anchor, bitfield})
 → AssuranceSent(128, distributing_id, recipient) × (V−1)
   ↘ AssuranceSendFailed(127, recipient)
 → AssuranceDistributed(129, distributing_id)
```

Получатели — **все валидаторы эпохи, кроме себя** (`assurance_tracker.rs:124-131`). На приёмнике: `AssuranceReceived(131, sender, anchor)` (`net/mod.rs:648`) либо `AssuranceReceiveFailed(130)`.

### 3.7 Восстановление сегментов (импорты гуаранта, 160-178)

Порядок стратегий (`availability/recovery/mod.rs:423-486`): локальный кэш/стор → полные данные у гуарантов → систематические шарды → обычные шарды → шарды у гуарантов.

```
[у гуаранта]  SendingSegmentRequest(173, submission_id, prev_guarantor, segments)
               → SegmentRequestSent(176) → SegmentsTransferred(178)
               ↘ SegmentRequestFailed(175)
[у ассуреров] SendingSegmentShardRequest(162, submission_id, assurer, proofs, shards)
               → SegmentShardRequestSent(165) → SegmentShardsTransferred(167)
               ↘ SegmentShardRequestFailed(164)
               → ReconstructingSegments(168, segments, kind) → SegmentsReconstructed(170)
                                                             ↘ SegmentReconstructionFailed(169)
               → SegmentsVerified(172, submission_id, segments)
```

Приёмные стороны: `ReceivingSegmentRequest(174)`/`SegmentRequestReceived(177, num)`, `ReceivingSegmentShardRequest(163, proofs)`/`SegmentShardRequestReceived(166, num)` (`node/stream_handler.rs:60-69`, `availability/net/mod.rs:458, 530`).

`ReconstructionKind::Trivial` — систематические шарды или шарды от гуарантов (`from_systematic.rs:153`, `from_guarantors.rs:148`); `NonTrivial` — обычные (`from_regular.rs:164`).

### 3.8 Аудит: восстановление бандла (140-153)

Всегда с `audit_id = 0`. Порядок (`availability/recovery/mod.rs:640-696`):

```
1) кэш / локальный стор                → событий нет
2) SendingBundleRequest(148, audit_id=0, guarantor) → BundleRequestSent(151) → BundleTransferred(153)
                                                    ↘ BundleRequestFailed(150)
3) локальный шард                      → событий нет
4) SendingBundleShardRequest(140, audit_id=0, assurer, shard) → BundleShardRequestSent(143)
                                                              → BundleShardTransferred(145)
                                                              ↘ BundleShardRequestFailed(142)
   → ReconstructingBundle(146, audit_id=0, kind) → BundleReconstructed(147, audit_id=0)
```

Приёмные стороны: `ReceivingBundleRequest(149)`/`BundleRequestReceived(152, erasure_root)`, `ReceivingBundleShardRequest(141)`/`BundleShardRequestReceived(144, erasure_root, shard)` (`node/stream_handler.rs:58, 67`; `availability/net/mod.rs:229, 275`).

Порог: нужно `recovery_threshold()` шардов = 2 (tiny) / 27 (mini) / 342 (full) (`crates/node/src/util/erasure.rs:34-36`).

### 3.9 Прообразы (190-199)

```
PreimageAnnounced(191, peer, announcer, service, hash, length)  [gossip/mod.rs:134 Remote, :214 Local]
 ↘ PreimageAnnouncementFailed(190)
SendingPreimageRequest(193, recipient, hash) → PreimageRequestSent(196) → PreimageTransferred(198, length)
                                             ↘ PreimageRequestFailed(195)
ReceivingPreimageRequest(194, sender) → PreimageRequestReceived(197, hash) → PreimageTransferred(198)
AnnouncedPreimageForgotten(192, service, hash, length, reason)  [requester.rs:83, 319, 338, 417]
PreimageDiscarded(199, hash, length, reason)                    [preimages/table.rs:162]
```

Анонсы стартуют по `start_announcing_preimages()` — epoch-слот E−1. `PreimageDiscardReason`: ProvidedOnChain / NotRequestedOnChain / TooMany / Other. `AnnouncedPreimageForgetReason`: ProvidedOnChain / NotRequestedOnChain / FailedToAcquire / TooMany / BadLength / Other.

### 3.10 Сеть (20-28)

```
[входящие]  ConnectingIn(21, from) → ConnectedIn(23, connecting_id, peer_id)
                                   ↘ ConnectInFailed(22, connecting_id)
            ConnectionRefused(20, from)  — при cooldown, без ConnectingIn
[исходящие] ConnectingOut(24, to: PeerDetails) → ConnectedOut(26, connecting_id)
                                               ↘ ConnectOutFailed(25)
Disconnected(27, peer, terminator: Option<ConnectionSide>, reason)
PeerMisbehaved(28, peer, reason)
```

`peer_manager/mod.rs:706-797`, `peer.rs:389`, `auxiliary.rs:220`, `rating.rs:177`. Валидатор-валидатор — всегда `ConnectionClass::Mandatory`, то есть **полная сетка**: каждый валидатор держит соединение со всеми остальными (`peer_manager/topology.rs:88-89`). Сторону инициации решает детерминированный тайбрейк по ed25519-ключам (`topology.rs:100-107`) — обе ноды всегда согласны, кто звонит. `ValGrid` (`util/val_grid.rs`) используется только для категоризации пиров при stream-limiting, не для ограничения связности.

---

## 4. Карта эмиссии по типам

| ID | Имя | Кто | Когда | Частота на ноду (tiny, V=6/C=2) |
|---|---|---|---|---|
| 0 | `Dropped` | транспорт | очередь >200 или реконнект | редко |
| 10 | `Status` | Any | таймер 2 с | 0.5/с (3 на слот) |
| 11 | `BestBlockChanged` | Any | смена лучшего блока + старт | ~1/слот |
| 12 | `FinalizedBlockChanged` | Any | продвижение финальности | ~1/слот |
| 13 | `SyncStatusChanged` | Any | старт (`synced:false`) и смена | редко |
| 20 | `ConnectionRefused` | Any | входящее в cooldown | редко |
| 21-23 | `ConnectingIn`/`ConnectInFailed`/`ConnectedIn` | Any | входящее соединение | на пира, при старте |
| 24-26 | `ConnectingOut`/`ConnectOutFailed`/`ConnectedOut` | Any | исходящее | на пира, при старте |
| 27 | `Disconnected` | Any | разрыв | редко |
| 28 | `PeerMisbehaved` | Any | нарушение протокола | редко |
| 40-42 | `Authoring`/`AuthoringFailed`/`Authored` | **A** | свой слот | 1/слот у 1 из 6 нод |
| 43-45 | `Importing`/`BlockVerificationFailed`/`BlockVerified` | **I** | приём блока | 1/слот у 5 из 6 |
| 46-47 | `BlockExecutionFailed`/`BlockExecuted` | A и I | после верификации | 1/слот на ноду |
| 60-61 | `BlockAnnouncementStreamOpened`/`Closed` | Any | открытие/закрытие стрима | на пира |
| 62 | `BlockAnnounced` | Any | каждый новый блок × каждый пир | ~5-10/слот |
| 63-68 | Block request/transfer | Any | только при отставании/синке | 0 в steady state |
| 80-82 | `GeneratingTickets`/`Failed`/`TicketsGenerated` | Val след. эпохи | epoch-слот ~2 | 1/эпоха |
| 83-84 | `TicketTransferFailed`/`TicketTransferred` | Val | там же | ~2N…2N·V за эпоху |
| 90 | `WorkPackageSubmission` | **G1** | билдер открыл CE 133/146 | 1 на WP |
| 91 | `WorkPackageBeingShared` | **G2** | primary открыл CE-sharing | 1 на WP × 2 ноды |
| 92 | `WorkPackageFailed` | G1/G2 | любая ошибка | при ошибках |
| 93 | `DuplicateWorkPackage` | G1/G2 | WP уже в таблице / refine идёт | редко |
| 94 | `WorkPackageReceived` | G1/G2 | после базовых проверок | 1 на WP на гуаранта |
| 95 | `Authorized` | G1/G2 | после `is_authorized` | 1 на WP на гуаранта |
| 96 | `ExtrinsicDataReceived` | G1/G2 | экстринсики прочитаны, хеши сверены | 1 |
| 97 | `ImportsReceived` | G1/G2 | импорты получены/собраны | 1 |
| 98-100 | `SharingWorkPackage`/`WorkPackageSharingFailed`/`BundleSent` | **G1** | на каждого secondary | ×2 на WP |
| 101 | `Refined` | G1/G2 | после refine всех items | 1 (costs — вектор по items) |
| 102 | `WorkReportBuilt` | G1/G2 | отчёт собран | 1 |
| 103 | `WorkReportSignatureSent` | **G2** | подпись отправлена primary | 1 |
| 104 | `WorkReportSignatureReceived` | **G1** | подпись проверена | ×2 |
| 105 | `GuaranteeBuilt` | **G1** | ≥2 подписи | 1 на WP |
| 106-108 | `SendingGuarantee`/`Failed`/`Sent` | **G1** | всем Val кроме гуарантов | ×(V−1)=5 |
| 109 | `GuaranteesDistributed` | **G1** | рассылка закончена | 1 |
| 110-112 | `ReceivingGuarantee`/`Failed`/`GuaranteeReceived` | Val\\{G} | приём CE 135 | 1 на WP |
| 113 | `GuaranteeDiscarded` | Val | вычистка пула | ~1 на WP |
| 120, 123, 125 | `SendingShardRequest`/`Sent`/`ShardsTransferred` | **Asr** | новый WP на коре | ≤C на слот |
| 121, 124 | `ReceivingShardRequest`/`Received` | **G** | входящий запрос шарда | ×(V−3) на WP |
| 122 | `ShardRequestFailed` | обе | ошибка | редко |
| 126, 128, 129 | `DistributingAssurance`/`AssuranceSent`/`Distributed` | **Val** | за 2 с до слота | 1 + (V−1) + 1 = 7/слот |
| 127 | `AssuranceSendFailed` | Val | ошибка | редко |
| 130-131 | `AssuranceReceiveFailed`/`AssuranceReceived` | Val | приём | (V−1)=5/слот |
| 140, 143, 145 | Bundle shard request (исх.) | **Aud** | реконструкция бандла | только если нет локально |
| 141, 144 | Bundle shard request (вх.) | Asr | входящий | " |
| 142 | `BundleShardRequestFailed` | обе | | |
| 146-147 | `ReconstructingBundle`/`BundleReconstructed` | **Aud** | набран порог шардов | " |
| 148, 151, 153 | Bundle request (исх.) | **Aud** | сначала пробуем у гуарантов | " |
| 149, 152 | Bundle request (вх.) | G | | |
| 150 | `BundleRequestFailed` | обе | | |
| 160 | `WorkPackageHashMapped` | **G1** | на каждый indirect import root | ≤ числа импортов |
| **161** | `SegmentsRootMapped` | — | **никогда не эмитится** | 0 |
| 162, 165, 167 | Segment shard request (исх.) | **G1** | восстановление импортов | зависит от WP |
| 163, 166 | Segment shard request (вх.) | Asr | | |
| 164 | `SegmentShardRequestFailed` | обе | | |
| 168-170 | `ReconstructingSegments`/`Failed`/`Reconstructed` | **G1** | шарды собраны | |
| **171** | `SegmentVerificationFailed` | — | **никогда не эмитится** | 0 |
| 172 | `SegmentsVerified` | **G1** | сегменты проверены | 1 на segment-root |
| 173, 176, 178 | Segment request (исх.) | **G1** | прямой запрос у прошлого гуаранта | |
| 174, 177 | Segment request (вх.) | G | | |
| 175 | `SegmentRequestFailed` | обе | | |
| 190-191 | `PreimageAnnouncementFailed`/`PreimageAnnounced` | Any | госсип прообразов | по потребности |
| 192 | `AnnouncedPreimageForgotten` | Any | 6 причин | |
| 193, 196, 198 | Preimage request (исх.) | Any | | |
| 194, 197, 198 | Preimage request (вх.) | Any | | |
| 195 | `PreimageRequestFailed` | обе | | |
| 199 | `PreimageDiscarded` | Any | прообраз больше не нужен | |

**Три типа не эмитятся вовсе**: 0 `Dropped` (только транспорт), 161 `SegmentsRootMapped`, 171 `SegmentVerificationFailed`. Проверено полным перебором `event!(<Name> ` по `crates/node/src/`.

Грубая оценка потока для tiny в steady state (1 WP на кор на слот): **~40-55 событий на ноду за 6-секундный слот**, то есть 7-9 событий/с/нода; из них ~12 приходится на ассуранс и 3 на Status. Это вывод из структуры кода, а не измерение.

---

## 5. Межнодовая согласованность

**Обязаны совпадать у всех нод:**

* `BestBlockChanged.hash` / `.slot` для одного слота — один и тот же блок (расхождение только при форке или отставании).
* `Authored.outline.hash` у автора = `Importing.outline.hash` у всех импортёров того же слота; совпадают также `size`, `num_tickets`, `num_preimages`, `preimages_size`, `num_guarantees`, `num_assurances`, `num_dispute_verdicts` — `BlockOutline` детерминирован от блока (`telemetry.rs:97-110`).
* `WorkPackageOutline.hash` и все его поля — у всех трёх гуарантов кора.
* `WorkReportOutline.hash`, `bundle_size`, `erasure_root`, `exports_root` — у трёх гуарантов, а `erasure_root` ещё и в `SendingShardRequest`/`ShardRequestReceived` у ассуреров.
* `GuaranteeOutline` (`report_hash`, `slot`, `guarantors`) — у primary в `GuaranteeBuilt` и у всех получателей в `GuaranteeReceived`. `guarantors` — отсортированный список `ValIndex` подписантов (`telemetry.rs:381-394`).
* **Парные send/receive**: `SendingGuarantee.recipient` на отправителе = `PeerId` отправителя в `ReceivingGuarantee.sender` на получателе. Аналогично пары 120/121, 140/141, 148/149, 162/163, 173/174, 193/194, 63/64. `PeerId` — это ed25519-ключ валидатора (`availability/net/mod.rs:1377-1379`: `PeerId(val_key)`).
* `TicketTransferred`: пара Local/Remote с одинаковыми `epoch`, `attempt`, `id`.
* `AssuranceSent.recipient` ↔ `AssuranceReceived.sender`; `DistributingAssurance.statement.anchor` = `AssuranceReceived.anchor`.
* `BlockAnnounced`: пара Local у отправителя / Remote у получателя с одинаковыми `slot` и `hash`.
* `NodeInfo.genesis` и `NodeInfo.params` — идентичны у всех нод сети.

**Индивидуально у каждой ноды:**

* Все `EventId` — счётчики своего соединения, между нодами не сравнимы.
* Все `Timestamp` — своя системная стрельба; допустимый разброс `MAX_CLOCK_DRIFT = 250 ms`.
* `Status.num_peers`, `num_val_peers`, `num_sync_peers`, `num_guarantees`, `num_preimages`, `preimages_size`.
* `IsAuthorizedCost`, `RefineCost`, `AccumulateCost`: `gas` детерминирован (одинаков при одинаковом входе), а `ns` и `load_ns` — измеренное wall-clock время, у каждой ноды своё.
* Роли: кто primary-гуарант, кто автор слота, кто аудирует какой отчёт.
* `ReconstructionKind` и выбор стратегии восстановления.

---

## 6. Status и его поля

`crates/node/src/node/mod.rs:182-240`, период 2 с.

| Поле | Тип | Что реально кладётся |
|---|---|---|
| `num_peers` | u32 | `NetStatus::num_peers()` — все установленные соединения |
| `num_val_peers` | u32 | `min(num_val_peers(), num_peers)` — явно клампится (:200) |
| `num_sync_peers` | u32 | `min(sync.status().num_peers, num_peers)` (:202) — пиры с открытым announcement-стримом |
| `num_guarantees` | `FixedVec<u8, CoreCount>` | длина = `core_count()`; `num[core] += 1` за каждую **FullySigned** гарантию в пуле (`guarantors/table.rs:545-553`). На не-валидаторе `guarantee_table = None` → вектор нулей |
| `num_shards` | u32 | **всегда 0** — `// TODO: @dave` (:216) |
| `shards_size` | u64 | **всегда 0** — `// TODO: @dave` (:217) |
| `num_preimages` | u32 | `PreimageTable::num_pending()` |
| `preimages_size` | u32 | `PreimageTable::pending_size()` |

Для правдоподобия синтетики: **`num_shards` и `shards_size` обязаны быть нулями** — иначе дамп сразу отличим от настоящего. В tiny `num_guarantees` — вектор длины 2 с малыми значениями (0-3); `num_peers` = 5 валидаторов + подключённые билдеры; `num_val_peers` = 5 (полная сетка); `num_sync_peers` ≤ 5.

---

## 7. Сигнатуры ключевых структур

Все из `crates/jam-std-common/src/telemetry.rs`.

```rust
// :84-95
pub struct BlockOutline {
    pub size: u32,               // block.encoded_size()
    pub hash: HeaderHash,
    pub num_tickets: u32,        // ≤ max_tickets_per_block (3 tiny / 16 full)
    pub num_preimages: u32,
    pub preimages_size: u32,     // сумма blob().len()
    pub num_guarantees: u32,     // ≤ core_count
    pub num_assurances: u32,     // ≤ val_count
    pub num_dispute_verdicts: u32,
}

// :344-352
pub struct WorkPackageOutline {
    pub size: u32,               // без extrinsic-данных; From: package.encoded_len()
    pub hash: WorkPackageHash,
    pub anchor: HeaderHash,
    pub lookup_anchor_slot: Slot,
    pub prerequisites: BoundedVec<WorkPackageHash, MaxDependencies>,  // ≤ 8
    pub items: BoundedVec<WorkItemOutline, MaxWorkItems>,             // ≤ 16
}

// :318-327
pub struct WorkItemOutline {
    pub service: ServiceId,
    pub payload_size: u32,
    pub refine_gas_limit: UnsignedGas,      // ≤ max_refine_gas
    pub accumulate_gas_limit: UnsignedGas,  // ≤ max_accumulate_gas
    pub extrinsic_size: u32,
    pub imports: BoundedVec<ImportSpec, MaxImports>,  // ≤ 3072; ImportSpec{root, index: u16}
    pub num_exports: u16,
}

// :355-361
pub struct WorkReportOutline {
    pub hash: WorkReportHash,
    pub bundle_size: u32,        // = report.package_spec.len; ≤ max_input
    pub erasure_root: ErasureRoot,
    pub exports_root: SegmentTreeRoot,
}

// :375-379
pub struct GuaranteeOutline {
    pub report_hash: WorkReportHash,
    pub slot: Slot,
    pub guarantors: BoundedVec<ValIndex, ConstU32<3>>,  // VALS_PER_CORE; реально 2 или 3
}

// :126-130, 140-148
pub struct ExecCost { pub gas: UnsignedGas, pub ns: u64 }
pub struct IsAuthorizedCost {
    pub total: ExecCost,   // total.gas ≤ 50_000_000
    pub load_ns: u64,      // входит в total.ns
    pub host_call: ExecCost,
}

// :157-198
pub struct RefineHostCallCost { lookup, vm, mem, invoke, other: ExecCost }
pub struct RefineCost { pub total: ExecCost, pub load_ns: u64, pub host_call: RefineHostCallCost }
pub type RefineCosts = BoundedVec<RefineCost, MaxWorkItems>;  // ровно по одному на work item

// :201-256
pub struct AccumulateHostCallCost { state, lookup, preimage, service, transfer: ExecCost,
                                    transfer_dest_gas: UnsignedGas, other: ExecCost }
pub struct AccumulateCost {
    pub num_calls: u32, pub num_transfers: u32, pub num_items: u32,
    pub total: ExecCost, pub load_ns: u64, pub host_call: AccumulateHostCallCost,
}
pub type AccumulateCosts = BoundedVec<(ServiceId, AccumulateCost), ConstU32<500>>;

// :439-455
pub struct NodeInfo {
    pub params: ProtocolParameters,     // ProtocolParameters::get()
    pub genesis: HeaderHash,
    pub details: PeerDetails,           // {id: PeerId, addr}
    pub flags: u32,                     // NODE_USES_PVM_RECOMPILER = 1<<0 при backend=Compiler
    pub impl_name: BoundedString<32>,   // "PolkaJam"
    pub impl_version: BoundedString<32>,// CARGO_PKG_VERSION
    pub gp_version: BoundedString<16>,  // "0.7.2"
    pub note: BoundedString<512>,       // пусто
}
```

`node_info` собирается в `crates/node/src/node/mod.rs:139-154`.

`ImportSegmentId` (`telemetry.rs:259-315`) кодируется как u16 с флагом в старшем бите: `Import(i)` → `i`, `ProofPage(i)` → `i | 0x8000`.

`Reason = BoundedString<128>` — все `reason` обрезаются до 128 байт через `.truncated()`.

Инварианты для реализма затрат: `load_ns ≤ total.ns`; сумма `host_call.*.ns` ≤ `total.ns`; `gas` растёт с размером payload; `AccumulateCost.num_items` ≤ числа work items в отчётах блока.

---

## 8. Чеклист для синтетического генератора

1. `PROTOCOL_VERSION = 1`; событие 61 — с `opened_id`, а не `peer`.
2. `Status.num_shards = 0`, `shards_size = 0` — всегда.
3. `audit_id = 0` во всех событиях 140/146/147/148.
4. События 161 и 171 не генерировать никогда.
5. Secondary-гуаранты не эмитят 105-109; рассылает гарантию только primary, ровно (V−1) получателям.
6. Билдер шлёт WP одному гуаранту → ровно одно событие 90 на WP во всей сети.
7. Автор блока идёт по ветке 40→42→47, импортёры — по 43→45→47; смешивать нельзя.
8. Status — по таймеру 2 с, не по слотам; фаза дрейфует относительно границ слота.
9. ID монотонно растут на 1 на событие в пределах соединения; `Dropped{num:N}` пропускает N.
10. `NodeInfo.impl_name = "PolkaJam"`, `gp_version = "0.7.2"`, `params` соответствует размеру сети (6/78/1023 валидатора).

## 9. Карта файлов

| Файл | Что содержит |
|---|---|
| `/home/claude/jam/polkajam/crates/jam-std-common/src/telemetry.rs` | определения всех событий JIP-3 (0.1.28) |
| `/home/claude/jam/polkajam/crates/node/src/telemetry.rs` | транспорт, очередь на 200, генерация `Dropped`, макрос `event!` |
| `/home/claude/jam/polkajam/crates/jam-types/src/simple.rs` | `ProtocolParameters` + `tiny()`/`mini()`/`full()` |
| `/home/claude/jam/polkajam/crates/node/src/util/periodic_event.rs` | вся периодика (слоты, эпохи, ротации, транши, ассуранс) |
| `/home/claude/jam/polkajam/crates/node/src/node/mod.rs` | эмиссия `Status`, сборка `NodeInfo` |
| `/home/claude/jam/polkajam/crates/node/src/chain/authoring/mod.rs` | авторинг (40-42, 46-47) |
| `/home/claude/jam/polkajam/crates/node/src/chain/authoring/ticket_communication.rs` | тикеты (80-84) |
| `/home/claude/jam/polkajam/crates/node/src/chain/mod.rs` | импорт блоков (43-47), 12, 13 |
| `/home/claude/jam/polkajam/crates/node/src/chain/leaves.rs` | `BestBlockChanged` (11) |
| `/home/claude/jam/polkajam/crates/node/src/sync/mod.rs` + `request_handler.rs` | 60-68 |
| `/home/claude/jam/polkajam/crates/node/src/node/stream_handler.rs` | точки входа входящих стримов (90, 91, 110, 121, 141, 149, 163, 174, 194) |
| `/home/claude/jam/polkajam/crates/node/src/chain/guarantors/` | 90-113, 160, 172 |
| `/home/claude/jam/polkajam/crates/node/src/availability/net/mod.rs` | 120-131, 140-153, 162-178 |
| `/home/claude/jam/polkajam/crates/node/src/availability/recovery/` | стратегии восстановления, 146-147, 168-170 |
| `/home/claude/jam/polkajam/crates/node/src/preimages/gossip/` | 190-199 |
| `/home/claude/jam/polkajam/crates/node/src/net/peer_manager/` | 20-28 |
| `/home/claude/jam/polkajam/crates/node/src/auditing/controller.rs` | `audit_id = EventId(0)` |
