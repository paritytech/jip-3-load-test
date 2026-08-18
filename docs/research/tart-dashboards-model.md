# Модель данных jamtart: ингест JIP-3, агрегаты, дашборды

Анализ репозитория `/home/claude/jam/jamtart` (READ-ONLY). Цель — определить, что должно быть «правдоподобным» в синтетическом JIP-3-трафике, чтобы дашборды Grafana выглядели как при настоящем трафике от сети JAM.

Дашбордов в `grafana/provisioning/dashboards/` фактически **10**, а не 11 (плюс `dashboards.yml`).

Ключевой факт: панели **не ходят в TimescaleDB напрямую** — все запросы идут через Infinity datasource в HTTP API `http://tart-backend:8080/api/grafana/*` (`grafana/provisioning/datasources/datasource.yml`, тип `yesoreyeram-infinity-datasource`). Поэтому «правдоподобность» определяется тем, что вернут эти эндпоинты, а они читают **только предагрегированные таблицы**, заполняемые in-memory-трекерами на этапе ингеста.

---

## 1. Путь ингеста

**TCP :9000** (`TELEMETRY_BIND`), 8 отдельных ingestion-рантаймов с `SO_REUSEPORT` (`INGESTION_THREADS`, по умолчанию 8), `src/server.rs:700+`.

1. Фрейминг: 4-байтовый префикс длины + тело (`decode_message_frame`), ограничение `MAX_MESSAGE_SIZE`.
2. **Первое сообщение — `NodeInformation`** (`src/decoder.rs:465`): первый байт — `TELEMETRY_PROTOCOL_VERSION`, иначе соединение рвётся с `InvalidDiscriminator`. Далее:
   - `params: ProtocolParameters` — из неё берётся **`core_count`**, он нужен для декодирования JIP-3-массивов фиксированного размера (`FixedVec<u8, 341>` в `Status`);
   - `genesis: HeaderHash`;
   - `details: PeerDetails { peer_id[32], peer_address { ipv6[16], port } }`;
   - `flags: u32`;
   - `implementation_name: NodeName`, `implementation_version: NodeVersion`;
   - `gp_version: BoundedString<16>`;
   - `additional_info: NodeNote`.
3. **`node_id = hex(peer_id)`** — ровно 64 hex-символа (`src/node_id.rs`, `src/server.rs:766`).
4. Далее поток событий: `Event::decode_event(&mut cursor, core_count)`.
5. **`event_id` неявный** (`src/server.rs:840`): счётчик с 0, +1 на событие; `Dropped { num }` увеличивает счётчик на `num`. Именно этими id адресуются ссылки `submission_id` / `built_id` / `sending_id` / `authoring_id` / `request_id` и т. д.
6. Rate limit (`src/rate_limiter.rs`): **1000 событий/с на ноду** (`MAX_EVENTS_PER_SECOND`), burst 200, `MAX_CONNECTIONS = 1024`. Превышение → тихий drop на стороне tart.
7. Далее событие проходит: `enricher` → `SlotTracker` → `WpTracker` → convergence-трекеры (guarantee + assurance) → `DaTracker` / `DaLatencyTracker` → `event_counter` → `batch_writer` (raw + `event_services` + `node_stats`) → WS-broadcast.

Флаши всех трекеров — раз в 5 с (`src/main.rs:331`), с порогами «возраст для вставки» 10 с и «возраст для вытеснения» 60 с.

### 1.1 Enricher — сердце «связности» (`src/enricher.rs`)

Без него `core`, `wp_hash`, `service_ids` останутся NULL, и **дашборд Cores и половина Services будут пустыми**.

| Что | Откуда берётся |
|---|---|
| `slot` | напрямую из `BestBlockChanged(11)`, `FinalizedBlockChanged(12)`, `Authoring(40)`, `Importing(43)`, `BlockAnnounced(62)`, `BlockTransferred(68)`; из `outline.slot` у `GuaranteeBuilt(105)` / `GuaranteeReceived(112)`; по цепочке `authoring_id` / `importing_id` для `Authored(42)`, `AuthoringFailed(41)`, `BlockVerified(45)`, `BlockVerificationFailed(44)`, `BlockExecuted(47)` |
| `core` | **только** из `WorkPackageReceived(94).core` и `DuplicateWorkPackage(93).core`, далее распространяется по `submission_or_share_id` |
| `wp_hash`, `service_ids` | **только** из `WorkPackageReceived(94).outline` (`work_package_hash`, `work_items[].service_id`) |
| цепочка `submissions` | ключ `submission_or_share_id` — пишут/читают 92, 93, 94, 95, 96, 97, 101, 102; ключ `submission_id` — 98, 99, 100, 104, 105, 109, 160, 161, 162, 168, 171, 172, 173; `share_id` — 103 |
| цепочка `built_ids` | `GuaranteeBuilt(105)` кладёт по своему `event_id` → читает `SendingGuarantee(106).built_id` |
| цепочка `sending_ids` | `SendingGuarantee(106)` кладёт по `event_id` → читают `GuaranteeSent(108)` / `GuaranteeSendFailed(107)` по `sending_id` |
| цепочка `request_ids` | `SendingSegmentShardRequest(162)` / `SendingSegmentRequest(173)` → читают 164–167, 175–178 по `request_id` |
| цепочка `reconstructing_ids` | `ReconstructingSegments(168)` → читают 169, 170 по `reconstructing_id` |

TTL записей — 60 с (`STALE_TTL`), максимум 50 000 на карту на ноду (`MAX_MAP_ENTRIES`). Enricher — `DashMap<node_id, NodeEventEnricher>`, поэтому **все ссылки должны разрешаться внутри одной ноды**.

---

## 2. Схема БД и извлекаемые поля

### 2.1 Что попадает в колонки, а что остаётся JSONB

`ingested_raw_events` (гипертаблица, **retention 1 час** после migration 020) — единственное место с полным событием:

```
timestamp, node_id, event_id, event_type, data JSONB, slot, core, submission_id, wp_hash
```

`data` — весь `Event`, сериализованный serde как externally tagged enum:
`{"Authored":{"timestamp":…,"authoring_id":…,"outline":{…}}}`.
Hot-колонки `slot/core/submission_id/wp_hash` заполняются из enricher (`src/store.rs:352`, COPY BINARY).

### 2.2 Событие → извлекаемые поля → таблица

| Событие | Извлекаемые поля | Таблица |
|---|---|---|
| **handshake `NodeInformation`** | `peer_id`→`peer_id`, `implementation_name`, `implementation_version`, весь объект→`node_info JSONB` (там `flags`, `gp_version`, `additional_info`, `genesis`, `params`), адрес TCP→`address` | `nodes` (upsert, `src/store.rs:160`) |
| **`Status(10)`** | `num_peers`, `num_val_peers`, `num_sync_peers`, `num_shards`, `shards_size`, `num_preimages`, `preimages_size`; массив `num_guarantees[core_count]` **схлопывается** в `min_guarantees` / `max_guarantees` / `avg_guarantees` / `zero_guarantee_cores` (`src/batch_writer.rs:649`) | `node_stats` |
| **любое из 115 типов** | `bucket` (30 с), `node_id`, `event_type`, `event_count` + доп. измерения | 14 count-таблиц |
| `Dropped(0)`, 10–13 | + `slot` | `status_counts` |
| 20–28 | + `reason` | `connection_counts` |
| 40–47 | + `slot`, `reason` | `block_counts` |
| 60–68 | + `slot`, `reason` | `block_distribution_counts` |
| 80–82 | + `reason` | `ticket_low_counts` |
| 83–84 | + `reason`, `from_proxy`, `epoch` | `ticket_counts` |
| 90–105 | + **`core`** (из enricher), `reason` | `wp_pipeline_counts` |
| 106–109 | + **`core`**, `reason` | `guarantee_sending_counts` |
| 110–113 | + `slot`, `reason` (у `GuaranteeDiscarded` формат `"{Reason:?}(n)"`) | `guarantee_receiving_counts` |
| 120–125 | + `reason` | `shard_counts` |
| 126–131 | + `reason` | `assurance_counts` |
| 140–153 | + `reason`, `kind` | `bundle_counts` |
| 160–178 | + **`core`**, `reason`, `kind` | `segment_counts` |
| 190–199 | + `reason`, `service_id` | `preimage_counts` |
| **`Authored(42)`, `Importing(43)`, `BlockAnnounced(62)`** | `outline: BlockSummary { size_bytes, hash, num_tickets, num_preimages, total_preimages_size, num_guarantees, num_assurances, num_dispute_verdicts }` — **остаётся только в JSONB**, читается через `data->'Authored'->'outline'->>…` | `ingested_raw_events` |
| **`Refined(101)`, `Authorized(95)`, `BlockExecuted(47)` + 21 тип** (список `SERVICE_EVENT_TYPES`: 47, 92–105, 109, 160, 161, 168, 170, 172) | `service_id` per work item, `gas_used`, `elapsed_ns`, `load_ns` | `event_services` |
| **42 / 11 / 12 / 40 / 43 по slot** | timestamps по нодам → перцентили относительно `Authored` | `slot_convergence` |
| **94, 95, 101, 102, 105, 109, 92** | `wp_hash`, `core`, `service_ids`, `node_id`, timestamps стадий, `refine_gas_used`, `failure_reason` | `wp_tracking` |
| **105 → 112** | `work_report_hash`, `slot`, `core`, `wp_hash`, `builder_node_id`, дельты приёма + 23-бакетная гистограмма | `guarantee_convergence`, `guarantee_convergence_slots` |
| **126 → 131** | `anchor`, `sender`, дельты (клампятся к ≥0) | `assurance_convergence`, `assurance_convergence_senders` |
| **120–125, 190–192** | счётчики + латентности 120→125 (assurer) и 121→124 (guarantor) | `da_node_stats`, `shard_latency_hist` |
| **140→145 / 147, 148→153, 146→147; 162→167, 173→178, 168→170; 193→198, 194→198** | гистограммы 23 бакета по `side` | `bundle_latency_hist`, `segment_latency_hist`, `preimage_latency_hist` |

Теряется полностью: `peer_address` / порт (только внутри `node_info` JSONB), поля `SyncStatusChanged`, тела запросов, идентификаторы шардов — кроме `active_shards` (мощность множества `shard` из событий 124/125).

### 2.3 Прочие таблицы

- `nodes` — `node_id` PK, `peer_id`, `implementation_name/version`, `node_info JSONB`, `connected_at`, `disconnected_at`, `last_seen_at`, `is_connected`, `event_count`, `total_events`, `address`.
- `slot_convergence` — PK `(slot, event_type)`, `node_count`, `p50/p75/p95/p99/p100_ms`, `authored_at`.
- `wp_tracking` — PK `wp_hash`, `core`, `service_ids INT[]`, стадийные timestamps (`received_at`, `authorized_at`, `refined_at`, `report_built_at`, `guarantee_built_at`, `distributed_at`, `failed_at`), `received_by`, `guaranteed_by`, `stage 0..5`, `node_id`, `refine_gas_used`, `failure_reason`, `discard_reason`.
- `da_node_stats` — счётчики шардов/преимиджей + средние латентности + `active_shards`.
- `event_services` — `(timestamp, node_id, event_type, service_id, gas_used, elapsed_ns, load_ns)`.

---

## 3. Агрегаты (continuous aggregates)

После migration 020 старые `event_stats_30s/1m/1h` и `core_stats_1m` **удалены**. Осталось:

- **14 групп count-таблиц × 2 уровня**: `<group>_counts_1m` (из raw, `SUM(event_count) GROUP BY bucket, node_id, event_type`) и `<group>_counts_1h` (иерархически из `_1m`). У `guarantee_sending_counts_1m/1h` и `segment_counts_1m/1h` дополнительно сохраняется измерение `core`.
- **UNION-вьюхи**: `all_event_stats_30s` (14 raw-таблиц), `all_event_stats_1m`, `all_event_stats_1h`, и `all_core_stats_1m` — **последняя собирается из raw `guarantee_sending_counts`, `segment_counts`, `wp_pipeline_counts` только там, где `core IS NOT NULL`**.
- `service_stats_1m` — из `event_services`: `COUNT(*)`, `SUM(gas_used)` по `(bucket, service_id, event_type)`.
- `node_stats_1m` — AVG/MIN/MAX по всем полям `node_stats` + `status_count`.
- Все — `materialized_only = false` (real-time tail, migration 017); политика: `end_offset` 2 мин, refresh 2 мин; retention 30 дней (`_1m`) / 365 дней (`_1h`), raw count-таблицы — 3 дня, `ingested_raw_events` — 1 час.

Выбор таблицы в `/timeseries` (`grafana_store.rs:136`): `group_by=core` или `core=` → `all_core_stats_1m`; `interval < 60s` → `all_event_stats_30s`; `< 1h` → `_1m`; иначе `_1h`. Retention-aware upgrade: если запрошенный диапазон старше retention, тихо переключается на более грубый уровень.

---

## 4. Дашборды

### 4.1 TART Global (`tart-global.json`, 22 панели, `now-1h`)

- **Live Stats (5 stat-панелей)** ← `/api/grafana/stats`: `active_nodes` (пороги red <1, yellow 1–2, **green ≥3**), `best_slot`, `finalized_slot`, `events_per_sec_10s` (green <100, yellow 100–1000, red >1000), `blocks_per_sec_10s`.
  Эти пять полей **не из БД** — накладываются из in-memory `LiveCounters` (`grafana.rs:567`): `best_slot` = max slot из событий **11**, `finalized_slot` = max slot из **12**, `blocks/sec` = частота событий **11**, `active_nodes` = число живых TCP-соединений.
- **Events by Type / by Node** ← `/timeseries?group_by=event_type|node_id`, поля `ts`, `event_type_name`, `count`, разбивка `partitionByValues`. Оверрайды цветов заданы для ~115 серий «Events 0…199».
- **Selected Events** ← `/timeseries?event_types=${event_type}`, где переменная `event_type` наполняется из `/event-types?group=${event_group}`.
- **Failure Rates** (table) ← `/timeseries?event_types=failures`.
- **Block Production / Pipeline Activity** ← `/timeseries?event_types=42`, `=wp_pipeline`, `=guarantee_receiving`.
- **Block Composition** ← `/blocks/contents`: `num_guarantees`, `num_assurances`, `num_preimages`, `num_tickets`, `num_disputes` (из JSONB `Authored`).
- **WP Funnel** (stat) и **Pipeline Bottlenecks** ← `/wp-funnel`, `/bottlenecks`.
- **Nodes** (table) ← `/nodes`: `node_id`, `implementation_name`, `implementation_version`, `is_connected`, `event_count`, `last_seen_at`.
- **Data Availability by Node / DA Aggregate** ← `/node-stats`, `/node-stats-aggregate`.

### 4.2 TART Node (`tart-node.json`, 12 панелей)

- Переменная `node_id` ← `/nodes` (селектор `node_id`) — **если таблица `nodes` пуста, весь дашборд мёртв**.
- **Node Info** (stat) ← `/node-stats?node=`: `num_peers`, `num_val_peers`, `num_sync_peers`, `num_shards`, `avg_guarantees`, `timestamp`.
- **Events by Category** — 8 запросов `/timeseries?node=…&event_types=blocks|wp_pipeline|guarantee_receiving|block_distribution|connections|tickets|shards|failures`.
- **All Nodes** (table) ← `/nodes`, включая колонку **`address`**.
- **Top 10 Slowest / Fastest Guarantors** ← `/validator-profiling?limit=10[&sort=asc]`, поле `avg_total_ms`, **пороги: green <5 мс, yellow 5–20, red >20** — единственные числовые пороги во всех дашбордах. Источник — `wp_tracking` c `node_id IS NOT NULL` и `(distributed_at IS NOT NULL OR failed_at IS NOT NULL)`.
- **Guarantor Pipeline Latency P50/P95** ← `/bottlenecks-timeseries`.

### 4.3 TART Cores (`tart-cores.json`, 21 панель)

- Переменная `core_index` ← `/cores` (селектор `core`) — список кор **динамический**, из `all_core_stats_1m`. Нет `core` → пустой список → ломаются все панели «Core ${core_index}».
- **Guarantees by Core** ← `/timeseries?group_by=core`.
- **Core Status Grid** (table) ← `/cores`: `core`, `work_packages` (94), `guarantees` (105), `failures` (92).
- **Pipeline Stats / Stage Timing** ← `/bottlenecks?core=`: `total_wps`, `failed_wps`, `failure_rate`, `avg_pipeline_ms`, вложенные `stage_timing.{authorize,refine,guarantee,pipeline_total}.{p50_ms,p95_ms}`.
- **Core Work Packages** (table) ← `/cores/${core_index}`, root `recent_work_packages`: `wp_hash`, `stage`, `first_seen`, `received_by`, `guaranteed_by`.
- **WP Pipeline Funnel** (bargauge) ← `/wp-funnel`: 7 полей `received → authorized → refined → report_built → guarantee_built → distributed` + `failed`.
- **6 панелей Bottlenecks** ← `/bottlenecks-timeseries?core=`: пары `authorize_p50/p95`, `refine_*`, `report_*`, `guarantee_*`, `distribute_*`, `pipeline_*`, unit `ms`.

### 4.4 TART Blocks (`tart-grafana-blocks.json`, 9 панелей, refresh 30s)

- **Block Rate** ← `/timeseries?event_types=42`.
- **Block Contents** ← `/blocks/contents`.
- **Три панели конвергенции** ← `/blocks/convergence?event_type=11|43|12` (Best Block Propagation / Importing / Finalization): `authored_at`, `p50_ms`, `p75_ms`, `p95_ms`, `p99_ms`. Данные из `slot_convergence`, перцентили = `(timestamp события − authored_at)` по всем нодам. `p100_ms` и `node_count` в панелях не используются.
- **Block Pipeline** ← `/timeseries?event_types=blocks` (40–47).

### 4.5 TART Data Availability (`tart-grafana-da.json`, 27 панелей, refresh 30s)

- **DA Storage** ← `/node-stats-aggregate` (`avg_shards_size`, unit bytes) и `/node-stats` (`num_shards`, `shards_size` по нодам).
- **Peer Health** ← `/node-stats` (`num_peers`), `/node-stats-aggregate` (`avg/min/max_peers`).
- **Preimage Stats** ← `/node-stats` (`num_preimages`, `preimages_size`).
- **Guarantee Convergence** ← `/guarantee-convergence?interval=`: `p50/p75/p95/p99/p100_ms` из гистограмм.
- **Assurance: Reception Convergence** ← `/assurance-convergence?interval=`; **Distribution Start Spread** ← `/assurance-convergence` **без** interval (поля `dist_start_p50/p95/p99/p100_ms`, ось времени — `slot_timestamp`, вычисляемый из `slot`, поэтому **slot у anchor должен резолвиться**).
- **Shard Latency** ← `/shard-latency?interval=`: `assurer_p50/p95/p99/p100` (120→125) и `guarantor_*` (121→124).
- **DA Operations** (table) ← `/da-stats`: `shard_requests_sent`, `shards_transferred`, `shard_failures`, `assurer_avg_latency_ms`, `guarantor_avg_latency_ms`, `active_shards`.
- **Bundle** ← `/bundle-latency`: `shard_req_*`, `shard_resp_*`, `full_req_*`, `full_resp_*`, `reconstruct_p50/95/99`, `e2e_p50/95/99/100`.
- **Segment** ← `/segment-latency` (те же поля без e2e); **Preimage** ← `/preimage-latency`: `req_p50/p95`, `resp_p50/p95`.

Ориентиры «нормальных» значений из `docs/grafana-guide.md`: assurer p50 ≈ 25 мс / p95 ≈ 80; guarantor p50 ≈ 8 мс; guarantee-конвергенция p50 ≈ 45 мс, p99 ≈ 250, `node_count ≈ 980`; assurance p50 ≈ 25 мс при `sender_count ≈ 1020`; окно доступности — 5 слотов (30 с).

### 4.6 TART Services (`tart-grafana-services.json`, 11 панелей)

- Переменная `service` ← `/services` (селектор `service_id`).
- **Service List / barcharts** ← `/services`: `work_packages` (94), `refinements` (101), `refinement_gas`, `authorizations` (95), `authorization_gas`, `executions` (47), `execution_gas` — из `service_stats_1m` ← `event_services`.
- **over Time** ← `/services/timeseries`.
- **Work Package Failure Reasons** (table) и **Failure Reason Distribution** (bargauge, парсер **uql/jsonata**) ← `/events?event_types=92,99&limit=500|2000`: читают `data.WorkPackageFailed.reason` и `data.WorkPackageSharingFailed.reason` из **сырого JSONB** — единственные панели, которым нужны сырые события, и они видят только **последний час**.

### 4.7 TART-api connectivity check (`tart-connectivity-check.json`, 7 панелей)

`/stats` (`connected_nodes` — из таблицы `nodes`, в отличие от `active_nodes`!), `/nodes`, `/bottlenecks` (`failure_rate`, `total_wps`, `failed_wps`), **Dropped Events** ← `/timeseries?event_types=system` (тип 0, порог yellow при ≥1), `/db-stats` (root `tables` и `row_counts`).

---

## 5. Группы событий (`src/event_type_meta.rs:633`)

| Группа | event_type |
|---|---|
| `system` | 0 |
| `status` | 10–13 |
| `connections` | 20–28 |
| `blocks` | 40–47 |
| `block_distribution` | 60–68 |
| `tickets` | 80–84 |
| `wp_pipeline` | 90–109 |
| `guarantee_receiving` | 110–113 |
| `shards` | 120–125 |
| `assurances` | 126–131 |
| `bundles` | 140–153 |
| `segments` | 160–178 |
| `preimages` | 190–199 |
| `failures` (виртуальная) | 20, 22, 25, 41, 44, 46, 65, 81, 83, 92, 93, 99, 107, 111, 113, 122, 127, 130, 142, 150, 164, 169, 171, 175, 190, 195, 199 |

`expand_event_types()` принимает смесь числовых id, имён групп и имён событий, умеет снимать фигурные скобки Grafana-мультиселекта `{a,b}`.

---

## 6. Onchain-зависимые дашборды

**`tart-onchain-cores`, `tart-onchain-services`, `tart-onchain-validators` (41 панель суммарно) наполнить телеметрией невозможно.**

Их таблицы `onchain_core_stats`, `onchain_service_stats`, `onchain_validator_stats`, `onchain_finalization` (migration 014) пишет **только** `onchain_stats::spawn_onchain_ingestion` (`src/main.rs:532`), который поднимается лишь при заданном `JAM_RPC_URL` и не при `--disable-onchain` / `--ingest-only`. Он открывает WS к JAM-ноде и подписывается на `subscribe_statistics(false)` и `subscribe_finalized_block()`, вставляя на каждый блок ~341 строку по корам, ~50 по сервисам и 1024 по валидаторам; при обрыве — реконнект каждые 5 с. Пересечения с телеметрийными таблицами — **ни одного**.

Поля панелей:
- cores: `gas_used`, `da_load`, `popularity(_avg)`, `imports`, `exports`, `extrinsic_count`, `extrinsic_size`, `bundle_size`;
- services: `refinement_gas/count`, `accumulate_gas/count`, `provided_count`, `imports`, `exports`, `extrinsic_count`;
- validators: `blocks_produced`, `tickets`, `guarantees`, `assurances`, `preimages`, `preimages_size` (эпохо-кумулятивные).

Переменные `core` / `service` / `validator` тоже читаются из onchain-эндпоинтов — без RPC не работает даже дропдаун.

**Вывод:** синтетический генератор покрывает 7 дашбордов из 10. Для onchain-дашбордов единственный путь — либо JSON-RPC-эндпоинт с этими двумя подписками, либо прямые вставки в три таблицы в обход tart.

---

## 7. Поля NodeInfo из handshake — где видны

| Поле | Колонка | Где показывается |
|---|---|---|
| `details.peer_id` | `peer_id` + `node_id` (hex) | `node_id` во всех таблицах и как переменная `node_id`; сам `peer_id` в панелях не выводится |
| `implementation_name` | `implementation_name` | Global → «Nodes» (Impl), Node → «All Nodes» |
| `implementation_version` | `implementation_version` | там же (Version) |
| `gp_version`, `additional_info` (note), `flags`, `genesis`, `params` | внутри `node_info JSONB` | **ни в одной панели не отображаются** (эндпоинт `/nodes` их отдаёт в поле `node_info`, но ни один `columns`-селектор его не берёт) |
| адрес TCP-соединения | `address` | Node → «All Nodes» (Address) |
| `params.core_count` | не хранится | используется как параметр декодера для массивов фиксированной длины |

---

## 8. Чек-лист правдоподобности (по убыванию заметности)

### Критично — без этого дашборды пустые

1. **Корректный handshake на каждое TCP-соединение**, иначе соединение закрывается и в `nodes` ничего не появится. `peer_id` уникален на ноду; `params.core_count` обязан совпадать с реальной длиной массива `Status.num_guarantees`, иначе декодер сломается на первом же `Status`.
2. **Ноды должны держать TCP-соединение всё время наблюдения**: `active_nodes` в Global берётся из числа живых сокетов, а не из БД. Разово залить события и отключиться — показать 0. Минимум 3 ноды для зелёного порога, реалистично — сотни (`val_count = 1023`).
3. **Регулярный `BestBlockChanged(11)` с монотонно растущим slot и `FinalizedBlockChanged(12)` с отставанием** (в `loadgen` — `slot − 2`; правдоподобно 2–3 слота). Это задаёт три из пяти верхних stat-панелей Global.
4. **Ровно одно `Authored(42)` на слот у одного автора**, события 11/43/12 у всех остальных нод **строго позже** — конвергенция считается как `t_события − t_Authored`, отрицательные дельты дадут бессмысленные графики (в slot_tracker клампа нет, в отличие от assurance).
5. **Слот-темп 6 с** (`slot_period_sec: 6`), эпоха 600 слотов.
6. **Не превышать 1000 событий/с на ноду** — иначе tart молча дропает и графики неестественно просядут.

### Для дашбордов Cores / Services / воронки

7. **Целостность цепочки WP на одной ноде**: `WorkPackageReceived(94)` с корректными `core`, `submission_or_share_id` и `outline` (с `work_package_hash` и `work_items[].service_id`), затем 95, 96, 97, 101, 102 с **тем же** `submission_or_share_id`, и 98/99/100/104/105/109 с `submission_id`, ссылающимся на нужный `event_id`. Без этого `core` / `wp_hash` / `service_ids` = NULL → пустые `wp_tracking`, `all_core_stats_1m`, `event_services`.
8. **Монотонная воронка**: `received ≥ authorized ≥ refined ≥ report_built ≥ guarantee_built ≥ distributed`; `failed` — небольшой процент (в документации `failure_rate ≈ 0.02`).
9. **Возрастающий порядок timestamps стадий** с правдоподобными интервалами (docs: authorize p50 ≈ 12 мс, refine ≈ 150 мс — самая долгая стадия, report ≈ 5, guarantee ≈ 3, distribute ≈ 8, total ≈ 180 мс). Панель профилирования красит `avg_total_ms > 20 мс` красным.
10. **Активность распределена по всем 341 корам** примерно равномерно, с сопоставимыми `work_packages` / `guarantees` и редкими `failures`.
11. **Осмысленные `service_id`**: небольшое стабильное множество (в тестнете — 2 и 6), с ненулевыми `gas_used` / `elapsed_ns` / `load_ns` в `Refined.costs` и `Authorized.cost`, иначе колонки Gas пустые.
12. **`GuaranteeBuilt(105)` и `GuaranteeReceived(112)` несут один и тот же `outline.work_report_hash`**; сначала 105 у одного гуарантора, затем 112 у многих валидаторов (docs: `node_count ≈ 980`).
13. **`DistributingAssurance(126).statement.anchor` = хэш блока из `Authored/Importing.outline.hash`** (иначе slot не резолвится и Distribution Start Spread остаётся без оси времени), а **`AssuranceReceived(131).sender` = 32-байтовый peer_id того, кто слал 126** — иначе дельты уходят в `pending_received` и теряются.

### Для DA и латентностей

14. **Парность запрос/ответ по `request_id` = event_id запроса**: 120→125 (assurer), 121→124 (guarantor), 140→145, 148→153, 146→147, 162→167, 173→178, 168→170, 193→198, 194→198. Дельты 5–100 мс заполняют средние бакеты гистограмм; ответ без запроса игнорируется.
15. **`Status(10)` каждые ~2 с от каждой ноды** с плавно меняющимися `num_peers` (десятки–сотни), `num_shards` (~342, по одному на кору), `shards_size`, `num_preimages`, и массивом `num_guarantees` длины `core_count`, где часть элементов нулевая (даёт `zero_guarantee_cores`).
16. **Небольшой ненулевой поток отказов** во всех группах (типы из группы `failures`) с осмысленными строками `reason` — панель Failure Reason Distribution строит гистограмму именно по тексту причины.

### Косметика, но заметно

17. **Не слать `Dropped(0)`** — панель Dropped Events в connectivity-check желтеет уже при 1.
18. **`implementation_name` / `implementation_version` правдоподобные и разные** между нодами (не `tart-loadgen`), они видны в двух таблицах.
19. **Events by Type должен показывать десятки серий** в реалистичных пропорциях: тяжёлые — 120–125 (шарды, ~341 запрос на слот на ассюрера), 126–131 (ассюрансы, ~1023 на блок), 160–178; средние — 60–68, 90–105; лёгкие — 40–47, 80–84.
20. **Ретеншн**: `ingested_raw_events` — 1 час (Failure Reasons и Block Composition видят только его), count-таблицы — 3 дня. Заливать историю «назад во времени» для этих панелей бессмысленно.

---

## 9. Ключевые файлы

- `/home/claude/jam/jamtart/src/server.rs` — ингест, handshake, маршрутизация в трекеры (строки 700–1400)
- `/home/claude/jam/jamtart/src/enricher.rs` — корреляция событий
- `/home/claude/jam/jamtart/src/event_counter.rs` — count-таблицы и их измерения
- `/home/claude/jam/jamtart/src/batch_writer.rs:545-750` — `event_services` и `node_stats`
- `/home/claude/jam/jamtart/src/grafana_store.rs` — все SQL за эндпоинтами
- `/home/claude/jam/jamtart/src/event_type_meta.rs:633` — группы событий
- `/home/claude/jam/jamtart/migrations/020_unified_event_architecture.sql` — актуальная архитектура таблиц
- `/home/claude/jam/jamtart/docs/grafana-guide.md` — примеры «нормальных» значений всех ответов API
- `/home/claude/jam/jamtart/src/bin/loadgen.rs` — эталонный handshake (`core_count: 341`, `val_count: 1023`, `slot_period_sec: 6`), но профиль событий примитивный (только 10/11/12/13)
