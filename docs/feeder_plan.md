# ZTF Archive → ALeRCE Pipeline Feeder

## Context

ALeRCE wants to reprocess ~7 years of ZTF public alerts (~2300 daily `ztf_public_YYYYMMDD.tar.gz` files, 5–15 GB each, already downloaded to a local disk) through the on-prem multisurvey pipeline. The pipeline is already deployed and managed by a colleague; our sole job is to populate its input Kafka topic (`ztf`) with the archive's alerts in chronological order, in batches small enough that the cluster never accumulates more than a few days of alerts at rest. The whole campaign is expected to run unattended for 2–3 weeks.

A `feeder` CLI in this repo:
1. Walks the tar.gz files chronologically.
2. Stream-extracts Avro alerts from each tar.gz (no full extraction to disk).
3. Normalizes each alert to schema v4.02 using [unify_schemas.py](src/ztf_archive_downloader/unify_schemas.py) `convert_alert_to_latest()` and produces it to the `ztf` topic as a **schemaless** Avro message (no per-message schema header) keyed by `objectId`.
4. After accumulating ≥ a configurable alert-count threshold (whole files only, no splits), pauses production and `producer.flush()`.
5. Waits for **the entire pipeline to be idle**: every user-supplied `(group, topic)` pair must have lag=0 stable for `--drain-stable-checks` consecutive polls. This is the "pipeline finished processing this batch" gate.
6. Wipes **every topic in the gate set** via the retention-flush trick (`retention.ms`/`retention.bytes` set to small values, sleep until empty, restore both to infinite). The `ztf` input topic and every intermediate inter-step topic are flushed together at this point — the pipeline is idle so nothing is in flight.
7. Persists a JSON checkpoint of the last fully-completed day, then begins the next batch.

## Key facts confirmed during exploration

- Pipeline `ingestion_step` consumes topic **`ztf`**, group **`sorting-hat-step-ingestion`** (charts/ingestion_step/values.yaml:54,63 in the pipeline repo).
- APF provides both framed (`apf.consumers.KafkaConsumer`, uses `fastavro.reader`) and schemaless (`apf.consumers.KafkaSchemalessConsumer` at libs/apf/apf/consumers/kafka.py:311 in the pipeline repo, uses `fastavro.schemaless_reader`). The current chart deploys the framed variant. **For this campaign the chart must be flipped to `apf.consumers.KafkaSchemalessConsumer`** to match what the feeder produces (schemaless saves ~1 KB header per message × millions of messages = real disk/bandwidth). This is a coordination item with the colleague deploying the pipeline. The schema path in the consumer config (`SCHEMA_PATH: "/schemas/ztf/alert.avsc"`) is already correct and is what the schemaless consumer uses to decode.
- Schema v4.02 lives at `pipeline/schemas/ztf/alert.avsc` plus its sibling files (`candidate.avsc`, `prv_candidate.avsc`, `fp_hist.avsc`, `cutout.avsc`).
- Stale code in pipeline: `ingestion_step/scripts/produce_sample.py:42` keys ZTF on `alertId`, but the schema has no such field. Live IPAC stream uses `objectId`; we will too.
- Existing `convert_alert_to_latest()` in [src/ztf_archive_downloader/unify_schemas.py](src/ztf_archive_downloader/unify_schemas.py) already handles all known older versions (1.8 → 4.02) by null-defaulting missing fields.
- Existing `iter_days`-style date-sorting logic exists in [src/ztf_archive_downloader/downloader.py](src/ztf_archive_downloader/downloader.py) lines 362–372 (`download_sample`); the feeder copies the regex/sort pattern.

## User decisions baked into this plan

| Question | Decision |
|---|---|
| Where the feeder runs | Ubuntu host in same on-prem subnet as k8s |
| Cutouts | Configurable `--strip-cutouts` flag, default **keep** |
| Pipeline scope | Out of our hands; we only fill the `ztf` topic |
| Batch unit | Whole-tar.gz files; cut a batch once cumulative alerts ≥ `--batch-alert-threshold` |
| Kafka admin access | Yes — use the retention-flush trick |
| Crash-resume | Persist last-fully-flushed day to JSON; resume on restart |
| Schema unify | Always run `convert_alert_to_latest()` on every alert |
| Idle gate | User-supplied `--pipeline-gate GROUP:TOPIC` (repeatable). All pairs must reach lag=0 stable before flushing. Includes the input gate `(sorting-hat-step-ingestion, ztf)` and every intermediate inter-step `(group, topic)` |
| Flush set | The unique set of topics in `--pipeline-gate`. All flushed at the same time, after the gate clears |
| Flush method | Set BOTH `retention.ms=1000` and `retention.bytes=1`, poll until topic empty, then restore both |
| Normal retention (when not flushing) | Infinite: `retention.ms=-1`, `retention.bytes=-1` |
| Avro encoding | Schemaless (`fastavro.schemaless_writer`) — chart consumer must be `KafkaSchemalessConsumer` |
| Message key | `objectId` |

## Implementation status (as of 2026-05-06)

The first iteration is already merged. This plan describes the **delta** to land next. Status of each file:

| File | Status | Action needed |
|---|---|---|
| [src/ztf_archive_downloader/feeder/__init__.py](src/ztf_archive_downloader/feeder/__init__.py) | exists | none |
| [src/ztf_archive_downloader/feeder/checkpoint.py](src/ztf_archive_downloader/feeder/checkpoint.py) | exists | none |
| [src/ztf_archive_downloader/feeder/iter_alerts.py](src/ztf_archive_downloader/feeder/iter_alerts.py) | exists | none |
| [src/ztf_archive_downloader/feeder/kafka_io.py](src/ztf_archive_downloader/feeder/kafka_io.py) | exists | small edits only — `wait_until_drained` already takes `list[tuple[group, topic]]`; `flush_topic_via_retention` already operates on a single topic. Update the **default `restore_ms`** from `86_400_000` to `-1` (infinite). Everything else stays. |
| [src/ztf_archive_downloader/feeder/cli.py](src/ztf_archive_downloader/feeder/cli.py) | exists | **rewrite the gate / flush plumbing**: replace `--drain-group` + `--ready-group` with a single `--pipeline-gate GROUP:TOPIC` (repeatable, required, ≥1). `finalize_batch` becomes one drain followed by a flush of every unique topic in the gate set. Default `--retention-normal-ms`/`--retention-normal-bytes` flip to `-1`/`-1`. |
| [tests/smoke/docker-compose.yml](tests/smoke/docker-compose.yml) | exists | none |
| [tests/smoke/mock_consumer.py](tests/smoke/mock_consumer.py) | exists | extend: optional `--produce-to TOPIC` flag so each consumed message is re-produced to a downstream topic (after the consume-side commit). This lets us chain two mock consumers into a 2-step pipeline. Schemaless pass-through of the value bytes; key kept. |
| [tests/smoke/test_feeder_smoke.py](tests/smoke/test_feeder_smoke.py) | exists | **rewrite** to match the new CLI: keep the `--skip-flush` smoke test; replace the single-step drain test with a **2-step-chain** test (see §Verification plan). |
| [pyproject.toml](pyproject.toml) | exists | already has `confluent-kafka` and the `feeder` entry point; nothing to do. |
| [README.md](README.md) | exists | update the feeder flag table + example to the new `--pipeline-gate` surface and infinite-retention defaults. |

No edits to `downloader.py`, `unify_schemas.py`, `get_schemas.py`.

## CLI surface (new / changed flags)

`feeder run` flags (env prefix `FEEDER_` unless noted):

**Changed**
- `--pipeline-gate GROUP:TOPIC` *(repeatable, required, env `FEEDER_PIPELINE_GATES`)* — every `(group, topic)` pair the pipeline uses. Drain gate = "all of these reach lag=0 stable". Flush set = the unique topics across these pairs. Replaces `--drain-group` and `--ready-group`.
- `--retention-normal-ms INT` (default **`-1`** = infinite; was `86_400_000`).
- `--retention-normal-bytes INT` (default **`-1`** = unlimited; unchanged).

**Unchanged**
- `--data-dir PATH` *(required, env `ARCHIVE_DOWNLOAD_DIR`)*
- `--bootstrap STR` *(required, env `KAFKA_BOOTSTRAP`)*
- `--schema-path PATH` *(required)* — pipeline's `alert.avsc`; siblings resolved from same dir.
- `--checkpoint-path PATH` (default `./feeder_checkpoint.json`)
- `--batch-alert-threshold INT` (default `2_000_000`)
- `--strip-cutouts/--keep-cutouts` (default keep)
- `--start-day YYYY-MM-DD` / `--end-day YYYY-MM-DD` (inclusive; optional)
- `--security-protocol`, `--sasl-mechanism`, `--sasl-username`, `--sasl-password`
- `--retention-flush-ms INT` (default `1000`)
- `--retention-flush-wait-s FLOAT` (default `60`)
- `--drain-stable-checks INT` (default `3`)
- `--drain-poll-seconds FLOAT` (default `5.0`)
- `--no-retention-bytes-flush` (debug)
- `--skip-flush` (debug; produce, don't drain or flush)

**Removed**
- `--drain-group`, `--ready-group` (subsumed by `--pipeline-gate`).

## Main loop (concise)

```python
schema   = fastavro.schema.load_schema(schema_path)
producer = build_producer(...)
admin    = build_admin(...)
gates    = parse_pipeline_gates(pipeline_gate)         # list[tuple[group, topic]]
flush_topics = sorted({t for _, t in gates})           # unique topics

if not skip_flush:
    check_flush_compatibility(admin, retention_flush_wait_s)

last_done = checkpoint.read(checkpoint_path)
todo      = [d for d in iter_days(...) if last_done is None or day_of(d) > last_done]

batch, batch_alerts, delivery_errors = [], 0, []

for tar_path in todo:
    if shutdown.flag: break
    n = produce_one_day(producer, schema, tar_path, ...)
    batch.append(tar_path); batch_alerts += n
    if batch_alerts < batch_alert_threshold:
        continue
    finalize_batch(producer, admin, gates, flush_topics, ...)
    checkpoint.write(checkpoint_path, day_of(batch[-1]))
    batch, batch_alerts = [], 0

if batch and not shutdown.flag:                        # tail batch
    finalize_batch(...); checkpoint.write(...)
```

`finalize_batch`:
1. `producer.flush(timeout=...)` — block until all in-flight messages acked.
2. Raise if any `delivery_errors` accumulated; do NOT checkpoint a batch with delivery errors.
3. `wait_until_drained(admin, gates, stable_checks, poll_s, consumer_conf, shutdown)` — every `(group, topic)` pair must hit lag=0 stable. This is the single "pipeline idle" gate.
4. For each `t` in `flush_topics`: `flush_topic_via_retention(admin, t, ...)` to wipe the topic and restore retention to `-1`/`-1`. (Sequential is fine — each flush blocks ~`retention-flush-wait-s` at most; pipeline is already idle so nothing repopulates them.)

Steps 3 and 4 collapse the previous "drain → flush input → wait for downstream" three-stage flow into "drain everything → flush everything", which matches the team's intended contract: a batch is done iff every consumer in the pipeline has zero lag, at which point every working topic can safely be wiped.

`produce_one_day`, `produce_alert`, and `compute_group_lag` stay as they are in [kafka_io.py](src/ztf_archive_downloader/feeder/kafka_io.py). `wait_until_drained` already accepts `list[tuple[str, str]]`, so no signature change.

## Producer config (confluent-kafka) — unchanged

```python
{
  "bootstrap.servers":            bootstrap,
  "enable.idempotence":           True,
  "compression.type":             "zstd",
  "compression.level":            3,
  "linger.ms":                    50,
  "batch.size":                   1_000_000,
  "queue.buffering.max.messages": 100_000,
  "queue.buffering.max.kbytes":   1_048_576,
  "message.max.bytes":            67_108_864,
  "delivery.timeout.ms":          300_000,
}
```

## Checkpoint format — unchanged

`feeder_checkpoint.json`:
```json
{"last_done_day": "2018-06-13", "schema_version": 1, "completed_at": "2026-04-29T17:23:01Z"}
```
Atomic write via `os.replace`.

## Edge cases

- Tar entries that aren't `.avro`: filter on `member.isfile() and name.endswith(".avro")`.
- Corrupt `.avro` member: log + skip; tally per day; emit per-day summary.
- Conversion failures: try/except per alert, log `objectId` + source `schemavsn` + error, skip.
- Producer `BufferError`: poll then retry.
- Delivery callback err: append to `delivery_errors`; abort batch, do not checkpoint, exit non-zero.
- `SIGTERM`/`SIGINT`: handler sets `shutdown.flag`; loops break at next alert; `producer.flush(timeout=120)`; do not checkpoint mid-batch; rerun resumes from last checkpoint.
- Empty `--pipeline-gate` list: CLI rejects (≥1 required) — without a gate we cannot tell when to flush.
- One gate group has never committed an offset (consumer just started): `wait_until_drained` already handles this with the "saw activity then drained" pattern.

## Verification plan

End-to-end smoke (no real pipeline) — under `tests/smoke/`:

1. **Local Kafka via docker-compose** (already in place: single-broker bitnami/kafka 3.3.1, port 9092). `KAFKA_CFG_LOG_RETENTION_CHECK_INTERVAL_MS=1000` keeps the log cleaner fast enough for the flush test.
2. **Test 1 — produce-only (kept):** `feeder run --skip-flush --pipeline-gate dummy:ztf ...` → expect `EXPECTED_TOTAL_ALERTS` messages on `ztf`, checkpoint at `2019-06-01`. (Gate is required by the CLI but unused with `--skip-flush`.)
3. **Test 2 — 2-step pipeline (replaces single-step drain test):**
   - Topology: `feeder → ztf → consumer-A (group=step-A) → ztf-intermediate → consumer-B (group=step-B)`.
   - Both mock consumers are slow (`--batch 10 --pause 0.5`) so the feeder observes non-zero lag on both.
   - `consumer-A` runs with `--produce-to ztf-intermediate` (new flag); `consumer-B` is a normal sink.
   - Run `feeder run --pipeline-gate step-A:ztf --pipeline-gate step-B:ztf-intermediate --batch-alert-threshold 150 --drain-poll-seconds 2 --drain-stable-checks 1 --retention-flush-wait-s 15`.
   - Files are 100 alerts each × 3 → with threshold 150, expect 1 mid-run finalize + 1 tail finalize = 2 drain+flush cycles.
   - Assertions:
     - feeder exit 0
     - both `ztf` and `ztf-intermediate` end at `count == 0` (poll up to 15 s)
     - `retention.ms == -1` and `retention.bytes == -1` on both topics after restore
     - checkpoint `last_done_day == "2019-06-01"`
4. **Schema-unify regression** (existing test, unrelated to this change): keep as-is.
5. **Decode-back sanity (manual, ad-hoc):** kcat one message off `ztf`, decode with `fastavro.schemaless_reader` against `alert.avsc`, expect `objectId` starting with `ZTF` and `schemavsn == "4.02"`.
6. **Crash-resume (manual):** `kill -TERM` mid-run, restart, verify checkpoint is the last fully-flushed batch and the run does not duplicate already-delivered messages.

Real-pipeline staging step (colleague-side, before unleashing on full archive):
- Point the feeder at the staging cluster with one day of input and a low `--batch-alert-threshold` so we exercise the drain+flush path on real infra. Provide every inter-step `(group, topic)` pair as `--pipeline-gate`. Confirm all gate groups return to lag=0 and that the retention flush actually purges each partition (check broker log dir sizes).

## Operational context already known

- **Bootstrap server:** `quimal-db3.alerce.online:9092` (used previously for similar tasks).
- **Auth:** SASL via a JAAS file historically (`--command-config jaas.client.conf` for the Java tools). For Python we translate this to `security.protocol`/`sasl.mechanism`/`sasl.username`/`sasl.password`. The exact mechanism (likely SCRAM-SHA-512, possibly PLAIN) and credentials wired via env (`FEEDER_SASL_USERNAME`, `FEEDER_SASL_PASSWORD`, `FEEDER_SASL_MECHANISM`, `FEEDER_SECURITY_PROTOCOL`).
- **Past flush approach:** the user's prior script used only `--add-config retention.ms=1` (no `retention.bytes`) and worked on this cluster. We default to setting both for safety, but expose `--no-retention-bytes-flush` to mirror the historical behavior if needed.
- **Lag monitoring fallback:** if the Python `AdminClient` lag computation hits issues, a shell-out to `kafka-consumer-groups.sh ... --all-groups --describe | awk '$2==TOPIC {lag[$1]+=$6} END {...}'` mirrors the user's working approach.

## Coordination items with the colleague deploying the pipeline

- **Chart change required:** in `pipeline/charts/ingestion_step/values.yaml:52`, change `CONSUMER_CONFIG.CLASS` from `apf.consumers.KafkaConsumer` to `apf.consumers.KafkaSchemalessConsumer`. `SCHEMA_PATH` stays the same.
- **Provide the full list of `(group, topic)` pairs** for every step in the pipeline, in order. Each becomes one `--pipeline-gate`. The feeder will flush every topic in this list at the end of each batch — confirm none of these topics is consumed by anything outside the pipeline (otherwise the flush would also wipe data those external consumers had not yet read).
- Confirm SASL mechanism (SCRAM-SHA-512 vs PLAIN vs ...) and provide credentials.
- Confirm the broker's `log.retention.check.interval.ms` is small enough that the log cleaner runs at least once during each flush window (`check_flush_compatibility` enforces this at startup).

## Unresolved (defer to runtime config)

- Final `--batch-alert-threshold` value — start at 2M, tune after observing one batch's drain time on real infra.

## Reference: prior shell snippets that worked on this cluster

```bash
# Alter retention to flush a topic
./kafka-configs.sh \
  --command-config jaas.client.conf \
  --bootstrap-server quimal-db3.alerce.online:9092 \
  -entity-type topics --entity-name <topic> \
  --alter --add-config retention.ms=1

# Aggregate consumer-group lag for a given topic
./kafka-consumer-groups.sh \
  --command-config jaas.client.conf \
  --bootstrap-server quimal-db3.alerce.online:9092 \
  --all-groups --describe \
  | awk '$2=="<topic>" {lag[$1]+=$6} END {for (g in lag) print g, lag[g]}'
```
