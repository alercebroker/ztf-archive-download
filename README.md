# ztf-archive-downloader

Scripts for downloading and working with the [ZTF public alert archive](https://ztf.uw.edu/alerts/public/).

Handles bulk downloading of `.tar.gz` alert files, integrity validation (MD5 + tar extraction + Avro parsing), schema extraction from the upstream [ztf-avro-alert](https://github.com/ZwickyTransientFacility/ztf-avro-alert) repo, and conversion of alerts across schema versions.

## Requirements

- Python >= 3.14
- [uv](https://docs.astral.sh/uv/)

## Installation

```bash
uv sync
```

To include development tools (ruff, pyright, polars, ipython):

```bash
uv sync --group dev
```

## Usage

The package provides two CLI entry points: `downloader` and `get_schemas`.

### downloader

Download and validate ZTF public alert files.

```bash
# List all available files in the archive
uv run downloader list-files

# Download a single file by date
uv run downloader download-one 20240101

# Download a sample (one file every ~6 months)
uv run downloader download-sample -d ./data

# Download all files
uv run downloader download-all -d ./data

# Validate downloaded files (MD5, extraction, Avro integrity)
uv run downloader validate -d ./data

# Validate a single file
uv run downloader validate -f ./data/ztf_public_20240101.tar.gz
```

The download directory can be set via `--output-dir`/`-d` or the `ARCHIVE_DOWNLOAD_DIR` environment variable.

#### Validation output formats

The `validate` command supports `--output-format` (`human`, `json`, `yaml`).

#### Proxy support

All commands accept `--proxy` (or `ARCHIVE_DOWNLOAD_PROXY` env var) for SOCKS/HTTP proxies:

```bash
uv run downloader list-files --proxy socks5://localhost:1080
```

### get_schemas

Extract all historical Avro schema versions from the upstream ZTF alert schema repository:

```bash
uv run get_schemas
```

Schemas are saved to `./schemas/<version>/`.

### feeder

Streams alerts from downloaded `.tar.gz` files into a Kafka topic, converting each alert to schema v4.02 on the fly. Designed for bulk replay of the full ZTF archive into the ALeRCE pipeline.

```bash
uv run feeder \
  --data-dir /data/ztf-archive \
  --bootstrap kafka:9092 \
  --schema-path /path/to/alert.avsc \
  --pipeline-gate sorting-hat-step-ingestion:ztf \
  --pipeline-gate prv-candidates-step:sorting-hat \
  --pipeline-gate lightcurve-step:prv-candidates
```

The feeder processes days in chronological order. After each batch it waits for every `(group, topic)` pair supplied via `--pipeline-gate` to reach lag=0 stable (pipeline idle), then wipes every topic in the gate set via the retention-flush trick (set retention small, wait for log cleaner, restore to infinite), then checkpoints and starts the next batch.

At startup (when flush is enabled) the feeder reads `log.retention.check.interval.ms` from the broker and raises immediately if it is ≥ `--retention-flush-wait-s`. This prevents silent failures where the log cleaner never runs within the flush window. To satisfy this check set `log.retention.check.interval.ms` to at most half of `--retention-flush-wait-s` on the broker (e.g. `5000` ms for the default 60 s wait).

#### Key options

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--data-dir` | `ARCHIVE_DOWNLOAD_DIR` | *(required)* | Directory of `ztf_public_*.tar.gz` files |
| `--bootstrap` | `KAFKA_BOOTSTRAP` | *(required)* | Kafka bootstrap servers |
| `--schema-path` | `FEEDER_SCHEMA_PATH` | *(required)* | Path to `alert.avsc` (siblings resolved from same dir) |
| `--topic` | `FEEDER_TOPIC` | `ztf` | Target Kafka topic |
| `--checkpoint-path` | `FEEDER_CHECKPOINT_PATH` | `./feeder_checkpoint.json` | Resume file; delete to restart from the beginning |
| `--start-day` | `FEEDER_START_DAY` | *(none)* | Skip days before this date (YYYY-MM-DD, inclusive) |
| `--end-day` | `FEEDER_END_DAY` | *(none)* | Stop after this date (YYYY-MM-DD, inclusive) |
| `--batch-alert-threshold` | `FEEDER_BATCH_ALERT_THRESHOLD` | `2000000` | Alerts per batch before draining and checkpointing |
| `--pipeline-gate` | `FEEDER_PIPELINE_GATES` | *(required)* | `GROUP:TOPIC` pair the pipeline uses (repeatable, ≥1 required). Drain gate = all pairs at lag=0; flush set = unique topics. |
| `--skip-flush` | `FEEDER_SKIP_FLUSH` | `false` | Skip drain+flush step (useful for testing) |
| `--strip-cutouts` | `FEEDER_STRIP_CUTOUTS` | `false` | Null out `cutoutScience/Template/Difference` bytes |
| `--retention-flush-ms` | `FEEDER_RETENTION_FLUSH_MS` | `1000` | `retention.ms` set during topic purge |
| `--retention-flush-wait-s` | `FEEDER_RETENTION_FLUSH_WAIT_S` | `60.0` | Seconds to wait for log cleaner during purge |
| `--retention-normal-ms` | `FEEDER_RETENTION_NORMAL_MS` | `-1` | `retention.ms` restored after purge (`-1` = infinite) |
| `--retention-normal-bytes` | `FEEDER_RETENTION_NORMAL_BYTES` | `-1` | `retention.bytes` restored after purge (`-1` = unlimited) |
| `--no-retention-bytes-flush` | *(none)* | `false` | Skip `retention.bytes` manipulation during purge |
| `--drain-poll-seconds` | `FEEDER_DRAIN_POLL_SECONDS` | `5.0` | Seconds between consumer-lag polls while draining |
| `--drain-stable-checks` | `FEEDER_DRAIN_STABLE_CHECKS` | `3` | Consecutive zero-lag readings required before topic is considered drained |
| `--security-protocol` | `FEEDER_SECURITY_PROTOCOL` | `PLAINTEXT` | Kafka security protocol |
| `--sasl-mechanism` | `FEEDER_SASL_MECHANISM` | `PLAIN` | SASL mechanism (e.g. `SCRAM-SHA-512`) |
| `--sasl-username` | `FEEDER_SASL_USERNAME` | *(none)* | SASL username |
| `--sasl-password` | `FEEDER_SASL_PASSWORD` | *(none)* | SASL password |

#### SASL example

```bash
uv run feeder \
  --data-dir /data/ztf-archive \
  --bootstrap kafka.example.com:9092 \
  --schema-path /opt/schemas/alert.avsc \
  --security-protocol SASL_PLAINTEXT \
  --sasl-mechanism SCRAM-SHA-512 \
  --sasl-username myuser \
  --sasl-password mypassword
```

#### Graceful shutdown

Send `SIGTERM` or `SIGINT` (`Ctrl+C`) to stop after the current file. The feeder only checkpoints at batch boundaries, so re-run from the same checkpoint to resume.

### Schema unification

The `unify_schemas` module converts alerts from schema version 1.8+ to the latest version (4.02), filling in missing fields with appropriate defaults. This is useful for building uniform datasets across the full archive history.

```python
from ztf_archive_downloader.unify_schemas import convert_alert_to_latest

converted = convert_alert_to_latest(alert_dict)
```

## Environment variables

| Variable | Description |
|---|---|
| `ARCHIVE_DOWNLOAD_DIR` | Default download/validation directory (also used as `--data-dir` for the feeder) |
| `ARCHIVE_DOWNLOAD_PROXY` | Default proxy URL |
| `ARCHIVE_HTTPX_LOG_LEVEL` | Set to `INFO` or `DEBUG` to enable httpx request logging |
| `KAFKA_BOOTSTRAP` | Feeder: Kafka bootstrap servers |
| `FEEDER_SCHEMA_PATH` | Feeder: path to `alert.avsc` |
| `FEEDER_TOPIC` | Feeder: target topic (default `ztf`) |
| `FEEDER_CHECKPOINT_PATH` | Feeder: resume checkpoint file |
| `FEEDER_SECURITY_PROTOCOL` | Feeder: Kafka security protocol |
| `FEEDER_SASL_MECHANISM` | Feeder: SASL mechanism |
| `FEEDER_SASL_USERNAME` | Feeder: SASL username |
| `FEEDER_SASL_PASSWORD` | Feeder: SASL password |

## Testing

```bash
uv run --group test pytest
```

## Project structure

```
src/ztf_archive_downloader/
  downloader.py      # CLI: download and validate archive files
  get_schemas.py     # CLI: extract schema versions from git history
  unify_schemas.py   # Convert alerts across schema versions
  feeder/
    cli.py           # CLI: stream archive into Kafka (feeder)
    iter_alerts.py   # Walk tar.gz files and yield converted alerts
    kafka_io.py      # Producer, lag computation, topic flush
    checkpoint.py    # Atomic JSON checkpoint for last-done day
scripts/
  redownload.sh      # Re-fetch corrupted/missing archive files in parallel
  verify_checksums.sh # MD5-verify a directory of archive files
docs/
  feeder_plan.md     # Feeder design notes
tests/
  avro_samples/      # Sample Avro files for testing (one per schema version)
  smoke/             # Integration smoke tests (requires Docker + Kafka)
  test_unify_schemas.py
```

---

*This README was auto-generated with the assistance of [Claude Code](https://claude.ai/claude-code).*
