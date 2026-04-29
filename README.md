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

### Schema unification

The `unify_schemas` module converts alerts from schema version 1.8+ to the latest version (4.02), filling in missing fields with appropriate defaults. This is useful for building uniform datasets across the full archive history.

```python
from ztf_archive_downloader.unify_schemas import convert_alert_to_latest

converted = convert_alert_to_latest(alert_dict)
```

## Environment variables

| Variable | Description |
|---|---|
| `ARCHIVE_DOWNLOAD_DIR` | Default download/validation directory |
| `ARCHIVE_DOWNLOAD_PROXY` | Default proxy URL |
| `ARCHIVE_HTTPX_LOG_LEVEL` | Set to `INFO` or `DEBUG` to enable httpx request logging |

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
tests/
  avro_samples/      # Sample Avro files for testing
  test_unify_schemas.py
```

---

*This README was auto-generated with the assistance of [Claude Code](https://claude.ai/claude-code).*
