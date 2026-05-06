import json
from datetime import date

from ztf_archive_downloader.feeder import checkpoint


def test_read_missing_file(tmp_path):
    assert checkpoint.read(tmp_path / "checkpoint.json") is None


def test_read_valid(tmp_path):
    p = tmp_path / "checkpoint.json"
    p.write_text(json.dumps({"last_done_day": "2024-03-15"}))
    assert checkpoint.read(p) == date(2024, 3, 15)


def test_read_missing_key(tmp_path):
    p = tmp_path / "checkpoint.json"
    p.write_text(json.dumps({"schema_version": 1}))
    assert checkpoint.read(p) is None


def test_write_creates_file(tmp_path):
    p = tmp_path / "checkpoint.json"
    checkpoint.write(p, date(2024, 3, 15))
    assert p.exists()
    data = json.loads(p.read_text())
    assert data["last_done_day"] == "2024-03-15"


def test_write_no_tmp_leftover(tmp_path):
    p = tmp_path / "checkpoint.json"
    checkpoint.write(p, date(2024, 3, 15))
    assert not p.with_suffix(".tmp").exists()


def test_roundtrip(tmp_path):
    p = tmp_path / "checkpoint.json"
    d = date(2024, 3, 15)
    checkpoint.write(p, d)
    assert checkpoint.read(p) == d


def test_write_overwrites(tmp_path):
    p = tmp_path / "checkpoint.json"
    checkpoint.write(p, date(2024, 3, 14))
    checkpoint.write(p, date(2024, 3, 15))
    assert checkpoint.read(p) == date(2024, 3, 15)


def test_write_contains_schema_version(tmp_path):
    p = tmp_path / "checkpoint.json"
    checkpoint.write(p, date(2024, 3, 15))
    data = json.loads(p.read_text())
    assert data["schema_version"] == 1
