import io
import tarfile
from datetime import date
from pathlib import Path

import pytest

from ztf_archive_downloader.feeder.iter_alerts import day_of, iter_alerts_in_tar, iter_days

SAMPLES_DIR = Path(__file__).parent.parent / "smoke" / "samples"
SAMPLE_20180601 = SAMPLES_DIR / "ztf_public_20180601.tar.gz"


# ---------------------------------------------------------------------------
# iter_days
# ---------------------------------------------------------------------------


def test_iter_days_chronological_order(tmp_path):
    for name in ("ztf_public_20190601.tar.gz", "ztf_public_20180601.tar.gz", "ztf_public_20181201.tar.gz"):
        (tmp_path / name).touch()
    result = list(iter_days(tmp_path))
    assert result == [
        tmp_path / "ztf_public_20180601.tar.gz",
        tmp_path / "ztf_public_20181201.tar.gz",
        tmp_path / "ztf_public_20190601.tar.gz",
    ]


def test_iter_days_ignores_non_matching(tmp_path):
    (tmp_path / "foo.tar.gz").touch()
    (tmp_path / "ztf_public_abc.tar.gz").touch()
    (tmp_path / "ztf_public_20180601.tar.gz").touch()
    result = list(iter_days(tmp_path))
    assert result == [tmp_path / "ztf_public_20180601.tar.gz"]


def test_iter_days_empty_dir(tmp_path):
    assert list(iter_days(tmp_path)) == []


def test_iter_days_start_day_filter(tmp_path):
    for name in ("ztf_public_20180601.tar.gz", "ztf_public_20181201.tar.gz", "ztf_public_20190601.tar.gz"):
        (tmp_path / name).touch()
    result = list(iter_days(tmp_path, start_day=date(2018, 12, 1)))
    assert result == [
        tmp_path / "ztf_public_20181201.tar.gz",
        tmp_path / "ztf_public_20190601.tar.gz",
    ]


def test_iter_days_end_day_filter(tmp_path):
    for name in ("ztf_public_20180601.tar.gz", "ztf_public_20181201.tar.gz", "ztf_public_20190601.tar.gz"):
        (tmp_path / name).touch()
    result = list(iter_days(tmp_path, end_day=date(2018, 12, 1)))
    assert result == [
        tmp_path / "ztf_public_20180601.tar.gz",
        tmp_path / "ztf_public_20181201.tar.gz",
    ]


def test_iter_days_exact_day(tmp_path):
    for name in ("ztf_public_20180601.tar.gz", "ztf_public_20181201.tar.gz", "ztf_public_20190601.tar.gz"):
        (tmp_path / name).touch()
    result = list(iter_days(tmp_path, start_day=date(2018, 12, 1), end_day=date(2018, 12, 1)))
    assert result == [tmp_path / "ztf_public_20181201.tar.gz"]


# ---------------------------------------------------------------------------
# day_of
# ---------------------------------------------------------------------------


def test_day_of_valid():
    assert day_of(Path("ztf_public_20180601.tar.gz")) == date(2018, 6, 1)


def test_day_of_invalid():
    with pytest.raises(ValueError):
        day_of(Path("foo.tar.gz"))


# ---------------------------------------------------------------------------
# iter_alerts_in_tar
# ---------------------------------------------------------------------------


def test_iter_alerts_count():
    alerts = list(iter_alerts_in_tar(SAMPLE_20180601, strip_cutouts=False))
    assert len(alerts) == 100


def test_iter_alerts_strip_cutouts():
    alerts = list(iter_alerts_in_tar(SAMPLE_20180601, strip_cutouts=True))
    assert len(alerts) > 0
    for alert in alerts:
        assert alert["cutoutScience"] is None
        assert alert["cutoutTemplate"] is None
        assert alert["cutoutDifference"] is None


def test_iter_alerts_keep_cutouts():
    alerts = list(iter_alerts_in_tar(SAMPLE_20180601, strip_cutouts=False))
    assert len(alerts) > 0
    assert any(
        alert.get("cutoutScience") is not None
        or alert.get("cutoutTemplate") is not None
        or alert.get("cutoutDifference") is not None
        for alert in alerts
    )


def test_iter_alerts_corrupt_tar(tmp_path):
    bad = tmp_path / "ztf_public_20180601.tar.gz"
    bad.write_bytes(b"this is not gzip data")
    alerts = list(iter_alerts_in_tar(bad, strip_cutouts=False))
    assert alerts == []


def test_iter_alerts_non_avro_member_ignored(tmp_path):
    tar_path = tmp_path / "ztf_public_20180601.tar.gz"
    buf = io.BytesIO()
    txt_content = b"hello world"
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        info = tarfile.TarInfo(name="20180601/readme.txt")
        info.size = len(txt_content)
        tar.addfile(info, io.BytesIO(txt_content))
    tar_path.write_bytes(buf.getvalue())
    alerts = list(iter_alerts_in_tar(tar_path, strip_cutouts=False))
    assert alerts == []
