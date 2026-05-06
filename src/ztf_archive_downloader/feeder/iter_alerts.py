import re
import tarfile
import logging
from datetime import date
from pathlib import Path
from typing import Any, Generator

import fastavro

from ztf_archive_downloader.unify_schemas import convert_alert_to_latest

logger = logging.getLogger(__name__)

_DATE_PATTERN = re.compile(r"ztf_public_(\d{8})\.tar\.gz")


def iter_days(
    data_dir: Path,
    start_day: date | None = None,
    end_day: date | None = None,
) -> Generator[Path, None, None]:
    dated: list[tuple[date, Path]] = []
    for p in data_dir.iterdir():
        m = _DATE_PATTERN.match(p.name)
        if m:
            s = m.group(1)
            d = date(int(s[:4]), int(s[4:6]), int(s[6:8]))
            dated.append((d, p))
    dated.sort()
    for d, p in dated:
        if start_day and d < start_day:
            continue
        if end_day and d > end_day:
            continue
        yield p


def day_of(tar_path: Path) -> date:
    m = _DATE_PATTERN.match(tar_path.name)
    if not m:
        raise ValueError(f"Cannot parse date from {tar_path.name}")
    s = m.group(1)
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))


def null_cutouts(alert: dict[str, Any]) -> None:
    alert["cutoutScience"] = None
    alert["cutoutTemplate"] = None
    alert["cutoutDifference"] = None


def iter_alerts_in_tar(
    tar_path: Path,
    strip_cutouts: bool,
    publisher: str = "ztf-archive-feeder",
) -> Generator[dict[str, Any], None, None]:
    corrupt = skipped = ok = 0
    try:
        tar_cm = tarfile.open(tar_path, "r:gz")
    except Exception as exc:
        logger.warning("%s: cannot open tar (skipping): %s", tar_path.name, exc)
        return
    with tar_cm as tar:
        while True:
            try:
                member = tar.next()
            except EOFError:
                logger.warning("%s: truncated tar (EOF mid-stream) — partial content used", tar_path.name)
                break
            if member is None:
                break
            if not (member.isfile() and member.name.endswith(".avro")):
                continue
            f = tar.extractfile(member)
            if f is None:
                continue
            try:
                for raw in fastavro.reader(f):
                    try:
                        alert = convert_alert_to_latest(raw, publisher=publisher)
                        if strip_cutouts:
                            null_cutouts(alert)
                        ok += 1
                        yield alert
                    except Exception as exc:
                        skipped += 1
                        logger.warning(
                            "conversion failed objectId=%s schemavsn=%s in %s/%s: %s",
                            raw.get("objectId"), raw.get("schemavsn"),
                            tar_path.name, member.name, exc,
                        )
            except Exception as exc:
                corrupt += 1
                logger.warning("corrupt avro %s in %s: %s", member.name, tar_path.name, exc)
    logger.info(
        "%s: ok=%d skipped=%d corrupt_members=%d",
        tar_path.name, ok, skipped, corrupt,
    )
