import json
import os
from datetime import date, datetime, timezone
from pathlib import Path


def read(path: Path) -> date | None:
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    raw = data.get("last_done_day")
    return date.fromisoformat(raw) if raw else None


def write(path: Path, last_done: date) -> None:
    tmp = path.with_suffix(".tmp")
    data = {
        "last_done_day": last_done.isoformat(),
        "schema_version": 1,
        "completed_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    tmp.write_text(json.dumps(data, indent=2))
    os.replace(tmp, path)
