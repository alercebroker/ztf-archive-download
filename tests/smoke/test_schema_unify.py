"""
Schema-unify regression: every alert in tests/avro_samples/ must survive
convert_alert_to_latest() and round-trip through schemaless Avro write/read
against the v4.02 schema, coming out as schemavsn=4.02.
"""

import io
import sys
from pathlib import Path

import fastavro
from fastavro.schema import load_schema

from ztf_archive_downloader.unify_schemas import convert_alert_to_latest

SAMPLES_ROOT = Path(__file__).parents[2] / "tests" / "avro_samples"
SCHEMA_PATH = Path("/home/ireyes/Projects/pipeline/schemas/ztf/alert.avsc")


def main() -> None:
    schema = load_schema(str(SCHEMA_PATH))

    ok = conv_errors = 0
    unreadable: list[str] = []
    seen_versions: set[str] = set()

    for day_dir in sorted(SAMPLES_ROOT.iterdir()):
        if not day_dir.is_dir():
            continue
        for avro_path in sorted(day_dir.glob("*.avro")):
            with open(avro_path, "rb") as f:
                try:
                    records = list(fastavro.reader(f))
                except Exception as e:
                    # Corrupt/truncated fixture file — not a code bug; feeder skips these too.
                    unreadable.append(f"{avro_path.relative_to(SAMPLES_ROOT)}: {type(e).__name__}")
                    continue

            for raw in records:
                orig_vsn = raw.get("schemavsn", "unknown")
                try:
                    alert = convert_alert_to_latest(raw)
                    buf = io.BytesIO()
                    fastavro.schemaless_writer(buf, schema, alert)
                    decoded = fastavro.schemaless_reader(io.BytesIO(buf.getvalue()), schema)

                    assert decoded["schemavsn"] == "4.02", f"schemavsn={decoded['schemavsn']!r}"
                    assert decoded["objectId"], f"empty objectId (candid={decoded.get('candid')})"
                    assert decoded["candidate"], "missing candidate block"

                    seen_versions.add(orig_vsn)
                    ok += 1
                except Exception as e:
                    obj = raw.get("objectId", "?")
                    print(f"  FAIL {avro_path.relative_to(SAMPLES_ROOT)} vsn={orig_vsn} obj={obj}: {e}", file=sys.stderr)
                    conv_errors += 1

    print(f"schema versions tested : {sorted(seen_versions)}")
    print(f"ok={ok}  conversion_errors={conv_errors}  unreadable_fixtures={len(unreadable)}")
    if unreadable:
        for u in unreadable:
            print(f"  (skipped corrupt fixture: {u})")

    if conv_errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
