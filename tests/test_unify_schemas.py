# pyright: reportExplicitAny=false
from pathlib import Path
from typing import Any, cast

import pytest
from fastavro import reader
from fastavro.schema import load_schema
from fastavro.validation import validate

from ztf_archive_downloader.unify_schemas import convert_alert_to_latest

LATEST_SCHEMA_PATH = Path("./schemas/4.02/ztf.alert.alert.avsc")
LATEST_SCHEMA = load_schema(str(LATEST_SCHEMA_PATH))
SAMPLES_DIR = Path("./data/unpacked/")


def find_all_sample_avros() -> list[str]:
    """
    Recursively find all .avro files under the test samples directory.
    """
    return [str(p) for p in SAMPLES_DIR.rglob("*.avro")]


@pytest.mark.parametrize("avro_path", find_all_sample_avros())
def test_convert_alert_to_latest(avro_path: str) -> None:
    with open(avro_path, "rb") as f:
        for alert in reader(f):
            converted = convert_alert_to_latest(cast(dict[str, Any], alert))
            assert converted["schemavsn"] == "4.02"
            assert validate(converted, LATEST_SCHEMA)
