# pyright: reportExplicitAny=false
import logging
from pathlib import Path
from typing import Any, cast

from fastavro.schema import load_schema
from fastavro.types import Schema

SCHEMAS_DIR = Path("./schemas")
LATEST_VERSION = "4.02"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_avro_schema(version: str) -> Schema:
    """
    Load an Avro schema JSON as a dict for a given version and schema name.
    """
    schema_path = SCHEMAS_DIR / version / "ztf.alert.alert.avsc"
    return load_schema(str(schema_path))


def convert_prv_candidate_to_latest(prv: dict[str, Any]) -> dict[str, Any]:
    """
    Convert a prv_candidate dict from schema 1.8+ to the latest schema version (4.02).
    Only adds new fields with their 4.02 defaults if missing.
    """
    new_fields_defaults = {
        "clrcoeff": None,
        "clrcounc": None,
        "magzpsci": None,
        "magzpscirms": None,
        "magzpsciunc": None,
        "rbversion": "",
    }
    for field, default in new_fields_defaults.items():
        if field not in prv:
            prv[field] = default
    return prv


def convert_candidate_to_latest(candidate: dict[str, Any]) -> dict[str, Any]:
    """
    Convert a candidate dict from schema 1.8+ to the latest schema version (4.02).
    Only adds new fields with their 4.02 defaults if missing.
    """
    new_fields_defaults = {
        "clrcoeff": None,
        "clrcounc": None,
        "clrmed": None,
        "clrrms": None,
        "drb": None,
        "dsdiff": None,
        "dsnrms": None,
        "exptime": None,
        "maggaia": None,
        "maggaiabright": None,
        "magzpsci": None,
        "magzpscirms": None,
        "magzpsciunc": None,
        "neargaia": None,
        "neargaiabright": None,
        "ssnrms": None,
        "zpclrcov": None,
        "zpmed": None,
        "drbversion": "",
        "rbversion": "",
        "nmatches": 0,
    }

    for field, default in new_fields_defaults.items():
        if field not in candidate:
            candidate[field] = default

    if "tooflag" not in candidate:
        candidate["tooflag"] = None

    return candidate


def convert_alert_to_latest(
    alert: dict[str, Any], publisher: str = "unknown"
) -> dict[str, Any]:
    """
    Convert an alert dict from schema 1.8+ to the latest schema version (4.02).
    Handles nested fields and fills missing fields as required.
    """
    base_version = alert.get("schemavsn", "unknown")
    logger.info(f"Converting alert from version {base_version} to {LATEST_VERSION}")
    return {
        "schemavsn": LATEST_VERSION,
        "publisher": publisher,
        "objectId": alert.get("objectId", ""),
        "candid": alert.get("candid"),
        "candidate": convert_candidate_to_latest(alert.get("candidate", {})),
        "prv_candidates": [
            convert_prv_candidate_to_latest(prv)
            for prv in cast(list[dict[str, Any]], alert.get("prv_candidates") or [])
        ]
        if alert.get("prv_candidates") is not None
        else None,
        "fp_hists": alert.get("fp_hists", None),
        "cutoutScience": alert.get("cutoutScience", None),
        "cutoutTemplate": alert.get("cutoutTemplate", None),
        "cutoutDifference": alert.get("cutoutDifference", None),
    }
