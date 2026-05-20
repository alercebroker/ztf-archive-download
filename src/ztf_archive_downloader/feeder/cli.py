import logging
import signal
import threading
from datetime import date
from pathlib import Path
from typing import Annotated, Optional

import typer
from fastavro.schema import load_schema

from . import checkpoint
from .iter_alerts import day_of, iter_alerts_in_tar, iter_days
from .kafka_io import (
    build_admin,
    build_producer,
    check_flush_compatibility,
    count_topic_messages,
    flush_topic_via_retention,
    produce_alert,
    wait_until_drained,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
# suppress per-alert INFO from unify_schemas
logging.getLogger("ztf_archive_downloader.unify_schemas").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

app = typer.Typer(add_completion=False)


def _parse_pipeline_gates(items: list[str]) -> list[tuple[str, str]]:
    result = []
    for item in items:
        parts = item.split(":", 1)
        if len(parts) != 2 or not parts[0] or not parts[1]:
            raise typer.BadParameter(f"expected GROUP:TOPIC, got {item!r}")
        result.append((parts[0], parts[1]))
    if not result:
        raise typer.BadParameter("at least one --pipeline-gate is required")
    return result


@app.command()
def run(
    data_dir: Annotated[
        Path,
        typer.Option("--data-dir", envvar="ARCHIVE_DOWNLOAD_DIR", help="Directory of ztf_public_*.tar.gz files"),
    ],
    bootstrap: Annotated[
        str,
        typer.Option("--bootstrap", envvar="KAFKA_BOOTSTRAP", help="Kafka bootstrap servers"),
    ],
    schema_path: Annotated[
        Path,
        typer.Option("--schema-path", envvar="FEEDER_SCHEMA_PATH", help="Path to alert.avsc (siblings resolved from same dir)"),
    ],
    topic: Annotated[str, typer.Option("--topic", envvar="FEEDER_TOPIC")] = "ztf",
    checkpoint_path: Annotated[Path, typer.Option("--checkpoint-path", envvar="FEEDER_CHECKPOINT_PATH")] = Path("./feeder_checkpoint.json"),
    pipeline_gate: Annotated[
        list[str],
        typer.Option("--pipeline-gate", envvar="FEEDER_PIPELINE_GATES", help="GROUP:TOPIC pairs (repeatable, ≥1 required). Drain gate = all pairs at lag=0 stable. Flush set = unique topics."),
    ] = [],
    extra_flush_topic: Annotated[
        list[str],
        typer.Option("--extra-flush-topic", envvar="FEEDER_EXTRA_FLUSH_TOPICS", help="Additional topics to flush via retention after each batch (no drain gate required)."),
    ] = [],
    batch_alert_threshold: Annotated[int, typer.Option("--batch-alert-threshold", envvar="FEEDER_BATCH_ALERT_THRESHOLD")] = 2_000_000,
    strip_cutouts: Annotated[bool, typer.Option("--strip-cutouts/--keep-cutouts", envvar="FEEDER_STRIP_CUTOUTS")] = False,
    start_day: Annotated[Optional[str], typer.Option("--start-day", envvar="FEEDER_START_DAY", help="YYYY-MM-DD inclusive")] = None,
    end_day: Annotated[Optional[str], typer.Option("--end-day", envvar="FEEDER_END_DAY", help="YYYY-MM-DD inclusive")] = None,
    security_protocol: Annotated[str, typer.Option("--security-protocol", envvar="FEEDER_SECURITY_PROTOCOL")] = "PLAINTEXT",
    sasl_username: Annotated[Optional[str], typer.Option("--sasl-username", envvar="FEEDER_SASL_USERNAME")] = None,
    sasl_password: Annotated[Optional[str], typer.Option("--sasl-password", envvar="FEEDER_SASL_PASSWORD")] = None,
    sasl_mechanism: Annotated[str, typer.Option("--sasl-mechanism", envvar="FEEDER_SASL_MECHANISM")] = "PLAIN",
    retention_flush_ms: Annotated[int, typer.Option("--retention-flush-ms", envvar="FEEDER_RETENTION_FLUSH_MS")] = 1000,
    retention_normal_ms: Annotated[int, typer.Option("--retention-normal-ms", envvar="FEEDER_RETENTION_NORMAL_MS")] = -1,
    retention_normal_bytes: Annotated[int, typer.Option("--retention-normal-bytes", envvar="FEEDER_RETENTION_NORMAL_BYTES", help="Restore value for retention.bytes (-1 = unlimited)")] = -1,
    retention_flush_wait_s: Annotated[float, typer.Option("--retention-flush-wait-s", envvar="FEEDER_RETENTION_FLUSH_WAIT_S")] = 60.0,
    drain_stable_checks: Annotated[int, typer.Option("--drain-stable-checks", envvar="FEEDER_DRAIN_STABLE_CHECKS")] = 3,
    drain_poll_seconds: Annotated[float, typer.Option("--drain-poll-seconds", envvar="FEEDER_DRAIN_POLL_SECONDS")] = 5.0,
    skip_flush: Annotated[bool, typer.Option("--skip-flush", envvar="FEEDER_SKIP_FLUSH")] = False,
    no_retention_bytes_flush: Annotated[bool, typer.Option("--no-retention-bytes-flush")] = False,
) -> None:
    start_date = date.fromisoformat(start_day) if start_day else None
    end_date = date.fromisoformat(end_day) if end_day else None
    gates = _parse_pipeline_gates(pipeline_gate)
    flush_topics = sorted({t for _, t in gates} | set(extra_flush_topic))

    schema = load_schema(str(schema_path))

    kafka_kwargs = dict(
        bootstrap=bootstrap,
        security_protocol=security_protocol,
        sasl_mechanism=sasl_mechanism,
        sasl_username=sasl_username,
        sasl_password=sasl_password,
    )
    from .kafka_io import _base_conf
    consumer_conf = _base_conf(bootstrap, security_protocol, sasl_mechanism, sasl_username, sasl_password)

    producer = build_producer(**kafka_kwargs)
    admin = build_admin(**kafka_kwargs)

    logger.info("pipeline-gate: %s", gates)
    logger.info("flush set (unique topics): %s", flush_topics)

    if not skip_flush:
        check_flush_compatibility(admin, retention_flush_wait_s)

    shutdown = threading.Event()

    def _handle_signal(signum: int, frame: object) -> None:
        logger.warning("signal %d received, shutting down after current file", signum)
        shutdown.set()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    last_done = checkpoint.read(checkpoint_path)
    logger.info("last completed day from checkpoint: %s", last_done)

    days = list(iter_days(data_dir, start_date, end_date))
    todo = [p for p in days if last_done is None or day_of(p) > last_done]
    logger.info("%d days to process (%d total in range)", len(todo), len(days))

    delivery_errors: list[Exception] = []

    def on_delivery(err: object, msg: object) -> None:
        if err is not None:
            delivery_errors.append(Exception(str(err)))

    batch: list[Path] = []
    batch_alerts = 0

    flush_bytes: int | None = None if no_retention_bytes_flush else 1

    def finalize_batch() -> None:
        logger.info(
            "--- BATCH COMPLETE: %d alerts in %d file(s) — flushing producer ---",
            batch_alerts, len(batch),
        )
        producer.flush(timeout=300)
        if delivery_errors:
            raise RuntimeError(f"delivery errors in batch: {delivery_errors}")

        for t in flush_topics:
            n = count_topic_messages(admin, t, consumer_conf)
            logger.info("topic %r: %d messages available for consumers", t, n)

        if not skip_flush:
            wait_until_drained(
                admin,
                gates,
                drain_stable_checks,
                drain_poll_seconds,
                consumer_conf,
                shutdown,
            )
            for t in flush_topics:
                flush_topic_via_retention(
                    admin,
                    t,
                    consumer_conf,
                    flush_ms=retention_flush_ms,
                    flush_bytes=flush_bytes,
                    wait_s=retention_flush_wait_s,
                    restore_ms=retention_normal_ms,
                    restore_bytes=retention_normal_bytes,
                )

    try:
        for tar_path in todo:
            if shutdown.is_set():
                break
            if delivery_errors:
                logger.error("aborting: delivery errors detected before processing %s", tar_path.name)
                raise SystemExit(1)

            logger.info(
                "producing %s  [batch so far: %d/%d alerts in %d file(s)]",
                tar_path.name, batch_alerts, batch_alert_threshold, len(batch),
            )
            count = 0
            produce_errors = 0
            for alert in iter_alerts_in_tar(tar_path, strip_cutouts):
                if shutdown.is_set():
                    break
                try:
                    produce_alert(producer, topic, schema, alert, on_delivery)
                    count += 1
                except Exception as exc:
                    produce_errors += 1
                    logger.warning(
                        "produce failed objectId=%s: %s",
                        alert.get("objectId"), exc,
                    )
            if produce_errors:
                logger.warning("%s: %d alerts skipped due to produce errors", tar_path.name, produce_errors)

            batch.append(tar_path)
            batch_alerts += count
            logger.info(
                "file done: %s — %d alerts  |  batch: %d/%d alerts in %d file(s)",
                tar_path.name, count, batch_alerts, batch_alert_threshold, len(batch),
            )

            if batch_alerts < batch_alert_threshold:
                logger.info(
                    "threshold not reached (%d/%d) — accumulating next file",
                    batch_alerts, batch_alert_threshold,
                )
                continue

            finalize_batch()
            last_day = day_of(batch[-1])
            checkpoint.write(checkpoint_path, last_day)
            logger.info("--- CHECKPOINT: %s ---", last_day)
            batch = []
            batch_alerts = 0

        if batch and not shutdown.is_set():
            logger.info(
                "--- TAIL BATCH: %d alerts in %d file(s) ---",
                batch_alerts, len(batch),
            )
            finalize_batch()
            last_day = day_of(batch[-1])
            checkpoint.write(checkpoint_path, last_day)
            logger.info("--- CHECKPOINT: %s ---", last_day)

    except (KeyboardInterrupt, RuntimeError) as exc:
        logger.warning("aborting: %s", exc)
        producer.flush(timeout=120)
        raise SystemExit(1)

    if shutdown.is_set():
        logger.info("shutdown cleanly — run again to resume from checkpoint")
        producer.flush(timeout=120)
    else:
        logger.info("all days processed")


if __name__ == "__main__":
    app()
