"""
Feeder smoke tests — require Docker.

Run with:
    docker compose -f tests/smoke/docker-compose.yml up -d
    pytest tests/smoke/test_feeder_smoke.py -v -s
    docker compose -f tests/smoke/docker-compose.yml down

Or let the session fixture manage Docker automatically (requires `docker compose` on PATH).
"""
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

SMOKE_DIR = Path(__file__).parent
SAMPLES_DIR = SMOKE_DIR / "samples"
MOCK_CONSUMER = SMOKE_DIR / "mock_consumer.py"
BOOTSTRAP = "localhost:9092"
TOPIC = "ztf"
SCHEMA_PATH = Path(
    os.environ.get(
        "FEEDER_SCHEMA_PATH",
        "/home/ireyes/Projects/pipeline/schemas/ztf/alert.avsc",
    )
)

# 3 sample files × 100 alerts each
EXPECTED_TOTAL_ALERTS = 300

# Invoke the feeder via its __main__ so subprocess args land in sys.argv correctly.
_FEEDER_BASE = [
    sys.executable,
    "-m",
    "ztf_archive_downloader.feeder.cli",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _wait_kafka(bootstrap: str, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=2)
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError(f"Kafka at {bootstrap} did not become ready within {timeout}s")


def _delete_topic(bootstrap: str, topic: str) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    futures = admin.delete_topics([topic], operation_timeout=10)
    for f in futures.values():
        try:
            f.result()
        except Exception:
            pass  # topic may not exist yet


def _create_topic(bootstrap: str, topic: str, num_partitions: int = 1) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    futures = admin.create_topics([NewTopic(topic, num_partitions=num_partitions, replication_factor=1)])
    for f in futures.values():
        try:
            f.result()
        except Exception:
            pass  # topic may already exist


def _get_topic_retention_ms(bootstrap: str, topic: str) -> int:
    from confluent_kafka.admin import ConfigResource  # type: ignore[attr-defined]
    admin = AdminClient({"bootstrap.servers": bootstrap})
    cr = ConfigResource(ConfigResource.Type.TOPIC, topic)
    configs = admin.describe_configs([cr])[cr].result()
    return int(configs["retention.ms"].value)


def _count_topic_messages(bootstrap: str, topic: str) -> int:
    """Sum (high - low) watermarks across all partitions."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    meta = admin.list_topics(topic, timeout=10)
    if topic not in meta.topics:
        return 0
    tmp = Consumer({"bootstrap.servers": bootstrap, "group.id": "_smoke-counter"})
    total = 0
    try:
        for pid in meta.topics[topic].partitions:
            tp = TopicPartition(topic, pid)
            low, high = tmp.get_watermark_offsets(tp, timeout=5)
            total += max(0, high - low)
    finally:
        tmp.close()
    return total


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def kafka_service():
    """Start docker compose, wait for Kafka, yield bootstrap address, then tear down."""
    subprocess.run(
        ["docker", "compose", "up", "-d"],
        cwd=SMOKE_DIR,
        check=True,
    )
    try:
        _wait_kafka(BOOTSTRAP)
        yield BOOTSTRAP
    finally:
        subprocess.run(
            ["docker", "compose", "down"],
            cwd=SMOKE_DIR,
            check=False,
        )


TOPIC_INTERMEDIATE = "ztf-intermediate"


@pytest.fixture
def fresh_topic(kafka_service):
    """Delete the test topic before each test so there is no leftover state."""
    _delete_topic(kafka_service, TOPIC)
    time.sleep(1)  # let Kafka finish the deletion before the test starts
    yield kafka_service


@pytest.fixture
def fresh_topics_two_step(kafka_service):
    """Delete and recreate both topics before the two-step test."""
    _delete_topic(kafka_service, TOPIC)
    _delete_topic(kafka_service, TOPIC_INTERMEDIATE)
    time.sleep(1)
    _create_topic(kafka_service, TOPIC)
    _create_topic(kafka_service, TOPIC_INTERMEDIATE)
    time.sleep(1)
    yield kafka_service


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_feeder_produces_correct_count(fresh_topic, tmp_path):
    """Feeder with --skip-flush must produce exactly EXPECTED_TOTAL_ALERTS messages."""
    checkpoint = tmp_path / "checkpoint.json"
    result = subprocess.run(
        _FEEDER_BASE
        + [
            "--data-dir", str(SAMPLES_DIR),
            "--bootstrap", fresh_topic,
            "--schema-path", str(SCHEMA_PATH),
            "--topic", TOPIC,
            "--checkpoint-path", str(checkpoint),
            "--pipeline-gate", "dummy:ztf",
            "--skip-flush",
        ],
        timeout=120,
    )
    assert result.returncode == 0, f"feeder exited {result.returncode} (see output above)"

    count = _count_topic_messages(fresh_topic, TOPIC)
    assert count == EXPECTED_TOTAL_ALERTS, (
        f"expected {EXPECTED_TOTAL_ALERTS} messages on topic, got {count}"
    )

    cp = json.loads(checkpoint.read_text())
    assert cp["last_done_day"] == "2019-06-01"


def test_feeder_two_step_pipeline(fresh_topics_two_step, tmp_path):
    """
    2-step chain: feeder → ztf → consumer-A (step-A, --produce-to ztf-intermediate)
    → ztf-intermediate → consumer-B (step-B, sink).  Both consumers are slow so
    the feeder observes non-zero lag on both.  Two drain+flush cycles happen
    (threshold=150 across 3×100-alert files).  Both topics must be empty and have
    retention.ms==-1 after the run.
    """
    checkpoint = tmp_path / "checkpoint.json"

    consumer_b = subprocess.Popen(
        [
            sys.executable,
            str(MOCK_CONSUMER),
            "--bootstrap", fresh_topics_two_step,
            "--topic", TOPIC_INTERMEDIATE,
            "--group", "step-B",
            "--batch", "10",
            "--pause", "0.5",
            "--idle-ticks", "0",
        ],
    )
    consumer_a = subprocess.Popen(
        [
            sys.executable,
            str(MOCK_CONSUMER),
            "--bootstrap", fresh_topics_two_step,
            "--topic", TOPIC,
            "--group", "step-A",
            "--batch", "10",
            "--pause", "0.5",
            "--idle-ticks", "0",
            "--produce-to", TOPIC_INTERMEDIATE,
        ],
    )
    try:
        result = subprocess.run(
            _FEEDER_BASE
            + [
                "--data-dir", str(SAMPLES_DIR),
                "--bootstrap", fresh_topics_two_step,
                "--schema-path", str(SCHEMA_PATH),
                "--topic", TOPIC,
                "--checkpoint-path", str(checkpoint),
                "--pipeline-gate", "step-A:ztf",
                "--pipeline-gate", "step-B:ztf-intermediate",
                "--batch-alert-threshold", "150",
                "--drain-poll-seconds", "2",
                "--drain-stable-checks", "1",
                "--retention-flush-wait-s", "15",
            ],
            timeout=360,
        )
    finally:
        consumer_a.terminate()
        consumer_b.terminate()
        try:
            consumer_a.wait(timeout=5)
        except subprocess.TimeoutExpired:
            consumer_a.kill()
        try:
            consumer_b.wait(timeout=5)
        except subprocess.TimeoutExpired:
            consumer_b.kill()

    assert result.returncode == 0, f"feeder exited {result.returncode} (see output above)"

    retention_ztf = _get_topic_retention_ms(fresh_topics_two_step, TOPIC)
    assert retention_ztf == -1, f"retention.ms not restored on {TOPIC}: got {retention_ztf}"

    retention_inter = _get_topic_retention_ms(fresh_topics_two_step, TOPIC_INTERMEDIATE)
    assert retention_inter == -1, f"retention.ms not restored on {TOPIC_INTERMEDIATE}: got {retention_inter}"

    for topic in (TOPIC, TOPIC_INTERMEDIATE):
        final_count = -1
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            final_count = _count_topic_messages(fresh_topics_two_step, topic)
            if final_count == 0:
                break
            time.sleep(2)
        assert final_count == 0, (
            f"topic {topic!r} should be empty after flush cycle, got {final_count} messages"
        )

    cp = json.loads(checkpoint.read_text())
    assert cp["last_done_day"] == "2019-06-01"
