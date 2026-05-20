import io
import logging
import time
from typing import Any, Callable

import fastavro
from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, Producer, TopicPartition
from confluent_kafka.admin import (
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
)

logger = logging.getLogger(__name__)


def _base_conf(
    bootstrap: str,
    security_protocol: str,
    sasl_mechanism: str,
    sasl_username: str | None,
    sasl_password: str | None,
) -> dict[str, Any]:
    conf: dict[str, Any] = {
        "bootstrap.servers": bootstrap,
        "security.protocol": security_protocol,
    }
    if "SASL" in security_protocol.upper():
        conf["sasl.mechanism"] = sasl_mechanism
        if sasl_username:
            conf["sasl.username"] = sasl_username
        if sasl_password:
            conf["sasl.password"] = sasl_password
    return conf


def build_producer(
    bootstrap: str,
    security_protocol: str = "PLAINTEXT",
    sasl_mechanism: str = "PLAIN",
    sasl_username: str | None = None,
    sasl_password: str | None = None,
) -> Producer:
    conf = _base_conf(bootstrap, security_protocol, sasl_mechanism, sasl_username, sasl_password)
    conf.update(
        {
            "enable.idempotence": True,
            "compression.type": "zstd",
            "compression.level": 3,
            "linger.ms": 50,
            "batch.size": 1_000_000,
            "queue.buffering.max.messages": 100_000,
            "queue.buffering.max.kbytes": 1_048_576,
            "message.max.bytes": 67_108_864,
            "delivery.timeout.ms": 300_000,
        }
    )
    return Producer(conf)


def build_admin(
    bootstrap: str,
    security_protocol: str = "PLAINTEXT",
    sasl_mechanism: str = "PLAIN",
    sasl_username: str | None = None,
    sasl_password: str | None = None,
) -> AdminClient:
    conf = _base_conf(bootstrap, security_protocol, sasl_mechanism, sasl_username, sasl_password)
    return AdminClient(conf)


def produce_alert(
    producer: Producer,
    topic: str,
    schema: Any,
    alert: dict[str, Any],
    on_delivery: Callable[..., None],
) -> None:
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, alert)
    key = (alert.get("objectId") or "").encode("utf-8")
    while True:
        try:
            producer.produce(topic, key=key, value=buf.getvalue(), on_delivery=on_delivery)
            break
        except BufferError:
            producer.poll(0.5)
    producer.poll(0)


def compute_group_lag(
    admin: AdminClient,
    group_id: str,
    topic: str,
    consumer_conf: dict[str, Any],
    *,
    probe: Consumer | None = None,
) -> int:
    meta = admin.list_topics(topic, timeout=10)
    if topic not in meta.topics:
        raise ValueError(f"Topic {topic!r} not found")

    tps = [TopicPartition(topic, p) for p in meta.topics[topic].partitions]

    futures = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group_id, tps)])
    committed_tps = futures[group_id].result().topic_partitions
    committed = {tp.partition: tp.offset for tp in committed_tps}

    own_consumer = probe is None
    tmp = Consumer({**consumer_conf, "group.id": "_feeder-lag-probe"}) if own_consumer else probe
    total = 0
    try:
        for tp in tps:
            low, high = tmp.get_watermark_offsets(tp, timeout=10)
            c = committed.get(tp.partition, -1)
            effective = c if c >= 0 else low
            total += max(0, high - effective)
    finally:
        if own_consumer:
            tmp.close()
    return total


def count_topic_messages(
    admin: AdminClient,
    topic: str,
    consumer_conf: dict[str, Any],
    *,
    probe: Consumer | None = None,
) -> int:
    """Return the number of messages currently retained on the topic."""
    meta = admin.list_topics(topic, timeout=10)
    if topic not in meta.topics:
        return 0
    own_consumer = probe is None
    tmp = Consumer({**consumer_conf, "group.id": "_feeder-msg-counter"}) if own_consumer else probe
    total = 0
    try:
        for pid in meta.topics[topic].partitions:
            tp = TopicPartition(topic, pid)
            low, high = tmp.get_watermark_offsets(tp, timeout=5)
            total += max(0, high - low)
    finally:
        if own_consumer:
            tmp.close()
    return total


def _group_has_committed(
    admin: AdminClient,
    group_id: str,
    topic: str,
    consumer_conf: dict[str, Any],
) -> bool:
    """Return True if the consumer group has at least one committed offset on the topic."""
    meta = admin.list_topics(topic, timeout=10)
    if topic not in meta.topics:
        return False
    tps = [TopicPartition(topic, p) for p in meta.topics[topic].partitions]
    futures = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group_id, tps)])
    committed_tps = futures[group_id].result().topic_partitions
    return any(tp.offset >= 0 for tp in committed_tps)


def wait_until_drained(
    admin: AdminClient,
    group_topics: list[tuple[str, str]],
    stable_checks: int,
    poll_s: float,
    consumer_conf: dict[str, Any],
    shutdown_flag: Any = None,
) -> None:
    saw_activity: dict[tuple[str, str], bool] = {gt: False for gt in group_topics}
    consecutive_zero: dict[tuple[str, str], int] = {gt: 0 for gt in group_topics}
    start = time.monotonic()

    probe = Consumer({**consumer_conf, "group.id": "_feeder-lag-probe"})
    try:
        while True:
            if shutdown_flag is not None and shutdown_flag.is_set():
                raise RuntimeError("shutdown requested during drain")

            all_ok = True
            elapsed = time.monotonic() - start
            for g, t in group_topics:
                lag = compute_group_lag(admin, g, t, consumer_conf, probe=probe)
                key = (g, t)
                if lag > 0:
                    saw_activity[key] = True
                    consecutive_zero[key] = 0
                    all_ok = False
                    logger.info("[drain] %s/%s  lag=%-8d  elapsed=%.0fs", g, t, lag, elapsed)
                    continue
                # lag == 0: check whether the consumer has actually started
                if not saw_activity[key]:
                    if _group_has_committed(admin, g, t, consumer_conf):
                        saw_activity[key] = True
                    else:
                        all_ok = False
                        logger.info(
                            "[drain] %s/%s  lag=0  waiting for consumer to start  elapsed=%.0fs",
                            g, t, elapsed,
                        )
                        continue
                consecutive_zero[key] += 1
                if consecutive_zero[key] < stable_checks:
                    all_ok = False
                    logger.info(
                        "[drain] %s/%s  lag=0  stable %d/%d  elapsed=%.0fs",
                        g, t, consecutive_zero[key], stable_checks, elapsed,
                    )
                else:
                    logger.info("[drain] %s/%s  DRAINED after %.0fs", g, t, elapsed)

            if all_ok:
                return
            time.sleep(poll_s)
    finally:
        probe.close()


def check_flush_compatibility(admin: AdminClient, retention_flush_wait_s: float) -> None:
    """Raise if the Kafka cluster's log cleaner interval won't fit inside the flush wait window."""
    meta = admin.list_topics(timeout=10)
    broker_id = str(next(iter(meta.brokers)))
    cr = ConfigResource(ConfigResource.Type.BROKER, broker_id)
    configs = admin.describe_configs([cr])[cr].result()
    interval_ms = int(configs["log.retention.check.interval.ms"].value)
    wait_ms = int(retention_flush_wait_s * 1000)
    if interval_ms >= wait_ms:
        raise RuntimeError(
            f"log.retention.check.interval.ms={interval_ms}ms >= flush wait {wait_ms}ms — "
            f"the log cleaner will never run within the flush window. "
            f"Raise --retention-flush-wait-s (currently {retention_flush_wait_s}s) or lower "
            f"log.retention.check.interval.ms on the broker."
        )
    logger.info(
        "flush compatibility OK: log.retention.check.interval.ms=%dms, flush wait=%dms",
        interval_ms, wait_ms,
    )


def flush_topic_via_retention(
    admin: AdminClient,
    topic: str,
    consumer_conf: dict[str, Any],
    flush_ms: int = 1000,
    flush_bytes: int | None = 1,
    wait_s: float = 60.0,
    restore_ms: int = -1,
    restore_bytes: int = -1,
) -> None:
    def _apply(pairs: list[tuple[str, str]]) -> None:
        entries = [
            ConfigEntry(k, v, incremental_operation=AlterConfigOpType.SET)
            for k, v in pairs
        ]
        cr = ConfigResource(ConfigResource.Type.TOPIC, topic, incremental_configs=entries)
        futures = admin.incremental_alter_configs([cr])
        futures[cr].result()

    flush_pairs: list[tuple[str, str]] = [("retention.ms", str(flush_ms))]
    restore_pairs: list[tuple[str, str]] = [("retention.ms", str(restore_ms))]
    if flush_bytes is not None:
        flush_pairs.append(("retention.bytes", str(flush_bytes)))
        restore_pairs.append(("retention.bytes", str(restore_bytes)))

    logger.info("flushing topic %s: retention.ms=%d bytes=%s", topic, flush_ms, flush_bytes)
    _apply(flush_pairs)

    probe = Consumer({**consumer_conf, "group.id": "_feeder-msg-counter"})
    try:
        deadline = time.monotonic() + wait_s
        poll_interval = 2.0
        while True:
            n = count_topic_messages(admin, topic, consumer_conf, probe=probe)
            if n == 0:
                logger.info("[flush] topic %s is empty — log cleaner done", topic)
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                logger.warning(
                    "[flush] topic %s still has %d messages after %.0fs — restoring retention anyway",
                    topic, n, wait_s,
                )
                break
            logger.info("[flush] topic %s: %d messages remaining (%.0fs left)", topic, n, remaining)
            time.sleep(min(poll_interval, remaining))

        logger.info("restoring topic %s: retention.ms=%d bytes=%s", topic, restore_ms, restore_bytes)
        _apply(restore_pairs)

        n = count_topic_messages(admin, topic, consumer_conf, probe=probe)
        if n > 0:
            logger.warning("[flush] topic %s has %d messages after retention restored — may repopulate", topic, n)
        else:
            logger.info("[flush] topic %s confirmed empty after retention restored", topic)
    finally:
        probe.close()
