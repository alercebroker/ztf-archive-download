"""
Simulated downstream consumer for feeder smoke tests.

Reads --batch messages per tick, commits offsets, sleeps --pause seconds, repeats.
Exits on SIGINT/SIGTERM, or after --idle-ticks consecutive empty ticks (0 = run forever).
Pass --produce-to TOPIC to re-produce each consumed message to a downstream topic,
enabling two chained instances to emulate a 2-step pipeline.
"""
import argparse
import logging
import signal
import time

from confluent_kafka import Consumer, KafkaError, Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--group", required=True)
    parser.add_argument("--batch", type=int, default=10, help="messages to consume per tick")
    parser.add_argument("--pause", type=float, default=1.0, help="seconds to sleep between ticks")
    parser.add_argument(
        "--idle-ticks",
        type=int,
        default=20,
        help="consecutive empty ticks before exit; 0 = run until signal",
    )
    parser.add_argument(
        "--produce-to",
        default=None,
        metavar="TOPIC",
        help="if set, re-produce each consumed message to this topic (chaining mode)",
    )
    args = parser.parse_args()

    producer = (
        Producer(
            {
                "bootstrap.servers": args.bootstrap,
                "enable.idempotence": True,
                "compression.type": "zstd",
                "linger.ms": 50,
            }
        )
        if args.produce_to
        else None
    )

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": args.group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "fetch.wait.max.ms": 500,
        }
    )
    consumer.subscribe([args.topic])

    running = True

    def _stop(sig: int, frame: object) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    idle = 0
    total = 0
    try:
        while running:
            msgs = []
            for _ in range(args.batch):
                msg = consumer.poll(timeout=0.5)
                if msg is None:
                    break
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.warning("consumer error: %s", msg.error())
                    break
                msgs.append(msg)

            if msgs:
                if producer is not None:
                    for msg in msgs:
                        producer.produce(args.produce_to, key=msg.key(), value=msg.value())
                    producer.flush()  # produce+flush before commit so a crash never advances consumer past unproduced messages
                consumer.commit(asynchronous=False)
                total += len(msgs)
                idle = 0
                logger.info("consumed %d this tick (total %d)", len(msgs), total)
                time.sleep(args.pause)
            else:
                idle += 1
                if args.idle_ticks > 0 and idle >= args.idle_ticks:
                    logger.info("idle timeout after %d ticks, exiting (total=%d)", idle, total)
                    break
                time.sleep(args.pause)
    finally:
        if producer is not None:
            producer.flush()
        consumer.close()

    logger.info("done, total=%d", total)


if __name__ == "__main__":
    main()
