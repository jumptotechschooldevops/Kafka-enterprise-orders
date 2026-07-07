"""
Payment Processing Consumer
Reads orders, simulates payment, publishes to payments topic.
Failed messages go to orders-dlq. Exposes metrics on :8002.
"""
import json
import os
import time
import logging
import threading
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from prometheus_client import Counter, Histogram, start_http_server

# ── Structured JSON logger ──────────────────────────────────────────────────
class _JSONFormatter(logging.Formatter):
    def format(self, record):
        log = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": "payment-service",
            "msg": record.getMessage(),
        }
        for k, v in record.__dict__.items():
            if k.startswith("_") or k in (
                "msg", "args", "levelname", "levelno", "pathname", "filename",
                "module", "exc_info", "exc_text", "stack_info", "lineno",
                "funcName", "created", "msecs", "relativeCreated", "thread",
                "threadName", "processName", "process", "name", "message",
            ):
                continue
            log[k] = v
        return json.dumps(log)

_h = logging.StreamHandler()
_h.setFormatter(_JSONFormatter())
logger = logging.getLogger("payment-service")
logger.addHandler(_h)
logger.setLevel(logging.INFO)

# ── Prometheus metrics ──────────────────────────────────────────────────────
PAYMENTS_PROCESSED = Counter("payment_processed_total", "Total payments processed")
PAYMENTS_FAILED    = Counter("payment_failed_total", "Total payment processing failures")
DLQ_SENDS          = Counter("payment_dlq_sends_total", "Messages sent to DLQ")
PROCESSING_TIME    = Histogram("payment_processing_seconds", "Payment processing latency")

# ── Config ──────────────────────────────────────────────────────────────────
BOOTSTRAP    = os.environ.get("KAFKA_BOOTSTRAP", os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
API_KEY      = os.environ.get("CONFLUENT_API_KEY", "")
API_SECRET   = os.environ.get("CONFLUENT_API_SECRET", "")
METRICS_PORT = int(os.environ.get("METRICS_PORT", "8002"))

ORDERS_TOPIC   = "orders"
PAYMENTS_TOPIC = "payments"
DLQ_TOPIC      = "orders-dlq"

MAX_RETRIES  = 3
RETRY_BACKOFF = [1, 2, 4]


def _kafka_config():
    cfg = {}
    if API_KEY and API_SECRET:
        cfg.update({
            "bootstrap_servers":   BOOTSTRAP,
            "security_protocol":   "SASL_SSL",
            "sasl_mechanism":      "PLAIN",
            "sasl_plain_username": API_KEY,
            "sasl_plain_password": API_SECRET,
        })
    else:
        cfg["bootstrap_servers"] = BOOTSTRAP
    return cfg


def _connect_with_retry(factory, label, max_tries=30, delay=2):
    for attempt in range(1, max_tries + 1):
        try:
            client = factory()
            logger.info("connected", extra={"client": label, "attempt": attempt})
            return client
        except NoBrokersAvailable:
            if attempt == max_tries:
                raise
            logger.warning("waiting_for_kafka", extra={"client": label, "attempt": attempt, "retry_in": delay})
            time.sleep(delay)


def _send_to_dlq(producer: KafkaProducer, msg, error: str):
    payload = {
        "source_topic":     msg.topic,
        "source_partition": msg.partition,
        "source_offset":    msg.offset,
        "payload":          msg.value,
        "error":            error,
        "failed_at":        datetime.now(timezone.utc).isoformat(),
        "service":          "payment-service",
    }
    producer.send(DLQ_TOPIC, value=json.dumps(payload).encode())
    DLQ_SENDS.inc()
    logger.error("sent_to_dlq", extra={"order_id": msg.value.get("order_id"), "error": error})


def process_message(msg, payment_producer: KafkaProducer, dlq_producer: KafkaProducer):
    order    = msg.value
    order_id = order.get("order_id")

    for attempt in range(MAX_RETRIES):
        try:
            with PROCESSING_TIME.time():
                time.sleep(0.1)  # simulate external payment gateway call
                payment = {
                    "order_id":    order_id,
                    "status":      "PAID",
                    "amount":      order["amount"],
                    "country":     order["country"],
                    "processed_at": datetime.now(timezone.utc).isoformat(),
                }
                payment_producer.send(PAYMENTS_TOPIC, value=json.dumps(payment).encode())
                PAYMENTS_PROCESSED.inc()
                logger.info("payment_processed", extra={
                    "order_id": order_id,
                    "amount":   order["amount"],
                })
            return

        except Exception as e:
            PAYMENTS_FAILED.inc()
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF[attempt]
                logger.warning("retry", extra={"order_id": order_id, "attempt": attempt + 1, "wait": wait, "error": str(e)})
                time.sleep(wait)
            else:
                _send_to_dlq(dlq_producer, msg, str(e))


def main():
    threading.Thread(target=lambda: start_http_server(METRICS_PORT), daemon=True).start()
    logger.info("metrics_server_started", extra={"port": METRICS_PORT})

    base_cfg = _kafka_config()

    consumer = _connect_with_retry(
        lambda: KafkaConsumer(
            ORDERS_TOPIC,
            **base_cfg,
            group_id="payments-group",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
        ),
        "consumer",
    )
    payment_producer = _connect_with_retry(lambda: KafkaProducer(**base_cfg), "payment-producer")
    dlq_producer     = _connect_with_retry(lambda: KafkaProducer(**base_cfg), "dlq-producer")

    logger.info("listening", extra={"topic": ORDERS_TOPIC})

    try:
        for msg in consumer:
            process_message(msg, payment_producer, dlq_producer)
    except KeyboardInterrupt:
        logger.info("shutting_down")
    finally:
        consumer.close()
        payment_producer.close()
        dlq_producer.close()


if __name__ == "__main__":
    main()
