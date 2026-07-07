"""
Analytics Consumer
Persists every order to Couchbase. Failed messages go to analytics-dlq.
Exposes Prometheus metrics on :8003.
"""
import json
import os
import time
import uuid
import logging
import threading
from datetime import datetime, timezone, timedelta

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, ClusterTimeoutOptions
from couchbase.auth import PasswordAuthenticator
from prometheus_client import Counter, Histogram, Gauge, start_http_server

# ── Structured JSON logger ──────────────────────────────────────────────────
class _JSONFormatter(logging.Formatter):
    def format(self, record):
        log = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": "analytics-service",
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
logger = logging.getLogger("analytics-service")
logger.addHandler(_h)
logger.setLevel(logging.INFO)

# ── Prometheus metrics ──────────────────────────────────────────────────────
ORDERS_SAVED      = Counter("analytics_orders_saved_total", "Total orders persisted to Couchbase")
ORDERS_FAILED     = Counter("analytics_orders_failed_total", "Orders that failed to persist")
DLQ_SENDS         = Counter("analytics_dlq_sends_total", "Messages sent to DLQ")
SAVE_LATENCY      = Histogram("analytics_save_seconds", "Couchbase upsert latency")
TOTAL_SALES_GAUGE = Gauge("analytics_total_sales_usd", "Running total of sales in USD")
ORDER_COUNT_GAUGE = Gauge("analytics_order_count", "Running count of processed orders")

# ── Config ──────────────────────────────────────────────────────────────────
BOOTSTRAP        = os.environ.get("KAFKA_BOOTSTRAP", os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
API_KEY          = os.environ.get("CONFLUENT_API_KEY", "")
API_SECRET       = os.environ.get("CONFLUENT_API_SECRET", "")
COUCHBASE_HOST   = os.environ.get("COUCHBASE_HOST", "couchbase")
COUCHBASE_BUCKET = os.environ.get("COUCHBASE_BUCKET", "order_analytics")
COUCHBASE_USER   = os.environ.get("COUCHBASE_USERNAME", "Administrator")
COUCHBASE_PASS   = os.environ.get("COUCHBASE_PASSWORD", "password")
METRICS_PORT     = int(os.environ.get("METRICS_PORT", "8003"))

ORDERS_TOPIC    = "orders"
ANALYTICS_TOPIC = "order-analytics"
DLQ_TOPIC       = "analytics-dlq"

MAX_RETRIES   = 3
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


def _connect_kafka_with_retry(factory, label, max_tries=30, delay=2):
    for attempt in range(1, max_tries + 1):
        try:
            client = factory()
            logger.info("kafka_connected", extra={"client": label, "attempt": attempt})
            return client
        except NoBrokersAvailable:
            if attempt == max_tries:
                raise
            logger.warning("waiting_for_kafka", extra={"client": label, "attempt": attempt})
            time.sleep(delay)


def _connect_couchbase():
    conn_str = (
        f"couchbases://{COUCHBASE_HOST}"
        if "cloud.couchbase.com" in COUCHBASE_HOST
        else f"couchbase://{COUCHBASE_HOST}"
    )
    for attempt in range(1, 11):
        try:
            cluster = Cluster(
                conn_str,
                ClusterOptions(
                    PasswordAuthenticator(COUCHBASE_USER, COUCHBASE_PASS),
                    timeout_options=ClusterTimeoutOptions(
                        kv_timeout=timedelta(seconds=10),
                        connect_timeout=timedelta(seconds=10),
                    ),
                ),
            )
            collection = cluster.bucket(COUCHBASE_BUCKET).default_collection()
            try:
                cluster.query(f"CREATE PRIMARY INDEX ON `{COUCHBASE_BUCKET}`")
            except Exception:
                pass  # index already exists
            logger.info("couchbase_connected", extra={"host": COUCHBASE_HOST, "bucket": COUCHBASE_BUCKET})
            return cluster, collection
        except Exception as e:
            logger.warning("couchbase_waiting", extra={"attempt": attempt, "error": str(e)})
            time.sleep(3)
    logger.error("couchbase_unavailable")
    return None, None


def _send_to_dlq(producer, msg, error: str):
    payload = {
        "source_topic":     msg.topic,
        "source_partition": msg.partition,
        "source_offset":    msg.offset,
        "payload":          msg.value,
        "error":            error,
        "failed_at":        datetime.now(timezone.utc).isoformat(),
        "service":          "analytics-service",
    }
    producer.send(DLQ_TOPIC, value=json.dumps(payload).encode())
    DLQ_SENDS.inc()
    logger.error("sent_to_dlq", extra={"order_id": msg.value.get("order_id"), "error": error})


def process_message(msg, collection, analytics_producer, dlq_producer, state: dict):
    order    = msg.value
    order_id = order.get("order_id")

    for attempt in range(MAX_RETRIES):
        try:
            doc_id = str(order_id or uuid.uuid4())
            doc = {
                **order,
                "processed_at":    datetime.now(timezone.utc).isoformat(),
                "kafka_offset":    msg.offset,
                "kafka_partition": msg.partition,
            }

            if collection:
                with SAVE_LATENCY.time():
                    collection.upsert(doc_id, doc)

            state["total_sales"] += float(order.get("amount", 0))
            state["order_count"] += 1
            ORDERS_SAVED.inc()
            TOTAL_SALES_GAUGE.set(state["total_sales"])
            ORDER_COUNT_GAUGE.set(state["order_count"])

            summary = {
                "total_sales": round(state["total_sales"], 2),
                "order_count": state["order_count"],
                "updated_at":  datetime.now(timezone.utc).isoformat(),
            }
            analytics_producer.send(ANALYTICS_TOPIC, value=json.dumps(summary).encode())

            logger.info("order_saved", extra={
                "order_id":    order_id,
                "total_sales": round(state["total_sales"], 2),
                "order_count": state["order_count"],
            })
            return

        except Exception as e:
            ORDERS_FAILED.inc()
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

    consumer = _connect_kafka_with_retry(
        lambda: KafkaConsumer(
            ORDERS_TOPIC,
            **base_cfg,
            group_id="analytics-group",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        ),
        "consumer",
    )
    analytics_producer = _connect_kafka_with_retry(lambda: KafkaProducer(**base_cfg), "analytics-producer")
    dlq_producer       = _connect_kafka_with_retry(lambda: KafkaProducer(**base_cfg), "dlq-producer")

    _, collection = _connect_couchbase()
    state = {"total_sales": 0.0, "order_count": 0}
    logger.info("listening", extra={"topic": ORDERS_TOPIC})

    try:
        for msg in consumer:
            process_message(msg, collection, analytics_producer, dlq_producer, state)
    except KeyboardInterrupt:
        logger.info("shutting_down")
    finally:
        consumer.close()
        analytics_producer.close()
        dlq_producer.close()


if __name__ == "__main__":
    main()
