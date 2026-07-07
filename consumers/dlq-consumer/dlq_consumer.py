"""
Dead Letter Queue (DLQ) Consumer
Reads all *-dlq topics, logs every failed message with full context,
and can forward to an alerting webhook (e.g. Slack, PagerDuty).

Why DLQ matters: without this, messages that fail processing after all
retries are silently dropped. This consumer makes those failures visible
and actionable without blocking the main consumer groups.
"""
import json
import os
import time
import logging
import threading
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from prometheus_client import Counter, start_http_server

# ── Structured JSON logger ──────────────────────────────────────────────────
class _JSONFormatter(logging.Formatter):
    def format(self, record):
        log = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": "dlq-consumer",
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
logger = logging.getLogger("dlq-consumer")
logger.addHandler(_h)
logger.setLevel(logging.INFO)

# ── Prometheus metrics ──────────────────────────────────────────────────────
DLQ_RECEIVED = Counter("dlq_messages_received_total", "Total DLQ messages received", ["topic", "source_service"])
DLQ_ALERTED  = Counter("dlq_alerts_sent_total", "Total webhook alerts sent")

# ── Config ──────────────────────────────────────────────────────────────────
BOOTSTRAP      = os.environ.get("KAFKA_BOOTSTRAP", os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
API_KEY        = os.environ.get("CONFLUENT_API_KEY", "")
API_SECRET     = os.environ.get("CONFLUENT_API_SECRET", "")
WEBHOOK_URL    = os.environ.get("ALERT_WEBHOOK_URL", "")  # Slack/PagerDuty webhook
METRICS_PORT   = int(os.environ.get("METRICS_PORT", "8004"))

# All DLQ topics to monitor
DLQ_TOPICS = [
    "orders-dlq",
    "analytics-dlq",
]


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


def _connect_with_retry(max_tries=30, delay=2):
    base_cfg = _kafka_config()
    for attempt in range(1, max_tries + 1):
        try:
            consumer = KafkaConsumer(
                *DLQ_TOPICS,
                **base_cfg,
                group_id="dlq-monitor-group",
                value_deserializer=lambda m: _safe_decode(m),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            )
            logger.info("connected", extra={"topics": DLQ_TOPICS, "attempt": attempt})
            return consumer
        except NoBrokersAvailable:
            if attempt == max_tries:
                raise
            logger.warning("waiting_for_kafka", extra={"attempt": attempt, "retry_in": delay})
            time.sleep(delay)


def _safe_decode(raw: bytes):
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return {"raw": raw.decode("utf-8", errors="replace")}


def _send_webhook_alert(failed_msg: dict):
    """Send a Slack/webhook notification for a DLQ message."""
    if not WEBHOOK_URL:
        return
    body = json.dumps({
        "text": (
            f":red_circle: *DLQ Alert* — `{failed_msg.get('source_topic', 'unknown')}` "
            f"| service: `{failed_msg.get('service', '?')}` "
            f"| error: `{failed_msg.get('error', '?')}` "
            f"| order_id: `{failed_msg.get('payload', {}).get('order_id', '?')}`"
        )
    }).encode()
    try:
        req = Request(WEBHOOK_URL, data=body, headers={"Content-Type": "application/json"}, method="POST")
        urlopen(req, timeout=5)
        DLQ_ALERTED.inc()
    except URLError as e:
        logger.warning("webhook_failed", extra={"error": str(e)})


def main():
    threading.Thread(target=lambda: start_http_server(METRICS_PORT), daemon=True).start()
    logger.info("metrics_server_started", extra={"port": METRICS_PORT})

    consumer = _connect_with_retry()
    logger.info("monitoring_dlq_topics", extra={"topics": DLQ_TOPICS})

    try:
        for msg in consumer:
            failed = msg.value
            source_service = failed.get("service", "unknown")
            source_topic   = failed.get("source_topic", msg.topic)

            DLQ_RECEIVED.labels(topic=msg.topic, source_service=source_service).inc()

            logger.error("dlq_message", extra={
                "dlq_topic":        msg.topic,
                "source_topic":     source_topic,
                "source_partition": failed.get("source_partition"),
                "source_offset":    failed.get("source_offset"),
                "service":          source_service,
                "error":            failed.get("error"),
                "failed_at":        failed.get("failed_at"),
                "order_id":         failed.get("payload", {}).get("order_id"),
            })

            _send_webhook_alert(failed)

    except KeyboardInterrupt:
        logger.info("shutting_down")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
