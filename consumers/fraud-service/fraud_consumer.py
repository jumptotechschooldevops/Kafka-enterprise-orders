"""
Fraud Detection Consumer
Reads orders, scores them with a multi-factor risk model,
publishes alerts to fraud-alerts, failed messages to orders-dlq.
Exposes Prometheus metrics on :8001/metrics.
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
            "service": "fraud-service",
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
logger = logging.getLogger("fraud-service")
logger.addHandler(_h)
logger.setLevel(logging.INFO)

# ── Prometheus metrics ──────────────────────────────────────────────────────
ORDERS_PROCESSED = Counter("fraud_orders_processed_total", "Total orders evaluated")
FRAUD_ALERTS     = Counter("fraud_alerts_total", "Total fraud alerts raised", ["reason"])
DLQ_SENDS        = Counter("fraud_dlq_sends_total", "Messages sent to DLQ")
PROCESSING_TIME  = Histogram("fraud_processing_seconds", "Time to process one order")

# ── Config ──────────────────────────────────────────────────────────────────
BOOTSTRAP    = os.environ.get("KAFKA_BOOTSTRAP", os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
API_KEY      = os.environ.get("CONFLUENT_API_KEY", "")
API_SECRET   = os.environ.get("CONFLUENT_API_SECRET", "")
METRICS_PORT = int(os.environ.get("METRICS_PORT", "8001"))

ORDERS_TOPIC = "orders"
ALERTS_TOPIC = "fraud-alerts"
DLQ_TOPIC    = "orders-dlq"

MAX_RETRIES   = 3
RETRY_BACKOFF = [1, 2, 4]

# High-risk countries based on common fraud patterns
HIGH_RISK_COUNTRIES    = {"CN", "NG", "RU", "UA", "VN", "PK", "BD"}
FRAUD_SCORE_THRESHOLD  = 50


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


def calculate_fraud_risk(order: dict):
    """
    Multi-factor fraud scoring. Returns (score 0-100, list of triggered reasons).
    Score >= FRAUD_SCORE_THRESHOLD triggers an alert.
    """
    score, reasons = 0, []
    amount   = float(order.get("amount", 0))
    country  = order.get("country", "")
    status   = order.get("status", "")
    currency = order.get("currency", "USD")

    # Amount thresholds
    if amount > 450:
        score += 40; reasons.append("VERY_HIGH_AMOUNT")
    elif amount > 300:
        score += 20; reasons.append("HIGH_AMOUNT")

    # Country risk
    if country in HIGH_RISK_COUNTRIES:
        score += 30; reasons.append("HIGH_RISK_COUNTRY")

    # Round-number amounts are statistically unusual in genuine purchases
    if amount > 50 and amount % 50 == 0:
        score += 10; reasons.append("SUSPICIOUS_ROUND_AMOUNT")

    # Cancelled high-value orders suggest return fraud
    if status == "CANCELLED" and amount > 200:
        score += 20; reasons.append("CANCELLED_HIGH_VALUE")

    # High-amount foreign-currency purchase
    if currency != "USD" and amount > 200:
        score += 10; reasons.append("FOREIGN_CURRENCY_HIGH_AMOUNT")

    return min(score, 100), reasons


def _send_to_dlq(producer, msg, error: str):
    payload = {
        "source_topic":     msg.topic,
        "source_partition": msg.partition,
        "source_offset":    msg.offset,
        "payload":          msg.value,
        "error":            error,
        "failed_at":        datetime.now(timezone.utc).isoformat(),
        "service":          "fraud-service",
    }
    producer.send(DLQ_TOPIC, value=json.dumps(payload).encode())
    DLQ_SENDS.inc()
    logger.error("sent_to_dlq", extra={"order_id": msg.value.get("order_id"), "error": error})


def process_message(msg, alert_producer, dlq_producer):
    order    = msg.value
    order_id = order.get("order_id")

    for attempt in range(MAX_RETRIES):
        try:
            with PROCESSING_TIME.time():
                score, reasons = calculate_fraud_risk(order)
                ORDERS_PROCESSED.inc()

            if score >= FRAUD_SCORE_THRESHOLD:
                alert = {
                    "order_id":   order_id,
                    "score":      score,
                    "reasons":    reasons,
                    "amount":     order.get("amount"),
                    "country":    order.get("country"),
                    "alerted_at": datetime.now(timezone.utc).isoformat(),
                }
                alert_producer.send(ALERTS_TOPIC, value=json.dumps(alert).encode())
                for r in reasons:
                    FRAUD_ALERTS.labels(reason=r).inc()
                logger.warning("fraud_detected", extra={
                    "order_id": order_id, "score": score, "reasons": reasons,
                    "amount": order.get("amount"), "country": order.get("country"),
                })
            else:
                logger.info("order_ok", extra={"order_id": order_id, "score": score})
            return

        except Exception as e:
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
            group_id="fraud-group",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        ),
        "consumer",
    )
    alert_producer = _connect_with_retry(lambda: KafkaProducer(**base_cfg), "alert-producer")
    dlq_producer   = _connect_with_retry(lambda: KafkaProducer(**base_cfg), "dlq-producer")

    logger.info("listening", extra={"topic": ORDERS_TOPIC})

    try:
        for msg in consumer:
            process_message(msg, alert_producer, dlq_producer)
    except KeyboardInterrupt:
        logger.info("shutting_down")
    finally:
        consumer.close()
        alert_producer.close()
        dlq_producer.close()


if __name__ == "__main__":
    main()
