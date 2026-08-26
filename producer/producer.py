import json
import os
import random
import time
import logging
from datetime import datetime, timezone
from faker import Faker
from kafka import KafkaProducer
from jsonschema import validate, ValidationError

# ── Structured JSON logger ──────────────────────────────────────────────────
class _JSONFormatter(logging.Formatter):
    def format(self, record):
        log = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": "producer",
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

_handler = logging.StreamHandler()
_handler.setFormatter(_JSONFormatter())
logger = logging.getLogger("producer")
logger.addHandler(_handler)
logger.setLevel(logging.INFO)

# ── Config ──────────────────────────────────────────────────────────────────
BOOTSTRAP    = os.environ.get("KAFKA_BOOTSTRAP", os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
API_KEY      = os.environ.get("CONFLUENT_API_KEY", "")
API_SECRET   = os.environ.get("CONFLUENT_API_SECRET", "")
TOPIC_NAME   = os.environ.get("TOPIC_NAME", "orders")
SLEEP_SECONDS = float(os.environ.get("SLEEP_SECONDS", "2"))

# ── Schema (embedded so no file-path dependency in Docker) ──────────────────
ORDER_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["order_id", "customer_id", "amount", "currency", "country", "status", "created_at"],
    "additionalProperties": False,
    "properties": {
        "order_id":    {"type": "integer", "minimum": 1},
        "customer_id": {"type": "integer", "minimum": 1000},
        "amount":      {"type": "number",  "minimum": 0.01, "maximum": 100000},
        "currency":    {"type": "string",  "enum": ["USD", "EUR", "GBP", "CAD", "AUD"]},
        "country":     {"type": "string",  "minLength": 2, "maxLength": 2},
        "status":      {"type": "string",  "enum": ["CREATED", "CONFIRMED", "CANCELLED"]},
        "created_at":  {"type": "string"},
    },
}

fake = Faker()


def build_kafka_config():
    config = {
        "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
        "key_serializer":   lambda k: str(k).encode("utf-8"),
        # acks="all" + retries gives strong durability guarantees.
        # kafka-python doesn't support enable_idempotence (confluent-kafka only),
        # so max_in_flight=1 prevents reordering on retry.
        "acks":    "all",
        "retries": 5,
        "max_in_flight_requests_per_connection": 1,
    }
    if API_KEY and API_SECRET:
        config.update({
            "bootstrap_servers":  BOOTSTRAP,
            "security_protocol":  "SASL_SSL",
            "sasl_mechanism":     "PLAIN",
            "sasl_plain_username": API_KEY,
            "sasl_plain_password": API_SECRET,
        })
    else:
        config.update({
            "bootstrap_servers": BOOTSTRAP,
            "api_version": (2, 5, 0),
        })
    return config


def generate_order(order_id: int) -> dict:
    return {
        "order_id":    order_id,
        "customer_id": fake.random_int(min=1000, max=9999),
        "amount":      round(random.uniform(10, 500), 2),
        "currency":    "USD",
        "country":     random.choice(["US", "CA", "DE", "IN", "GB", "FR", "CN", "BR"]),
        "status":      random.choice(["CREATED", "CONFIRMED", "CANCELLED"]),
        "created_at":  datetime.utcnow().isoformat() + "Z",
    }


def main():
    logger.info("starting", extra={"bootstrap": BOOTSTRAP, "topic": TOPIC_NAME})
    producer = None
    for attempt in range(1, 31):
        try:
            producer = KafkaProducer(**build_kafka_config())
            logger.info("connected", extra={"attempt": attempt})
            break
        except Exception as e:
            if attempt == 30:
                raise
            logger.warning("waiting_for_kafka", extra={"attempt": attempt, "error": str(e)})
            time.sleep(2)

    order_id = 1
    while True:
        order = generate_order(order_id)

        # Validate against schema before publishing — catches bugs early
        try:
            validate(instance=order, schema=ORDER_SCHEMA)
        except ValidationError as e:
            logger.error("schema_invalid", extra={"order_id": order_id, "error": e.message})
            order_id += 1
            time.sleep(SLEEP_SECONDS)
            continue

        try:
            meta = producer.send(TOPIC_NAME, key=order["order_id"], value=order).get(timeout=20)
            logger.info("published", extra={
                "order_id":  order_id,
                "partition": meta.partition,
                "offset":    meta.offset,
                "amount":    order["amount"],
                "country":   order["country"],
            })
        except Exception as e:
            logger.error("publish_failed", extra={"order_id": order_id, "error": str(e)})

        order_id += 1
        time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    main()
