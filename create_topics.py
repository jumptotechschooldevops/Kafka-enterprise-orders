"""
Create Kafka topics on Confluent Cloud or local Kafka.

Usage:
  # Confluent Cloud
  export KAFKA_BOOTSTRAP_SERVERS=pkc-xxxx.us-east-2.aws.confluent.cloud:9092
  export CONFLUENT_API_KEY=your_key
  export CONFLUENT_API_SECRET=your_secret
  python create_topics.py

  # Local docker-compose
  export KAFKA_BOOTSTRAP=kafka:9092
  python create_topics.py

NEVER hardcode credentials in source code.
Rotate the Confluent Cloud API key if it was previously committed to git.
"""
import os
import sys
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

BOOTSTRAP   = os.environ.get("KAFKA_BOOTSTRAP", os.environ.get("KAFKA_BOOTSTRAP_SERVERS"))
API_KEY     = os.environ.get("CONFLUENT_API_KEY", "")
API_SECRET  = os.environ.get("CONFLUENT_API_SECRET", "")

if not BOOTSTRAP:
    print("ERROR: Set KAFKA_BOOTSTRAP or KAFKA_BOOTSTRAP_SERVERS env var")
    sys.exit(1)

# Topics — includes DLQ topics for failed message handling
TOPICS = [
    NewTopic(name="orders",          num_partitions=6, replication_factor=3),
    NewTopic(name="payments",        num_partitions=6, replication_factor=3),
    NewTopic(name="fraud-alerts",    num_partitions=6, replication_factor=3),
    NewTopic(name="order-analytics", num_partitions=6, replication_factor=3),
    # Dead Letter Queue topics — failed messages land here after all retries
    NewTopic(name="orders-dlq",      num_partitions=3, replication_factor=3),
    NewTopic(name="analytics-dlq",   num_partitions=3, replication_factor=3),
]

cfg = {
    "bootstrap_servers":  BOOTSTRAP,
    "api_version":        (2, 6, 0),
    "request_timeout_ms": 60000,
}
if API_KEY and API_SECRET:
    cfg.update({
        "security_protocol":   "SASL_SSL",
        "sasl_mechanism":      "PLAIN",
        "sasl_plain_username": API_KEY,
        "sasl_plain_password": API_SECRET,
    })

print(f"Connecting to {BOOTSTRAP} ...")
try:
    admin = KafkaAdminClient(**cfg)
    print("Connected.")
except Exception as e:
    print(f"Connection failed: {e}")
    sys.exit(1)

print("\nCreating topics ...")
for topic in TOPICS:
    try:
        admin.create_topics([topic], validate_only=False)
        print(f"  ✓ created: {topic.name}  (partitions={topic.num_partitions})")
    except TopicAlreadyExistsError:
        print(f"  - exists:  {topic.name}")
    except Exception as e:
        print(f"  ✗ error:   {topic.name} — {e}")

print("\nAll topics:")
for t in sorted(admin.list_topics()):
    print(f"  {t}")

admin.close()
print("\nDone.")
