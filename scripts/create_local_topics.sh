#!/bin/bash
# Create local Kafka topics with replication-factor=1 (single broker).
# Used by the kafka-init Compose service. Do not use RF=3 against local Kafka.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"

echo "Waiting for Kafka at ${BOOTSTRAP} ..."
for i in $(seq 1 60); do
  if kafka-topics --bootstrap-server "${BOOTSTRAP}" --list >/dev/null 2>&1; then
    echo "Kafka is reachable."
    break
  fi
  if [ "${i}" -eq 60 ]; then
    echo "ERROR: Kafka did not become reachable at ${BOOTSTRAP}"
    exit 1
  fi
  echo "  attempt ${i}/60 ..."
  sleep 2
done

create_topic() {
  local name="$1"
  local partitions="$2"
  echo "Ensuring topic '${name}' (partitions=${partitions}, replication-factor=1)"
  kafka-topics --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists \
    --topic "${name}" \
    --partitions "${partitions}" \
    --replication-factor 1
}

# Primary business topics
create_topic "orders" 6
create_topic "fraud-alerts" 6
create_topic "payments" 6
create_topic "order-analytics" 6

# Dead-letter topics
create_topic "orders-dlq" 3
create_topic "analytics-dlq" 3

echo
echo "Topic list:"
kafka-topics --bootstrap-server "${BOOTSTRAP}" --list

echo
echo "Topic details:"
kafka-topics --bootstrap-server "${BOOTSTRAP}" --describe \
  --topic orders,fraud-alerts,payments,order-analytics,orders-dlq,analytics-dlq

echo
echo "Kafka topic initialization complete."
