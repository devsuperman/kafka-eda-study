#!/bin/bash
# Creates every topic explicitly (auto-create is disabled on the broker).
set -euo pipefail
BOOTSTRAP=kafka:29092
for topic in orders.events inventory.events payments.events shipping.events; do
  for name in "$topic" "$topic.dlq"; do
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
      --create --if-not-exists --topic "$name" --partitions 3 --replication-factor 1
  done
done
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list
