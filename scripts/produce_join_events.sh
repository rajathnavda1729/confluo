#!/usr/bin/env bash
# Produce sample events to join-input for testing the Omni-Joiner (inner join on order_id).
# Requires: Docker Compose with Redpanda running; topics join-input and join-output created.
# Usage: from repo root, ./scripts/produce_join_events.sh [rpk_brokers]
# Default brokers: localhost:19092 (when using docker compose exec from host).

set -e

BROKERS="${1:-localhost:19092}"
RPK="docker compose exec -T redpanda rpk topic produce join-input --brokers $BROKERS"

echo "Producing to join-input (brokers=$BROKERS). Each message needs header x-stream-id and body with order_id."

# Order 1: orders then shipments -> one joined record on join-output
echo '{"order_id":"ord-1","price":99.99}' | $RPK -H "x-stream-id:orders" -k "ord-1"
echo '{"order_id":"ord-1","status":"shipped"}' | $RPK -H "x-stream-id:shipments" -k "ord-1"

# Order 2: orders then shipments
echo '{"order_id":"ord-2","price":149.50}' | $RPK -H "x-stream-id:orders" -k "ord-2"
echo '{"order_id":"ord-2","status":"delivered"}' | $RPK -H "x-stream-id:shipments" -k "ord-2"

echo "Done. Consume join-output to see joined records: docker compose exec -T redpanda rpk topic consume join-output --brokers $BROKERS -n 4"
