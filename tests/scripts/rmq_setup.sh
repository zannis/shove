#!/usr/bin/env bash
set -euo pipefail

CONTAINER_NAME="shove-rmq-test"

# Remove any orphan from a previous run (stop + remove if running, or just
# remove if stopped).  The --rm flag on `docker run` auto-removes the
# container once it stops, so after a clean exit there is nothing to remove;
# this guard handles SIGKILL'd runs where --rm never fired.
docker rm -f "$CONTAINER_NAME" 2>/dev/null || true

# Start the broker.  --rm ensures auto-removal when the container stops.
docker run -d --rm \
  --name "$CONTAINER_NAME" \
  -p 127.0.0.1::5672/tcp \
  -p 127.0.0.1::15672/tcp \
  rabbitmq:3.8.22-management

# Determine mapped ports (awk grabs the last colon-delimited field to handle
# both "0.0.0.0:PORT" and "127.0.0.1:PORT" output formats).
AMQP_PORT=$(docker port "$CONTAINER_NAME" 5672/tcp  | tail -1 | awk -F: '{print $NF}')
MGMT_PORT=$(docker port "$CONTAINER_NAME" 15672/tcp | tail -1 | awk -F: '{print $NF}')

# Wait for the management API to become available (up to 90 s).
echo "Waiting for RabbitMQ management API on port ${MGMT_PORT}..."
for i in $(seq 1 90); do
  if curl -sf -u guest:guest "http://127.0.0.1:${MGMT_PORT}/api/overview" >/dev/null 2>&1; then
    break
  fi
  if [ "$i" -eq 90 ]; then
    echo "ERROR: RabbitMQ did not start within 90 s" >&2
    exit 1
  fi
  sleep 1
done

# Enable the consistent-hash exchange plugin (needed for sequenced topics).
docker exec "$CONTAINER_NAME" rabbitmq-plugins enable rabbitmq_consistent_hash_exchange

# Publish URLs into the nextest environment file, then exit.
# nextest starts tests as soon as this script exits with code 0.
# The container continues running as a daemon; cleanup happens at the start
# of the next run via `docker rm -f` above.
printf 'RABBITMQ_AMQP_URL=amqp://guest:guest@127.0.0.1:%s\n' "$AMQP_PORT" >> "$NEXTEST_ENV"
printf 'RABBITMQ_MGMT_URL=http://127.0.0.1:%s\n'              "$MGMT_PORT" >> "$NEXTEST_ENV"

echo "RabbitMQ ready — AMQP :${AMQP_PORT}  MGMT :${MGMT_PORT}"
