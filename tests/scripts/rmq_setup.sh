#!/usr/bin/env bash
set -euo pipefail

CONTAINER_NAME="shove-rmq-test"

# Capture the parent (cargo-nextest) PID before spawning anything else; the
# background watcher below uses it to know when the test run has finished.
NEXTEST_PID=$PPID

# Remove any orphan from a previous run (the previous run's watcher should
# have cleaned up, but we guard against SIGKILL'd runs where it could not).
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
    docker kill "$CONTAINER_NAME" 2>/dev/null || true
    exit 1
  fi
  sleep 1
done

# Enable the consistent-hash exchange plugin (needed for sequenced topics).
docker exec "$CONTAINER_NAME" rabbitmq-plugins enable rabbitmq_consistent_hash_exchange

# Publish URLs into the nextest environment file.  nextest starts tests as
# soon as this script exits with code 0.
printf 'RABBITMQ_AMQP_URL=amqp://guest:guest@127.0.0.1:%s\n' "$AMQP_PORT" >> "$NEXTEST_ENV"
printf 'RABBITMQ_MGMT_URL=http://127.0.0.1:%s\n'              "$MGMT_PORT" >> "$NEXTEST_ENV"

echo "RabbitMQ ready — AMQP :${AMQP_PORT}  MGMT :${MGMT_PORT}"

# Spawn a detached watcher that polls for cargo-nextest's exit and then
# tears down the container.  Stdio is redirected to /dev/null and SIGHUP is
# ignored so the watcher survives our own exit and is not killed when the
# launching shell exits.  --rm on the container ensures `docker kill` also
# removes it.
{
  trap '' HUP
  while kill -0 "$NEXTEST_PID" 2>/dev/null; do
    sleep 2
  done
  docker kill "$CONTAINER_NAME" 2>/dev/null || true
} </dev/null >/dev/null 2>&1 &
disown
