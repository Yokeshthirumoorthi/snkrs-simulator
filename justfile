# ─── Services ──────────────────────────────────────────────────────

# Start all services (minio, otel-collector, event-generator)
up:
    docker compose up -d --build

# Stop all services
down:
    docker compose down -v

# Start the event generator
start-sim:
    docker compose start event-generator

# Stop the event generator
stop-sim:
    docker compose stop event-generator

# Tail event generator logs
logs:
    docker compose logs -f event-generator

# ─── Load Generator (OTLP pipeline) ─────────────────────────────
# Runs directly on the host, sends to the already-running OTEL collector on localhost:4317

gen_dir := "load-generator"

# Generate 10k span smoke test via OTEL pipeline
gen-xs:
    cd {{gen_dir}} && python3 generate.py --tier xs --otel-endpoint localhost:4317

# Generate 100k spans via OTEL pipeline
gen-s:
    cd {{gen_dir}} && python3 generate.py --tier s --otel-endpoint localhost:4317

# Generate 1M spans via OTEL pipeline
gen-m:
    cd {{gen_dir}} && python3 generate.py --tier m --otel-endpoint localhost:4317

# Generate 10M spans via OTEL pipeline
gen-l:
    cd {{gen_dir}} && python3 generate.py --tier l --otel-endpoint localhost:4317

# Generate 100M spans via OTEL pipeline
gen-xl:
    cd {{gen_dir}} && python3 generate.py --tier xl --otel-endpoint localhost:4317

# ─── Utilities ────────────────────────────────────────────────────

# List files in the MinIO traces bucket (all signal prefixes)
minio-ls:
    docker run --rm --network=snkrs-simulator_default --entrypoint /bin/sh minio/mc -c \
        "mc alias set local http://minio:9000 minioadmin minioadmin > /dev/null 2>&1 && echo '=== traces ===' && mc ls --recursive local/traces/incoming/ && echo '=== metrics ===' && mc ls --recursive local/traces/metrics/ && echo '=== logs ===' && mc ls --recursive local/traces/logs/"
