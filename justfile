# ─── Services ──────────────────────────────────────────────────────

# Start collectors (nginx LB + OTEL collector replicas)
up:
    docker compose up -d

# Stop collectors
down:
    docker compose down

# ─── Load Generator (runs on host, sends to collectors) ──────────

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
