# SNKRS Load Generator

Generates realistic Nike SNKRS e-commerce telemetry at scale — correlated traces,
logs, and metrics across a microservice architecture. Designed to produce genuinely
diverse observability data that requires real reasoning to analyze, not pattern-matching.

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Generate template corpuses (one-time, no API key needed)
python gen_corpus.py --provider fallback

# 3a. File output (no infrastructure needed)
python generate.py --tier xs --output-mode file --output-dir ./output

# 3b. Or send to OTEL Collector via gRPC
just up                          # Start docker compose stack (MinIO + Collector)
python generate.py --tier xs     # Sends to localhost:4317
```

## Volume Tiers

| Tier | Target Spans | Drops | Time Range | Est. Output | Est. Duration |
|------|-------------|-------|------------|-------------|---------------|
| `xs` | 10k | 1 | 1h | ~35 MB | ~2 sec |
| `s` | 100k | 3 | 6h | ~350 MB | ~15 sec |
| `m` | 1M | 10 | 24h | ~3.5 GB | ~3 min |
| `l` | 10M | 50 | 7 days | ~35 GB | ~30 min |
| `xl` | 100M | 200 | 30 days | ~350 GB | ~5 hrs |
| `xxl` | 1B | 1,000 | 90 days | ~1 TB+ | ~20-40 min* |

*xxl with `--output-mode file` and 8+ workers.

## CLI Options

```bash
python generate.py \
  --tier xs                       # Volume tier (xs/s/m/l/xl/xxl)
  --workers 8                     # Parallel worker processes (default: cpu_count - 4)
  --output-mode file              # Output: "grpc" (OTEL Collector) or "file" (JSON)
  --output-dir ./output           # Directory for file output
  --compress                      # Gzip compress file output
  --otel-endpoint localhost:4317  # OTEL Collector gRPC endpoint
  --batch-size 5000               # Events per batch
```

### Examples

```bash
# Smoke test — writes JSON files locally
python generate.py --tier xs --output-mode file

# Medium load — send to collector
python generate.py --tier m --workers 8

# TB-scale generation — compressed file output
python generate.py --tier xxl --output-mode file --output-dir /data/snkrs --compress --workers 16
```

## How It Works

### Pipeline Overview

```
generate.py (orchestrator)
    |
    +-- [1/4] Generate user profiles (users.py)
    +-- [2/4] Build drop schedule (product + incident per drop)
    +-- [3/4] Spawn workers for spans + logs (multiprocessing)
    +-- [4/4] Generate metrics centrally
    |
    v
OTLPWriter (gRPC -> Collector -> S3)    or    FileWriter (JSON -> disk)
```

### What Gets Generated

The simulator models a sneaker e-commerce platform where limited-edition shoes are
released ("dropped") at scheduled times. Users compete to buy them. The telemetry
captures the full lifecycle:

**Traces** — Distributed request trees across 30+ microservices:
- Browse product -> auth -> product service -> CDN -> cache -> inventory check
- Enter draw -> auth -> product -> draw service -> Kafka -> DynamoDB
- Checkout -> auth -> inventory reserve -> payment (Stripe) -> order -> notifications
- Plus: search, wishlist, recommendations, feed, account operations

**Logs** — Correlated with traces via TraceId/SpanId:
- 4,860+ log templates across 30 services and 4 severity levels
- Includes Kubernetes context (pod name, namespace, node), request IDs, thread names
- Realistic infrastructure messages (GC pauses, connection pools, cache stats)

**Metrics** — 53 metric types at 10-second intervals:
- Infrastructure: CPU, memory, GC duration, thread count
- Cache: hit rate, evictions, latency
- Database: query p50/p99, connection pool, replication lag, deadlocks
- Queue: Kafka consumer lag, DLQ depth
- Business: cart abandonment, conversion rate, revenue, bot detections
- Network: DNS lookup, active connections

### Diversity Features

The generator produces genuinely diverse data through several mechanisms:

| Dimension | Count | How |
|-----------|-------|-----|
| Log templates | 4,860 | Pre-generated corpus across 30 services x 4 severities |
| Products | 248 | Multiple brands, colorways, collabs, price points |
| Incidents | 62 | Memory leaks, DNS failures, split-brain, cascading failures |
| Services | 30 | Full microservice topology with dependencies |
| Endpoints | 52 | Browse, purchase, account, search, shipping, returns |
| Trace topologies | 1000s | Probabilistic graph traversal (cache hits, retries, circuit breakers) |
| Behavior types | 11 | casual, hype, reseller, collector, deal hunter, international, etc. |
| Latency models | 4 | Bimodal, Pareto/long-tail, periodic spikes, degradation ramp |
| Error messages | 871 | Realistic stack traces in Java, Go, Python, Node |

**No fixed RNG seeds** — each run produces unique data.

## Architecture

### File Overview

```
load-generator/
  generate.py          # CLI orchestrator — tiers, workers, drop scheduling
  corpus_loader.py     # Loads JSON corpuses from corpus/ with fallbacks
  gen_corpus.py        # One-time corpus generator (LLM or fallback)
  users.py             # User profile generation (11 behavior types)
  inventory.py         # Product catalog + per-size stock tracking
  patterns.py          # Behavioral session templates + 62 incident types
  spans.py             # Distributed trace trees (original 5 endpoints)
  topology.py          # Probabilistic service graph (8 flow roots, 50 nodes)
  distributions.py     # 4 latency distribution models
  logs.py              # Correlated log records with k8s context
  metrics.py           # 53 metric types with per-service identity
  writer.py            # OTLPWriter (gRPC) + FileWriter (JSON files)
  corpus/              # Pre-generated JSON corpuses
    log_templates.json
    products.json
    incidents.json
    services.json
    error_messages.json
    endpoints.json
```

### Data Flow

```
Users (users.py)                 Products (inventory.py)
  |                                |
  v                                v
Behavior Patterns (patterns.py)  Drop Schedule (generate.py)
  |                                |
  +-----> Worker (writer.py) <-----+
              |
              +-- For each drop:
              |     For each user:
              |       Select behavior pattern
              |       For each session action:
              |         Generate spans (spans.py or topology.py)
              |         Generate correlated logs (logs.py)
              |         Write batch to output
              |
              +-- Latency distributions (distributions.py)
              +-- Incident injection (patterns.py)
              +-- Stock tracking (inventory.py)
```

### Service Topology

The simulated platform has 30+ microservices. The core 8 services use
hand-crafted span trees (`spans.py`). Additional services use the probabilistic
topology graph (`topology.py`) which produces different trace shapes every time
based on:

- **Cache branching** — Redis hit skips the DB call (different tree shape)
- **Circuit breakers** — High error rate triggers fast-fail spans
- **Retry loops** — Failed calls produce 1-3 retry child spans
- **Fan-out** — Order service calls notification + analytics in parallel
- **Async fire-and-forget** — Analytics calls don't block the parent

### Latency Models

Instead of simple Gaussian noise, each span type uses an appropriate distribution:

| Model | Use Case | Behavior |
|-------|----------|----------|
| Bimodal | Cache operations | 80% fast (hit) + 20% slow (miss) |
| Long-tail (Pareto) | External APIs (Stripe, CDN) | 95% normal + 5% heavy tail (2-20x) |
| Periodic spikes | GC pauses, cron jobs | Baseline + periodic elevated windows |
| Degradation ramp | Memory leaks | Exponential latency increase over time |

### Incident System

62 incident types are injected with Poisson distribution (~25% of drops):

- Connection pool exhaustion, memory leaks, DNS failures
- TLS certificate expiry, Kafka rebalances, Redis eviction storms
- Thread pool exhaustion, GC stop-the-world, disk full
- Network partitions, split-brain, hot partitions, config drift
- Cascading failures (service A down triggers B degradation)

Each incident has a progression type: `instant`, `ramp_up`, `ramp_down`, `spike`, `oscillating`.

## Corpus Generation

The `corpus/` directory contains pre-generated JSON files that provide template
diversity. Generate them once:

```bash
# Free — programmatic expansion (no API needed)
python gen_corpus.py --provider fallback

# Higher quality — uses Claude API (~$5-15)
export ANTHROPIC_API_KEY=sk-...
python gen_corpus.py --provider anthropic

# Or OpenAI
export OPENAI_API_KEY=sk-...
python gen_corpus.py --provider openai

# Generate specific corpuses only
python gen_corpus.py --provider fallback --corpus products,incidents

# List available corpuses
python gen_corpus.py --list
```

The load generator works without corpuses (falls back to hardcoded defaults),
but diversity is significantly reduced.

## Output Formats

### gRPC Mode (default)

Sends OTLP protobuf messages to an OpenTelemetry Collector:

```
generate.py --output-mode grpc --otel-endpoint localhost:4317
    |
    v
OTEL Collector (config/otel-collector-config.yaml)
    |
    v
MinIO (S3) --> s3-loader --> ClickHouse
```

### File Mode

Writes OTLP-compatible JSON Lines directly to disk, partitioned by time:

```
generate.py --output-mode file --output-dir ./output
    |
    v
output/
  traces/
    year=2026/month=02/day=22/hour=19/minute=30/
      w0_000001.jsonl    # Worker 0, batch 1
      w1_000001.jsonl    # Worker 1, batch 1
  logs/
    year=2026/month=02/day=22/hour=19/minute=30/
      w0_000001.jsonl
  metrics/
    year=2026/month=02/day=22/hour=19/minute=30/
      w999_000001.jsonl  # Metrics worker
```

Each `.jsonl` file contains one JSON object per line matching the OTLP schema.
With `--compress`, files are `.jsonl.gz` (gzip level 1 for speed).

## Docker Compose Stack

```bash
# Start MinIO + OTEL Collector
docker compose up -d

# Run load generator (optional profile)
docker compose --profile load-test up load-generator

# MinIO Console: http://localhost:9001 (minioadmin/minioadmin)
# Collector health: http://localhost:13133
```

### Services

| Service | Port | Purpose |
|---------|------|---------|
| MinIO | 9002 (API), 9001 (Console) | S3-compatible storage |
| OTEL Collector | 4317 (gRPC), 4318 (HTTP) | Receives OTLP, writes to S3 |
| load-generator | — | Generates telemetry (opt-in profile) |

## Extending

### Adding a New Service

1. Add the service to `gen_corpus.py` → `SERVICES_LIST`
2. Add log templates to the corpus (re-run `gen_corpus.py`)
3. Add nodes to `topology.py` → `SERVICE_GRAPH`
4. Wire into a flow root in `topology.py` → `FLOW_ROOTS`

### Adding a New Endpoint

1. Add to `corpus/endpoints.json` (or re-generate)
2. Add flow root in `topology.py` → `FLOW_ROOTS`
3. Add to `patterns.py` behavior patterns if it should appear in user sessions

### Adding a New Incident Type

1. Add to `corpus/incidents.json` (or re-generate)
2. Incidents are automatically loaded and Poisson-distributed across drops

### Adding a New Metric

1. Add definition to `metrics.py` → `METRIC_DEFS`
2. Add generation logic in `generate_metrics_for_interval()`

### Adding a New Behavior Type

1. Add to `users.py` → `BEHAVIOR_TYPES` with weight
2. Add session pattern to `patterns.py` → `PATTERNS`
