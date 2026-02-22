# SNKRS Simulator

Realistic Nike SNKRS drop traffic simulator that generates OpenTelemetry traces, logs, and metrics. Produces telemetry for 8 microservices across 3 drop phases, exported to S3 (MinIO) via an OTEL Collector.

## Architecture

```
Event Generator ──► OTEL Collector ──► S3 (MinIO)
                     (gRPC:4317)        ├── incoming/  (traces)
                                        ├── metrics/
                                        └── logs/
```

## Services

| Container | What it does | Port |
|-----------|-------------|------|
| minio | S3-compatible storage | 9002 (API), 9001 (Console) |
| otel-collector | Receives OTLP, batches, exports to S3 | 4317 (gRPC), 4318 (HTTP) |
| event-generator | Continuous SNKRS drop simulator | — |
| load-generator | Bulk OTLP sender (on-demand) | — |

## Prerequisites

- Docker & Docker Compose
- [just](https://github.com/casey/just) command runner
- Python 3.11+ (for load-generator only, runs on host)

## Quick Start

```bash
just up       # start minio + otel-collector + event-generator
just logs     # tail the simulator output
just minio-ls # verify files appearing in S3
```

## The Drop Simulation

The event generator cycles through three phases, simulating a real SNKRS release:

| Phase | Duration | Workers | What happens |
|-------|----------|---------|-------------|
| PRE_DROP | 60s | 5 | Calm browsing, account checks, feed loads |
| DROP_LIVE | 90s | 40 | Draw entries, checkouts, inventory race |
| POST_DROP | 60s | 8 | Cooldown, order checks, notifications |

After each cycle the inventory resets and a new drop begins.

## Load Generator

For bulk data generation (bypasses continuous simulation):

```bash
pip install -r load-generator/requirements.txt

just gen-xs   # 10k spans (~10 MB, ~5 sec)
just gen-s    # 100k spans (~100 MB, ~30 sec)
just gen-m    # 1M spans (~1 GB, ~5 min)
just gen-l    # 10M spans (~10 GB, ~45 min)
just gen-xl   # 100M spans (~100 GB, ~6-8 hrs)
```

| Tier | Spans | Logs | Metrics | Size | Drops | Time Range |
|------|-------|------|---------|------|-------|------------|
| xs | 10k | 5k | 2k | ~10 MB | 1 | 1h |
| s | 100k | 50k | 20k | ~100 MB | 3 | 6h |
| m | 1M | 500k | 200k | ~1 GB | 10 | 24h |
| l | 10M | 5M | 2M | ~10 GB | 50 | 7d |
| xl | 100M | 50M | 20M | ~100 GB | 200 | 30d |

## Just Commands

| Command | What it does |
|---------|-------------|
| `just up` | Start all services |
| `just down` | Stop all services and remove volumes |
| `just start-sim` | Start event generator |
| `just stop-sim` | Stop event generator |
| `just logs` | Tail event generator logs |
| `just gen-xs` | Generate 10k spans via OTEL pipeline |
| `just gen-s` | Generate 100k spans |
| `just gen-m` | Generate 1M spans |
| `just gen-l` | Generate 10M spans |
| `just gen-xl` | Generate 100M spans |
| `just minio-ls` | List files in MinIO S3 buckets |

## Integration with click-ai

To feed this simulator's output into the [click-ai](https://github.com/Yokeshthirumoorthi/click-ai) pipeline, point click-ai's s3-loader at this repo's MinIO instance (localhost:9002).

## Cleanup

```bash
just down   # stop + delete all data
```
