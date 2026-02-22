#!/usr/bin/env python3
"""
Corpus pre-generator — calls an LLM API (Claude or OpenAI) to generate
diverse template corpuses for the SNKRS load generator.

Run once to populate the corpus/ directory, then the load generator
loads these at runtime for 100x+ diversity.

Usage:
    export ANTHROPIC_API_KEY=sk-...
    python gen_corpus.py --provider anthropic --all
    python gen_corpus.py --provider openai --corpus log_templates
    python gen_corpus.py --provider anthropic --corpus products,incidents

Estimated cost: ~$5-15 for full generation.
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

CORPUS_DIR = Path(__file__).parent / "corpus"
CORPUS_DIR.mkdir(exist_ok=True)


# ── LLM Client Abstraction ──────────────────────────────────────────────

def call_llm(prompt: str, provider: str = "anthropic", max_tokens: int = 8192) -> str:
    """Call LLM API and return text response."""
    if provider == "anthropic":
        return _call_anthropic(prompt, max_tokens)
    elif provider == "openai":
        return _call_openai(prompt, max_tokens)
    else:
        raise ValueError(f"Unknown provider: {provider}")


def _call_anthropic(prompt: str, max_tokens: int) -> str:
    try:
        import anthropic
    except ImportError:
        print("pip install anthropic")
        sys.exit(1)

    client = anthropic.Anthropic()
    resp = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}],
    )
    return resp.content[0].text


def _call_openai(prompt: str, max_tokens: int) -> str:
    try:
        import openai
    except ImportError:
        print("pip install openai")
        sys.exit(1)

    client = openai.OpenAI()
    resp = client.chat.completions.create(
        model="gpt-4o",
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}],
    )
    return resp.choices[0].message.content


def extract_json(text: str) -> list | dict:
    """Extract JSON from LLM response (handles markdown fences)."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        start = 1
        end = len(lines)
        for i in range(1, len(lines)):
            if lines[i].strip() == "```":
                end = i
                break
        text = "\n".join(lines[start:end])
    return json.loads(text)


def generate_in_batches(prompt_fn, batch_count: int, provider: str,
                        max_tokens: int = 8192, delay: float = 1.0) -> list:
    """Generate corpus items in batches to stay within token limits."""
    all_items = []
    for i in range(batch_count):
        print(f"    Batch {i+1}/{batch_count}...", end=" ", flush=True)
        prompt = prompt_fn(i, batch_count)
        try:
            text = call_llm(prompt, provider, max_tokens)
            items = extract_json(text)
            if isinstance(items, list):
                all_items.extend(items)
            elif isinstance(items, dict):
                all_items.append(items)
            print(f"got {len(items) if isinstance(items, list) else 1} items")
        except Exception as e:
            print(f"ERROR: {e}")
        if i < batch_count - 1:
            time.sleep(delay)
    return all_items


def save_corpus(name: str, data: list | dict):
    """Save corpus to JSON file."""
    path = CORPUS_DIR / f"{name}.json"
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    count = len(data) if isinstance(data, list) else sum(
        sum(len(v) for v in sev.values()) if isinstance(sev, dict) else len(sev)
        for sev in data.values()
    ) if isinstance(data, dict) else 0
    print(f"  Saved {path} ({count} items)")


# ── Corpus Generators ───────────────────────────────────────────────────

SERVICES_LIST = [
    "api-gateway", "auth-service", "product-service", "draw-service",
    "inventory-service", "payment-service", "order-service",
    "notification-service", "feed-service", "search-service",
    "recommendation-service", "shipping-service", "returns-service",
    "wishlist-service", "analytics-service", "pricing-service",
    "review-service", "loyalty-service", "fraud-service",
    "image-service", "cache-service", "queue-service",
    "config-service", "rate-limiter-service", "geo-service",
    "content-service", "ab-test-service", "user-preference-service",
    "session-service", "audit-service",
]

SEVERITIES = ["DEBUG", "INFO", "WARN", "ERROR"]


def gen_log_templates(provider: str):
    """Generate 10,000+ log templates across services and severities."""
    print("\n[1/6] Generating log templates...")

    def prompt_fn(batch_idx, total):
        # Assign services to batches to get full coverage
        batch_size = max(1, len(SERVICES_LIST) // total)
        start = batch_idx * batch_size
        end = min(len(SERVICES_LIST), start + batch_size + (
            len(SERVICES_LIST) % total if batch_idx == total - 1 else 0))
        services = SERVICES_LIST[start:end]

        return f"""Generate realistic log message templates for microservices in a sneaker e-commerce platform (Nike SNKRS style).

For each service below, generate templates for DEBUG, INFO, WARN, and ERROR severities.

Services for this batch: {json.dumps(services)}

Requirements:
- Use {{placeholder}} syntax for dynamic values
- Common placeholders: {{user_id}}, {{request_id}}, {{correlation_id}}, {{sku}}, {{product_id}}, {{size}}, {{stock}}, {{price}}, {{amount}}, {{latency}}, {{cache_hit}}, {{ttl}}, {{active}}, {{idle}}, {{max_conn}}, {{miss_rate}}, {{rate}}, {{timeout}}, {{retries}}, {{attempt}}, {{queue_depth}}, {{partition}}, {{tx_id}}, {{order_id}}, {{method}}, {{route}}, {{status_code}}, {{service}}, {{error_msg}}, {{email}}, {{fraud_score}}, {{payment_method}}, {{thread_name}}, {{heap_used_mb}}, {{gc_pause_ms}}, {{query_duration_ms}}, {{pod_name}}, {{container_id}}, {{az}}, {{node_name}}, {{namespace}}
- Include Kubernetes/infrastructure context in some templates
- Include database queries, cache operations, queue operations
- Include stack trace fragments in ERROR templates
- Make templates realistic and varied — different phrasings, formats
- Generate at least 30 templates per service per severity (120+ per service)

Return a JSON object where keys are service names and values are objects with severity keys mapping to arrays of template strings.
Example: {{"auth-service": {{"DEBUG": ["template1...", "template2..."], "INFO": [...], "WARN": [...], "ERROR": [...]}}}}

Return ONLY valid JSON, no markdown fences."""

    templates = {}
    batch_count = 6  # ~5 services per batch
    for i in range(batch_count):
        print(f"    Batch {i+1}/{batch_count}...", end=" ", flush=True)
        prompt = prompt_fn(i, batch_count)
        try:
            text = call_llm(prompt, provider, max_tokens=8192)
            batch = extract_json(text)
            if isinstance(batch, dict):
                for svc, sevs in batch.items():
                    if svc not in templates:
                        templates[svc] = {}
                    for sev, msgs in sevs.items():
                        if sev not in templates[svc]:
                            templates[svc][sev] = []
                        templates[svc][sev].extend(msgs)
            total = sum(len(m) for s in templates.values() for m in s.values())
            print(f"total templates so far: {total}")
        except Exception as e:
            print(f"ERROR: {e}")
        if i < batch_count - 1:
            time.sleep(1)

    save_corpus("log_templates", templates)
    return templates


def gen_products(provider: str):
    """Generate 500+ sneaker products."""
    print("\n[2/6] Generating products...")

    def prompt_fn(batch_idx, total):
        categories = ["lifestyle", "running", "basketball", "skateboarding",
                       "training", "slides", "boots", "retro"]
        release_types = ["general_release", "limited_edition", "exclusive_access",
                          "draw", "fcfs", "raffle"]
        return f"""Generate {100} realistic sneaker products for a Nike SNKRS-style platform.
Batch {batch_idx+1} of {total} — generate DIFFERENT products each batch.

Each product should be a JSON object with these fields:
- "id": short alphanumeric ID (e.g., "AJ1-CHI", "DF-PANDA")
- "name": full product name with colorway (e.g., "Air Jordan 1 Retro High OG 'Chicago'")
- "sku": realistic SKU format (e.g., "555088-101")
- "price": integer price in USD (70-350)
- "base_stock": integer stock (50-3000)
- "hype_level": float 0.0-1.0
- "category": one of {json.dumps(categories)}
- "release_type": one of {json.dumps(release_types)}
- "brand": "Nike", "Jordan", "Adidas", "New Balance", "Puma", "Converse", "Reebok", "ASICS"
- "collab": collaborator name or null (e.g., "Travis Scott", "Off-White", null)
- "colorway": color description (e.g., "University Blue/White")

Include mix of brands, price points, hype levels, categories.
Return ONLY a JSON array of product objects, no markdown fences."""

    all_products = generate_in_batches(prompt_fn, 6, provider, max_tokens=8192)
    # Deduplicate by id
    seen = set()
    unique = []
    for p in all_products:
        pid = p.get("id", "")
        if pid and pid not in seen:
            seen.add(pid)
            unique.append(p)
    save_corpus("products", unique)
    return unique


def gen_incidents(provider: str):
    """Generate 50+ incident types."""
    print("\n[3/6] Generating incidents...")

    def prompt_fn(batch_idx, total):
        existing_types = [
            "connection_pool_exhaustion", "inventory_race_condition", "bot_storm",
            "cdn_outage", "database_failover", "regional_outage",
        ]
        services = SERVICES_LIST[:15]
        return f"""Generate {30} realistic infrastructure incident types for a sneaker e-commerce microservice platform.
Batch {batch_idx+1} of {total}. Avoid duplicating these existing types: {json.dumps(existing_types)}

Each incident should be a JSON object:
- "name": snake_case name (e.g., "kafka_partition_rebalance")
- "description": 1-2 sentence description
- "affected_service": service name or "all" (services: {json.dumps(services)})
- "affected_endpoints": array of endpoint strings like "POST /v1/checkout", "GET /v1/products/{{id}}", "POST /v1/draw/enter", "GET /v1/account/profile", "GET /v1/feed", "GET /v1/search", "POST /v1/wishlist", "GET /v1/recommendations", "POST /v1/returns", "GET /v1/shipping/status"
- "error_rate_override": float 0.05-1.0
- "latency_multiplier": float 1.0-20.0
- "duration_fraction": float 0.05-0.60 (fraction of drop window affected)
- "error_message": realistic error message string
- "progression": one of "instant", "ramp_up", "ramp_down", "spike", "oscillating"
- "cascades_to": array of service names this incident triggers degradation in (can be empty)

Include diverse failure modes: memory leaks, DNS failures, TLS cert expiry, split-brain,
Kafka rebalance, Redis eviction storms, thread pool exhaustion, GC pauses, disk full,
network partition, certificate rotation, dependency timeout cascades, config drift, etc.

Return ONLY a JSON array, no markdown fences."""

    all_incidents = generate_in_batches(prompt_fn, 2, provider, max_tokens=8192)
    save_corpus("incidents", all_incidents)
    return all_incidents


def gen_services(provider: str):
    """Generate 30-50 service definitions with dependency graph."""
    print("\n[4/6] Generating services...")

    prompt = f"""Generate a service dependency graph for a sneaker e-commerce platform with 35 microservices.

Each service should be a JSON object:
- "name": service name (kebab-case, e.g., "auth-service")
- "version": semver version (vary across services, e.g., "4.2.1", "3.0.0", "2.1.5")
- "language": "java", "go", "python", "node", "rust"
- "framework": matching framework (e.g., "spring-boot", "gin", "fastapi", "express", "actix")
- "port": integer port number
- "dependencies": array of service names this service calls
- "databases": array of objects [{{"type": "postgresql"|"redis"|"dynamodb"|"mongodb"|"elasticsearch", "name": "db-name"}}]
- "queues": array of objects [{{"system": "kafka"|"sqs"|"rabbitmq", "topics": ["topic1"]}}]
- "replicas": integer 2-10
- "cpu_limit": string like "500m", "1000m", "2000m"
- "memory_limit": string like "512Mi", "1Gi", "2Gi"
- "k8s_namespace": "snkrs-platform" or "snkrs-infra"

Include the 8 existing services: api-gateway, auth-service, product-service, draw-service,
inventory-service, payment-service, order-service, notification-service.
Add 25+ more: search, recommendation, shipping, returns, wishlist, analytics, pricing,
review, loyalty, fraud, image, cache, queue, config, rate-limiter, geo, content, ab-test,
user-preference, session, audit, cdn-proxy, monitoring, feature-flag, etc.

Return ONLY a JSON array, no markdown fences."""

    print("    Generating...", end=" ", flush=True)
    try:
        text = call_llm(prompt, provider, max_tokens=8192)
        services = extract_json(text)
        print(f"got {len(services)} services")
    except Exception as e:
        print(f"ERROR: {e}")
        services = []

    save_corpus("services", services)
    return services


def gen_error_messages(provider: str):
    """Generate 1,000+ realistic error messages and stack traces."""
    print("\n[5/6] Generating error messages...")

    def prompt_fn(batch_idx, total):
        categories = [
            "database", "cache", "network", "authentication", "payment",
            "validation", "serialization", "timeout", "resource_exhaustion",
            "concurrency", "configuration", "dependency", "filesystem",
        ]
        # Rotate categories across batches
        batch_cats = categories[batch_idx * 3:(batch_idx + 1) * 3]
        if not batch_cats:
            batch_cats = categories[:3]

        languages = ["java", "python", "go", "node"]

        return f"""Generate {120} realistic error messages and partial stack traces for microservices.
Batch {batch_idx+1} of {total}. Focus on these error categories: {json.dumps(batch_cats)}

Each error should be a JSON object:
- "category": one of {json.dumps(categories)}
- "language": one of {json.dumps(languages)}
- "message": the error message string (1-3 lines)
- "stack_trace": partial stack trace (3-8 lines, realistic package names)
- "service_hint": which type of service typically produces this (e.g., "payment-service", "any")
- "severity": "ERROR" or "FATAL"
- "http_status": associated HTTP status code (400-599) or null

Include: SQL errors, connection refused, timeout, OOM, NPE, type errors,
serialization failures, SSL/TLS errors, DNS resolution failures, deadlocks,
pool exhaustion, circuit breaker open, rate limit exceeded, etc.

Stack traces should use realistic package paths (com.nike.snkrs.*, github.com/nike/*,
@nike/snkrs-*, etc.)

Return ONLY a JSON array, no markdown fences."""

    all_errors = generate_in_batches(prompt_fn, 8, provider, max_tokens=8192)
    save_corpus("error_messages", all_errors)
    return all_errors


def gen_endpoints(provider: str):
    """Generate 50-100 API endpoints."""
    print("\n[6/6] Generating endpoints...")

    prompt = f"""Generate 80 realistic API endpoints for a sneaker e-commerce platform (Nike SNKRS style).

Each endpoint should be a JSON object:
- "method": "GET", "POST", "PUT", "DELETE", "PATCH"
- "route": URL path with {{placeholders}} (e.g., "/v1/products/{{id}}")
- "service": primary handling service
- "description": short description
- "auth_required": boolean
- "rate_limit": requests per minute (10-1000)
- "typical_latency_ms": typical p50 latency
- "error_rate_baseline": normal error rate (0.001-0.05)
- "category": "browse", "purchase", "account", "social", "admin", "search", "shipping", "returns", "analytics"

Include the 5 existing endpoints:
- GET /v1/products/{{id}}
- POST /v1/draw/enter
- POST /v1/checkout
- GET /v1/account/profile
- GET /v1/feed

Add 75+ more: search, filters, wishlist CRUD, reviews, recommendations,
shipping tracking, returns, cart operations, size availability, price alerts,
release calendar, notifications preferences, payment methods, addresses,
order history, loyalty points, referrals, social sharing, etc.

Return ONLY a JSON array, no markdown fences."""

    print("    Generating...", end=" ", flush=True)
    try:
        text = call_llm(prompt, provider, max_tokens=8192)
        endpoints = extract_json(text)
        print(f"got {len(endpoints)} endpoints")
    except Exception as e:
        print(f"ERROR: {e}")
        endpoints = []

    save_corpus("endpoints", endpoints)
    return endpoints


# ── Fallback: Generate corpuses without LLM ─────────────────────────────

def gen_fallback_corpuses():
    """Generate reasonable corpuses without calling an LLM API.
    Uses programmatic expansion from the existing hardcoded data."""
    import random
    import itertools

    print("\nGenerating fallback corpuses (no LLM API)...")

    # ── Log templates (expand from patterns) ──────────────────────────
    print("  [1/6] Log templates...")
    verbs = ["processing", "handling", "executing", "validating", "checking",
             "fetching", "loading", "computing", "resolving", "dispatching",
             "routing", "forwarding", "serializing", "deserializing", "parsing",
             "encoding", "decoding", "compressing", "decompressing", "caching",
             "evicting", "replicating", "syncing", "indexing", "querying",
             "inserting", "updating", "deleting", "scanning", "filtering"]

    resources = ["user profile", "session token", "product data", "inventory record",
                 "payment transaction", "order record", "draw entry", "notification",
                 "cache entry", "queue message", "config value", "feature flag",
                 "rate limit counter", "audit log", "search index", "recommendation",
                 "shipping label", "return request", "wishlist item", "review",
                 "loyalty points", "price alert", "image asset", "cdn resource"]

    db_ops = ["SELECT", "INSERT", "UPDATE", "DELETE", "UPSERT", "BEGIN", "COMMIT", "ROLLBACK"]
    cache_ops = ["GET", "SET", "DEL", "INCR", "DECR", "EXPIRE", "HGET", "HSET", "SADD", "SISMEMBER",
                 "LPUSH", "RPOP", "ZADD", "ZRANGE", "PUBLISH", "SUBSCRIBE", "MGET", "PIPELINE"]
    queue_ops = ["produce", "consume", "ack", "nack", "requeue", "commit_offset", "seek"]

    templates = {}
    for svc in SERVICES_LIST:
        templates[svc] = {"DEBUG": [], "INFO": [], "WARN": [], "ERROR": []}

        svc_short = svc.replace("-service", "").replace("-", "_")

        # DEBUG templates
        for verb in verbs:
            templates[svc]["DEBUG"].append(
                f"{verb.capitalize()} request for user={{user_id}} on pod={{pod_name}} thread={{thread_name}}")
        for res in resources:
            templates[svc]["DEBUG"].append(
                f"Cache lookup for {res} key={{user_id}} hit={{cache_hit}} ttl={{ttl}}s")
            templates[svc]["DEBUG"].append(
                f"Loaded {res} from datastore in {{query_duration_ms}}ms")
        for op in cache_ops:
            templates[svc]["DEBUG"].append(
                f"Redis {op} on {svc_short}:{{user_id}} completed in {{latency}}ms")
        for op in db_ops:
            templates[svc]["DEBUG"].append(
                f"PostgreSQL {op} on {svc_short}_table completed rows_affected={{retries}} duration={{query_duration_ms}}ms")

        # INFO templates
        for res in resources:
            templates[svc]["INFO"].append(
                f"Successfully processed {res} for user={{user_id}} latency={{latency}}ms")
        templates[svc]["INFO"].extend([
            f"{svc} health check passed pod={{pod_name}} uptime={{ttl}}s",
            f"Request completed {{method}} {{route}} status={{status_code}} latency={{latency}}ms user={{user_id}}",
            f"Metric checkpoint: active_connections={{active}} idle={{idle}} max={{max_conn}}",
            f"GC completed: heap_used={{heap_used_mb}}MB pause={{gc_pause_ms}}ms",
            f"Config reloaded for {svc} version={{service_version}} pod={{pod_name}}",
            f"Connection pool stats: active={{active}}/{{max_conn}} idle={{idle}} wait_time={{latency}}ms",
        ])

        # WARN templates
        templates[svc]["WARN"].extend([
            f"Elevated latency on {svc}: {{latency}}ms exceeds p99 threshold",
            f"Connection pool nearing capacity: {{active}}/{{max_conn}} (threshold: 80%)",
            f"Cache miss rate elevated: {{miss_rate:.1f}}% on {svc_short} cache",
            f"Retry attempt {{attempt}}/{{retries}} for {{method}} {{route}} user={{user_id}}",
            f"GC pause exceeded threshold: {{gc_pause_ms}}ms (limit: 200ms) on pod={{pod_name}}",
            f"Heap usage elevated: {{heap_used_mb}}MB/1024MB on pod={{pod_name}}",
            f"Rate limit approaching for user={{user_id}}: {{rate}}/100 in window",
            f"Slow query detected: {{query_duration_ms}}ms on {svc_short}_table",
            f"Queue depth elevated: {{queue_depth}} messages pending on {svc_short}.events",
            f"Circuit breaker half-open for {svc}: testing with probe request",
            f"Thread pool saturation: {{active}}/{{max_conn}} threads busy on pod={{pod_name}}",
            f"DNS resolution slow for {svc_short}.internal: {{latency}}ms",
            f"TLS certificate expiring in {{ttl}} days for {svc}",
        ])

        # ERROR templates
        templates[svc]["ERROR"].extend([
            f"Request failed {{method}} {{route}} status={{status_code}} error={{error_msg}} user={{user_id}}",
            f"Database connection failed: pool exhausted {{active}}/{{max_conn}} on {svc}",
            f"Timeout after {{timeout}}ms calling downstream service from {svc}",
            f"Circuit breaker OPEN for {svc}: {{rate}} failures in last 60s",
            f"OOM warning: heap at {{heap_used_mb}}MB on pod={{pod_name}} container={{container_id}}",
            f"Unhandled exception in {svc}: {{error_msg}}",
            f"Dead letter queue overflow: {{queue_depth}} messages on {svc_short}.dlq",
            f"Connection refused to {{service}}: dial tcp {{pod_name}}:{{active}} connect: connection refused",
            f"SSL handshake failed connecting to {{service}}: certificate verify failed",
            f"Kafka producer error on {svc_short}.events partition={{partition}}: {{error_msg}}",
            f"Redis READONLY: cannot write to replica on {svc_short} cache",
            f"PostgreSQL deadlock detected on {svc_short}_table: {{error_msg}}",
            f"Request body deserialization failed: {{error_msg}} user={{user_id}}",
            f"Dependency health check failed: {{service}} unreachable from {svc} pod={{pod_name}}",
            f"Memory pressure: GC overhead {{gc_pause_ms}}ms heap={{heap_used_mb}}MB on pod={{pod_name}}",
        ])

    save_corpus("log_templates", templates)

    # ── Products (expand programmatically) ────────────────────────────
    print("  [2/6] Products...")
    brands = ["Nike", "Jordan", "Adidas", "New Balance", "Puma", "Converse",
              "Reebok", "ASICS", "Vans", "Saucony"]
    models = {
        "Nike": ["Air Force 1", "Air Max 90", "Air Max 95", "Air Max 97", "Air Max 1",
                 "Air Max Plus", "Dunk Low", "Dunk High", "Blazer Mid", "Vaporfly",
                 "Pegasus", "React", "Air Presto", "Air Huarache", "Cortez",
                 "Waffle One", "Air Rift", "Zoom Fly", "Air Zoom", "Free Run"],
        "Jordan": ["Air Jordan 1 High", "Air Jordan 1 Low", "Air Jordan 1 Mid",
                   "Air Jordan 3", "Air Jordan 4", "Air Jordan 5", "Air Jordan 6",
                   "Air Jordan 11", "Air Jordan 12", "Air Jordan 13"],
        "Adidas": ["Yeezy 350 V2", "Yeezy Slide", "Yeezy 500", "Yeezy 700",
                   "Ultraboost", "NMD R1", "Forum Low", "Gazelle", "Samba", "Campus"],
        "New Balance": ["550", "2002R", "990v6", "574", "327", "1906R", "530", "9060"],
        "Puma": ["Suede", "RS-X", "Slipstream", "Palermo", "Clyde"],
        "Converse": ["Chuck 70", "Chuck Taylor", "One Star", "Run Star Hike"],
        "Reebok": ["Club C 85", "Classic Leather", "Instapump Fury", "Question Mid"],
        "ASICS": ["Gel-Lyte III", "Gel-Kayano 14", "Gel-1130", "GT-2160"],
        "Vans": ["Old Skool", "Sk8-Hi", "Authentic", "Era", "Slip-On"],
        "Saucony": ["Shadow 6000", "Grid Shadow 2", "Jazz Original"],
    }
    colorways = [
        "Triple White", "Triple Black", "Bred", "Chicago", "Royal Blue",
        "University Blue", "Pine Green", "Sail", "Bone", "Panda",
        "Infrared", "Zebra", "Onyx", "Slate", "Desert Sand",
        "Obsidian", "Shadow", "Court Purple", "Mocha", "Hyper Royal",
        "Lucky Green", "Fire Red", "Cool Grey", "Light Smoke Grey",
        "Photon Dust", "Midnight Navy", "Gym Red", "Volt", "Laser Orange",
        "Pink Foam", "Barely Rose", "Plum Eclipse", "Rust Pink",
        "Sea Glass", "Coconut Milk", "Natural Indigo", "Dark Iris",
    ]
    collabs = [
        None, None, None, None, None,  # 50% no collab
        "Travis Scott", "Off-White", "fragment design", "sacai",
        "Stussy", "Union", "A Ma Maniere", "Social Status",
        "KITH", "Concepts", "Patta", "JJJJound", "Aime Leon Dore",
        "Fear of God", "AMBUSH", "Comme des Garcons", "UNDERCOVER",
        "CLOT", "Billie Eilish", "J Balvin",
    ]
    categories = ["lifestyle", "running", "basketball", "skateboarding",
                  "training", "slides", "retro", "performance"]
    release_types = ["general_release", "limited_edition", "exclusive_access",
                     "draw", "fcfs", "raffle"]

    rng = random.Random(42)
    products = []
    seen_ids = set()
    idx = 0
    for brand in brands:
        for model in models.get(brand, ["Classic"]):
            for _ in range(rng.randint(2, 5)):
                cw = rng.choice(colorways)
                collab = rng.choice(collabs)
                name_parts = []
                if collab:
                    name_parts.append(f"{model} x {collab}")
                else:
                    name_parts.append(model)
                name_parts.append(f"'{cw}'")
                full_name = " ".join(name_parts)

                short_model = model.replace(" ", "")[:4].upper()
                short_cw = cw.replace(" ", "")[:3].upper()
                pid = f"{short_model}-{short_cw}-{idx}"
                if pid in seen_ids:
                    idx += 1
                    pid = f"{short_model}-{short_cw}-{idx}"
                seen_ids.add(pid)

                sku_num = rng.randint(100000, 999999)
                sku_sfx = rng.randint(0, 999)
                sku = f"{sku_num}-{sku_sfx:03d}"

                hype = round(rng.uniform(0.1, 1.0), 2)
                if collab:
                    hype = round(min(1.0, hype + 0.2), 2)

                products.append({
                    "id": pid,
                    "name": full_name,
                    "sku": sku,
                    "price": rng.choice([70, 90, 100, 110, 120, 130, 140, 150,
                                         160, 170, 175, 180, 200, 220, 225, 250, 300, 350]),
                    "base_stock": rng.randint(50, 3000),
                    "hype_level": hype,
                    "category": rng.choice(categories),
                    "release_type": rng.choice(release_types),
                    "brand": brand,
                    "collab": collab,
                    "colorway": cw,
                })
                idx += 1

    save_corpus("products", products)

    # ── Incidents (expand programmatically) ───────────────────────────
    print("  [3/6] Incidents...")
    incident_templates = [
        ("connection_pool_exhaustion_{svc}", "{svc} connection pool exhausted", "{svc}",
         0.40, 3.0, 0.25, "Connection pool exhausted: max connections reached ({max_conn}/{max_conn})"),
        ("memory_leak_{svc}", "Memory leak in {svc} causing GC pressure", "{svc}",
         0.15, 5.0, 0.50, "OutOfMemoryError: Java heap space in {svc}"),
        ("dns_resolution_failure_{svc}", "DNS resolution failure for {svc}", "{svc}",
         0.30, 8.0, 0.15, "DNS resolution failed: NXDOMAIN for {svc}.internal"),
        ("tls_cert_expiry_{svc}", "TLS certificate expiry for {svc}", "{svc}",
         0.50, 2.0, 0.30, "SSL: CERTIFICATE_VERIFY_FAILED for {svc}.internal:443"),
        ("kafka_rebalance_{svc}", "Kafka consumer group rebalancing in {svc}", "{svc}",
         0.10, 4.0, 0.20, "Consumer group rebalancing: partitions revoked for {svc}"),
        ("redis_eviction_storm_{svc}", "Redis eviction storm affecting {svc} cache", "{svc}",
         0.25, 6.0, 0.35, "Redis maxmemory reached: evicting keys for {svc} cache"),
        ("thread_pool_exhaustion_{svc}", "Thread pool exhaustion in {svc}", "{svc}",
         0.35, 4.0, 0.20, "Thread pool exhausted: {max_conn}/{max_conn} threads busy in {svc}"),
        ("gc_stop_the_world_{svc}", "Long GC pause in {svc}", "{svc}",
         0.20, 10.0, 0.10, "GC pause exceeded 5s: Full GC triggered in {svc}"),
        ("disk_full_{svc}", "Disk full on {svc} pod", "{svc}",
         0.60, 1.0, 0.15, "No space left on device: /var/log full on {svc}"),
        ("network_partition_{svc}", "Network partition isolating {svc}", "{svc}",
         0.80, 1.0, 0.20, "Network unreachable: {svc} isolated from cluster"),
        ("config_drift_{svc}", "Configuration drift detected in {svc}", "{svc}",
         0.15, 2.0, 0.40, "Config version mismatch: expected v42 got v41 in {svc}"),
        ("dependency_cascade_{svc}", "Cascading failure from {svc} dependency", "{svc}",
         0.30, 8.0, 0.35, "Circuit breaker OPEN: {svc} dependency unreachable"),
        ("split_brain_{svc}", "Split-brain scenario in {svc} cluster", "{svc}",
         0.50, 3.0, 0.25, "Split-brain detected: multiple leaders in {svc} cluster"),
        ("hot_partition_{svc}", "Hot partition in {svc} database", "{svc}",
         0.20, 7.0, 0.30, "Partition key hotspot detected: shard overloaded in {svc}"),
    ]

    all_endpoints = [
        "POST /v1/checkout", "POST /v1/draw/enter", "GET /v1/products/{id}",
        "GET /v1/feed", "GET /v1/account/profile", "GET /v1/search",
        "POST /v1/wishlist", "GET /v1/recommendations", "POST /v1/returns",
        "GET /v1/shipping/status",
    ]
    progressions = ["instant", "ramp_up", "ramp_down", "spike", "oscillating"]

    incidents = []
    target_services = SERVICES_LIST[:15]
    for tmpl in incident_templates:
        name_t, desc_t, svc_t, err_rate, lat_mult, dur_frac, msg_t = tmpl
        for svc in rng.sample(target_services, min(4, len(target_services))):
            name = name_t.replace("{svc}", svc.replace("-service", "").replace("-", "_"))
            desc = desc_t.replace("{svc}", svc)
            affected_svc = svc
            msg = msg_t.replace("{svc}", svc).replace("{max_conn}", str(rng.randint(20, 100)))

            n_endpoints = rng.randint(1, 4)
            affected_eps = rng.sample(all_endpoints, min(n_endpoints, len(all_endpoints)))

            cascade_targets = []
            if rng.random() < 0.3:
                cascade_targets = rng.sample(
                    [s for s in target_services if s != svc],
                    rng.randint(1, 3))

            incidents.append({
                "name": name,
                "description": desc,
                "affected_service": affected_svc,
                "affected_endpoints": affected_eps,
                "error_rate_override": round(err_rate * rng.uniform(0.5, 1.5), 2),
                "latency_multiplier": round(lat_mult * rng.uniform(0.7, 1.3), 1),
                "duration_fraction": round(dur_frac * rng.uniform(0.8, 1.2), 2),
                "error_message": msg,
                "progression": rng.choice(progressions),
                "cascades_to": cascade_targets,
            })

    # Add the 6 original incidents for backward compat
    from patterns import INCIDENTS as ORIG_INCIDENTS
    for inc in ORIG_INCIDENTS:
        inc_copy = dict(inc)
        inc_copy.setdefault("progression", "instant")
        inc_copy.setdefault("cascades_to", [])
        incidents.append(inc_copy)

    save_corpus("incidents", incidents)

    # ── Services ──────────────────────────────────────────────────────
    print("  [4/6] Services...")
    languages = ["java", "go", "python", "node", "rust"]
    frameworks = {
        "java": "spring-boot", "go": "gin", "python": "fastapi",
        "node": "express", "rust": "actix",
    }
    namespaces = ["snkrs-platform", "snkrs-infra"]

    services = []
    for i, svc in enumerate(SERVICES_LIST):
        lang = rng.choice(languages)
        deps = rng.sample([s for s in SERVICES_LIST if s != svc],
                          min(rng.randint(0, 4), len(SERVICES_LIST) - 1))
        db_types = ["postgresql", "redis", "dynamodb", "mongodb", "elasticsearch"]
        dbs = []
        for _ in range(rng.randint(1, 3)):
            db_type = rng.choice(db_types)
            dbs.append({"type": db_type, "name": f"{svc.replace('-service', '')}-{db_type[:5]}"})

        queues = []
        if rng.random() < 0.6:
            q_system = rng.choice(["kafka", "sqs", "rabbitmq"])
            queues.append({
                "system": q_system,
                "topics": [f"snkrs.{svc.replace('-service', '')}.events"],
            })

        major = rng.randint(1, 5)
        minor = rng.randint(0, 12)
        patch = rng.randint(0, 20)

        services.append({
            "name": svc,
            "version": f"{major}.{minor}.{patch}",
            "language": lang,
            "framework": frameworks[lang],
            "port": 8080 + i,
            "dependencies": deps,
            "databases": dbs,
            "queues": queues,
            "replicas": rng.randint(2, 10),
            "cpu_limit": rng.choice(["250m", "500m", "1000m", "2000m"]),
            "memory_limit": rng.choice(["256Mi", "512Mi", "1Gi", "2Gi"]),
            "k8s_namespace": rng.choice(namespaces),
        })

    save_corpus("services", services)

    # ── Error messages ────────────────────────────────────────────────
    print("  [5/6] Error messages...")
    error_categories = [
        "database", "cache", "network", "authentication", "payment",
        "validation", "serialization", "timeout", "resource_exhaustion",
        "concurrency", "configuration", "dependency", "filesystem",
    ]
    java_packages = ["com.nike.snkrs", "com.nike.platform", "org.springframework",
                     "io.grpc", "io.netty", "com.zaxxer.hikari", "org.apache.kafka"]
    go_packages = ["github.com/nike/snkrs", "google.golang.org/grpc",
                   "database/sql", "net/http", "github.com/go-redis/redis"]
    python_packages = ["snkrs.api", "snkrs.services", "fastapi", "sqlalchemy",
                       "redis", "grpc", "kafka"]
    node_packages = ["@nike/snkrs-api", "express", "ioredis", "pg",
                     "kafkajs", "@grpc/grpc-js"]

    error_templates = [
        ("database", "java", "java.sql.SQLException: Connection pool exhausted",
         "at com.zaxxer.hikari.HikariPool.getConnection(HikariPool.java:155)\nat com.nike.snkrs.{svc}.repository.{Entity}Repository.find(Repository.java:{line})\nat com.nike.snkrs.{svc}.service.{Entity}Service.get(Service.java:{line})"),
        ("database", "java", "org.postgresql.util.PSQLException: ERROR: deadlock detected",
         "at org.postgresql.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:{line})\nat com.nike.snkrs.{svc}.repository.{Entity}Repository.update(Repository.java:{line})"),
        ("database", "go", "pq: connection refused",
         "database/sql.(*DB).conn(db.go:{line})\ngithub.com/nike/snkrs/{svc}/store.(*Store).Get(store.go:{line})"),
        ("database", "python", "sqlalchemy.exc.OperationalError: (psycopg2.OperationalError) connection to server timed out",
         "File \"/app/snkrs/{svc}/models.py\", line {line}, in query\nFile \"/usr/lib/python3.11/sqlalchemy/engine/base.py\", line {line}, in execute"),
        ("cache", "java", "redis.clients.jedis.exceptions.JedisConnectionException: Could not get a resource from the pool",
         "at redis.clients.jedis.JedisPool.getResource(JedisPool.java:{line})\nat com.nike.snkrs.{svc}.cache.RedisCache.get(RedisCache.java:{line})"),
        ("cache", "go", "redis: connection pool timeout",
         "github.com/go-redis/redis.(*baseClient).process(redis.go:{line})\ngithub.com/nike/snkrs/{svc}/cache.(*Cache).Get(cache.go:{line})"),
        ("cache", "node", "MaxRetriesPerRequestError: Reached the max retries per request limit",
         "at /app/node_modules/ioredis/built/redis/index.js:{line}\nat /app/src/{svc}/cache.js:{line}"),
        ("network", "java", "io.grpc.StatusRuntimeException: UNAVAILABLE: io exception",
         "at io.grpc.stub.ClientCalls.toStatusRuntimeException(ClientCalls.java:{line})\nat com.nike.snkrs.{svc}.client.{Target}Client.call(Client.java:{line})"),
        ("network", "go", "rpc error: code = Unavailable desc = connection error: desc = transport: Error while dialing",
         "google.golang.org/grpc.(*ccBalancerWrapper).NewSubConn(...)\ngithub.com/nike/snkrs/{svc}/client.(*Client).Call(client.go:{line})"),
        ("network", "python", "grpc._channel._InactiveRpcError: <_InactiveRpcError of RPC that terminated with: status = StatusCode.UNAVAILABLE",
         "File \"/app/snkrs/{svc}/client.py\", line {line}, in call_service\nFile \"/usr/lib/python3.11/grpc/_channel.py\", line {line}, in __call__"),
        ("timeout", "java", "java.util.concurrent.TimeoutException: Timeout waiting for task",
         "at java.util.concurrent.FutureTask.get(FutureTask.java:{line})\nat com.nike.snkrs.{svc}.service.{Entity}Service.process(Service.java:{line})"),
        ("timeout", "go", "context deadline exceeded",
         "context.(*timerCtx).Err(...)\ngithub.com/nike/snkrs/{svc}/handler.(*Handler).Handle(handler.go:{line})"),
        ("authentication", "java", "io.jsonwebtoken.ExpiredJwtException: JWT expired at {ts}",
         "at io.jsonwebtoken.impl.DefaultJwtParser.parse(DefaultJwtParser.java:{line})\nat com.nike.snkrs.auth.JwtValidator.validate(JwtValidator.java:{line})"),
        ("payment", "node", "StripeInvalidRequestError: No such payment method: pm_{id}",
         "at /app/node_modules/stripe/lib/Error.js:{line}\nat /app/src/payment/processor.js:{line}"),
        ("resource_exhaustion", "java", "java.lang.OutOfMemoryError: Java heap space",
         "at java.util.Arrays.copyOf(Arrays.java:{line})\nat com.nike.snkrs.{svc}.buffer.EventBuffer.append(EventBuffer.java:{line})"),
        ("concurrency", "go", "fatal error: concurrent map writes",
         "runtime.throw({...})\nruntime.mapassign_faststr(...)\ngithub.com/nike/snkrs/{svc}/state.(*State).Update(state.go:{line})"),
        ("serialization", "python", "json.decoder.JSONDecodeError: Expecting value: line 1 column 1 (char 0)",
         "File \"/app/snkrs/{svc}/handler.py\", line {line}, in parse_request\nFile \"/usr/lib/python3.11/json/__init__.py\", line {line}, in loads"),
        ("validation", "node", "ValidationError: \"shoe_size\" must be a number",
         "at /app/src/{svc}/validator.js:{line}\nat /app/src/{svc}/handler.js:{line}"),
        ("configuration", "go", "config: key not found: {key}",
         "github.com/nike/snkrs/config.(*Config).Get(...)\ngithub.com/nike/snkrs/{svc}/service.(*Service).Init(service.go:{line})"),
        ("filesystem", "python", "OSError: [Errno 28] No space left on device",
         "File \"/app/snkrs/{svc}/storage.py\", line {line}, in write\nFile \"/usr/lib/python3.11/io.py\", line {line}, in write"),
    ]

    entities = ["User", "Product", "Order", "Payment", "Draw", "Inventory",
                "Notification", "Session", "Cart", "Wishlist", "Review", "Shipping"]
    svc_shorts = ["auth", "product", "order", "payment", "draw", "inventory",
                  "notification", "shipping", "search", "recommendation"]
    targets = ["AuthService", "ProductService", "InventoryService", "PaymentService"]
    config_keys = ["db.pool.max", "redis.timeout", "grpc.deadline", "kafka.batch.size",
                   "rate.limit.max", "circuit.breaker.threshold"]

    errors = []
    http_status_map = {
        "database": 500, "cache": 500, "network": 502, "authentication": 401,
        "payment": 402, "validation": 400, "serialization": 400,
        "timeout": 504, "resource_exhaustion": 503, "concurrency": 500,
        "configuration": 500, "dependency": 502, "filesystem": 500,
    }

    for cat, lang, msg_t, trace_t in error_templates:
        for _ in range(rng.randint(30, 60)):
            svc = rng.choice(svc_shorts)
            entity = rng.choice(entities)
            target = rng.choice(targets)
            line = rng.randint(30, 500)
            key = rng.choice(config_keys)
            ts = f"2025-{rng.randint(1,12):02d}-{rng.randint(1,28):02d}T{rng.randint(0,23):02d}:{rng.randint(0,59):02d}"
            pid = f"pm_{rng.randint(100000,999999)}"

            msg = msg_t.replace("{svc}", svc).replace("{Entity}", entity).replace(
                "{Target}", target).replace("{line}", str(line)).replace(
                "{ts}", ts).replace("{id}", pid).replace("{key}", key)
            trace = trace_t.replace("{svc}", svc).replace("{Entity}", entity).replace(
                "{Target}", target).replace("{line}", str(rng.randint(30, 500)))

            errors.append({
                "category": cat,
                "language": lang,
                "message": msg,
                "stack_trace": trace,
                "service_hint": f"{svc}-service",
                "severity": rng.choice(["ERROR", "ERROR", "ERROR", "FATAL"]),
                "http_status": http_status_map.get(cat),
            })

    save_corpus("error_messages", errors)

    # ── Endpoints ─────────────────────────────────────────────────────
    print("  [6/6] Endpoints...")
    endpoints = [
        # Original 5
        {"method": "GET", "route": "/v1/products/{id}", "service": "product-service",
         "description": "Get product details", "auth_required": False,
         "rate_limit": 500, "typical_latency_ms": 45, "error_rate_baseline": 0.01,
         "category": "browse"},
        {"method": "POST", "route": "/v1/draw/enter", "service": "draw-service",
         "description": "Enter SNKRS draw", "auth_required": True,
         "rate_limit": 100, "typical_latency_ms": 200, "error_rate_baseline": 0.05,
         "category": "purchase"},
        {"method": "POST", "route": "/v1/checkout", "service": "payment-service",
         "description": "Complete checkout", "auth_required": True,
         "rate_limit": 50, "typical_latency_ms": 800, "error_rate_baseline": 0.03,
         "category": "purchase"},
        {"method": "GET", "route": "/v1/account/profile", "service": "auth-service",
         "description": "Get user profile", "auth_required": True,
         "rate_limit": 200, "typical_latency_ms": 60, "error_rate_baseline": 0.01,
         "category": "account"},
        {"method": "GET", "route": "/v1/feed", "service": "feed-service",
         "description": "Get homepage feed", "auth_required": False,
         "rate_limit": 300, "typical_latency_ms": 35, "error_rate_baseline": 0.005,
         "category": "browse"},
        # New endpoints
        {"method": "GET", "route": "/v1/search", "service": "search-service",
         "description": "Search products", "auth_required": False,
         "rate_limit": 400, "typical_latency_ms": 80, "error_rate_baseline": 0.01,
         "category": "search"},
        {"method": "GET", "route": "/v1/search/suggestions", "service": "search-service",
         "description": "Search autocomplete", "auth_required": False,
         "rate_limit": 600, "typical_latency_ms": 30, "error_rate_baseline": 0.005,
         "category": "search"},
        {"method": "GET", "route": "/v1/products/{id}/reviews", "service": "review-service",
         "description": "Get product reviews", "auth_required": False,
         "rate_limit": 300, "typical_latency_ms": 50, "error_rate_baseline": 0.008,
         "category": "browse"},
        {"method": "POST", "route": "/v1/products/{id}/reviews", "service": "review-service",
         "description": "Submit product review", "auth_required": True,
         "rate_limit": 20, "typical_latency_ms": 150, "error_rate_baseline": 0.02,
         "category": "social"},
        {"method": "GET", "route": "/v1/recommendations", "service": "recommendation-service",
         "description": "Get personalized recommendations", "auth_required": True,
         "rate_limit": 200, "typical_latency_ms": 120, "error_rate_baseline": 0.015,
         "category": "browse"},
        {"method": "GET", "route": "/v1/wishlist", "service": "wishlist-service",
         "description": "Get user wishlist", "auth_required": True,
         "rate_limit": 200, "typical_latency_ms": 40, "error_rate_baseline": 0.008,
         "category": "account"},
        {"method": "POST", "route": "/v1/wishlist", "service": "wishlist-service",
         "description": "Add to wishlist", "auth_required": True,
         "rate_limit": 100, "typical_latency_ms": 60, "error_rate_baseline": 0.01,
         "category": "account"},
        {"method": "DELETE", "route": "/v1/wishlist/{id}", "service": "wishlist-service",
         "description": "Remove from wishlist", "auth_required": True,
         "rate_limit": 100, "typical_latency_ms": 45, "error_rate_baseline": 0.008,
         "category": "account"},
        {"method": "GET", "route": "/v1/cart", "service": "order-service",
         "description": "Get shopping cart", "auth_required": True,
         "rate_limit": 200, "typical_latency_ms": 35, "error_rate_baseline": 0.005,
         "category": "purchase"},
        {"method": "POST", "route": "/v1/cart/items", "service": "order-service",
         "description": "Add item to cart", "auth_required": True,
         "rate_limit": 100, "typical_latency_ms": 80, "error_rate_baseline": 0.02,
         "category": "purchase"},
        {"method": "DELETE", "route": "/v1/cart/items/{id}", "service": "order-service",
         "description": "Remove cart item", "auth_required": True,
         "rate_limit": 100, "typical_latency_ms": 40, "error_rate_baseline": 0.008,
         "category": "purchase"},
        {"method": "GET", "route": "/v1/orders", "service": "order-service",
         "description": "Get order history", "auth_required": True,
         "rate_limit": 100, "typical_latency_ms": 90, "error_rate_baseline": 0.01,
         "category": "account"},
        {"method": "GET", "route": "/v1/orders/{id}", "service": "order-service",
         "description": "Get order details", "auth_required": True,
         "rate_limit": 150, "typical_latency_ms": 60, "error_rate_baseline": 0.008,
         "category": "account"},
        {"method": "GET", "route": "/v1/orders/{id}/tracking", "service": "shipping-service",
         "description": "Get shipping tracking", "auth_required": True,
         "rate_limit": 200, "typical_latency_ms": 100, "error_rate_baseline": 0.015,
         "category": "shipping"},
        {"method": "POST", "route": "/v1/returns", "service": "returns-service",
         "description": "Initiate return", "auth_required": True,
         "rate_limit": 20, "typical_latency_ms": 200, "error_rate_baseline": 0.03,
         "category": "returns"},
        {"method": "GET", "route": "/v1/returns/{id}", "service": "returns-service",
         "description": "Get return status", "auth_required": True,
         "rate_limit": 100, "typical_latency_ms": 50, "error_rate_baseline": 0.01,
         "category": "returns"},
        {"method": "GET", "route": "/v1/products/{id}/sizes", "service": "inventory-service",
         "description": "Get size availability", "auth_required": False,
         "rate_limit": 500, "typical_latency_ms": 25, "error_rate_baseline": 0.005,
         "category": "browse"},
        {"method": "GET", "route": "/v1/releases/calendar", "service": "product-service",
         "description": "Get release calendar", "auth_required": False,
         "rate_limit": 300, "typical_latency_ms": 60, "error_rate_baseline": 0.005,
         "category": "browse"},
        {"method": "GET", "route": "/v1/releases/upcoming", "service": "product-service",
         "description": "Get upcoming releases", "auth_required": False,
         "rate_limit": 300, "typical_latency_ms": 55, "error_rate_baseline": 0.005,
         "category": "browse"},
        {"method": "GET", "route": "/v1/account/addresses", "service": "auth-service",
         "description": "Get saved addresses", "auth_required": True,
         "rate_limit": 100, "typical_latency_ms": 40, "error_rate_baseline": 0.008,
         "category": "account"},
        {"method": "POST", "route": "/v1/account/addresses", "service": "auth-service",
         "description": "Add address", "auth_required": True,
         "rate_limit": 50, "typical_latency_ms": 80, "error_rate_baseline": 0.015,
         "category": "account"},
        {"method": "GET", "route": "/v1/account/payment-methods", "service": "payment-service",
         "description": "Get payment methods", "auth_required": True,
         "rate_limit": 100, "typical_latency_ms": 50, "error_rate_baseline": 0.01,
         "category": "account"},
        {"method": "POST", "route": "/v1/account/payment-methods", "service": "payment-service",
         "description": "Add payment method", "auth_required": True,
         "rate_limit": 30, "typical_latency_ms": 150, "error_rate_baseline": 0.02,
         "category": "account"},
        {"method": "GET", "route": "/v1/notifications", "service": "notification-service",
         "description": "Get notifications", "auth_required": True,
         "rate_limit": 200, "typical_latency_ms": 35, "error_rate_baseline": 0.005,
         "category": "account"},
        {"method": "PUT", "route": "/v1/notifications/preferences", "service": "notification-service",
         "description": "Update notification preferences", "auth_required": True,
         "rate_limit": 50, "typical_latency_ms": 60, "error_rate_baseline": 0.01,
         "category": "account"},
        {"method": "GET", "route": "/v1/loyalty/points", "service": "loyalty-service",
         "description": "Get loyalty points", "auth_required": True,
         "rate_limit": 200, "typical_latency_ms": 30, "error_rate_baseline": 0.005,
         "category": "account"},
        {"method": "POST", "route": "/v1/loyalty/redeem", "service": "loyalty-service",
         "description": "Redeem loyalty points", "auth_required": True,
         "rate_limit": 20, "typical_latency_ms": 200, "error_rate_baseline": 0.03,
         "category": "purchase"},
        {"method": "GET", "route": "/v1/products/{id}/similar", "service": "recommendation-service",
         "description": "Get similar products", "auth_required": False,
         "rate_limit": 300, "typical_latency_ms": 100, "error_rate_baseline": 0.01,
         "category": "browse"},
        {"method": "POST", "route": "/v1/price-alerts", "service": "pricing-service",
         "description": "Set price alert", "auth_required": True,
         "rate_limit": 50, "typical_latency_ms": 60, "error_rate_baseline": 0.01,
         "category": "account"},
        {"method": "GET", "route": "/v1/price-alerts", "service": "pricing-service",
         "description": "Get price alerts", "auth_required": True,
         "rate_limit": 100, "typical_latency_ms": 35, "error_rate_baseline": 0.005,
         "category": "account"},
        {"method": "GET", "route": "/v1/categories", "service": "product-service",
         "description": "Get product categories", "auth_required": False,
         "rate_limit": 500, "typical_latency_ms": 15, "error_rate_baseline": 0.002,
         "category": "browse"},
        {"method": "GET", "route": "/v1/categories/{id}/products", "service": "product-service",
         "description": "Get products by category", "auth_required": False,
         "rate_limit": 300, "typical_latency_ms": 70, "error_rate_baseline": 0.01,
         "category": "browse"},
        {"method": "GET", "route": "/v1/brands", "service": "product-service",
         "description": "Get brands list", "auth_required": False,
         "rate_limit": 500, "typical_latency_ms": 10, "error_rate_baseline": 0.002,
         "category": "browse"},
        {"method": "GET", "route": "/v1/brands/{id}/products", "service": "product-service",
         "description": "Get products by brand", "auth_required": False,
         "rate_limit": 300, "typical_latency_ms": 65, "error_rate_baseline": 0.01,
         "category": "browse"},
        {"method": "POST", "route": "/v1/auth/login", "service": "auth-service",
         "description": "User login", "auth_required": False,
         "rate_limit": 30, "typical_latency_ms": 150, "error_rate_baseline": 0.05,
         "category": "account"},
        {"method": "POST", "route": "/v1/auth/refresh", "service": "auth-service",
         "description": "Refresh auth token", "auth_required": True,
         "rate_limit": 100, "typical_latency_ms": 30, "error_rate_baseline": 0.01,
         "category": "account"},
        {"method": "POST", "route": "/v1/auth/logout", "service": "auth-service",
         "description": "User logout", "auth_required": True,
         "rate_limit": 100, "typical_latency_ms": 20, "error_rate_baseline": 0.002,
         "category": "account"},
        {"method": "GET", "route": "/v1/draw/{id}/status", "service": "draw-service",
         "description": "Get draw entry status", "auth_required": True,
         "rate_limit": 200, "typical_latency_ms": 40, "error_rate_baseline": 0.008,
         "category": "purchase"},
        {"method": "GET", "route": "/v1/draw/history", "service": "draw-service",
         "description": "Get draw history", "auth_required": True,
         "rate_limit": 100, "typical_latency_ms": 70, "error_rate_baseline": 0.01,
         "category": "account"},
        {"method": "GET", "route": "/v1/shipping/methods", "service": "shipping-service",
         "description": "Get shipping methods", "auth_required": False,
         "rate_limit": 200, "typical_latency_ms": 30, "error_rate_baseline": 0.005,
         "category": "shipping"},
        {"method": "POST", "route": "/v1/shipping/estimate", "service": "shipping-service",
         "description": "Estimate shipping cost", "auth_required": False,
         "rate_limit": 200, "typical_latency_ms": 80, "error_rate_baseline": 0.01,
         "category": "shipping"},
        {"method": "GET", "route": "/v1/inventory/status", "service": "inventory-service",
         "description": "Get inventory status", "auth_required": False,
         "rate_limit": 300, "typical_latency_ms": 20, "error_rate_baseline": 0.005,
         "category": "browse"},
        {"method": "POST", "route": "/v1/analytics/events", "service": "analytics-service",
         "description": "Track analytics event", "auth_required": False,
         "rate_limit": 1000, "typical_latency_ms": 10, "error_rate_baseline": 0.001,
         "category": "analytics"},
        {"method": "GET", "route": "/v1/content/banners", "service": "content-service",
         "description": "Get promotional banners", "auth_required": False,
         "rate_limit": 500, "typical_latency_ms": 20, "error_rate_baseline": 0.003,
         "category": "browse"},
        {"method": "GET", "route": "/v1/content/stories", "service": "content-service",
         "description": "Get editorial stories", "auth_required": False,
         "rate_limit": 300, "typical_latency_ms": 40, "error_rate_baseline": 0.005,
         "category": "browse"},
        {"method": "GET", "route": "/health", "service": "api-gateway",
         "description": "Health check", "auth_required": False,
         "rate_limit": 1000, "typical_latency_ms": 2, "error_rate_baseline": 0.001,
         "category": "admin"},
        {"method": "GET", "route": "/ready", "service": "api-gateway",
         "description": "Readiness check", "auth_required": False,
         "rate_limit": 1000, "typical_latency_ms": 5, "error_rate_baseline": 0.002,
         "category": "admin"},
    ]

    save_corpus("endpoints", endpoints)
    return endpoints


# ── Main ────────────────────────────────────────────────────────────────

CORPUS_GENERATORS = {
    "log_templates": gen_log_templates,
    "products": gen_products,
    "incidents": gen_incidents,
    "services": gen_services,
    "error_messages": gen_error_messages,
    "endpoints": gen_endpoints,
}


def main():
    parser = argparse.ArgumentParser(description="Generate diverse corpuses for SNKRS load generator")
    parser.add_argument("--provider", choices=["anthropic", "openai", "fallback"],
                        default="fallback",
                        help="LLM provider or 'fallback' for no-API generation (default: fallback)")
    parser.add_argument("--corpus", default="all",
                        help="Comma-separated corpus names or 'all' (default: all)")
    parser.add_argument("--list", action="store_true", help="List available corpuses")

    args = parser.parse_args()

    if args.list:
        print("Available corpuses:")
        for name in CORPUS_GENERATORS:
            print(f"  {name}")
        return

    if args.provider == "fallback":
        gen_fallback_corpuses()
        return

    corpuses = list(CORPUS_GENERATORS.keys()) if args.corpus == "all" else args.corpus.split(",")

    for name in corpuses:
        name = name.strip()
        if name not in CORPUS_GENERATORS:
            print(f"Unknown corpus: {name}")
            continue
        CORPUS_GENERATORS[name](args.provider)

    print("\nDone! Corpuses saved to:", CORPUS_DIR)


if __name__ == "__main__":
    main()
