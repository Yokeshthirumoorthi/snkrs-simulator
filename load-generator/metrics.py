"""
Time-series metric data point generator — produces ClickHouse-ready metric rows
at configurable intervals.

Expanded from 7 to 50+ metric types with per-service resource identity
and high-cardinality dimensions (k8s pod, instance, AZ).
"""

import random
from datetime import datetime, timedelta

from corpus_loader import get_services

# ── Metric definitions (50+ types) ──────────────────────────────────────

METRIC_DEFS = [
    # Original 7
    {"name": "snkrs.requests",     "description": "Total API requests",      "unit": "1",  "type": "Sum"},
    {"name": "snkrs.inventory",    "description": "Remaining inventory",     "unit": "1",  "type": "Gauge"},
    {"name": "snkrs.queue_depth",  "description": "Draw queue depth",        "unit": "1",  "type": "Gauge"},
    {"name": "snkrs.errors",       "description": "Total errors",            "unit": "1",  "type": "Sum"},
    {"name": "snkrs.payments",     "description": "Payment attempts",        "unit": "1",  "type": "Sum"},
    {"name": "snkrs.latency_p99",  "description": "P99 latency per service", "unit": "ms", "type": "Gauge"},
    {"name": "snkrs.active_users", "description": "Active users",            "unit": "1",  "type": "Gauge"},

    # Infrastructure metrics
    {"name": "system.cpu.utilization",        "description": "CPU utilization",           "unit": "%",    "type": "Gauge"},
    {"name": "system.memory.usage",           "description": "Memory usage",              "unit": "By",   "type": "Gauge"},
    {"name": "system.memory.limit",           "description": "Memory limit",              "unit": "By",   "type": "Gauge"},
    {"name": "runtime.jvm.gc.duration",       "description": "GC pause duration",         "unit": "ms",   "type": "Gauge"},
    {"name": "runtime.jvm.gc.count",          "description": "GC collection count",       "unit": "1",    "type": "Sum"},
    {"name": "runtime.jvm.threads.count",     "description": "Active thread count",       "unit": "1",    "type": "Gauge"},
    {"name": "runtime.jvm.heap.usage",        "description": "JVM heap usage",            "unit": "By",   "type": "Gauge"},
    {"name": "process.runtime.go.goroutines", "description": "Active goroutines",         "unit": "1",    "type": "Gauge"},
    {"name": "process.cpu.time",              "description": "Process CPU time",           "unit": "s",    "type": "Sum"},

    # Network metrics
    {"name": "http.server.request.duration",  "description": "HTTP request duration",      "unit": "ms",   "type": "Gauge"},
    {"name": "http.server.request.size",      "description": "HTTP request body size",     "unit": "By",   "type": "Gauge"},
    {"name": "http.server.response.size",     "description": "HTTP response body size",    "unit": "By",   "type": "Gauge"},
    {"name": "dns.lookup.duration",           "description": "DNS resolution time",        "unit": "ms",   "type": "Gauge"},
    {"name": "tls.handshake.duration",        "description": "TLS handshake duration",     "unit": "ms",   "type": "Gauge"},
    {"name": "net.connections.active",        "description": "Active network connections",  "unit": "1",    "type": "Gauge"},

    # Cache metrics
    {"name": "cache.hit_rate",                "description": "Cache hit rate",              "unit": "%",    "type": "Gauge"},
    {"name": "cache.miss_rate",               "description": "Cache miss rate",             "unit": "%",    "type": "Gauge"},
    {"name": "cache.evictions",               "description": "Cache evictions",             "unit": "1",    "type": "Sum"},
    {"name": "cache.memory.usage",            "description": "Cache memory usage",          "unit": "By",   "type": "Gauge"},
    {"name": "cache.keys.count",              "description": "Total cached keys",           "unit": "1",    "type": "Gauge"},
    {"name": "cache.latency.avg",             "description": "Cache operation latency",     "unit": "ms",   "type": "Gauge"},

    # Queue metrics
    {"name": "messaging.kafka.consumer.lag",          "description": "Kafka consumer lag",          "unit": "1",    "type": "Gauge"},
    {"name": "messaging.kafka.producer.record.count", "description": "Kafka records produced",       "unit": "1",    "type": "Sum"},
    {"name": "messaging.kafka.consumer.fetch.rate",   "description": "Kafka consumer fetch rate",    "unit": "1/s",  "type": "Gauge"},
    {"name": "messaging.sqs.queue.depth",             "description": "SQS queue depth",              "unit": "1",    "type": "Gauge"},
    {"name": "messaging.dlq.depth",                   "description": "Dead letter queue depth",      "unit": "1",    "type": "Gauge"},

    # Database metrics
    {"name": "db.query.duration.p50",         "description": "DB query p50 latency",       "unit": "ms",   "type": "Gauge"},
    {"name": "db.query.duration.p99",         "description": "DB query p99 latency",       "unit": "ms",   "type": "Gauge"},
    {"name": "db.connections.active",         "description": "Active DB connections",       "unit": "1",    "type": "Gauge"},
    {"name": "db.connections.idle",           "description": "Idle DB connections",         "unit": "1",    "type": "Gauge"},
    {"name": "db.connections.max",            "description": "Max DB connections",          "unit": "1",    "type": "Gauge"},
    {"name": "db.replication.lag",            "description": "DB replication lag",           "unit": "ms",   "type": "Gauge"},
    {"name": "db.deadlocks",                  "description": "Database deadlocks",          "unit": "1",    "type": "Sum"},
    {"name": "db.rows.read",                  "description": "Rows read per interval",      "unit": "1",    "type": "Sum"},
    {"name": "db.rows.written",               "description": "Rows written per interval",   "unit": "1",    "type": "Sum"},

    # Business metrics
    {"name": "snkrs.cart.abandonment_rate",   "description": "Cart abandonment rate",       "unit": "%",    "type": "Gauge"},
    {"name": "snkrs.conversion_rate",         "description": "Checkout conversion rate",    "unit": "%",    "type": "Gauge"},
    {"name": "snkrs.revenue",                 "description": "Revenue per interval",        "unit": "USD",  "type": "Sum"},
    {"name": "snkrs.draw.entries",            "description": "Draw entries submitted",      "unit": "1",    "type": "Sum"},
    {"name": "snkrs.draw.winners",            "description": "Draw winners selected",       "unit": "1",    "type": "Sum"},
    {"name": "snkrs.notifications.sent",      "description": "Notifications sent",          "unit": "1",    "type": "Sum"},
    {"name": "snkrs.notifications.failed",    "description": "Notifications failed",        "unit": "1",    "type": "Sum"},
    {"name": "snkrs.bot.detections",          "description": "Bot detections",              "unit": "1",    "type": "Sum"},
    {"name": "snkrs.fraud.blocks",            "description": "Fraud blocked transactions",  "unit": "1",    "type": "Sum"},
    {"name": "snkrs.search.queries",          "description": "Search queries",              "unit": "1",    "type": "Sum"},
    {"name": "snkrs.wishlist.additions",      "description": "Wishlist additions",          "unit": "1",    "type": "Sum"},
]

ENDPOINTS = ["/v1/products/{id}", "/v1/draw/enter", "/v1/checkout",
             "/v1/account/profile", "/v1/feed", "/v1/search",
             "/v1/wishlist", "/v1/recommendations", "/v1/returns",
             "/v1/orders/{id}", "/v1/cart"]
SERVICES = ["api-gateway", "auth-service", "product-service", "draw-service",
            "inventory-service", "payment-service", "order-service", "notification-service",
            "search-service", "recommendation-service", "shipping-service",
            "feed-service", "wishlist-service", "analytics-service"]
REGIONS = ["us-east", "us-west", "eu-west", "ap-southeast", "ap-northeast"]
REGION_WEIGHTS = {"us-east": 0.40, "us-west": 0.25, "eu-west": 0.20,
                  "ap-southeast": 0.10, "ap-northeast": 0.05}
AZS = {
    "us-east": ["us-east-1a", "us-east-1b", "us-east-1c"],
    "us-west": ["us-west-2a", "us-west-2b"],
    "eu-west": ["eu-west-1a", "eu-west-1b"],
    "ap-southeast": ["ap-southeast-1a"],
    "ap-northeast": ["ap-northeast-1a"],
}
DEVICES = ["ios", "android", "web"]
DEVICE_WEIGHTS = {"ios": 0.55, "android": 0.30, "web": 0.15}
STATUSES = ["200", "201", "400", "401", "402", "403", "404", "410", "429", "500", "503"]
ERROR_TYPES = ["timeout", "connection_pool", "validation", "auth", "inventory",
               "payment", "fraud", "rate_limit", "serialization", "dns"]
PAYMENT_STATUSES = ["success", "failed", "fraud_blocked"]

# Per-service base latencies
SERVICE_LATENCIES = {
    "api-gateway": 50, "auth-service": 30, "product-service": 40,
    "draw-service": 60, "inventory-service": 35, "payment-service": 250,
    "order-service": 80, "notification-service": 120, "search-service": 65,
    "recommendation-service": 90, "shipping-service": 100, "feed-service": 35,
    "wishlist-service": 25, "analytics-service": 15,
}

# Pod name cache per service (simulates k8s deployment)
_pod_names = {}


def _get_pod_name(service: str, replica_idx: int, rng: random.Random) -> str:
    """Generate a stable pod name for a service replica."""
    key = (service, replica_idx)
    if key not in _pod_names:
        suffix = f"{rng.randint(0, 999):03d}-{chr(rng.randint(97, 102))}{rng.randint(0, 9)}{chr(rng.randint(97, 102))}{rng.randint(0, 9)}{chr(rng.randint(97, 102))}"
        _pod_names[key] = f"{service}-{suffix}"
    return _pod_names[key]


def _make_resource_attrs(service: str, region: str = "us-east-1",
                          pod_name: str = None, az: str = None,
                          instance_id: str = None) -> dict:
    """Create per-service resource attributes (replaces single global RESOURCE_ATTRS)."""
    attrs = {
        "service.name": service,
        "service.version": "4.2.1",
        "deployment.environment": "production",
        "cloud.provider": "aws",
        "cloud.region": region,
    }
    if pod_name:
        attrs["k8s.pod.name"] = pod_name
        attrs["k8s.namespace"] = "snkrs-platform"
    if az:
        attrs["cloud.availability_zone"] = az
    if instance_id:
        attrs["host.id"] = instance_id
    return attrs


def generate_metrics_for_interval(ts: datetime, phase: str, drop_product: dict,
                                  stock_remaining: int, queue_depth: int,
                                  active_users: int, span_stats: dict = None,
                                  incident: dict = None,
                                  rng: random.Random = None) -> list[dict]:
    """Generate metric data points for a single 10s interval."""
    r = rng or random
    metrics = []

    is_drop = phase == "drop_live"
    is_pre = phase == "pre_drop"

    base_rate = 5 if is_pre else (200 if is_drop else 15)

    # ── snkrs.requests — per endpoint/status/region ──────────
    for endpoint in ENDPOINTS:
        for region in REGIONS:
            region_weight = REGION_WEIGHTS[region]
            success_count = max(0, int(base_rate * region_weight * r.uniform(0.8, 1.2)))

            if success_count > 0:
                status = "201" if endpoint.startswith("POST") else "200"
                metrics.append(_make_metric(
                    ts, "snkrs.requests", "Total API requests", "1", "Sum",
                    float(success_count),
                    {"endpoint": endpoint, "status": status, "region": region},
                    service="api-gateway", region=region, rng=r))

            err_rate = 0.02 if not is_drop else 0.08
            if incident:
                if endpoint in incident.get("affected_endpoints", []):
                    err_rate = incident["error_rate_override"]
            err_count = max(0, int(success_count * err_rate * r.uniform(0.5, 2.0)))
            if err_count > 0:
                err_status = r.choice(["500", "503", "429", "402"])
                metrics.append(_make_metric(
                    ts, "snkrs.requests", "Total API requests", "1", "Sum",
                    float(err_count),
                    {"endpoint": endpoint, "status": err_status, "region": region},
                    service="api-gateway", region=region, rng=r))

    # ── snkrs.inventory — per product/size ──────────
    if drop_product:
        from inventory import SIZES, SIZE_WEIGHTS
        for size in SIZES:
            weight = SIZE_WEIGHTS[size]
            size_stock = max(0, int(stock_remaining * weight * r.uniform(0.8, 1.2)))
            metrics.append(_make_metric(
                ts, "snkrs.inventory", "Remaining inventory", "1", "Gauge",
                float(size_stock),
                {"product.id": drop_product["id"], "product.sku": drop_product["sku"], "shoe.size": size},
                service="inventory-service", rng=r))

    # ── snkrs.queue_depth ──────────
    metrics.append(_make_metric(
        ts, "snkrs.queue_depth", "Draw queue depth", "1", "Gauge",
        float(queue_depth),
        {"product.id": drop_product["id"] if drop_product else "none"},
        service="draw-service", rng=r))

    # ── snkrs.errors — per endpoint/error_type ──────────
    for endpoint in ENDPOINTS[:5]:  # Primary endpoints
        for err_type in ERROR_TYPES:
            if err_type == "payment" and "checkout" not in endpoint:
                continue
            if err_type == "inventory" and "checkout" not in endpoint:
                continue
            if err_type == "fraud" and "checkout" not in endpoint:
                continue

            err_base = 0 if is_pre else (r.randint(0, 5) if is_drop else r.randint(0, 1))
            if incident and endpoint in incident.get("affected_endpoints", []):
                err_base = int(err_base * 3 * r.uniform(1, 2))

            if err_base > 0:
                metrics.append(_make_metric(
                    ts, "snkrs.errors", "Total errors", "1", "Sum",
                    float(err_base),
                    {"endpoint": endpoint, "error_type": err_type},
                    service="api-gateway", rng=r))

    # ── snkrs.payments — per status ──────────
    if is_drop or (not is_pre and r.random() < 0.3):
        for pay_status in PAYMENT_STATUSES:
            if pay_status == "success":
                count = int(base_rate * 0.15 * r.uniform(0.8, 1.2))
            elif pay_status == "failed":
                count = int(base_rate * 0.03 * r.uniform(0.5, 2.0))
            else:
                count = int(base_rate * 0.01 * r.uniform(0.5, 2.0))

            if incident and incident.get("affected_service") == "payment-service":
                if pay_status == "failed":
                    count = int(count * 5)

            if count > 0:
                metrics.append(_make_metric(
                    ts, "snkrs.payments", "Payment attempts", "1", "Sum",
                    float(count),
                    {"status": pay_status, "product.id": drop_product["id"] if drop_product else "none"},
                    service="payment-service", rng=r))

    # ── snkrs.latency_p99 — per service ──────────
    for service in SERVICES:
        base_latency = SERVICE_LATENCIES.get(service, 50)
        mult = 1.0
        if is_drop:
            mult = r.uniform(1.5, 3.0)
        if incident:
            if incident.get("affected_service") in ("all", service):
                mult *= incident["latency_multiplier"]

        latency = base_latency * mult * r.uniform(0.8, 1.2)
        metrics.append(_make_metric(
            ts, "snkrs.latency_p99", "P99 latency per service", "ms", "Gauge",
            round(latency, 1),
            {"service.name": service},
            service=service, rng=r))

    # ── snkrs.active_users — per region/device ──────────
    for region in REGIONS:
        for device in DEVICES:
            region_weight = REGION_WEIGHTS[region]
            device_weight = DEVICE_WEIGHTS[device]
            users = max(0, int(active_users * region_weight * device_weight * r.uniform(0.7, 1.3)))
            if incident and incident.get("affected_region") == region:
                users = 0
            if users > 0:
                metrics.append(_make_metric(
                    ts, "snkrs.active_users", "Active users", "1", "Gauge",
                    float(users),
                    {"region": region, "device": device},
                    service="api-gateway", rng=r))

    # ── Infrastructure metrics — per service/pod ──────────
    for service in SERVICES:
        n_replicas = r.randint(2, 5)
        for replica in range(n_replicas):
            pod = _get_pod_name(service, replica, r)
            region = r.choice(REGIONS)
            az = r.choice(AZS[region])

            # CPU utilization
            cpu_base = 20 if not is_drop else 65
            if incident and incident.get("affected_service") in ("all", service):
                cpu_base = min(99, cpu_base * 2)
            cpu = min(100, cpu_base * r.uniform(0.6, 1.4))
            metrics.append(_make_metric(
                ts, "system.cpu.utilization", "CPU utilization", "%", "Gauge",
                round(cpu, 1),
                {"k8s.pod.name": pod, "cloud.availability_zone": az},
                service=service, region=region, rng=r))

            # Memory usage
            mem_limit = r.choice([512, 1024, 2048]) * 1024 * 1024  # bytes
            mem_pct = 0.4 if not is_drop else 0.7
            if incident and incident.get("affected_service") in ("all", service):
                mem_pct = min(0.95, mem_pct * 1.5)
            mem_used = int(mem_limit * mem_pct * r.uniform(0.8, 1.1))
            metrics.append(_make_metric(
                ts, "system.memory.usage", "Memory usage", "By", "Gauge",
                float(mem_used),
                {"k8s.pod.name": pod, "cloud.availability_zone": az},
                service=service, region=region, rng=r))

            # GC duration (JVM services)
            if r.random() < 0.3:  # ~30% of intervals have GC
                gc_ms = r.uniform(5, 50)
                if incident and incident.get("affected_service") in ("all", service):
                    gc_ms *= 3
                metrics.append(_make_metric(
                    ts, "runtime.jvm.gc.duration", "GC pause duration", "ms", "Gauge",
                    round(gc_ms, 1),
                    {"k8s.pod.name": pod, "gc.type": r.choice(["young", "old", "full"])},
                    service=service, region=region, rng=r))

            # Thread/goroutine count
            threads = r.randint(20, 100) if not is_drop else r.randint(50, 200)
            metrics.append(_make_metric(
                ts, "runtime.jvm.threads.count", "Active thread count", "1", "Gauge",
                float(threads),
                {"k8s.pod.name": pod},
                service=service, region=region, rng=r))

    # ── Network metrics — per service ──────────
    for service in SERVICES[:8]:  # Core services
        # DNS lookup duration
        dns_ms = r.uniform(0.5, 5)
        if incident and incident.get("name", "").startswith("dns"):
            dns_ms = r.uniform(100, 5000)
        metrics.append(_make_metric(
            ts, "dns.lookup.duration", "DNS resolution time", "ms", "Gauge",
            round(dns_ms, 2),
            {"dns.target": f"{service}.snkrs-platform.svc.cluster.local"},
            service=service, rng=r))

        # Active connections
        conns = r.randint(10, 50) if not is_drop else r.randint(30, 150)
        metrics.append(_make_metric(
            ts, "net.connections.active", "Active network connections", "1", "Gauge",
            float(conns), {},
            service=service, rng=r))

    # ── Cache metrics — for services with Redis ──────────
    cache_services = ["auth-service", "product-service", "inventory-service",
                      "draw-service", "feed-service", "search-service"]
    for service in cache_services:
        hit_rate = r.uniform(85, 99) if not is_drop else r.uniform(60, 95)
        if incident and incident.get("affected_service") in ("all", service):
            hit_rate = r.uniform(20, 60)
        metrics.append(_make_metric(
            ts, "cache.hit_rate", "Cache hit rate", "%", "Gauge",
            round(hit_rate, 1),
            {"cache.backend": "redis"},
            service=service, rng=r))

        evictions = 0 if not is_drop else r.randint(0, 50)
        if incident and "eviction" in incident.get("name", ""):
            evictions = r.randint(100, 1000)
        if evictions > 0:
            metrics.append(_make_metric(
                ts, "cache.evictions", "Cache evictions", "1", "Sum",
                float(evictions),
                {"cache.backend": "redis"},
                service=service, rng=r))

        cache_latency = r.uniform(0.5, 3) if not is_drop else r.uniform(1, 10)
        metrics.append(_make_metric(
            ts, "cache.latency.avg", "Cache operation latency", "ms", "Gauge",
            round(cache_latency, 2),
            {"cache.backend": "redis"},
            service=service, rng=r))

    # ── Queue metrics ──────────
    kafka_topics = ["snkrs.draw.entries", "snkrs.orders.created", "snkrs.notifications",
                    "snkrs.analytics.events", "snkrs.inventory.updates"]
    for topic in kafka_topics:
        lag = r.randint(0, 10) if not is_drop else r.randint(0, 5000)
        if incident and "kafka" in incident.get("name", ""):
            lag = r.randint(10000, 100000)
        metrics.append(_make_metric(
            ts, "messaging.kafka.consumer.lag", "Kafka consumer lag", "1", "Gauge",
            float(lag),
            {"messaging.destination": topic, "messaging.kafka.consumer.group": f"{topic.split('.')[-1]}-consumer"},
            service="queue-service", rng=r))

    # DLQ depth
    dlq_depth = 0 if not is_drop else r.randint(0, 10)
    if incident:
        dlq_depth = r.randint(5, 100)
    if dlq_depth > 0:
        metrics.append(_make_metric(
            ts, "messaging.dlq.depth", "Dead letter queue depth", "1", "Gauge",
            float(dlq_depth),
            {"messaging.destination": "snkrs.dlq"},
            service="queue-service", rng=r))

    # ── Database metrics — per service ──────────
    db_services = ["auth-service", "inventory-service", "payment-service",
                   "order-service", "draw-service"]
    for service in db_services:
        base_p50 = SERVICE_LATENCIES.get(service, 50) * 0.3
        base_p99 = SERVICE_LATENCIES.get(service, 50) * 1.5

        mult = 1.0 if not is_drop else r.uniform(1.2, 2.5)
        if incident and incident.get("affected_service") in ("all", service):
            mult *= incident["latency_multiplier"] * 0.5

        metrics.append(_make_metric(
            ts, "db.query.duration.p50", "DB query p50 latency", "ms", "Gauge",
            round(base_p50 * mult * r.uniform(0.8, 1.2), 1),
            {"db.system": "postgresql"},
            service=service, rng=r))
        metrics.append(_make_metric(
            ts, "db.query.duration.p99", "DB query p99 latency", "ms", "Gauge",
            round(base_p99 * mult * r.uniform(0.8, 1.2), 1),
            {"db.system": "postgresql"},
            service=service, rng=r))

        # Connection pool
        max_conn = r.choice([20, 30, 50])
        active = r.randint(3, max_conn // 2) if not is_drop else r.randint(max_conn // 2, max_conn)
        if incident and incident.get("affected_service") in ("all", service):
            active = min(max_conn, int(active * 1.8))
        idle = max(0, max_conn - active - r.randint(0, 5))

        metrics.append(_make_metric(
            ts, "db.connections.active", "Active DB connections", "1", "Gauge",
            float(active), {"db.system": "postgresql"},
            service=service, rng=r))
        metrics.append(_make_metric(
            ts, "db.connections.idle", "Idle DB connections", "1", "Gauge",
            float(idle), {"db.system": "postgresql"},
            service=service, rng=r))

        # Replication lag
        rep_lag = r.uniform(0.1, 5) if not is_drop else r.uniform(1, 50)
        if incident and "database" in incident.get("name", ""):
            rep_lag = r.uniform(100, 5000)
        metrics.append(_make_metric(
            ts, "db.replication.lag", "DB replication lag", "ms", "Gauge",
            round(rep_lag, 1), {"db.system": "postgresql"},
            service=service, rng=r))

        # Deadlocks (rare)
        if is_drop and r.random() < 0.05:
            metrics.append(_make_metric(
                ts, "db.deadlocks", "Database deadlocks", "1", "Sum",
                float(r.randint(1, 3)), {"db.system": "postgresql"},
                service=service, rng=r))

    # ── Business metrics ──────────
    if is_drop:
        # Cart abandonment rate
        abandonment = r.uniform(30, 70)
        metrics.append(_make_metric(
            ts, "snkrs.cart.abandonment_rate", "Cart abandonment rate", "%", "Gauge",
            round(abandonment, 1), {},
            service="order-service", rng=r))

        # Conversion rate
        conversion = r.uniform(2, 15)
        metrics.append(_make_metric(
            ts, "snkrs.conversion_rate", "Checkout conversion rate", "%", "Gauge",
            round(conversion, 1), {},
            service="order-service", rng=r))

        # Revenue
        avg_price = drop_product["price"] if drop_product else 150
        revenue = base_rate * 0.1 * avg_price * r.uniform(0.5, 1.5)
        metrics.append(_make_metric(
            ts, "snkrs.revenue", "Revenue per interval", "USD", "Sum",
            round(revenue, 2), {},
            service="payment-service", rng=r))

        # Draw entries
        draw_entries = int(base_rate * 0.4 * r.uniform(0.5, 1.5))
        metrics.append(_make_metric(
            ts, "snkrs.draw.entries", "Draw entries submitted", "1", "Sum",
            float(draw_entries), {},
            service="draw-service", rng=r))

        # Bot detections
        bot_count = r.randint(0, int(base_rate * 0.05))
        if incident and "bot" in incident.get("name", ""):
            bot_count = r.randint(10, 100)
        if bot_count > 0:
            metrics.append(_make_metric(
                ts, "snkrs.bot.detections", "Bot detections", "1", "Sum",
                float(bot_count), {},
                service="auth-service", rng=r))

        # Fraud blocks
        fraud_count = r.randint(0, int(base_rate * 0.02))
        if fraud_count > 0:
            metrics.append(_make_metric(
                ts, "snkrs.fraud.blocks", "Fraud blocked transactions", "1", "Sum",
                float(fraud_count), {},
                service="payment-service", rng=r))

    # Search queries (always active)
    search_count = int(base_rate * 0.3 * r.uniform(0.5, 1.5))
    if search_count > 0:
        metrics.append(_make_metric(
            ts, "snkrs.search.queries", "Search queries", "1", "Sum",
            float(search_count), {},
            service="search-service", rng=r))

    # Notifications
    if is_drop:
        notif_sent = r.randint(1, int(base_rate * 0.2))
        notif_failed = r.randint(0, max(1, notif_sent // 20))
        metrics.append(_make_metric(
            ts, "snkrs.notifications.sent", "Notifications sent", "1", "Sum",
            float(notif_sent), {},
            service="notification-service", rng=r))
        if notif_failed > 0:
            metrics.append(_make_metric(
                ts, "snkrs.notifications.failed", "Notifications failed", "1", "Sum",
                float(notif_failed), {},
                service="notification-service", rng=r))

    return metrics


def _make_metric(ts: datetime, name: str, description: str, unit: str,
                 metric_type: str, value: float, attrs: dict,
                 service: str = "snkrs-platform", region: str = None,
                 rng: random.Random = None) -> dict:
    """Create a single metric row with per-service resource identity."""
    r = rng or random
    resource_attrs = _make_resource_attrs(service, region or "us-east-1")

    return {
        "Timestamp": ts,
        "MetricName": name,
        "MetricDescription": description,
        "MetricUnit": unit,
        "MetricType": metric_type,
        "Value": value,
        "ServiceName": service,
        "ResourceAttributes": resource_attrs,
        "MetricAttributes": {k: str(v) for k, v in attrs.items()},
    }
