"""
Correlated log record generator — produces log records linked to span trees
via TraceId/SpanId.
"""

import random
from datetime import datetime, timedelta

from corpus_loader import get_log_templates

# Severity mapping matching ClickHouse schema
SEVERITY_MAP = {
    "TRACE": 1,
    "DEBUG": 5,
    "INFO": 9,
    "WARN": 13,
    "ERROR": 17,
    "FATAL": 21,
}

# Hardcoded fallback (original 54 templates)
_FALLBACK_TEMPLATES = {
    "auth-service": {
        "DEBUG": [
            "JWT token validated for user {user_id}, expires in {ttl}s",
            "Session cache lookup key=session:{user_id} hit={cache_hit}",
            "Account standing check passed for user {user_id}",
            "Redis connection pool: active={active} idle={idle}",
        ],
        "INFO": [
            "Authentication successful user={user_id} device={device} method=jwt",
            "Session refreshed for user {user_id}, new TTL=3600s",
            "User {user_id} account verified, standing=active loyalty={loyalty}",
        ],
        "WARN": [
            "Elevated bot score detected user={user_id} score={fraud_score:.1f} threshold=70",
            "Session cache miss rate elevated: {miss_rate:.1f}% (threshold: 20%)",
            "Rate limit approaching for user {user_id}: {rate}/100 requests in window",
        ],
        "ERROR": [
            "Account flagged: suspected bot activity user={user_id} fingerprint_mismatch=true",
            "JWT validation failed: token expired user={user_id}",
            "Database connection pool exhausted: {active}/{max_conn} connections in use",
            "Account suspended user={user_id} reason=terms_violation",
        ],
    },
    "product-service": {
        "DEBUG": ["Product cache hit sku={sku} ttl_remaining={ttl}s"],
        "INFO": ["Product details served sku={sku} price=${price} stock={stock}"],
        "WARN": ["CDN response slow for {product_id}: {latency}ms (threshold: 500ms)"],
        "ERROR": ["CDN timeout: upstream provider returned 504 after 30s product={product_id}"],
    },
    "inventory-service": {
        "DEBUG": ["Stock check sku={sku} size={size} remaining={stock}"],
        "INFO": ["Inventory reserved user={user_id} sku={sku} size={size} remaining={stock}"],
        "WARN": ["Low stock alert sku={sku} size={size} remaining={stock} threshold=10"],
        "ERROR": ["SOLD OUT: No inventory remaining sku={sku} size={size}"],
    },
    "payment-service": {
        "DEBUG": ["Stripe charge initiated amount=${amount:.2f} currency=USD"],
        "INFO": ["Payment processed user={user_id} amount=${amount:.2f} method={payment_method} provider=stripe"],
        "WARN": ["Fraud score elevated user={user_id} score={fraud_score:.1f} (threshold=85)"],
        "ERROR": ["Stripe API timeout after 30s user={user_id} amount=${amount:.2f}"],
    },
    "draw-service": {
        "DEBUG": ["Dedup check user={user_id} sku={sku} result=unique"],
        "INFO": ["Draw entry submitted user={user_id} sku={sku} size={size}"],
        "WARN": ["Draw queue depth elevated: {queue_depth} entries (threshold: 5000)"],
        "ERROR": ["Duplicate draw entry detected user={user_id} sku={sku}"],
    },
    "order-service": {
        "DEBUG": ["Order record created user={user_id} sku={sku} size={size}"],
        "INFO": ["Order created user={user_id} sku={sku} size={size} amount=${amount:.2f} order_id={order_id}"],
        "WARN": ["Order processing delayed: upstream payment confirmation pending for {user_id}"],
        "ERROR": ["Order creation failed: database constraint violation user={user_id}"],
    },
    "notification-service": {
        "DEBUG": ["APNS push payload constructed for user={user_id} type={notif_type}"],
        "INFO": ["Notification sent user={user_id} channel=push+email type={notif_type}"],
        "WARN": ["APNS delivery delayed: retry in 5s user={user_id}"],
        "ERROR": ["APNS delivery failed user={user_id}: device token expired"],
    },
    "api-gateway": {
        "DEBUG": ["Request routed {method} {route} → {service}"],
        "INFO": ["{method} {route} completed status={status_code} latency={latency}ms user={user_id}"],
        "WARN": ["Request latency exceeded SLO: {method} {route} took {latency}ms (SLO: 1000ms)"],
        "ERROR": ["Request failed {method} {route} status={status_code} error={error_msg}"],
    },
}

# Load from corpus (falls back to hardcoded)
LOG_TEMPLATES = get_log_templates(fallback=_FALLBACK_TEMPLATES)


def generate_logs_for_spans(spans: list[dict], user: dict, product: dict = None,
                            rng: random.Random = None) -> list[dict]:
    """Generate correlated log records for a span tree.

    Distribution: ~60% DEBUG, 25% INFO, 10% WARN, 5% ERROR.
    Error logs correlate with failed spans.
    """
    r = rng or random
    logs = []

    for span in spans:
        service = span["ServiceName"]
        trace_id = span["TraceId"]
        span_id = span["SpanId"]
        is_error = span["StatusCode"] == "ERROR"
        ts = span["Timestamp"]
        attrs = span.get("SpanAttributes", {})

        templates = LOG_TEMPLATES.get(service)
        if not templates:
            continue

        # Always generate at least one log per span
        # Error spans always get an ERROR log
        if is_error and "ERROR" in templates:
            severity = "ERROR"
            template = r.choice(templates["ERROR"])
            logs.append(_make_log(trace_id, span_id, severity, template,
                                  ts, service, span, user, product, r))

        # Generate DEBUG/INFO logs based on distribution
        severity_roll = r.random()
        if severity_roll < 0.60:
            severity = "DEBUG"
        elif severity_roll < 0.85:
            severity = "INFO"
        elif severity_roll < 0.95:
            severity = "WARN"
        else:
            severity = "ERROR" if not is_error else "WARN"  # avoid double error

        if severity in templates:
            template = r.choice(templates[severity])
            logs.append(_make_log(trace_id, span_id, severity, template,
                                  ts, service, span, user, product, r))

    return logs


def _make_log(trace_id: str, span_id: str, severity: str, template: str,
              ts: datetime, service: str, span: dict, user: dict,
              product: dict = None, rng: random.Random = None) -> dict:
    """Create a single log record matching the otel_logs schema."""
    r = rng or random
    attrs = span.get("SpanAttributes", {})

    # Build template context — includes k8s/infrastructure fields
    svc_short = service.replace("-service", "").replace("-", "_")
    pod_suffix = f"{r.randint(0, 999):03d}-{r.choice('abcdef')}{r.choice('0123456789')}{r.choice('abcdef')}{r.choice('0123456789')}{r.choice('abcdef')}"
    ctx = {
        "user_id": user["user_id"],
        "device": user["device"],
        "loyalty": user["loyalty_tier"],
        "payment_method": user["payment_method"],
        "fraud_score": user["fraud_risk"] * 100 * r.uniform(0.5, 1.5),
        "cache_hit": attrs.get("cache.hit", "true"),
        "ttl": r.randint(300, 3600),
        "active": r.randint(10, 50),
        "idle": r.randint(1, 10),
        "max_conn": r.choice([20, 50, 100, 200]),
        "miss_rate": r.uniform(1, 40),
        "rate": r.randint(10, 95),
        "latency": r.randint(5, 2000),
        "timeout": r.randint(1000, 30000),
        "retries": r.randint(1, 5),
        "attempt": r.randint(1, 3),
        "queue_depth": r.randint(100, 10000),
        "queue_pos": r.randint(1, 5000),
        "partition": r.randint(0, 11),
        "consumed": r.randint(100, 400),
        "provisioned": 500,
        "tx_id": f"tx_{r.randint(100000, 999999)}",
        "order_id": f"ord_{r.randint(100000, 999999)}",
        "window_start": "10:00:00",
        "window_end": "10:15:00",
        "method": attrs.get("http.method", "GET"),
        "route": attrs.get("http.route", "/unknown"),
        "status_code": attrs.get("http.status_code", "200"),
        "service": service,
        "error_msg": span.get("StatusMessage", "unknown error"),
        "notif_type": attrs.get("notification.type", "order_confirmation"),
        "email": user.get("email", "user@example.com"),
        # Infrastructure/k8s context
        "pod_name": f"{service}-{pod_suffix}",
        "container_id": f"{r.randint(0, 0xffffffffffff):012x}",
        "az": r.choice(["us-east-1a", "us-east-1b", "us-east-1c", "us-west-2a", "us-west-2b",
                         "eu-west-1a", "eu-west-1b", "ap-southeast-1a"]),
        "node_name": f"ip-10-{r.randint(0,255)}-{r.randint(0,255)}-{r.randint(0,255)}.ec2.internal",
        "namespace": "snkrs-platform",
        "request_id": f"req_{r.randint(100000000, 999999999)}",
        "correlation_id": span["TraceId"][:16],
        "thread_name": f"{svc_short}-worker-{r.randint(0, 31)}",
        "heap_used_mb": r.randint(128, 1800),
        "gc_pause_ms": round(r.uniform(0.5, 500), 1),
        "query_duration_ms": round(r.uniform(0.1, 2000), 1),
        "service_version": span.get("ResourceAttributes", {}).get("service.version", "4.2.1"),
    }

    if product:
        ctx.update({
            "sku": product["sku"],
            "product_id": product["id"],
            "price": product["price"],
            "size": user["shoe_size"],
            "stock": r.randint(0, product["base_stock"]),
            "amount": product["price"] + r.uniform(0, 30),
        })
    else:
        ctx.update({
            "sku": "unknown",
            "product_id": "unknown",
            "price": 0,
            "size": user["shoe_size"],
            "stock": 0,
            "amount": 0,
        })

    try:
        body = template.format(**ctx)
    except (KeyError, ValueError):
        body = template  # fallback if template has unknown keys

    # Offset log timestamp slightly within the span
    log_offset_ns = r.randint(0, max(1, span["Duration"] // 2))
    log_ts = ts + timedelta(microseconds=log_offset_ns // 1000)

    resource_attrs = {
        "service.name": service,
        "service.version": "4.2.1",
        "deployment.environment": "production",
        "cloud.provider": "aws",
        "cloud.region": user["region"],
    }

    return {
        "Timestamp": log_ts,
        "TraceId": trace_id,
        "SpanId": span_id,
        "SeverityNumber": SEVERITY_MAP[severity],
        "SeverityText": severity,
        "Body": body,
        "ServiceName": service,
        "ResourceAttributes": resource_attrs,
        "LogAttributes": {
            "user.id": user["user_id"],
            "log.source": f"{service}.handler",
            "k8s.pod.name": ctx["pod_name"],
            "k8s.namespace": ctx["namespace"],
            "k8s.node.name": ctx["node_name"],
            "request.id": ctx["request_id"],
            "thread.name": ctx["thread_name"],
        },
    }
