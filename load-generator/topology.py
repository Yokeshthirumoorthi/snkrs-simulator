"""
Probabilistic service topology — dynamic trace tree generation via
graph traversal with probabilistic branching.

Replaces the 5 fixed trace topologies with 1000s of possible trace shapes
based on cache hits/misses, circuit breakers, retries, fan-out, and async calls.
"""

import random
import uuid
from datetime import datetime, timedelta

from distributions import duration_ns
from patterns import should_apply_incident
from inventory import StockTracker

# ── Service dependency graph ─────────────────────────────────────────────

# Each node defines:
#   - children: list of (child_name, edge_config) tuples
#   - edge_config can include:
#       probability: 0.0-1.0 chance this edge is followed
#       condition: "cache_miss", "auth_ok", "stock_available", "payment_ok", etc.
#       is_async: True for fire-and-forget (doesn't add to parent duration)
#       retry_on_fail: max retries
#       circuit_breaker: True to skip on high error rates

SERVICE_GRAPH = {
    # ── API Gateway (entry point) ────────────────────────────────────
    "api-gateway": {
        "span_kind": "SERVER",
        "latency_base_ms": 2,
        "db_system": None,
        "children": {
            "auth-service:validate_request": {"probability": 0.95},
        },
    },

    # ── Auth Service ─────────────────────────────────────────────────
    "auth-service:validate_request": {
        "span_kind": "INTERNAL",
        "latency_base_ms": 3,
        "children": {
            "auth-service:verify_jwt": {"probability": 1.0},
            "auth-service:redis.get[session]": {"probability": 1.0},
            "auth-service:check_account_standing": {"probability": 1.0},
        },
    },
    "auth-service:verify_jwt": {
        "span_kind": "INTERNAL",
        "latency_base_ms": 8,
        "span_name_hint": "verify_jwt",
        "children": {},
    },
    "auth-service:redis.get[session]": {
        "span_kind": "CLIENT",
        "latency_base_ms": 3,
        "db_system": "redis",
        "span_name_hint": "redis.get",
        "children": {
            "auth-service:postgres.query[users]": {"condition": "cache_miss", "cache_miss_rate": 0.15},
        },
    },
    "auth-service:postgres.query[users]": {
        "span_kind": "CLIENT",
        "latency_base_ms": 25,
        "db_system": "postgresql",
        "span_name_hint": "postgres.query",
        "children": {},
    },
    "auth-service:check_account_standing": {
        "span_kind": "INTERNAL",
        "latency_base_ms": 3,
        "span_name_hint": "check_account_standing",
        "error_rate": 0.02,
        "children": {},
    },

    # ── Product Service ──────────────────────────────────────────────
    "product-service:get_product": {
        "span_kind": "INTERNAL",
        "latency_base_ms": 5,
        "children": {
            "product-service:redis.get[product_cache]": {"probability": 1.0},
            "product-service:cdn.get[images]": {"probability": 0.85},
            "inventory-service:get_stock": {"probability": 0.90},
            "recommendation-service:get_similar": {"probability": 0.20, "is_async": True},
        },
    },
    "product-service:redis.get[product_cache]": {
        "span_kind": "CLIENT",
        "latency_base_ms": 2,
        "db_system": "redis",
        "span_name_hint": "redis.get",
        "children": {
            "product-service:postgres.query[products]": {"condition": "cache_miss", "cache_miss_rate": 0.10},
        },
    },
    "product-service:postgres.query[products]": {
        "span_kind": "CLIENT",
        "latency_base_ms": 20,
        "db_system": "postgresql",
        "span_name_hint": "postgres.query",
        "children": {},
    },
    "product-service:cdn.get[images]": {
        "span_kind": "CLIENT",
        "latency_base_ms": 12,
        "span_name_hint": "cdn.get",
        "children": {},
    },
    "inventory-service:get_stock": {
        "span_kind": "INTERNAL",
        "latency_base_ms": 8,
        "span_name_hint": "redis.get",
        "children": {},
    },
    "recommendation-service:get_similar": {
        "span_kind": "INTERNAL",
        "latency_base_ms": 40,
        "children": {
            "recommendation-service:redis.get[user_prefs]": {"probability": 1.0},
            "recommendation-service:elasticsearch.query": {"probability": 0.70},
        },
    },
    "recommendation-service:redis.get[user_prefs]": {
        "span_kind": "CLIENT",
        "latency_base_ms": 3,
        "db_system": "redis",
        "span_name_hint": "redis.get",
        "children": {},
    },
    "recommendation-service:elasticsearch.query": {
        "span_kind": "CLIENT",
        "latency_base_ms": 30,
        "db_system": "elasticsearch",
        "children": {},
    },

    # ── Draw Service ─────────────────────────────────────────────────
    "draw-service:enter_draw": {
        "span_kind": "INTERNAL",
        "latency_base_ms": 5,
        "children": {
            "draw-service:validate_entry_window": {"probability": 1.0},
            "draw-service:redis.get[entry_dedup]": {"probability": 1.0},
            "draw-service:kafka.produce[draw_entries]": {"condition": "no_dedup_fail"},
            "draw-service:dynamodb.put[draw_entries]": {"condition": "no_dedup_fail"},
        },
    },
    "draw-service:validate_entry_window": {
        "span_kind": "INTERNAL",
        "latency_base_ms": 3,
        "span_name_hint": "validate_entry_window",
        "children": {},
    },
    "draw-service:redis.get[entry_dedup]": {
        "span_kind": "CLIENT",
        "latency_base_ms": 4,
        "db_system": "redis",
        "span_name_hint": "redis.get",
        "error_rate": 0.03,  # 3% duplicate detection
        "children": {},
    },
    "draw-service:kafka.produce[draw_entries]": {
        "span_kind": "PRODUCER",
        "latency_base_ms": 15,
        "span_name_hint": "kafka.produce",
        "children": {},
    },
    "draw-service:dynamodb.put[draw_entries]": {
        "span_kind": "CLIENT",
        "latency_base_ms": 20,
        "db_system": "dynamodb",
        "span_name_hint": "dynamodb.put",
        "error_rate": 0.005,
        "children": {},
    },

    # ── Inventory Reserve ────────────────────────────────────────────
    "inventory-service:reserve_pair": {
        "span_kind": "INTERNAL",
        "latency_base_ms": 5,
        "children": {
            "inventory-service:redis.decr[stock]": {"probability": 1.0},
            "inventory-service:postgres.update[inventory]": {"condition": "stock_available"},
        },
    },
    "inventory-service:redis.decr[stock]": {
        "span_kind": "CLIENT",
        "latency_base_ms": 8,
        "db_system": "redis",
        "span_name_hint": "redis.decr",
        "children": {},
    },
    "inventory-service:postgres.update[inventory]": {
        "span_kind": "CLIENT",
        "latency_base_ms": 25,
        "db_system": "postgresql",
        "span_name_hint": "postgres.update",
        "children": {},
    },

    # ── Payment Service ──────────────────────────────────────────────
    "payment-service:process_payment": {
        "span_kind": "INTERNAL",
        "latency_base_ms": 5,
        "children": {
            "payment-service:validate_payment_method": {"probability": 1.0},
            "payment-service:fraud.score_check": {"condition": "validation_ok"},
            "payment-service:stripe.charge": {"condition": "fraud_ok"},
            "payment-service:postgres.insert[transactions]": {"condition": "charge_ok"},
        },
    },
    "payment-service:validate_payment_method": {
        "span_kind": "INTERNAL",
        "latency_base_ms": 10,
        "span_name_hint": "validate_payment_method",
        "error_rate": 0.03,
        "children": {},
    },
    "payment-service:fraud.score_check": {
        "span_kind": "INTERNAL",
        "latency_base_ms": 35,
        "span_name_hint": "fraud.score_check",
        "children": {},
    },
    "payment-service:stripe.charge": {
        "span_kind": "CLIENT",
        "latency_base_ms": 200,
        "span_name_hint": "stripe.charge",
        "error_rate": 0.03,
        "retry_on_fail": 2,
        "children": {},
    },
    "payment-service:postgres.insert[transactions]": {
        "span_kind": "CLIENT",
        "latency_base_ms": 15,
        "db_system": "postgresql",
        "span_name_hint": "postgres.insert",
        "children": {},
    },

    # ── Order Service ────────────────────────────────────────────────
    "order-service:create_order": {
        "span_kind": "INTERNAL",
        "latency_base_ms": 5,
        "children": {
            "order-service:postgres.insert[orders]": {"probability": 1.0},
            "order-service:kafka.produce[order_created]": {"probability": 1.0},
            "notification-service:send_confirmation": {"probability": 0.95, "is_async": True},
            "analytics-service:track_purchase": {"probability": 0.80, "is_async": True},
        },
    },
    "order-service:postgres.insert[orders]": {
        "span_kind": "CLIENT",
        "latency_base_ms": 20,
        "db_system": "postgresql",
        "span_name_hint": "postgres.insert",
        "children": {},
    },
    "order-service:kafka.produce[order_created]": {
        "span_kind": "PRODUCER",
        "latency_base_ms": 10,
        "span_name_hint": "kafka.produce",
        "children": {},
    },

    # ── Notification Service ─────────────────────────────────────────
    "notification-service:send_confirmation": {
        "span_kind": "INTERNAL",
        "latency_base_ms": 5,
        "children": {
            "notification-service:apns.push": {"probability": 0.90},
            "notification-service:ses.send_email": {"probability": 0.95},
            "notification-service:sms.send": {"probability": 0.15},
        },
    },
    "notification-service:apns.push": {
        "span_kind": "CLIENT",
        "latency_base_ms": 50,
        "span_name_hint": "apns.push",
        "error_rate": 0.01,
        "children": {},
    },
    "notification-service:ses.send_email": {
        "span_kind": "CLIENT",
        "latency_base_ms": 80,
        "span_name_hint": "ses.send_email",
        "children": {},
    },
    "notification-service:sms.send": {
        "span_kind": "CLIENT",
        "latency_base_ms": 120,
        "error_rate": 0.02,
        "children": {},
    },

    # ── Analytics Service (async) ────────────────────────────────────
    "analytics-service:track_purchase": {
        "span_kind": "INTERNAL",
        "latency_base_ms": 5,
        "children": {
            "analytics-service:kafka.produce[analytics]": {"probability": 1.0},
            "analytics-service:redis.set[realtime_stats]": {"probability": 0.70},
        },
    },
    "analytics-service:kafka.produce[analytics]": {
        "span_kind": "PRODUCER",
        "latency_base_ms": 8,
        "span_name_hint": "kafka.produce",
        "children": {},
    },
    "analytics-service:redis.set[realtime_stats]": {
        "span_kind": "CLIENT",
        "latency_base_ms": 3,
        "db_system": "redis",
        "span_name_hint": "redis.set",
        "children": {},
    },

    # ── Feed Service ─────────────────────────────────────────────────
    "feed-service:get_feed": {
        "span_kind": "INTERNAL",
        "latency_base_ms": 3,
        "children": {
            "feed-service:redis.get[feed_cache]": {"probability": 1.0},
            "feed-service:cdn.fetch[images]": {"probability": 0.85},
            "content-service:get_banners": {"probability": 0.40, "is_async": True},
        },
    },
    "feed-service:redis.get[feed_cache]": {
        "span_kind": "CLIENT",
        "latency_base_ms": 4,
        "db_system": "redis",
        "span_name_hint": "redis.get",
        "children": {
            "feed-service:postgres.query[feed_items]": {"condition": "cache_miss", "cache_miss_rate": 0.08},
        },
    },
    "feed-service:postgres.query[feed_items]": {
        "span_kind": "CLIENT",
        "latency_base_ms": 30,
        "db_system": "postgresql",
        "span_name_hint": "postgres.query",
        "children": {},
    },
    "feed-service:cdn.fetch[images]": {
        "span_kind": "CLIENT",
        "latency_base_ms": 30,
        "span_name_hint": "cdn.fetch",
        "children": {},
    },
    "content-service:get_banners": {
        "span_kind": "INTERNAL",
        "latency_base_ms": 15,
        "children": {
            "content-service:redis.get[banners]": {"probability": 1.0},
        },
    },
    "content-service:redis.get[banners]": {
        "span_kind": "CLIENT",
        "latency_base_ms": 2,
        "db_system": "redis",
        "span_name_hint": "redis.get",
        "children": {},
    },

    # ── Search Service ───────────────────────────────────────────────
    "search-service:search": {
        "span_kind": "INTERNAL",
        "latency_base_ms": 5,
        "children": {
            "search-service:elasticsearch.query": {"probability": 1.0},
            "search-service:redis.get[search_cache]": {"probability": 0.60},
            "search-service:redis.set[search_cache]": {"probability": 0.40},
        },
    },
    "search-service:elasticsearch.query": {
        "span_kind": "CLIENT",
        "latency_base_ms": 40,
        "db_system": "elasticsearch",
        "children": {},
    },
    "search-service:redis.get[search_cache]": {
        "span_kind": "CLIENT",
        "latency_base_ms": 2,
        "db_system": "redis",
        "span_name_hint": "redis.get",
        "children": {},
    },
    "search-service:redis.set[search_cache]": {
        "span_kind": "CLIENT",
        "latency_base_ms": 2,
        "db_system": "redis",
        "span_name_hint": "redis.set",
        "children": {},
    },

    # ── Wishlist Service ─────────────────────────────────────────────
    "wishlist-service:manage_wishlist": {
        "span_kind": "INTERNAL",
        "latency_base_ms": 3,
        "children": {
            "wishlist-service:redis.get[wishlist]": {"probability": 1.0},
            "wishlist-service:postgres.update[wishlist]": {"probability": 0.50},
        },
    },
    "wishlist-service:redis.get[wishlist]": {
        "span_kind": "CLIENT",
        "latency_base_ms": 3,
        "db_system": "redis",
        "span_name_hint": "redis.get",
        "children": {},
    },
    "wishlist-service:postgres.update[wishlist]": {
        "span_kind": "CLIENT",
        "latency_base_ms": 15,
        "db_system": "postgresql",
        "span_name_hint": "postgres.update",
        "children": {},
    },
}


# ── Flow definitions (endpoint → graph traversal root) ───────────────────

FLOW_ROOTS = {
    "GET /v1/products/{id}": [
        "auth-service:validate_request",
        "product-service:get_product",
    ],
    "POST /v1/draw/enter": [
        "auth-service:validate_request",
        "product-service:get_product",
        "draw-service:enter_draw",
    ],
    "POST /v1/checkout": [
        "auth-service:validate_request",
        "inventory-service:reserve_pair",
        "payment-service:process_payment",
        "order-service:create_order",
    ],
    "GET /v1/account/profile": [
        "auth-service:validate_request",
        "auth-service:postgres.query[users]",  # profile data
    ],
    "GET /v1/feed": [
        "feed-service:get_feed",
    ],
    "GET /v1/search": [
        "search-service:search",
    ],
    "POST /v1/wishlist": [
        "auth-service:validate_request",
        "wishlist-service:manage_wishlist",
    ],
    "GET /v1/recommendations": [
        "auth-service:validate_request",
        "recommendation-service:get_similar",
    ],
}


# ── Helpers ──────────────────────────────────────────────────────────────

def _trace_id() -> str:
    return uuid.uuid4().hex

def _span_id() -> str:
    return uuid.uuid4().hex[:16]

def _resource_attrs(service_name: str, region: str) -> dict:
    return {
        "service.name": service_name,
        "service.version": "4.2.1",
        "deployment.environment": "production",
        "cloud.provider": "aws",
        "cloud.region": region,
    }

def _make_span(trace_id: str, parent_span_id: str, span_name: str,
               service_name: str, span_kind: str, timestamp: datetime,
               duration_ns_val: int, status: str, region: str,
               span_attrs: dict = None, status_message: str = "") -> dict:
    return {
        "Timestamp": timestamp,
        "TraceId": trace_id,
        "SpanId": _span_id(),
        "ParentSpanId": parent_span_id,
        "TraceState": "",
        "SpanName": span_name,
        "SpanKind": span_kind,
        "ServiceName": service_name,
        "ResourceAttributes": _resource_attrs(service_name, region),
        "ScopeName": "snkrs-simulator",
        "ScopeVersion": "1.0.0",
        "SpanAttributes": span_attrs or {},
        "Duration": duration_ns_val,
        "StatusCode": status,
        "StatusMessage": status_message,
        "Events.Timestamp": [],
        "Events.Name": [],
        "Events.Attributes": [],
        "Links.TraceId": [],
        "Links.SpanId": [],
        "Links.TraceState": [],
        "Links.Attributes": [],
    }


# ── Graph walker ─────────────────────────────────────────────────────────

class TraceContext:
    """Mutable context carried through graph traversal."""
    def __init__(self, user, product, stock_tracker, incident, progress, is_drop, rng):
        self.user = user
        self.product = product
        self.stock_tracker = stock_tracker
        self.incident = incident
        self.progress = progress
        self.is_drop = is_drop
        self.rng = rng
        self.errored = False
        self.error_message = ""
        self.dedup_failed = False
        self.stock_available = True
        self.validation_ok = True
        self.fraud_ok = True
        self.charge_ok = True


def _walk_node(node_key: str, trace_id: str, parent_id: str,
               ts: datetime, region: str, ctx: TraceContext) -> tuple[list, int]:
    """Recursively walk a graph node and produce spans.

    Returns (spans_list, total_duration_ns).
    """
    node = SERVICE_GRAPH.get(node_key)
    if not node:
        return [], 0

    r = ctx.rng
    spans = []
    service = node_key.split(":")[0]
    span_name_raw = node_key.replace(":", ": ", 1)

    # Determine latency
    base_ms = node.get("latency_base_ms", 5)
    if ctx.is_drop:
        base_ms *= r.uniform(1.0, 2.0)

    # Incident effects
    inc_active = should_apply_incident(ctx.incident, service, "", region, ctx.progress)
    lat_mult = ctx.incident["latency_multiplier"] if inc_active else 1.0
    err_override = ctx.incident["error_rate_override"] if inc_active else None

    hint = node.get("span_name_hint", "default")
    dur = duration_ns(hint, base_ms, rng=r, progress=ctx.progress)
    dur = int(dur * lat_mult)

    # Error determination
    base_err_rate = node.get("error_rate", 0.0)
    err_rate = err_override if err_override else base_err_rate
    node_errored = r.random() < err_rate

    # Context-specific error logic
    if "check_account_standing" in node_key:
        if ctx.user.get("is_bot") and r.random() < 0.40:
            node_errored = True

    status = "ERROR" if node_errored else "OK"
    status_msg = ""
    if node_errored:
        if "dedup" in node_key:
            ctx.dedup_failed = True
            status_msg = "Duplicate draw entry detected"
        elif "validate_payment" in node_key:
            ctx.validation_ok = False
            status_msg = "Payment method validation failed"
        elif "stripe" in node_key:
            ctx.charge_ok = False
            status_msg = ctx.incident.get("error_message", "Stripe API timeout") if ctx.incident else "Stripe API timeout"
        elif "account_standing" in node_key:
            ctx.errored = True
            status_msg = "Account flagged: suspected bot activity"
        elif "dynamodb" in node_key:
            status_msg = "DynamoDB write capacity exceeded"

    # Build span attributes
    span_attrs = {}
    if node.get("db_system"):
        span_attrs["db.system"] = node["db_system"]
    if "user_id" in str(node_key) or "auth" in service or "validate" in node_key:
        span_attrs["user.id"] = ctx.user["user_id"]
    if ctx.product and ("product" in service or "inventory" in service):
        span_attrs["product.sku"] = ctx.product["sku"]
    if "redis" in node_key:
        cache_hit = "miss" not in node_key and r.random() > 0.15
        span_attrs["cache.hit"] = str(cache_hit).lower()

    # Create this node's span
    span = _make_span(trace_id, parent_id, span_name_raw, service,
                      node.get("span_kind", "INTERNAL"), ts, dur,
                      status, region, span_attrs, status_msg)
    my_span_id = span["SpanId"]
    spans.append(span)

    child_offset_ns = dur
    sync_child_dur = 0
    async_child_dur = 0

    # Walk children
    for child_key, edge_config in node.get("children", {}).items():
        # Check probability
        prob = edge_config.get("probability", 1.0)
        if r.random() > prob:
            continue

        # Check conditions
        condition = edge_config.get("condition")
        if condition:
            if condition == "cache_miss":
                miss_rate = edge_config.get("cache_miss_rate", 0.15)
                if ctx.is_drop:
                    miss_rate *= 2
                if r.random() > miss_rate:
                    continue
            elif condition == "no_dedup_fail" and ctx.dedup_failed:
                continue
            elif condition == "stock_available" and not ctx.stock_available:
                continue
            elif condition == "validation_ok" and not ctx.validation_ok:
                continue
            elif condition == "fraud_ok" and not ctx.fraud_ok:
                continue
            elif condition == "charge_ok" and not ctx.charge_ok:
                continue
            elif condition == "auth_ok" and ctx.errored:
                continue

        is_async = edge_config.get("is_async", False)

        # Circuit breaker: skip with fast-fail if error rate is high
        if edge_config.get("circuit_breaker") and inc_active:
            if r.random() < 0.50:
                # Fast fail span
                fail_dur = duration_ns("default", 1, rng=r)
                spans.append(_make_span(trace_id, my_span_id,
                    f"{child_key.split(':')[0]}: circuit_breaker_open",
                    child_key.split(":")[0], "INTERNAL",
                    ts + timedelta(microseconds=child_offset_ns // 1000),
                    fail_dur, "ERROR", region,
                    {"circuit_breaker.state": "open"},
                    "Circuit breaker OPEN"))
                continue

        # Retry logic
        max_retries = edge_config.get("retry_on_fail", 0)
        attempts = 1 + max_retries

        for attempt in range(attempts):
            child_ts = ts + timedelta(microseconds=child_offset_ns // 1000)
            child_spans, child_dur = _walk_node(
                child_key, trace_id, my_span_id, child_ts, region, ctx)

            if is_async:
                async_child_dur = max(async_child_dur, child_dur)
            else:
                child_offset_ns += child_dur
                sync_child_dur += child_dur

            spans.extend(child_spans)

            # Check if child errored (last span is typically the root of child)
            child_errored = any(s["StatusCode"] == "ERROR" for s in child_spans)
            if not child_errored or attempt == attempts - 1:
                break
            # Add retry delay
            retry_delay = duration_ns("default", 50 * (attempt + 1), rng=r)
            child_offset_ns += retry_delay

        if node_errored:
            break  # Don't continue to sibling children if this node errored

    # Update this node's duration to encompass children
    total_dur = max(dur, child_offset_ns)
    span["Duration"] = total_dur

    return spans, total_dur


def generate_trace_from_graph(endpoint: str, user: dict, product: dict,
                               ts: datetime, stock_tracker: StockTracker,
                               incident: dict, progress: float,
                               rng: random.Random) -> list[dict]:
    """Generate a full trace by walking the service graph for an endpoint.

    This is the main entry point — replaces the 5 fixed flow generators.
    """
    roots = FLOW_ROOTS.get(endpoint)
    if not roots:
        return []

    trace = _trace_id()
    region = user["region"]
    root_id = _span_id()
    is_drop = progress > 0.25 and progress < 0.75

    ctx = TraceContext(user, product, stock_tracker, incident, progress, is_drop, rng)

    # Handle stock reservation for checkout
    if "checkout" in endpoint and stock_tracker and product:
        success, _ = stock_tracker.try_reserve(product["id"], user["shoe_size"])
        ctx.stock_available = success

    # Handle fraud check for checkout
    if "checkout" in endpoint:
        fraud_score = user["fraud_risk"] * 100 * rng.uniform(0.5, 1.5)
        ctx.fraud_ok = fraud_score <= 85

    all_spans = []
    offset_ns = 0

    for root_key in roots:
        child_ts = ts + timedelta(microseconds=offset_ns // 1000)
        spans, dur = _walk_node(root_key, trace, root_id, child_ts, region, ctx)
        all_spans.extend(spans)
        offset_ns += dur

        # Short-circuit on auth failure
        if ctx.errored and "auth" in root_key:
            break

    # Create the root API gateway span
    any_error = ctx.errored or not ctx.stock_available or not ctx.validation_ok or not ctx.fraud_ok or not ctx.charge_ok

    # Determine HTTP status
    http_code = "200"
    if "POST" in endpoint:
        http_code = "201"
    if any_error:
        if ctx.errored:
            http_code = "429" if user.get("is_bot") else "500"
        elif not ctx.stock_available:
            http_code = "410"
        elif not ctx.fraud_ok:
            http_code = "403"
        elif not ctx.validation_ok or not ctx.charge_ok:
            http_code = "402"
        else:
            http_code = "500"

    root_attrs = {
        "http.method": endpoint.split(" ")[0],
        "http.route": endpoint.split(" ")[1] if " " in endpoint else endpoint,
        "http.status_code": http_code,
        "user.id": user["user_id"],
        "cloud.region": region,
        "device.type": user["device"],
    }
    if product:
        root_attrs["product.id"] = product["id"]
        root_attrs["product.sku"] = product["sku"]
    if "checkout" in endpoint and product:
        root_attrs["order.amount"] = f"{product['price'] + rng.uniform(0, 30):.2f}"
        root_attrs["shoe.size"] = user["shoe_size"]

    status = "ERROR" if any_error else "OK"
    root_span = _make_span(trace, "", endpoint, "api-gateway", "SERVER",
                           ts, offset_ns, status, region, root_attrs)
    root_span["SpanId"] = root_id
    all_spans.append(root_span)

    return all_spans
