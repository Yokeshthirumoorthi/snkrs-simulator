"""
OTLP writer — sends generated spans/logs/metrics through the OTEL pipeline
as protobuf messages via gRPC, or writes directly to JSON files for TB scale.
"""

import gzip
import json
import os
import sys
import time
from datetime import datetime
from collections import defaultdict
from pathlib import Path

_this_module = sys.modules[__name__]

import grpc
from opentelemetry.proto.common.v1.common_pb2 import (
    AnyValue, KeyValue, InstrumentationScope,
)
from opentelemetry.proto.resource.v1.resource_pb2 import Resource
from opentelemetry.proto.trace.v1.trace_pb2 import (
    Span, Status, ScopeSpans, ResourceSpans, TracesData,
)
from opentelemetry.proto.logs.v1.logs_pb2 import (
    LogRecord, ScopeLogs, ResourceLogs, LogsData,
)
from opentelemetry.proto.metrics.v1.metrics_pb2 import (
    Metric, Gauge, NumberDataPoint, Sum,
    ScopeMetrics, ResourceMetrics, MetricsData,
)
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)
from opentelemetry.proto.collector.trace.v1.trace_service_pb2_grpc import (
    TraceServiceStub,
)
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import (
    ExportLogsServiceRequest,
)
from opentelemetry.proto.collector.logs.v1.logs_service_pb2_grpc import (
    LogsServiceStub,
)
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest,
)
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2_grpc import (
    MetricsServiceStub,
)

# ── SpanKind string → protobuf enum ────────────────────────────────────
SPAN_KIND_MAP = {
    "SPAN_KIND_INTERNAL": Span.SPAN_KIND_INTERNAL,
    "SPAN_KIND_SERVER": Span.SPAN_KIND_SERVER,
    "SPAN_KIND_CLIENT": Span.SPAN_KIND_CLIENT,
    "SPAN_KIND_PRODUCER": Span.SPAN_KIND_PRODUCER,
    "SPAN_KIND_CONSUMER": Span.SPAN_KIND_CONSUMER,
    "INTERNAL": Span.SPAN_KIND_INTERNAL,
    "SERVER": Span.SPAN_KIND_SERVER,
    "CLIENT": Span.SPAN_KIND_CLIENT,
    "PRODUCER": Span.SPAN_KIND_PRODUCER,
    "CONSUMER": Span.SPAN_KIND_CONSUMER,
}

STATUS_CODE_MAP = {
    "STATUS_CODE_OK": 1,
    "STATUS_CODE_ERROR": 2,
    "STATUS_CODE_UNSET": 0,
    "OK": 1,
    "ERROR": 2,
    "UNSET": 0,
}

SEVERITY_TEXT_TO_NUMBER = {
    "TRACE": 1, "DEBUG": 5, "INFO": 9, "WARN": 13, "ERROR": 17, "FATAL": 21,
}

METRIC_TYPE_SUM = {"Sum"}
METRIC_TYPE_GAUGE = {"Gauge"}


def _dict_to_kv(d: dict) -> list:
    """Convert a dict to a list of KeyValue protobuf objects."""
    kvs = []
    for k, v in d.items():
        if isinstance(v, bool):
            kvs.append(KeyValue(key=k, value=AnyValue(bool_value=v)))
        elif isinstance(v, int):
            kvs.append(KeyValue(key=k, value=AnyValue(int_value=v)))
        elif isinstance(v, float):
            kvs.append(KeyValue(key=k, value=AnyValue(double_value=v)))
        else:
            kvs.append(KeyValue(key=k, value=AnyValue(string_value=str(v))))
    return kvs


def _ts_to_nano(ts: datetime) -> int:
    """Convert datetime to nanoseconds since epoch."""
    return int(ts.timestamp() * 1_000_000_000)


def _hex_to_bytes(hex_str: str) -> bytes:
    """Convert hex string to bytes, padding if needed."""
    if not hex_str:
        return b""
    return bytes.fromhex(hex_str)


# ── Build protobuf messages from flat dicts ─────────────────────────────

def _spans_to_proto(spans: list[dict]) -> ExportTraceServiceRequest:
    """Convert flat span dicts into an ExportTraceServiceRequest.

    Groups spans by (ServiceName, ResourceAttributes) to build
    proper ResourceSpans → ScopeSpans → Span hierarchy.
    """
    # Group by resource identity (service name + sorted resource attrs)
    resource_groups = defaultdict(list)
    for s in spans:
        resource_key = (s["ServiceName"], tuple(sorted(s["ResourceAttributes"].items())))
        resource_groups[resource_key].append(s)

    resource_spans_list = []
    for (service_name, _), group_spans in resource_groups.items():
        resource = Resource(attributes=_dict_to_kv(group_spans[0]["ResourceAttributes"]))

        proto_spans = []
        for s in group_spans:
            start_ns = _ts_to_nano(s["Timestamp"])
            end_ns = start_ns + s["Duration"]

            span = Span(
                trace_id=_hex_to_bytes(s["TraceId"]),
                span_id=_hex_to_bytes(s["SpanId"]),
                parent_span_id=_hex_to_bytes(s["ParentSpanId"]),
                trace_state=s.get("TraceState", ""),
                name=s["SpanName"],
                kind=SPAN_KIND_MAP.get(s["SpanKind"], Span.SPAN_KIND_INTERNAL),
                start_time_unix_nano=start_ns,
                end_time_unix_nano=end_ns,
                attributes=_dict_to_kv(s.get("SpanAttributes", {})),
                status=Status(
                    code=STATUS_CODE_MAP.get(s["StatusCode"], 0),
                    message=s.get("StatusMessage", ""),
                ),
            )
            proto_spans.append(span)

        scope = InstrumentationScope(
            name=group_spans[0].get("ScopeName", "snkrs-simulator"),
            version=group_spans[0].get("ScopeVersion", "1.0.0"),
        )

        resource_spans_list.append(ResourceSpans(
            resource=resource,
            scope_spans=[ScopeSpans(scope=scope, spans=proto_spans)],
        ))

    return ExportTraceServiceRequest(resource_spans=resource_spans_list)


def _logs_to_proto(logs: list[dict]) -> ExportLogsServiceRequest:
    """Convert flat log dicts into an ExportLogsServiceRequest."""
    resource_groups = defaultdict(list)
    for l in logs:
        resource_key = (l["ServiceName"], tuple(sorted(l["ResourceAttributes"].items())))
        resource_groups[resource_key].append(l)

    resource_logs_list = []
    for (service_name, _), group_logs in resource_groups.items():
        resource = Resource(attributes=_dict_to_kv(group_logs[0]["ResourceAttributes"]))

        proto_logs = []
        for l in group_logs:
            log_record = LogRecord(
                time_unix_nano=_ts_to_nano(l["Timestamp"]),
                severity_number=l.get("SeverityNumber", SEVERITY_TEXT_TO_NUMBER.get(l.get("SeverityText", "INFO"), 9)),
                severity_text=l.get("SeverityText", "INFO"),
                body=AnyValue(string_value=l.get("Body", "")),
                attributes=_dict_to_kv(l.get("LogAttributes", {})),
                trace_id=_hex_to_bytes(l.get("TraceId", "")),
                span_id=_hex_to_bytes(l.get("SpanId", "")),
            )
            proto_logs.append(log_record)

        resource_logs_list.append(ResourceLogs(
            resource=resource,
            scope_logs=[ScopeLogs(
                scope=InstrumentationScope(name="snkrs-simulator", version="1.0.0"),
                log_records=proto_logs,
            )],
        ))

    return ExportLogsServiceRequest(resource_logs=resource_logs_list)


def _metrics_to_proto(metrics: list[dict]) -> ExportMetricsServiceRequest:
    """Convert flat metric dicts into an ExportMetricsServiceRequest."""
    resource_groups = defaultdict(list)
    for m in metrics:
        resource_key = (m["ServiceName"], tuple(sorted(m["ResourceAttributes"].items())))
        resource_groups[resource_key].append(m)

    resource_metrics_list = []
    for (service_name, _), group_metrics in resource_groups.items():
        resource = Resource(attributes=_dict_to_kv(group_metrics[0]["ResourceAttributes"]))

        # Group by metric name within each resource
        metric_name_groups = defaultdict(list)
        for m in group_metrics:
            metric_name_groups[m["MetricName"]].append(m)

        proto_metrics = []
        for metric_name, mgroup in metric_name_groups.items():
            sample = mgroup[0]
            data_points = []
            for m in mgroup:
                dp = NumberDataPoint(
                    time_unix_nano=_ts_to_nano(m["Timestamp"]),
                    as_double=float(m["Value"]),
                    attributes=_dict_to_kv(m.get("MetricAttributes", {})),
                )
                data_points.append(dp)

            if sample["MetricType"] in METRIC_TYPE_SUM:
                metric = Metric(
                    name=metric_name,
                    description=sample.get("MetricDescription", ""),
                    unit=sample.get("MetricUnit", ""),
                    sum=Sum(
                        data_points=data_points,
                        aggregation_temporality=1,  # DELTA
                        is_monotonic=True,
                    ),
                )
            else:
                metric = Metric(
                    name=metric_name,
                    description=sample.get("MetricDescription", ""),
                    unit=sample.get("MetricUnit", ""),
                    gauge=Gauge(data_points=data_points),
                )
            proto_metrics.append(metric)

        resource_metrics_list.append(ResourceMetrics(
            resource=resource,
            scope_metrics=[ScopeMetrics(
                scope=InstrumentationScope(name="snkrs-simulator", version="1.0.0"),
                metrics=proto_metrics,
            )],
        ))

    return ExportMetricsServiceRequest(resource_metrics=resource_metrics_list)


# ── OTLPWriter ──────────────────────────────────────────────────────────

class OTLPWriter:
    """Batched OTLP gRPC writer — sends spans/logs/metrics through the OTEL pipeline."""

    def __init__(self, endpoint: str = "localhost:4317", batch_size: int = 5_000):
        self.endpoint = endpoint
        self.batch_size = batch_size
        self.channel = None
        self.trace_stub = None
        self.logs_stub = None
        self.metrics_stub = None

        # Buffers
        self._spans = []
        self._logs = []
        self._metrics = []

        # Stats
        self.spans_written = 0
        self.logs_written = 0
        self.metrics_written = 0
        self.errors = 0
        self._start_time = time.time()

    def connect(self):
        """Establish gRPC channel."""
        self.channel = grpc.insecure_channel(
            self.endpoint,
            options=[
                ("grpc.max_send_message_length", 64 * 1024 * 1024),
                ("grpc.max_receive_message_length", 64 * 1024 * 1024),
            ],
        )
        self.trace_stub = TraceServiceStub(self.channel)
        self.logs_stub = LogsServiceStub(self.channel)
        self.metrics_stub = MetricsServiceStub(self.channel)

    def close(self):
        """Flush remaining and close."""
        self.flush_all()
        if self.channel:
            self.channel.close()

    def add_spans(self, spans: list[dict]):
        """Add spans to the buffer, flushing when batch_size is reached."""
        self._spans.extend(spans)
        while len(self._spans) >= self.batch_size:
            self._flush_spans(self._spans[:self.batch_size])
            self._spans = self._spans[self.batch_size:]

    def add_logs(self, logs: list[dict]):
        """Add logs to the buffer."""
        self._logs.extend(logs)
        while len(self._logs) >= self.batch_size:
            self._flush_logs(self._logs[:self.batch_size])
            self._logs = self._logs[self.batch_size:]

    def add_metrics(self, metrics: list[dict]):
        """Add metrics to the buffer."""
        self._metrics.extend(metrics)
        while len(self._metrics) >= self.batch_size:
            self._flush_metrics(self._metrics[:self.batch_size])
            self._metrics = self._metrics[self.batch_size:]

    def flush_all(self):
        """Flush all remaining buffered data."""
        if self._spans:
            self._flush_spans(self._spans)
            self._spans = []
        if self._logs:
            self._flush_logs(self._logs)
            self._logs = []
        if self._metrics:
            self._flush_metrics(self._metrics)
            self._metrics = []

    def _flush_with_retry(self, flush_fn, batch, label):
        """Retry a flush with exponential backoff on transient gRPC errors."""
        max_retries = 5
        delay = 1.0
        for attempt in range(max_retries + 1):
            try:
                flush_fn(batch)
                return
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE and attempt < max_retries:
                    print(f"  [RETRY] {label}: {e.details()} — retrying in {delay:.0f}s (attempt {attempt+1}/{max_retries})")
                    time.sleep(delay)
                    delay = min(delay * 2, 30)
                else:
                    print(f"  [ERROR] Failed to send {len(batch)} {label}: {e}")
                    self.errors += 1
                    return  # don't re-raise — avoids pickle crash

    def _flush_spans(self, batch: list[dict]):
        if not batch:
            return
        def _send(b):
            request = _spans_to_proto(b)
            self.trace_stub.Export(request)
            self.spans_written += len(b)
        self._flush_with_retry(_send, batch, "spans")

    def _flush_logs(self, batch: list[dict]):
        if not batch:
            return
        def _send(b):
            request = _logs_to_proto(b)
            self.logs_stub.Export(request)
            self.logs_written += len(b)
        self._flush_with_retry(_send, batch, "logs")

    def _flush_metrics(self, batch: list[dict]):
        if not batch:
            return
        def _send(b):
            request = _metrics_to_proto(b)
            self.metrics_stub.Export(request)
            self.metrics_written += len(b)
        self._flush_with_retry(_send, batch, "metrics")

    def stats(self) -> dict:
        """Return current stats."""
        elapsed = time.time() - self._start_time
        total = self.spans_written + self.logs_written + self.metrics_written
        return {
            "spans": self.spans_written,
            "logs": self.logs_written,
            "metrics": self.metrics_written,
            "total": total,
            "errors": self.errors,
            "elapsed_s": round(elapsed, 1),
            "rows_per_sec": round(total / max(1, elapsed)),
        }

    def print_progress(self):
        """Print a one-line progress update."""
        s = self.stats()
        print(f"  spans={s['spans']:,}  logs={s['logs']:,}  metrics={s['metrics']:,}  "
              f"total={s['total']:,}  errors={s['errors']}  elapsed={s['elapsed_s']}s  "
              f"rate={s['rows_per_sec']:,} rows/sec")


# ── FileWriter — direct JSON output for TB scale ──────────────────────────

class FileWriter:
    """Write OTLP-compatible JSON files directly, bypassing the collector.

    Output format matches OTEL collector's file exporter / S3 exporter format.
    Partitioned by 5-minute time buckets for downstream pipeline compatibility.
    """

    def __init__(self, output_dir: str = "./output", batch_size: int = 5_000,
                 compress: bool = False, worker_id: int = 0):
        self.output_dir = Path(output_dir)
        self.batch_size = batch_size
        self.compress = compress
        self.worker_id = worker_id

        # Buffers
        self._spans = []
        self._logs = []
        self._metrics = []

        # Stats
        self.spans_written = 0
        self.logs_written = 0
        self.metrics_written = 0
        self._start_time = time.time()
        self._bytes_written = 0

        # File counters per bucket
        self._file_counters = defaultdict(int)

    def connect(self):
        """Create output directories."""
        for signal in ["traces", "logs", "metrics"]:
            (self.output_dir / signal).mkdir(parents=True, exist_ok=True)

    def close(self):
        """Flush remaining."""
        self.flush_all()

    def add_spans(self, spans: list[dict]):
        self._spans.extend(spans)
        while len(self._spans) >= self.batch_size:
            self._flush_spans(self._spans[:self.batch_size])
            self._spans = self._spans[self.batch_size:]

    def add_logs(self, logs: list[dict]):
        self._logs.extend(logs)
        while len(self._logs) >= self.batch_size:
            self._flush_logs(self._logs[:self.batch_size])
            self._logs = self._logs[self.batch_size:]

    def add_metrics(self, metrics: list[dict]):
        self._metrics.extend(metrics)
        while len(self._metrics) >= self.batch_size:
            self._flush_metrics(self._metrics[:self.batch_size])
            self._metrics = self._metrics[self.batch_size:]

    def flush_all(self):
        if self._spans:
            self._flush_spans(self._spans)
            self._spans = []
        if self._logs:
            self._flush_logs(self._logs)
            self._logs = []
        if self._metrics:
            self._flush_metrics(self._metrics)
            self._metrics = []

    def _flush_spans(self, batch: list[dict]):
        if not batch:
            return
        rows = [self._span_to_json(s) for s in batch]
        self._write_jsonl("traces", batch[0]["Timestamp"], rows)
        self.spans_written += len(batch)

    def _flush_logs(self, batch: list[dict]):
        if not batch:
            return
        rows = [self._log_to_json(l) for l in batch]
        self._write_jsonl("logs", batch[0]["Timestamp"], rows)
        self.logs_written += len(batch)

    def _flush_metrics(self, batch: list[dict]):
        if not batch:
            return
        rows = [self._metric_to_json(m) for m in batch]
        self._write_jsonl("metrics", batch[0]["Timestamp"], rows)
        self.metrics_written += len(batch)

    def _get_bucket_path(self, signal: str, ts: datetime) -> Path:
        """Get time-partitioned file path."""
        # 5-minute bucket: round down to nearest 5 min
        minute_bucket = (ts.minute // 5) * 5
        bucket_dir = self.output_dir / signal / (
            f"year={ts.year}/month={ts.month:02d}/day={ts.day:02d}/"
            f"hour={ts.hour:02d}/minute={minute_bucket:02d}")
        bucket_dir.mkdir(parents=True, exist_ok=True)

        bucket_key = f"{signal}_{ts.year}{ts.month:02d}{ts.day:02d}_{ts.hour:02d}{minute_bucket:02d}"
        self._file_counters[bucket_key] += 1
        counter = self._file_counters[bucket_key]

        ext = ".jsonl.gz" if self.compress else ".jsonl"
        return bucket_dir / f"w{self.worker_id}_{counter:06d}{ext}"

    def _write_jsonl(self, signal: str, ts: datetime, rows: list[dict]):
        """Write rows as JSON Lines to time-partitioned file."""
        path = self._get_bucket_path(signal, ts)
        try:
            data = "\n".join(json.dumps(r, default=str) for r in rows) + "\n"
            encoded = data.encode("utf-8")

            if self.compress:
                encoded = gzip.compress(encoded, compresslevel=1)

            with open(path, "ab") as f:
                f.write(encoded)
            self._bytes_written += len(encoded)
        except Exception as e:
            print(f"  [ERROR] Failed to write {len(rows)} rows to {path}: {e}")

    def _span_to_json(self, s: dict) -> dict:
        """Convert span dict to OTLP JSON format."""
        ts = s["Timestamp"]
        start_ns = int(ts.timestamp() * 1_000_000_000)
        return {
            "Timestamp": ts.isoformat() + "Z",
            "TraceId": s["TraceId"],
            "SpanId": s["SpanId"],
            "ParentSpanId": s["ParentSpanId"],
            "TraceState": s.get("TraceState", ""),
            "SpanName": s["SpanName"],
            "SpanKind": s["SpanKind"],
            "ServiceName": s["ServiceName"],
            "ResourceAttributes": s["ResourceAttributes"],
            "ScopeName": s.get("ScopeName", "snkrs-simulator"),
            "ScopeVersion": s.get("ScopeVersion", "1.0.0"),
            "SpanAttributes": s.get("SpanAttributes", {}),
            "Duration": s["Duration"],
            "StatusCode": s["StatusCode"],
            "StatusMessage": s.get("StatusMessage", ""),
            "Events.Timestamp": s.get("Events.Timestamp", []),
            "Events.Name": s.get("Events.Name", []),
            "Events.Attributes": s.get("Events.Attributes", []),
            "Links.TraceId": s.get("Links.TraceId", []),
            "Links.SpanId": s.get("Links.SpanId", []),
            "Links.TraceState": s.get("Links.TraceState", []),
            "Links.Attributes": s.get("Links.Attributes", []),
        }

    def _log_to_json(self, l: dict) -> dict:
        """Convert log dict to OTLP JSON format."""
        return {
            "Timestamp": l["Timestamp"].isoformat() + "Z",
            "TraceId": l.get("TraceId", ""),
            "SpanId": l.get("SpanId", ""),
            "SeverityNumber": l.get("SeverityNumber", 9),
            "SeverityText": l.get("SeverityText", "INFO"),
            "Body": l.get("Body", ""),
            "ServiceName": l["ServiceName"],
            "ResourceAttributes": l["ResourceAttributes"],
            "LogAttributes": l.get("LogAttributes", {}),
        }

    def _metric_to_json(self, m: dict) -> dict:
        """Convert metric dict to OTLP JSON format."""
        return {
            "Timestamp": m["Timestamp"].isoformat() + "Z",
            "MetricName": m["MetricName"],
            "MetricDescription": m.get("MetricDescription", ""),
            "MetricUnit": m.get("MetricUnit", ""),
            "MetricType": m["MetricType"],
            "Value": m["Value"],
            "ServiceName": m["ServiceName"],
            "ResourceAttributes": m["ResourceAttributes"],
            "MetricAttributes": m.get("MetricAttributes", {}),
        }

    def stats(self) -> dict:
        elapsed = time.time() - self._start_time
        total = self.spans_written + self.logs_written + self.metrics_written
        return {
            "spans": self.spans_written,
            "logs": self.logs_written,
            "metrics": self.metrics_written,
            "total": total,
            "elapsed_s": round(elapsed, 1),
            "rows_per_sec": round(total / max(1, elapsed)),
            "bytes_written": self._bytes_written,
            "mb_written": round(self._bytes_written / (1024 * 1024), 1),
        }

    def print_progress(self):
        s = self.stats()
        print(f"  spans={s['spans']:,}  logs={s['logs']:,}  metrics={s['metrics']:,}  "
              f"total={s['total']:,}  {s['mb_written']}MB  "
              f"rate={s['rows_per_sec']:,} rows/sec")


def create_writer(output_mode: str = "grpc", output_dir: str = "./output",
                  endpoint: str = "localhost:4317", batch_size: int = 5_000,
                  compress: bool = False, worker_id: int = 0):
    """Factory function to create the appropriate writer."""
    if output_mode == "file":
        writer = FileWriter(output_dir=output_dir, batch_size=batch_size,
                           compress=compress, worker_id=worker_id)
    else:
        writer = OTLPWriter(endpoint=endpoint, batch_size=batch_size)
    writer.connect()
    return writer


def worker_fn(args: tuple) -> dict:
    """Worker function for multiprocessing.

    Args:
        args: (user_chunk, time_range, drop_configs, otel_config)
            user_chunk: list of user dicts
            time_range: (start_datetime, end_datetime)
            drop_configs: list of (drop_index, product, incident, drop_start, drop_end)
            otel_config: dict with endpoint, batch_size

    Returns:
        Stats dict with counts.
    """
    user_chunk, time_range, drop_configs, otel_config = args
    shared_counter = getattr(_this_module, '_shared_counter', None)

    # Import here to avoid issues with multiprocessing
    import random as _random
    from spans import (generate_browse_product, generate_enter_draw,
                       generate_checkout, generate_account_check, generate_feed)
    from logs import generate_logs_for_spans
    from metrics import generate_metrics_for_interval
    from inventory import StockTracker, select_drop_product
    from patterns import PATTERNS, get_incident_for_drop

    # Create the appropriate writer (gRPC or file)
    output_mode = otel_config.get("output_mode", "grpc")
    worker_id = otel_config.get("worker_id", 0)
    writer = create_writer(
        output_mode=output_mode,
        output_dir=otel_config.get("output_dir", "./output"),
        endpoint=otel_config.get("endpoint", "localhost:4317"),
        batch_size=otel_config.get("batch_size", 5_000),
        compress=otel_config.get("compress", False),
        worker_id=worker_id,
    )

    rng = _random.Random()
    stock_tracker = StockTracker()

    for drop_idx, product, incident, drop_start, drop_end in drop_configs:
        stock_tracker.reset_for_drop(product, rng)

        drop_duration = (drop_end - drop_start).total_seconds()

        # Phase boundaries within the drop
        pre_end = drop_start + (drop_end - drop_start) * 0.25
        live_end = drop_start + (drop_end - drop_start) * 0.75

        for user in user_chunk:
            pattern = PATTERNS.get(user["behavior_type"], PATTERNS["casual_browser"])
            sessions = rng.randint(*pattern["sessions_per_drop"])

            for _ in range(sessions):
                # Pick a random time within the drop for this session
                session_ts = drop_start + (drop_end - drop_start) * rng.random()
                progress = (session_ts - drop_start).total_seconds() / max(1, drop_duration)

                # Determine phase
                if session_ts < pre_end:
                    phase = "pre_drop"
                    actions = pattern.get("pre_drop", [])
                elif session_ts < live_end:
                    phase = "drop_live"
                    actions = pattern.get("drop_live", [])
                else:
                    phase = "post_drop"
                    actions = pattern.get("post_drop", [])

                action_ts = session_ts
                for endpoint, config in actions:
                    # Check probability
                    if rng.random() > config.get("probability", 1.0):
                        continue

                    # Repeat
                    repeat_range = config.get("repeat", (1, 1))
                    if isinstance(repeat_range, int):
                        repeats = repeat_range
                    else:
                        repeats = rng.randint(*repeat_range)

                    for _ in range(repeats):
                        # Generate spans
                        spans = _generate_flow(
                            endpoint, user, product, action_ts,
                            stock_tracker, incident, progress, rng)

                        if spans:
                            writer.add_spans(spans)

                            # Generate correlated logs
                            logs = generate_logs_for_spans(spans, user, product, rng)
                            if logs:
                                writer.add_logs(logs)

                        # Advance time
                        gap_ms = rng.randint(*pattern["session_gap_ms"])
                        from datetime import timedelta
                        action_ts = action_ts + timedelta(milliseconds=gap_ms)

            if shared_counter is not None:
                with shared_counter.get_lock():
                    shared_counter.value += 1

    writer.flush_all()
    stats = writer.stats()
    writer.close()
    return stats


def _generate_flow(endpoint: str, user: dict, product: dict, ts,
                   stock_tracker, incident, progress, rng):
    """Generate the appropriate span tree for an endpoint.

    Uses topology-based graph traversal for additional endpoints,
    and original generators for the 5 core endpoints (backward compat).
    """
    from spans import (generate_browse_product, generate_enter_draw,
                       generate_checkout, generate_account_check, generate_feed)

    # Original 5 endpoints use the detailed hand-crafted generators
    if endpoint == "GET /v1/products/{id}":
        return generate_browse_product(user, product, ts, stock_tracker,
                                       incident, progress, rng)
    elif endpoint == "POST /v1/draw/enter":
        return generate_enter_draw(user, product, ts, stock_tracker,
                                   incident, progress, rng)
    elif endpoint == "POST /v1/checkout":
        return generate_checkout(user, product, ts, stock_tracker,
                                 incident, progress, rng)
    elif endpoint == "GET /v1/account/profile":
        return generate_account_check(user, ts, incident, progress, rng)
    elif endpoint == "GET /v1/feed":
        return generate_feed(user, ts, incident, progress, rng)

    # Additional endpoints use topology-based graph traversal
    try:
        from topology import generate_trace_from_graph
        return generate_trace_from_graph(endpoint, user, product, ts,
                                         stock_tracker, incident, progress, rng)
    except Exception:
        return []
