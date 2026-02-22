"""
Realistic latency distribution models — replaces the single Gaussian
with 4 distribution types that match real-world service behavior.
"""

import math
import random


class LatencyDistribution:
    """Base class for latency distributions."""

    def sample(self, base_ms: float, rng: random.Random = None) -> float:
        """Return a latency sample in milliseconds."""
        raise NotImplementedError


class BimodalDistribution(LatencyDistribution):
    """Cache hit/miss pattern: fast path (80%) vs slow path (20%).

    Use for: Redis lookups, CDN fetches, any operation with caching.
    """

    def __init__(self, fast_fraction: float = 0.80, slow_multiplier: float = 5.0):
        self.fast_fraction = fast_fraction
        self.slow_multiplier = slow_multiplier

    def sample(self, base_ms: float, rng: random.Random = None) -> float:
        r = rng or random
        if r.random() < self.fast_fraction:
            # Fast path (cache hit): tight distribution around base
            return max(0.1, r.gauss(base_ms, base_ms * 0.15))
        else:
            # Slow path (cache miss): wider distribution, higher base
            slow_base = base_ms * self.slow_multiplier
            return max(0.1, r.gauss(slow_base, slow_base * 0.25))


class LongTailDistribution(LatencyDistribution):
    """Pareto/long-tail for external API calls (Stripe, CDN).

    95% of requests are normal, 5% have heavy tail (2x-20x base).
    Use for: Stripe charges, external API calls, DNS lookups.
    """

    def __init__(self, tail_fraction: float = 0.05, alpha: float = 1.5):
        self.tail_fraction = tail_fraction
        self.alpha = alpha  # Pareto shape — lower = heavier tail

    def sample(self, base_ms: float, rng: random.Random = None) -> float:
        r = rng or random
        if r.random() < (1 - self.tail_fraction):
            # Normal range
            return max(0.1, r.gauss(base_ms, base_ms * 0.20))
        else:
            # Heavy tail: Pareto distribution
            # Pareto: x = base * (1/U)^(1/alpha) where U ~ Uniform(0,1)
            u = r.random()
            multiplier = (1.0 / max(0.001, u)) ** (1.0 / self.alpha)
            # Cap at 20x to avoid absurd values
            multiplier = min(multiplier, 20.0)
            return max(0.1, base_ms * multiplier)


class PeriodicSpikeDistribution(LatencyDistribution):
    """Periodic latency spikes (GC pauses, cron jobs).

    Baseline with periodic windows of elevated latency.
    Use for: JVM services, services with batch jobs, periodic cleanup.
    """

    def __init__(self, spike_probability: float = 0.03, spike_multiplier: float = 8.0):
        self.spike_probability = spike_probability
        self.spike_multiplier = spike_multiplier

    def sample(self, base_ms: float, rng: random.Random = None) -> float:
        r = rng or random
        if r.random() < self.spike_probability:
            # GC pause / spike
            spike_base = base_ms * self.spike_multiplier
            return max(0.1, r.gauss(spike_base, spike_base * 0.30))
        else:
            # Normal operation
            return max(0.1, r.gauss(base_ms, base_ms * 0.20))

    def sample_with_progress(self, base_ms: float, progress: float,
                              rng: random.Random = None) -> float:
        """Sample with time-based periodicity (progress 0.0-1.0 through drop)."""
        r = rng or random
        # Create periodic windows using sin wave
        # Spike probability increases during periodic windows
        cycle_freq = 6.0  # ~6 cycles per drop
        wave = math.sin(progress * cycle_freq * 2 * math.pi)
        spike_prob = self.spike_probability * (1 + max(0, wave) * 3)
        if r.random() < spike_prob:
            spike_base = base_ms * self.spike_multiplier
            return max(0.1, r.gauss(spike_base, spike_base * 0.30))
        return max(0.1, r.gauss(base_ms, base_ms * 0.20))


class DegradationRampDistribution(LatencyDistribution):
    """Gradual latency increase over time (memory leak, connection leak).

    Latency ramps from base to max_multiplier * base over the drop window.
    Use for: Memory leaks, connection pool exhaustion, log buffer fills.
    """

    def __init__(self, max_multiplier: float = 5.0):
        self.max_multiplier = max_multiplier

    def sample(self, base_ms: float, rng: random.Random = None) -> float:
        # Without progress info, just use normal distribution
        r = rng or random
        return max(0.1, r.gauss(base_ms, base_ms * 0.25))

    def sample_with_progress(self, base_ms: float, progress: float,
                              rng: random.Random = None) -> float:
        """Sample with degradation ramp based on progress (0.0-1.0)."""
        r = rng or random
        # Exponential ramp: slow at first, accelerating
        ramp = 1.0 + (self.max_multiplier - 1.0) * (progress ** 2)
        ramped_base = base_ms * ramp
        return max(0.1, r.gauss(ramped_base, ramped_base * 0.15))


class GaussianDistribution(LatencyDistribution):
    """Original simple Gaussian (backward compatibility)."""

    def __init__(self, jitter: float = 0.30):
        self.jitter = jitter

    def sample(self, base_ms: float, rng: random.Random = None) -> float:
        r = rng or random
        return max(0.1, r.gauss(base_ms, base_ms * self.jitter))


# ── Pre-built distribution instances for span types ──────────────────────

# Map of span operation type → appropriate distribution
SPAN_DISTRIBUTIONS = {
    # Cache operations: bimodal (hit vs miss)
    "redis.get": BimodalDistribution(fast_fraction=0.80, slow_multiplier=4.0),
    "redis.decr": BimodalDistribution(fast_fraction=0.90, slow_multiplier=3.0),
    "redis.set": GaussianDistribution(jitter=0.20),

    # Database queries: long-tail (most fast, some slow due to locks/contention)
    "postgres.query": LongTailDistribution(tail_fraction=0.08, alpha=1.8),
    "postgres.insert": LongTailDistribution(tail_fraction=0.05, alpha=2.0),
    "postgres.update": LongTailDistribution(tail_fraction=0.10, alpha=1.5),
    "dynamodb.put": LongTailDistribution(tail_fraction=0.03, alpha=2.5),

    # External APIs: long-tail (network variability)
    "stripe.charge": LongTailDistribution(tail_fraction=0.05, alpha=1.2),
    "cdn.get": LongTailDistribution(tail_fraction=0.04, alpha=2.0),
    "cdn.fetch": LongTailDistribution(tail_fraction=0.04, alpha=2.0),
    "apns.push": LongTailDistribution(tail_fraction=0.06, alpha=1.8),
    "ses.send_email": LongTailDistribution(tail_fraction=0.03, alpha=2.0),

    # Kafka: periodic spikes (rebalancing, batch flushes)
    "kafka.produce": PeriodicSpikeDistribution(spike_probability=0.02, spike_multiplier=6.0),

    # Internal compute: Gaussian (predictable)
    "verify_jwt": GaussianDistribution(jitter=0.25),
    "validate_entry_window": GaussianDistribution(jitter=0.15),
    "validate_payment_method": GaussianDistribution(jitter=0.20),
    "fraud.score_check": PeriodicSpikeDistribution(spike_probability=0.05, spike_multiplier=4.0),
    "check_account_standing": GaussianDistribution(jitter=0.20),

    # Default fallback
    "default": GaussianDistribution(jitter=0.30),
}


def get_distribution(span_name: str) -> LatencyDistribution:
    """Get the appropriate latency distribution for a span operation.

    Matches by checking if any key is a substring of the span name.
    """
    # Try exact key match first
    if span_name in SPAN_DISTRIBUTIONS:
        return SPAN_DISTRIBUTIONS[span_name]

    # Try substring match
    for key, dist in SPAN_DISTRIBUTIONS.items():
        if key != "default" and key in span_name:
            return dist

    return SPAN_DISTRIBUTIONS["default"]


def sample_latency(span_name: str, base_ms: float, rng: random.Random = None,
                   progress: float = None) -> float:
    """Sample a latency value for a given span operation.

    Args:
        span_name: The span/operation name to match distribution.
        base_ms: Base latency in milliseconds.
        rng: Random instance.
        progress: Optional 0.0-1.0 progress through drop (for time-aware distributions).

    Returns:
        Latency in milliseconds.
    """
    dist = get_distribution(span_name)
    if progress is not None and hasattr(dist, 'sample_with_progress'):
        return dist.sample_with_progress(base_ms, progress, rng)
    return dist.sample(base_ms, rng)


def duration_ns(span_name: str, base_ms: float, rng: random.Random = None,
                progress: float = None) -> int:
    """Sample a latency and return as nanoseconds (drop-in replacement for _duration_ns)."""
    ms = sample_latency(span_name, base_ms, rng, progress)
    return int(ms * 1_000_000)
