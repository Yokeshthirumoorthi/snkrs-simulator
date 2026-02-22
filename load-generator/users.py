"""
User population generator — realistic SNKRS users with persistent profiles.
Unseeded by default for diverse generation; seed via --seed CLI flag if needed.
"""

import random
import uuid
from faker import Faker

fake = Faker()
# No fixed seeds — each run produces unique users

REGIONS = [
    ("us-east", 0.40),
    ("us-west", 0.25),
    ("eu-west", 0.20),
    ("ap-southeast", 0.05),
    ("ap-northeast", 0.05),
    ("sa-east", 0.03),
    ("af-south", 0.02),
]

DEVICES = [
    ("ios", 0.55),
    ("android", 0.30),
    ("web", 0.15),
]

PAYMENT_METHODS = [
    ("card", 0.55),
    ("apple_pay", 0.25),
    ("paypal", 0.15),
    ("google_pay", 0.05),
]

BEHAVIOR_TYPES = [
    # Original types
    ("casual_browser", 0.25),
    ("hype_buyer", 0.18),
    ("reseller", 0.10),
    ("window_shopper", 0.10),
    ("new_user", 0.07),
    # New diverse types
    ("loyal_collector", 0.08),
    ("deal_hunter", 0.06),
    ("international_buyer", 0.05),
    ("corporate_gifter", 0.03),
    ("returning_customer", 0.04),
    ("mobile_power_user", 0.04),
]

LOYALTY_TIERS = [
    ("bronze", 0.40),
    ("silver", 0.30),
    ("gold", 0.20),
    ("platinum", 0.10),
]

SIZES = ["6", "6.5", "7", "7.5", "8", "8.5", "9", "9.5", "10", "10.5", "11", "11.5", "12", "13", "14"]

# User-agent strings for diversity
_USER_AGENTS = [
    "SNKRS/4.8.0 (iPhone14,2; iOS 17.4)",
    "SNKRS/4.8.0 (iPhone15,3; iOS 17.5)",
    "SNKRS/4.7.2 (iPhone13,4; iOS 16.7)",
    "SNKRS/4.8.0 (iPhone16,1; iOS 18.0)",
    "SNKRS/4.8.0 (SM-S918B; Android 14)",
    "SNKRS/4.7.5 (Pixel 8 Pro; Android 14)",
    "SNKRS/4.8.0 (SM-S911B; Android 13)",
    "SNKRS/4.7.2 (Pixel 7; Android 13)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/605.1",
    "Mozilla/5.0 (X11; Linux x86_64) Firefox/123.0",
]


def _weighted_choice(options, rng=None):
    """Pick from list of (value, weight) tuples."""
    r = rng or random
    values, weights = zip(*options)
    return r.choices(values, weights=weights, k=1)[0]


def generate_user(idx: int, rng: random.Random = None) -> dict:
    """Generate a single user profile."""
    r = rng or random
    behavior = _weighted_choice(BEHAVIOR_TYPES, r)
    account_age = r.randint(1, 2000)

    # Bots are mostly resellers
    is_bot = (behavior == "reseller" and r.random() < 0.20) or r.random() < 0.01

    # Fraud risk correlates with new accounts, bots, resellers
    base_fraud = r.uniform(0.01, 0.15)
    if is_bot:
        base_fraud += r.uniform(0.3, 0.5)
    if behavior == "reseller":
        base_fraud += r.uniform(0.1, 0.2)
    if account_age < 30:
        base_fraud += r.uniform(0.1, 0.3)
    if behavior == "new_user":
        base_fraud += r.uniform(0.05, 0.15)
    fraud_risk = min(1.0, base_fraud)

    # New users get bronze
    loyalty = "bronze" if behavior == "new_user" else _weighted_choice(LOYALTY_TIERS, r)

    # Device-specific user agent
    device = _weighted_choice(DEVICES, r)
    if device == "ios":
        ua = r.choice([ua for ua in _USER_AGENTS if "iPhone" in ua])
    elif device == "android":
        ua = r.choice([ua for ua in _USER_AGENTS if "Android" in ua])
    else:
        ua = r.choice([ua for ua in _USER_AGENTS if "Mozilla" in ua])

    # IP address generation
    ip_octets = [r.randint(1, 254) for _ in range(4)]
    # Avoid private ranges for realism
    if ip_octets[0] in (10, 127) or (ip_octets[0] == 172 and 16 <= ip_octets[1] <= 31):
        ip_octets[0] = r.choice([50, 72, 98, 104, 142, 168, 185, 203])

    return {
        "user_id": f"user_{100000 + idx}",
        "name": fake.name(),
        "email": fake.email(),
        "region": _weighted_choice(REGIONS, r),
        "device": device,
        "payment_method": _weighted_choice(PAYMENT_METHODS, r),
        "account_age_days": account_age,
        "is_bot": is_bot,
        "behavior_type": behavior,
        "shoe_size": r.choice(SIZES),
        "fraud_risk": round(fraud_risk, 3),
        "loyalty_tier": loyalty,
        # New attributes
        "ip_address": f"{ip_octets[0]}.{ip_octets[1]}.{ip_octets[2]}.{ip_octets[3]}",
        "user_agent": ua,
        "session_count": r.randint(1, 500) if behavior != "new_user" else r.randint(1, 5),
        "lifetime_orders": r.randint(0, 200) if behavior != "new_user" else 0,
    }


def generate_users(count: int = 10000, seed: int = None) -> list[dict]:
    """Generate the full user population.

    Args:
        count: Number of users to generate.
        seed: Optional seed for reproducibility. None = unseeded (diverse).
    """
    rng = random.Random(seed)
    if seed is not None:
        Faker.seed(seed)
    return [generate_user(i, rng) for i in range(count)]


# Pre-built population for import
_USERS = None


def get_users(count: int = 10000, seed: int = None) -> list[dict]:
    """Get or generate the user population (cached)."""
    global _USERS
    if _USERS is None or len(_USERS) != count:
        _USERS = generate_users(count, seed=seed)
    return _USERS
