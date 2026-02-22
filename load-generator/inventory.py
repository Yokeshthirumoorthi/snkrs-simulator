"""
Product catalog and per-(product, size) stock tracking.
"""

import random
import threading

from corpus_loader import get_products

# Hardcoded fallback (original 24 products)
_FALLBACK_PRODUCTS = [
    {"id": "AJ1-CHI",  "name": "Air Jordan 1 Retro High OG 'Chicago'",     "sku": "555088-101", "price": 180, "base_stock": 500,  "hype_level": 0.95},
    {"id": "SB-TRAV",  "name": "SB Dunk Low x Travis Scott",                "sku": "CT5053-001", "price": 150, "base_stock": 200,  "hype_level": 0.98},
    {"id": "AF1-WHT",  "name": "Air Force 1 '07 Triple White",              "sku": "CW2288-111", "price": 90,  "base_stock": 2000, "hype_level": 0.30},
    {"id": "AJ1-TS",   "name": "Air Jordan 1 x Travis Scott 'Mocha'",     "sku": "CD4487-100", "price": 175, "base_stock": 200,  "hype_level": 0.99},
    {"id": "DF-PANDA", "name": "Nike Dunk Low 'Panda'",                     "sku": "DD1391-100", "price": 110, "base_stock": 800,  "hype_level": 0.70},
    {"id": "YZ-350Z",  "name": "Yeezy Boost 350 V2 'Zebra'",               "sku": "CP9654",     "price": 220, "base_stock": 300,  "hype_level": 0.90},
]

# Load from corpus, normalize fields for backward compat
_raw_products = get_products(fallback=_FALLBACK_PRODUCTS)
PRODUCTS = []
for p in _raw_products:
    PRODUCTS.append({
        "id": p["id"],
        "name": p["name"],
        "sku": p["sku"],
        "price": p["price"],
        "base_stock": p.get("base_stock", 500),
        "hype_level": p.get("hype_level", 0.5),
        "category": p.get("category", "lifestyle"),
        "release_type": p.get("release_type", "general_release"),
        "brand": p.get("brand", "Nike"),
        "collab": p.get("collab"),
        "colorway": p.get("colorway", ""),
    })

SIZES = ["6", "6.5", "7", "7.5", "8", "8.5", "9", "9.5", "10", "10.5", "11", "11.5", "12", "13", "14"]

# Size distribution weights (bell curve centered on 10)
SIZE_WEIGHTS = {
    "6": 0.02, "6.5": 0.03, "7": 0.05, "7.5": 0.07, "8": 0.08,
    "8.5": 0.10, "9": 0.12, "9.5": 0.13, "10": 0.13, "10.5": 0.10,
    "11": 0.07, "11.5": 0.04, "12": 0.03, "13": 0.02, "14": 0.01,
}


class StockTracker:
    """Thread-safe stock tracker per (product_id, size)."""

    def __init__(self):
        self._stock = {}
        self._lock = threading.Lock()

    def reset_for_drop(self, product: dict, rng: random.Random = None):
        """Initialize stock levels for a product drop."""
        r = rng or random
        base = product["base_stock"]
        with self._lock:
            for size in SIZES:
                # Distribute stock by size popularity
                weight = SIZE_WEIGHTS[size]
                qty = max(1, int(base * weight * r.uniform(0.8, 1.2)))
                self._stock[(product["id"], size)] = qty

    def try_reserve(self, product_id: str, size: str) -> tuple[bool, int]:
        """Attempt to reserve one unit. Returns (success, remaining)."""
        with self._lock:
            key = (product_id, size)
            current = self._stock.get(key, 0)
            if current <= 0:
                return False, 0
            self._stock[key] = current - 1
            return True, current - 1

    def get_stock(self, product_id: str, size: str) -> int:
        with self._lock:
            return self._stock.get((product_id, size), 0)

    def get_total_stock(self, product_id: str) -> int:
        with self._lock:
            return sum(v for (pid, _), v in self._stock.items() if pid == product_id)

    def force_negative(self, product_id: str, size: str, amount: int = 5):
        """Force stock negative for overselling incident simulation."""
        with self._lock:
            key = (product_id, size)
            self._stock[key] = self._stock.get(key, 0) - amount


def select_drop_product(drop_index: int, rng: random.Random = None) -> dict:
    """Select a product for a given drop, weighted by hype level."""
    r = rng or random
    weights = [p["hype_level"] for p in PRODUCTS]
    # Cycle through products but with randomness
    r_state = r.getstate()
    r.seed(drop_index * 7919)  # deterministic per drop
    product = r.choices(PRODUCTS, weights=weights, k=1)[0]
    r.setstate(r_state)
    return product
