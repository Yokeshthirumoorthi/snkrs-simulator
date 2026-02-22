"""
Corpus loader — loads pre-generated JSON corpuses from the corpus/ directory.
Falls back to hardcoded defaults if corpus files don't exist.
"""

import json
from pathlib import Path

CORPUS_DIR = Path(__file__).parent / "corpus"

_cache = {}


def load_corpus(name: str, default=None):
    """Load a corpus JSON file by name (without .json extension).

    Returns the parsed JSON data, or default if the file doesn't exist.
    Results are cached after first load.
    """
    if name in _cache:
        return _cache[name]

    path = CORPUS_DIR / f"{name}.json"
    if not path.exists():
        if default is not None:
            return default
        raise FileNotFoundError(f"Corpus file not found: {path}. Run gen_corpus.py first.")

    with open(path) as f:
        data = json.load(f)

    _cache[name] = data
    return data


def get_log_templates(fallback: dict = None) -> dict:
    """Load log templates corpus. Returns {service: {severity: [templates]}}."""
    return load_corpus("log_templates", default=fallback or {})


def get_products(fallback: list = None) -> list:
    """Load products corpus. Returns list of product dicts."""
    return load_corpus("products", default=fallback or [])


def get_incidents(fallback: list = None) -> list:
    """Load incidents corpus. Returns list of incident dicts."""
    return load_corpus("incidents", default=fallback or [])


def get_services(fallback: list = None) -> list:
    """Load services corpus. Returns list of service dicts."""
    return load_corpus("services", default=fallback or [])


def get_error_messages(fallback: list = None) -> list:
    """Load error messages corpus. Returns list of error dicts."""
    return load_corpus("error_messages", default=fallback or [])


def get_endpoints(fallback: list = None) -> list:
    """Load endpoints corpus. Returns list of endpoint dicts."""
    return load_corpus("endpoints", default=fallback or [])
