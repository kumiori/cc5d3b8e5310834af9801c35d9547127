from __future__ import annotations

import time
from typing import Any, Callable, TypeVar

import streamlit as st

T = TypeVar("T")

_CACHE_KEY = "_admin_runtime_cache"


def _cache_store() -> dict[str, dict[str, Any]]:
    if _CACHE_KEY not in st.session_state:
        st.session_state[_CACHE_KEY] = {}
    return st.session_state[_CACHE_KEY]


def get_cached_value(
    key: str,
    loader: Callable[[], T],
    *,
    ttl_seconds: float,
    force_refresh: bool = False,
) -> T:
    now = time.time()
    cache = _cache_store()
    cached = cache.get(key)
    if (
        not force_refresh
        and isinstance(cached, dict)
        and now - float(cached.get("ts", 0.0)) <= ttl_seconds
    ):
        return cached["value"]  # type: ignore[return-value]
    value = loader()
    cache[key] = {"ts": now, "value": value}
    return value


def set_cached_value(key: str, value: Any) -> None:
    cache = _cache_store()
    cache[key] = {"ts": time.time(), "value": value}


def invalidate_cached_value(key: str) -> None:
    cache = _cache_store()
    cache.pop(key, None)


def invalidate_cache_prefix(prefix: str) -> None:
    cache = _cache_store()
    for key in list(cache.keys()):
        if key.startswith(prefix):
            cache.pop(key, None)
