"""Pure scoring math shared by all flow-opportunity detectors.

Extracted from ai_detector.py so the same primitives can be reused by the
FlowOpportunityDetector (CHAOS-2218) and any future detector without
importing the AI-specific module.

All functions are side-effect-free and fully deterministic.  Tests live in
``tests/metrics/opportunities/test_scoring.py``.
"""

from __future__ import annotations

import hashlib
from typing import Any


def clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    """Clamp *value* to the closed interval [lo, hi].

    >>> clamp(1.5)
    1.0
    >>> clamp(-0.1)
    0.0
    >>> clamp(0.5)
    0.5
    """
    return max(lo, min(hi, value))


def score_ratio(value: float, threshold: float) -> float:
    """Score a ratio relative to *threshold*.

    Returns a value in [0, 1] where 0.5 corresponds to *value* == *threshold*
    and the score increases as *value* grows above the threshold.

    >>> score_ratio(2.0, 1.0)  # twice the threshold → 1.0
    1.0
    >>> score_ratio(1.0, 1.0)  # exactly at threshold → 0.5
    0.5
    >>> score_ratio(0.0, 1.0)  # below threshold → 0.0
    0.0
    """
    if threshold <= 0:
        return 0.0
    return clamp((value / threshold - 1.0) / 2.0 + 0.50)


def score_delta(value: float, threshold: float) -> float:
    """Score an absolute delta relative to *threshold*.

    Identical semantics to :func:`score_ratio` — the name communicates intent
    to callers that deal with absolute differences rather than multipliers.

    >>> score_delta(0.2, 0.1)  # twice the threshold → 1.0
    1.0
    >>> score_delta(0.1, 0.1)  # exactly at threshold → 0.5
    0.5
    """
    if threshold <= 0:
        return 0.0
    return clamp((value / threshold - 1.0) / 2.0 + 0.50)


def stable_opportunity_id(kind: Any, entity_id: str, secondary_id: str | None) -> str:
    """Return a stable, deterministic 24-hex-char SHA-256 prefix.

    The identifier is derived from ``kind.value`` (or ``str(kind)``),
    *entity_id*, and *secondary_id*.  The output is stable across process
    restarts and deterministic for the same inputs, making it safe to use
    as a deduplication key in the API.

    >>> id1 = stable_opportunity_id("HIGH_REVIEW_LATENCY", "repo-abc", None)
    >>> id2 = stable_opportunity_id("HIGH_REVIEW_LATENCY", "repo-abc", None)
    >>> id1 == id2
    True
    >>> len(id1)
    24
    """
    kind_str = kind.value if hasattr(kind, "value") else str(kind)
    raw = f"{kind_str}:{entity_id}:{secondary_id or ''}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
