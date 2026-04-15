"""Confidence scoring for release impact and feature flag link attribution.

Computes confidence scores that reflect how trustworthy a derived metric is,
based on provenance quality, telemetry coverage, sample size, and the
presence of confounders (concurrent deploys / flags).

Confidence bands (PRD lines 182-185):
    1.0       — native provenance (tag-based release_ref, explicit flag link)
    0.8-0.9   — explicit_text provenance (text-matched flag refs in PR/commit)
    0.3       — heuristic provenance (deployment_id fallback, timing correlation)

Display gates (PRD lines 296-299):
    show      — coverage >= 0.70 and min sample met
    warn      — coverage >= 0.50 and min sample met
    suppress  — coverage < 0.50 or min sample not met
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Literal


class Provenance(str, Enum):
    """How a release_ref or flag link was established."""

    native = "native"
    explicit_text = "explicit_text"
    heuristic = "heuristic"


#: Base confidence for each provenance tier (PRD lines 182-185).
PROVENANCE_CONFIDENCE: dict[Provenance, float] = {
    Provenance.native: 1.0,
    Provenance.explicit_text: 0.85,
    Provenance.heuristic: 0.3,
}

#: Minimum telemetry coverage before results are considered meaningful.
COVERAGE_WARN_THRESHOLD = 0.70
COVERAGE_SUPPRESS_THRESHOLD = 0.50

#: Default minimum session count for statistical significance (PRD minimum).
DEFAULT_MIN_SAMPLE = 30

DisplayGate = Literal["show", "warn", "suppress"]


def provenance_base_confidence(provenance: Provenance | str) -> float:
    """Return the base confidence for a provenance tier.

    Accepts either a :class:`Provenance` enum member or a raw string.
    Unknown provenance values fall back to the heuristic band.
    """
    if isinstance(provenance, str):
        try:
            provenance = Provenance(provenance)
        except ValueError:
            return PROVENANCE_CONFIDENCE[Provenance.heuristic]
    return PROVENANCE_CONFIDENCE.get(
        provenance, PROVENANCE_CONFIDENCE[Provenance.heuristic]
    )


def compute_impact_confidence(
    coverage_ratio: float,
    sample_size: int,
    concurrent_deploy_count: int,
    release_ref_confidence: float,
    min_sample: int = DEFAULT_MIN_SAMPLE,
) -> float:
    """Compute confidence score for a release impact measurement.

    The score starts from the ``release_ref_confidence`` base (reflecting how
    the release reference was established) and is attenuated by:

    * **Coverage ratio** — fraction of releases with telemetry.  Values below
      0.70 incur a significant penalty.
    * **Sample size** — ``min(1.0, sample_size / min_sample)`` ensures small
      windows are penalised.
    * **Concurrent deploys** — each additional concurrent deployment dilutes
      attribution via ``1.0 / (1 + concurrent_deploy_count)``.

    Returns a float in ``[0.0, 1.0]``.
    """
    base = _clamp(release_ref_confidence)

    # Coverage factor: linear scaling with extra penalty below threshold.
    cov = _clamp(coverage_ratio)
    if cov < COVERAGE_WARN_THRESHOLD:
        # Below 0.70 coverage the penalty is steeper — square the ratio
        # so that e.g. 0.50 coverage yields 0.25 factor instead of 0.50.
        coverage_factor = cov * cov
    else:
        coverage_factor = cov

    # Sample size factor: ramp linearly up to min_sample.
    effective_min = max(1, min_sample)
    sample_factor = _clamp(sample_size / effective_min)

    # Confounder penalty: each concurrent deploy halves attribution certainty.
    if concurrent_deploy_count > 0:
        confounder_factor = 1.0 / (1 + concurrent_deploy_count)
    else:
        confounder_factor = 1.0

    score = base * coverage_factor * sample_factor * confounder_factor
    return _clamp(score)


def compute_cohort_contamination(
    flag_key: str,
    environment: str,
    window_start: datetime,
    window_end: datetime,
    concurrent_flags: list[str],
) -> float:
    """Return contamination ratio (0.0-1.0) for a flag evaluation window.

    Contamination estimates the fraction of sessions that were exposed to
    *multiple* flags simultaneously, making it harder to attribute impact to
    a single flag.

    When no concurrent flags exist the contamination is 0.0.  Otherwise the
    estimate grows with the number of concurrent flags, approaching 1.0
    asymptotically.  The formula ``1 - 1 / (1 + len(concurrent_flags))``
    models the intuition that each additional flag increases overlap
    probability.

    Parameters
    ----------
    flag_key:
        The flag under evaluation (excluded from concurrent list if present).
    environment:
        Deployment environment (used for future per-env estimation).
    window_start, window_end:
        Evaluation window boundaries.
    concurrent_flags:
        Other flags active in the same environment during the window.
    """
    others = [f for f in concurrent_flags if f != flag_key]
    if not others:
        return 0.0

    # Asymptotic contamination: 1 flag → 0.50, 2 → 0.67, 3 → 0.75, …
    return 1.0 - 1.0 / (1 + len(others))


def classify_display_gate(coverage: float, min_sample_met: bool) -> DisplayGate:
    """Return ``'show'``, ``'warn'``, or ``'suppress'`` based on data quality.

    PRD lines 296-299:
    * ``suppress`` — coverage < 0.50 **or** minimum sample not met
    * ``warn``     — coverage in [0.50, 0.70)
    * ``show``     — coverage >= 0.70 and sample requirement satisfied
    """
    if coverage < COVERAGE_SUPPRESS_THRESHOLD or not min_sample_met:
        return "suppress"
    if coverage < COVERAGE_WARN_THRESHOLD:
        return "warn"
    return "show"


def compute_link_attribution_confidence(
    provenance: Provenance | str,
    text_match_count: int = 0,
) -> float:
    """Confidence for a flag-to-work-item link.

    * Native links (provider-explicit) get 1.0.
    * Text-matched links scale within the explicit_text band (0.8-0.9)
      based on how many independent text matches were found.
    * Heuristic links (timing correlation only) get 0.3.
    """
    if isinstance(provenance, str):
        try:
            provenance = Provenance(provenance)
        except ValueError:
            return PROVENANCE_CONFIDENCE[Provenance.heuristic]

    if provenance == Provenance.native:
        return 1.0

    if provenance == Provenance.explicit_text:
        # Scale within [0.8, 0.9] — each additional match adds confidence.
        # 1 match → 0.80, 2 → 0.85, 3+ → 0.90
        bonus = min(text_match_count, 3) / 30.0  # 0.0, ~0.033, ~0.067, 0.10
        return _clamp(0.80 + bonus, 0.80, 0.90)

    return PROVENANCE_CONFIDENCE[Provenance.heuristic]


def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    """Clamp *value* between *lo* and *hi*."""
    return max(lo, min(hi, value))
