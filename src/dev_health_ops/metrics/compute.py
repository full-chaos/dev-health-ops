from __future__ import annotations

from collections.abc import Sequence

# CHAOS-5308/CHAOS-3092: compute_daily_metrics (repo_user_commit's Python
# compute) is deleted entirely -- RepoUserCommitExecutor (native Go,
# CHAOS-4275) is the only writer of repo_metrics_daily/user_metrics_daily/
# commit_metrics now. rg confirmed zero production callers outside
# job_daily.py's now-deleted call site. commit_size_bucket, _utc_day_window,
# _median, _mean, _CommitAgg, and _UserAgg -- all private helpers that
# existed solely to serve compute_daily_metrics -- are deleted with it; none
# had any other caller. _percentile below STAYS: it has a real, unrelated
# caller (tests/fixtures/generate_fma_golden.py, which imports it directly
# as `_percentile_float` for an FMA numerical-precision comparison against
# compute_capacity._percentile's int version -- unrelated to repo_user_commit
# entirely).


def _percentile(values: Sequence[float], percentile: float) -> float:
    """
    Compute a percentile using linear interpolation between closest ranks.

    Returns 0.0 when values is empty.
    """
    if not values:
        return 0.0
    if percentile <= 0:
        return float(min(values))
    if percentile >= 100:
        return float(max(values))

    sorted_vals = sorted(float(v) for v in values)
    if len(sorted_vals) == 1:
        return float(sorted_vals[0])

    rank = (len(sorted_vals) - 1) * (float(percentile) / 100.0)
    lo = int(rank)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = rank - lo
    return sorted_vals[lo] * (1.0 - frac) + sorted_vals[hi] * frac
