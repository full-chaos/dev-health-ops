from __future__ import annotations

# Deployment statuses that count as a FAILED change. Must be PROVIDER-AGNOSTIC:
# deployment rows are persisted from the raw provider status with no
# normalization, so both vocabularies coexist in the ``deployments`` table:
#   * GitHub deployment state  -> 'failure', 'error'   (GitHub Deployment API)
#   * GitLab deployment status -> 'failed', 'canceled' (GitLab Deployment API)
# Counting only 'failure' (or only {failed,error,canceled}) silently drops one
# provider's failures and biases DORA change-failure-rate toward 0. This is the
# single source of truth shared with ``compute_dora`` and the ClickHouse
# ``deployment_daily_rollup`` MV (CHAOS-2382 / CHAOS-2395).
#
# CHAOS-5234/CHAOS-3092: compute_deploy_metrics_daily, DeploymentBucket,
# _utc_day_window, and _percentile used to live in this module too, until the
# deploy family's native Go executor (CHAOS-4293) became the sole production
# writer and the Python compute was deleted entirely. This constant is the
# ONLY symbol from this module still in production use (by compute_dora.py).
DEPLOYMENT_FAILURE_STATUSES = {"failure", "failed", "error", "canceled"}
