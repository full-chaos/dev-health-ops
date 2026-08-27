"""Custom Prometheus metrics for dev-health-ops.

Defines application-level counters, histograms, and gauges for:
  - Celery task execution
  - ClickHouse query latency
  - LLM API calls (OpenAI / Anthropic)
  - GitHub API calls (requests by endpoint/status, rate limit remaining)

Usage:
    from dev_health_ops.metrics.prometheus import (
        CELERY_TASKS_TOTAL,
        CELERY_TASK_DURATION_SECONDS,
        record_celery_task,
        CLICKHOUSE_QUERY_DURATION_SECONDS,
        LLM_REQUESTS_TOTAL,
        LLM_TOKENS_TOTAL,
        record_llm_call,
        GITHUB_API_REQUESTS_TOTAL,
        GITHUB_RATE_LIMIT_REMAINING,
        record_github_api_request,
        record_github_rate_limit,
    )
"""

from __future__ import annotations

import time
from collections.abc import Generator
from contextlib import contextmanager
from importlib import import_module
from typing import Any

try:
    _prometheus_client_module: Any = import_module("prometheus_client")
except ImportError:
    _prometheus_client_module = None

_PROMETHEUS_AVAILABLE = _prometheus_client_module is not None


def _noop_counter(*args, **kwargs):
    class _Noop:
        def labels(self, **kw):
            return self

        def inc(self, amount=1):
            pass

        def observe(self, amount):
            pass

    return _Noop()


def _noop_histogram(*args, **kwargs):
    return _noop_counter()


def _noop_gauge(*args, **kwargs):
    class _NoopGauge:
        def labels(self, **kw):
            return self

        def set(self, value):
            pass

        def inc(self, amount=1):
            pass

        def dec(self, amount=1):
            pass

    return _NoopGauge()


if _PROMETHEUS_AVAILABLE:
    assert _prometheus_client_module is not None

    # ---------------------------------------------------------------------------
    # Celery metrics
    # ---------------------------------------------------------------------------
    CELERY_TASKS_TOTAL = _prometheus_client_module.Counter(
        "devhealth_celery_tasks_total",
        "Total number of Celery task executions",
        ["task_name", "state"],
    )

    CELERY_TASK_DURATION_SECONDS = _prometheus_client_module.Histogram(
        "devhealth_celery_task_duration_seconds",
        "Celery task execution duration in seconds",
        ["task_name"],
        buckets=(0.1, 0.5, 1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0),
    )

    REPORT_RUN_LEASE_EXPIRED_TOTAL = _prometheus_client_module.Counter(
        "worker_report_run_lease_expired_total",
        "Expired report execution leases by bounded durable result",
        ["result"],
    )

    SYNC_COVERAGE_DATASETS_EXCLUDED_BY_INTENT_TOTAL = _prometheus_client_module.Counter(
        "sync_coverage_datasets_excluded_by_intent_total",
        "Datasets dropped from a sync coverage scope because their "
        "integration_datasets row is disabled",
        ["provider"],
    )

    SYNC_TARGET_DATASET_DRIFT_REPAIRED_TOTAL = _prometheus_client_module.Counter(
        "sync_target_dataset_drift_repaired_total",
        "IntegrationDataset rows disabled because the config's sync_targets "
        "could not account for them (CHAOS-4106 drift repair)",
        ["provider"],
    )

    CREDENTIAL_MAPPING_REJECTED_TOTAL = _prometheus_client_module.Counter(
        "credential_mapping_rejected_total",
        "Stored credentials a provider resolver refused to build, by the "
        "field it could not find. The resolver returns None and the sync "
        "reports only that the mapping was invalid, so without this the "
        "failure is indistinguishable from having no credential at all "
        "(CHAOS-4224)",
        ["provider", "missing_field"],
    )

    DEV_HEALTH_TEAM_AUTOIMPORT_ROSTER_PRESERVATION_FAILED_TOTAL = (
        _prometheus_client_module.Counter(
            "dev_health_team_autoimport_roster_preservation_failed_total",
            "A members-off team-autoimport run could not confirm a team's "
            "currently persisted roster (ClickHouse unavailable, a query "
            "error, or a sink incapable of a raw read) and skipped the "
            "team-dimension write rather than risk overwriting it with an "
            "empty one (CHAOS-4323). The skip is correct and prevents data "
            "loss, but it also means that team's name/description/"
            "repo_patterns silently stop refreshing until a later run "
            "succeeds -- run_team_autoimport still reports status=success, "
            "so this counter is the only signal a degraded run happened. "
            "Alert wiring is a follow-up (see CHAOS-4184's alert pattern).",
            ["provider"],
        )
    )

    # ---------------------------------------------------------------------------
    # ClickHouse metrics
    # ---------------------------------------------------------------------------
    CLICKHOUSE_QUERY_DURATION_SECONDS = _prometheus_client_module.Histogram(
        "devhealth_clickhouse_query_duration_seconds",
        "ClickHouse query latency in seconds",
        ["query_type"],
        buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
    )

    CLICKHOUSE_QUERIES_TOTAL = _prometheus_client_module.Counter(
        "devhealth_clickhouse_queries_total",
        "Total ClickHouse queries executed",
        ["query_type", "status"],
    )

    CLICKHOUSE_ORG_SCOPE_ROWS_FILTERED_TOTAL = _prometheus_client_module.Counter(
        "devhealth_clickhouse_org_scope_rows_filtered_total",
        "Rows excluded from a ClickHouseDataLoader read by its org_id scope "
        "-- i.e. rows that matched the query's time/repo predicate but "
        "belong to a different tenant (CHAOS-4324). Computed as the delta "
        "between a same-predicate count without the org filter and the "
        "actual org-scoped row count. A near-zero rate on a busy multi-org "
        "deployment is the leak-guard signal: the previous unparenthesized "
        "OR in the PR query silently dropped org scoping for any row whose "
        "created_at matched the window, which this counter would have "
        "made visible as filtered_count == 0 despite other tenants having "
        "matching rows.",
        ["table"],
    )

    # ---------------------------------------------------------------------------
    # LLM metrics
    # ---------------------------------------------------------------------------
    LLM_REQUESTS_TOTAL = _prometheus_client_module.Counter(
        "devhealth_llm_requests_total",
        "Total LLM API requests",
        ["provider", "model", "status"],
    )

    LLM_TOKENS_TOTAL = _prometheus_client_module.Counter(
        "devhealth_llm_tokens_total",
        "Total LLM tokens consumed",
        ["provider", "model", "token_type"],
    )

    LLM_REQUEST_DURATION_SECONDS = _prometheus_client_module.Histogram(
        "devhealth_llm_request_duration_seconds",
        "LLM API request latency in seconds",
        ["provider", "model"],
        buckets=(0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
    )

    BYO_LLM_BASE_URL_FALLBACK_TOTAL = _prometheus_client_module.Counter(
        "devhealth_byo_llm_base_url_fallback_total",
        "Org BYO LLM base_url fallbacks by provider and reason",
        ["provider", "reason_code", "audit_inserted"],
    )

    BYO_LLM_BASE_URL_FALLBACK_ALERT_TOTAL = _prometheus_client_module.Counter(
        "devhealth_byo_llm_base_url_fallback_alert_total",
        "Sustained org BYO LLM base_url fallback alert signals",
        ["provider", "reason_code"],
    )

    # ---------------------------------------------------------------------------
    # GitHub API metrics
    # ---------------------------------------------------------------------------
    GITHUB_API_REQUESTS_TOTAL = _prometheus_client_module.Counter(
        "devhealth_github_api_requests_total",
        "Total GitHub API requests by endpoint and status code",
        ["endpoint", "status_code"],
    )

    GITHUB_RATE_LIMIT_REMAINING = _prometheus_client_module.Gauge(
        "devhealth_github_rate_limit_remaining",
        "GitHub API rate limit remaining calls by resource type",
        ["resource"],
    )

    INVESTMENT_MEMBERSHIP_SCOPE_STALE_TOTAL = _prometheus_client_module.Counter(
        "devhealth_investment_membership_scope_stale_total",
        "Investment reads that fell back to unscoped results due to stale membership projection",
        ["scope_mode"],
    )

    INVESTMENT_MEMBERSHIP_SCOPE_LAG_SECONDS = _prometheus_client_module.Gauge(
        "devhealth_investment_membership_scope_lag_seconds",
        "Lag between latest work_unit_investments row and latest membership run when stale",
        ["scope_mode"],
    )

    # ---------------------------------------------------------------------------
    # Ask Dev metrics
    # ---------------------------------------------------------------------------
    ASK_DEV_UNREGISTERED_TERMINAL_CODE_TOTAL = _prometheus_client_module.Counter(
        "devhealth_ask_dev_unregistered_terminal_code_total",
        "Ask Dev orchestrator terminal error codes outside terminal_frames."
        "ORCHESTRATOR_ERROR_CODES, falling back to the internal_error frame bucket "
        "(a closed-registry drift bug, not a transient failure)",
        ["code"],
    )

    ASK_DEV_TOOL_EXECUTOR_FAULT_TOTAL = _prometheus_client_module.Counter(
        "devhealth_ask_dev_tool_executor_fault_total",
        "Ask Dev tool executors that raised outside their declared contract "
        "(not a rejection, timeout, or cancellation) and were degraded to one "
        "failed tool result instead of terminating the run",
        ["tool_id", "exception_type"],
    )

    ASK_DEV_UNHANDLED_RUN_FAULT_TOTAL = _prometheus_client_module.Counter(
        "devhealth_ask_dev_unhandled_run_fault_total",
        "Ask Dev runs terminated by the orchestrator's catch-all internal_error "
        "handler (every increment is an unclassified server defect)",
        ["exception_type"],
    )

    ASK_DEV_INTERNAL_TOKEN_LEAK_TOTAL = _prometheus_client_module.Counter(
        "devhealth_ask_dev_internal_token_leak_total",
        "Ask Dev terminals rejected at the boundary because a user-visible "
        "string carried an internal vocabulary token (CHAOS-3367). Every "
        "increment is a producer defect that would otherwise have reached a "
        "customer; this must stay at zero.",
        ["token", "terminal_kind"],
    )

    ASK_DEV_PLAN_REGISTRY_GAP_TOTAL = _prometheus_client_module.Counter(
        "devhealth_ask_dev_plan_registry_gap_total",
        "Ask Dev requests for an intent preflight_outcomes.PLAN_ID_BY_INTENT names "
        "a real plan for, that fell back to the legacy model-tool-choice loop "
        "because this runtime's plan_registry does not carry that plan yet "
        "(CHAOS-3300 finding: a silent capability downgrade unless observed here)",
        ["intent"],
    )

    ASK_DEV_NARRATIVE_FALLBACK_TOTAL = _prometheus_client_module.Counter(
        "devhealth_ask_dev_narrative_fallback_total",
        "Ask Dev narrative provider calls that fell back to the deterministic "
        "narrative built from the frame alone, by safe failure code "
        "(CHAOS-3297 stack #4: answer_frames.narrative_fallback). The "
        "'provider_unknown_failure' code labels a provider exception outside "
        "the closed classification table -- a classifier gap, not a genuine "
        "unclassifiable failure, and should trend to zero.",
        ["failure_code"],
    )

    # ---------------------------------------------------------------------------
    # Ask Dev Question Understanding Agent shadow mode (CHAOS-3389)
    # ---------------------------------------------------------------------------
    ASK_DEV_QUA_SHADOW_TOTAL = _prometheus_client_module.Counter(
        "devhealth_ask_dev_qua_shadow_total",
        "Ask Dev QUA shadow evaluations by outcome status (qua_shadow."
        "QUAShadowStatus, content-free) and the deterministic preflight "
        "decision (proceed/terminate) it ran alongside. Every increment is "
        "shadow-only telemetry -- it never affects any live run.",
        ["status", "deterministic_decision"],
    )

    # CHAOS-3525: a QUA proposal that actually became the run's subject.
    # Distinct from the shadow counter above on purpose -- that one counts
    # observations, this one counts DECISIONS, and conflating them would hide
    # the promotion rate inside the evaluation rate. Labelled by entity kind
    # only: content-free, like every other Ask Dev metric.
    ASK_DEV_QUA_COMMIT_TOTAL = _prometheus_client_module.Counter(
        "devhealth_ask_dev_qua_commit_total",
        "Ask Dev subjects committed from a verified QUA proposal, by entity "
        "kind. Increments only where the deterministic layer declined and the "
        "proposal passed the commit-time authorization re-check.",
        ["entity_kind"],
    )

    ASK_DEV_QUA_SHADOW_LATENCY_SECONDS = _prometheus_client_module.Histogram(
        "devhealth_ask_dev_qua_shadow_latency_seconds",
        "Ask Dev QUA shadow provider-call latency, by outcome status. Feeds "
        "the future probe-certification's latency budget evidence -- "
        "recorded, never gated on.",
        ["status"],
        buckets=(0.1, 0.25, 0.5, 1.0, 1.5, 2.0, 2.5, 5.0),
    )

    ASK_DEV_PLATFORM_MODEL_UNPRICED_TOTAL = _prometheus_client_module.Counter(
        "devhealth_ask_dev_platform_model_unpriced_total",
        "Ask Dev platform providers constructed with an OpenAI model that has "
        "no entry in _PLATFORM_MODEL_PRICES. Every such run books the "
        "worst-case admission reservation as its cost instead of a real "
        "figure, so allowance usage for this organization is an overstatement "
        "-- CHAOS-3552 measured 222x on gpt-5-nano. Non-zero means an operator "
        "must price the model or change LLM_MODEL; it is a configuration "
        "defect, not a provider fault.",
        ["model", "reason"],
    )
    ASK_DEV_QUA_SHADOW_FAULT_TOTAL = _prometheus_client_module.Counter(
        "devhealth_ask_dev_qua_shadow_fault_total",
        "Ask Dev QUA shadow evaluations or shadow-record writes that raised "
        "outside qua_shadow.py's own defensive handling, caught at the "
        "orchestrator call site so a shadow-mode bug can never fail or roll "
        "back the live run it shadows. Every increment is a shadow-path "
        "defect, not a live-path one.",
        ["exception_type"],
    )

    ASK_DEV_QUA_SHADOW_CARDINALITY_UNCORROBORATED_TOTAL = (
        _prometheus_client_module.Counter(
            "devhealth_ask_dev_qua_shadow_cardinality_uncorroborated_total",
            "Ask Dev QUA shadow proposals with cardinality=organization_wide "
            "that the deterministic interpreter did NOT independently reach "
            "the same cardinality for (CHAOS-3389 adversarial critique "
            "hardening condition: an org-wide proposal is a model-proposed "
            "widening channel and must never be trusted without deterministic "
            "corroboration -- shadow mode records this but never acts on it).",
            ["intent"],
        )
    )

    # CHAOS-4026 (2026-08-21): the Ask Dev retention sweep (CHAOS-3404)
    # prometheus instruments (ASK_DEV_RETENTION_SWEEP_TOTAL,
    # ASK_DEV_RETENTION_SWEEP_PURGED_TOTAL,
    # ASK_DEV_RETENTION_SWEEP_LAST_SUCCESS_TIMESTAMP) and
    # record_ask_dev_retention_sweep() below were deleted along with their
    # sole caller, workers/ask_dev_retention.py -- Go now owns this cadence
    # (CHAOS-3481, producer_version 3 active).

    # ---------------------------------------------------------------------------
    # team_metrics_daily per-repo fan-out (CHAOS-4329)
    # ---------------------------------------------------------------------------
    DEV_HEALTH_TEAM_METRICS_DAILY_REPO_COUNT = _prometheus_client_module.Histogram(
        "dev_health_team_metrics_daily_repo_count",
        "Distinct repo_id rows written to team_metrics_daily per (team_id, "
        "day) in one write_team_metrics call. Before repo_id existed on this "
        "table, a team spanning N repos always wrote N rows into the SAME "
        "(team_id, day) key and every reader's argMax(computed_at) kept only "
        "the last-written repo's slice -- the other N-1 were silently "
        "invisible. This makes the real per-team repo fan-out an observable "
        "series instead of something only a bug report could surface. A "
        "value of 1 is an ordinary single-repo team, not a defect.",
        buckets=(1, 2, 3, 5, 8, 13, 21, 34, 55),
    )

    # ---------------------------------------------------------------------------
    # Recommendations readiness gate (CHAOS-4073)
    # ---------------------------------------------------------------------------
    RECOMMENDATIONS_READINESS_GATE_FAIL_OPEN_TOTAL = _prometheus_client_module.Counter(
        "devhealth_recommendations_readiness_gate_fail_open_total",
        "_daily_metrics_ready reads (daily_metrics_runs / "
        "fixed_schedule_occurrences) that raised, so the gate fell open and "
        "let recommendations proceed with no positive completion evidence "
        "for the day. OWNER RULING (CHAOS-4073 item 2): fail-open stays -- "
        "fail-closed would wire an unknown gate-error rate directly to an "
        "org-wide recommendations wedge with no tombstones (CHAOS-2373) -- "
        "but every occurrence must be loud and alertable here, not merely a "
        "logger line, so a sustained outage or schema drift cannot sit "
        "silently vacuous the way the CHAOS-4066 dead checkpoint did.",
        ["exception_type"],
    )

    # ---------------------------------------------------------------------------
    # Integration credentials (issue 3694)
    # ---------------------------------------------------------------------------
    INTEGRATION_CREDENTIAL_DECRYPT_FAILED_TOTAL = _prometheus_client_module.Counter(
        "devhealth_integration_credential_decrypt_failed_total",
        "Stored integration credential rows whose credentials_encrypted "
        "payload existed but decrypt_value/json.loads raised when read -- "
        "a key-mismatch class of failure (e.g. a rotated "
        "SETTINGS_ENCRYPTION_KEY that can no longer decrypt rows written "
        "under the old one). Every increment means a real credential a "
        "caller believed was configured is now unusable; this must trend "
        "to zero, never merely be a grep-able log line.",
        ["provider"],
    )

    # ---------------------------------------------------------------------------
    # metrics.daily_partition per-family visibility (CHAOS-4246)
    # ---------------------------------------------------------------------------
    DEV_HEALTH_METRICS_FAMILY_FAILURES_TOTAL = _prometheus_client_module.Counter(
        "dev_health_metrics_family_failures_total",
        "job_daily.py sub-families (cicd/deploy/incident/testops_risk) that "
        "computed zero rows for a (org, repo, day) the partition otherwise "
        "reported succeeded for. Zero rows can be a legitimate day (no CI "
        "activity) or a genuine gap (upstream sync lag, unmapped join) -- "
        "this counter does not distinguish the two, it only makes the "
        "silence visible (CHAOS-4246: cicd/deploy/testops_risk went stale "
        "for 16 days with every partition reporting success).",
        ["family", "cause"],
    )

    WORK_ITEM_TEAM_ATTRIBUTIONS_WRITTEN_TOTAL = _prometheus_client_module.Counter(
        "dev_health_work_item_team_attributions_written_total",
        "work_item_team_attributions rows written, by provider and winning "
        "source (CHAOS-4244). source='unassigned' is the residual: "
        "sum(...{source='unassigned'}) / sum(...) is chris's <=2% target, "
        "readable per provider without a ClickHouse query.",
        ["provider", "source"],
    )

    TEAM_ATTRIBUTION_MEMBERSHIP_LAYER_TOTAL = _prometheus_client_module.Counter(
        "dev_health_team_attribution_membership_layer_total",
        "assignee_membership/author_membership resolutions by which layer "
        "resolved them (CHAOS-4321, chris 2026-08-26 10:39 PT: 'admin is an "
        "override, not a default'). layer='admin_override' means "
        "identities.team_ids or teams.manual_members had a single-team "
        "answer; layer='provider_fallback' means the admin layer had zero "
        "candidates and team_memberships or teams.members (the unreviewed "
        "auto-import roster) resolved it instead. Incremented at "
        "resolution time in _resolve_membership, not derived post-hoc from "
        "the written row, since specificity alone cannot reliably "
        "distinguish a real team_memberships row's arbitrary specificity "
        "from the fixed values this ticket's two synthetic untyped-facet "
        "pools use.",
        ["layer"],
    )

    # ---------------------------------------------------------------------------
    # Metric compatibility bridge runner subprocess (CHAOS-4264)
    # ---------------------------------------------------------------------------
    DEV_HEALTH_METRIC_COMPAT_RUNNER_RSS_BYTES = _prometheus_client_module.Gauge(
        "dev_health_metric_compat_runner_rss_bytes",
        "Peak resident set size observed for the metric compatibility bridge "
        "runner subprocess (worker_metrics_runner), sampled from "
        "/proc/<pid>/status while the child runs so a kernel OOM kill "
        "(SIGKILL, no graceful exit) still leaves a reading (CHAOS-4264: "
        "the runner reached 1.7 GB inside a 2 GiB api container with no "
        "cgroup-level signal reaching Docker or SigNoz).",
        ["worker_kind"],
    )
    DEV_HEALTH_METRIC_COMPAT_PROCESS_EXITS_TOTAL = _prometheus_client_module.Counter(
        "dev_health_metric_compat_process_exits_total",
        "Metric compatibility bridge runner subprocess exits by classified "
        "reason. 'success' and 'process_failed' are ordinary outcomes; "
        "'process_signaled' and 'resource_exhausted' mean the process never "
        "returned its own exit path (killed) or hit its self-imposed "
        "memory bound -- both are the CHAOS-4264 failure class that used to "
        "surface only as an opaque -9 in Sentry.",
        ["reason"],
    )
    DEV_HEALTH_METRIC_COMPAT_EXECUTION_DURATION_SECONDS = (
        _prometheus_client_module.Histogram(
            "dev_health_metric_compat_execution_duration_seconds",
            "Wall-clock duration of one metric compatibility bridge "
            "execution (subprocess spawn through exit), labeled by "
            "worker_kind/operation so a daily partition's long tail is "
            "distinguishable from a remaining-metrics family's.",
            ["worker_kind", "operation"],
            buckets=(1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0, 600.0),
        )
    )
    # CHAOS-4319: the bounded terminal disposition of an ambiguous_refused
    # metric-compatibility-execution ledger row. "retry_authorized" is
    # emitted here, in _mark_retry_authorized, the moment a classified
    # runner failure is handed straight back to River as retryable instead
    # of sticking at "ambiguous"; "persisted_failed" is the Go-side mirror
    # label (internal/jobruntime, same metric name and axis) for the case
    # that still lands genuinely stuck. The two halves of one decision live
    # in different languages because each is only ever observed from the
    # side that made it.
    DEV_HEALTH_METRIC_COMPAT_RETRY_TOTAL = _prometheus_client_module.Counter(
        "dev_health_metric_compat_retry_total",
        "Terminal disposition of an ambiguous_refused metrics.daily "
        "compatibility-bridge execution, by worker_kind and bounded "
        "decision (CHAOS-4319).",
        ["worker_kind", "decision"],
    )

    # ---------------------------------------------------------------------------
    # Metric compatibility bridge liveness bound (CHAOS-4316)
    # ---------------------------------------------------------------------------
    DEV_HEALTH_METRIC_COMPAT_LIVENESS_KILL_TOTAL = _prometheus_client_module.Counter(
        "dev_health_metric_compat_liveness_kill_total",
        "Daily-metrics compatibility bridge runner subprocesses killed by "
        "the bridge's own liveness watchdog because ComputePartition had no "
        "wall-clock/renewal-based bound (CHAOS-4316). 'stalled' means no "
        "progress line arrived within the per-repo-derived stall window; "
        "'timeout' means the hard ceiling backstop fired despite trickling "
        "progress; 'oom' means the kill coincided with a real memcg OOM "
        "signal (preferred) or, when that signal is unavailable, peak RSS "
        "near the configured memory bound (CHAOS-4264) -- distinguishing a "
        "genuine hang from a memory-pressure kill misclassified as a hang.",
        ["reason"],
    )
    DEV_HEALTH_METRIC_COMPAT_CHILD_SILENCE_SECONDS = (
        _prometheus_client_module.Histogram(
            "dev_health_metric_compat_child_silence_seconds",
            "Seconds since the last progress line (or process start, if "
            "none was ever seen) at the moment the CHAOS-4316 liveness "
            "watchdog decided to kill the runner subprocess. High values "
            "with reason='stalled' point at a genuinely hung child; the "
            "same metric for 'timeout' shows how close trickling progress "
            "came to dodging the interval check before the hard ceiling won.",
            ["reason"],
            buckets=(30.0, 60.0, 120.0, 300.0, 600.0, 1200.0, 1800.0),
        )
    )
    DEV_HEALTH_METRIC_COMPAT_RUNNER_SLOTS_IN_USE = _prometheus_client_module.Gauge(
        "dev_health_metric_compat_runner_slots_in_use",
        "Runner subprocess slots currently held against this replica's "
        "process-local _RUNNER_CONCURRENCY_SEMAPHORE (CHAOS-4316). A value "
        "pinned at the configured max concurrency for an extended period is "
        "the same signature the 2026-08-26 incident showed: every partition "
        "routed to this replica queues behind one stuck child while sibling "
        "replicas keep working.",
    )

    # CHAOS-4350: per-partition row volume for the ClickHouseDataLoader
    # testops reads (test_suite_results / test_case_results). These two
    # tables are the highest-cardinality reads in metrics/loaders/clickhouse.py
    # -- a single load_testops_test_data call spans a rolling 30-day window
    # org-wide with no cap -- so this histogram is what makes an
    # unexpectedly huge partition visible before it OOMs the compatibility
    # runner, rather than after.
    DEV_HEALTH_TESTOPS_LOADER_ROWS_LOADED = _prometheus_client_module.Histogram(
        "devhealth_testops_loader_rows_loaded",
        "Rows returned by a single ClickHouseDataLoader testops query, by "
        "source table (CHAOS-4350).",
        ["table"],
        buckets=(10, 100, 1_000, 10_000, 50_000, 100_000, 200_000, 500_000),
    )

    # Incremented whenever a testops loader read hit the hard row cap. Unlike
    # an ordinary degrade counter, a nonzero rate here means the READ FAILED
    # (TestopsRowCapExceeded, a MemoryError subclass) rather than returning a
    # partial/truncated sample -- test_suite_results/test_case_results are
    # ordered by (repo_id, run_id, ...), not event time, so letting compute
    # proceed on a capped-but-unordered result could silently produce wrong
    # testops metrics (drop today's rows or whole repos while keeping stale
    # ones). See the accompanying error log line for the org/table/count.
    DEV_HEALTH_TESTOPS_LOADER_ROW_CAP_EXCEEDED_TOTAL = (
        _prometheus_client_module.Counter(
            "devhealth_testops_loader_row_cap_exceeded_total",
            "Testops loader reads that hit the hard row cap and were refused "
            "(the read raises instead of computing on a partial result) "
            "(CHAOS-4350).",
            ["table"],
        )
    )

    # ---------------------------------------------------------------------------
    # Metric compatibility bridge pids/thread capacity bound (CHAOS-4317)
    # ---------------------------------------------------------------------------
    DEV_HEALTH_METRIC_COMPAT_PIDS_CURRENT = _prometheus_client_module.Gauge(
        "dev_health_metric_compat_pids_current",
        "This api container's live cgroup pids.current, sampled every time "
        "the CHAOS-4317 capacity gate checks headroom before spawning a "
        "runner subprocess. Reacts to every thread/process consumer in the "
        "container -- OTel init, sync_run threads, other requests -- not "
        "just this feature's own subprocess count, which is what the "
        "2026-08-26 incident's pthread_create failures actually exhausted.",
    )
    DEV_HEALTH_METRIC_COMPAT_PIDS_CEILING = _prometheus_client_module.Gauge(
        "dev_health_metric_compat_pids_ceiling",
        "This container's cgroup pids.max at capacity-gate check time, or "
        "a documented fallback constant when pids.max is unset/unbounded "
        "or unreadable. RLIMIT_NPROC and host kernel.threads-max are "
        "deliberately NOT consulted (codex review, PR #1931 round 2): "
        "neither is scoped to this container's cgroup, so mixing them in "
        "could under-report a real host-wide exhaustion. Exists so an "
        "alert can compare dev_health_metric_compat_pids_current against "
        "the REAL current limit instead of a hardcoded number (CHAOS-4317 "
        "to-do item 3: alert at 80% of this ratio).",
    )
    DEV_HEALTH_METRIC_COMPAT_PIDS_WAIT_SECONDS = _prometheus_client_module.Histogram(
        "dev_health_metric_compat_pids_wait_seconds",
        "Seconds one partition's runner spawn waited on the CHAOS-4317 "
        "capacity gate before pids headroom was available. Zero in the "
        "common case; the visible proof that a partition queued for "
        "capacity rather than being dropped or erroring immediately.",
        buckets=(0.0, 1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0),
    )
    DEV_HEALTH_METRIC_COMPAT_CAPACITY_WAIT_EXHAUSTED_TOTAL = (
        _prometheus_client_module.Counter(
            "dev_health_metric_compat_capacity_wait_exhausted_total",
            "Runner spawns that waited past the CHAOS-4317 capacity gate's "
            "derived wait ceiling and gave up, returning a retryable "
            "capacity_exhausted outcome to the Go caller instead of "
            "spawning over budget. Should be near-zero in steady state; a "
            "real alert signal for sustained (not burst) pids starvation.",
        )
    )

    DEV_HEALTH_OTEL_INIT_FAILURES_TOTAL = _prometheus_client_module.Counter(
        "dev_health_otel_init_failures_total",
        "OpenTelemetry tracer initialisation failures (tracing.py "
        "init_tracing), by attempt. Before CHAOS-4317 a failure here (e.g. "
        "pthread_create hitting the same pids ceiling metric-bridge "
        "spawning does) was a bare logger.warning that never surfaced "
        "anywhere else -- tracing silently stayed off for the rest of the "
        "process. 'final' means the retry was also exhausted: the process "
        "is still running (a disabled tracer is not worth crashing the api "
        "over) but this counter is the durable, alertable signal that it is.",
        ["attempt"],
    )

    # CHAOS-4350 PR 2: per-call size of the historical-failed-case-names
    # aggregate query, which replaced fetching every raw case row for the
    # 29-day historical window with a `GROUP BY case_name` bounded by
    # distinct failing test names. ROWS_FETCHED is what actually crossed the
    # wire into Python (the aggregate row count -- small); ROWS_AGGREGATED_FROM
    # is a separate unfiltered `count()` over the same joined window/scope
    # (codex round 2: summing the failure-only aggregate's `occurrences`
    # undercounted this -- it only ever reflected failed rows), i.e. the raw
    # case-row volume this aggregation replaced -- the gap between the two IS
    # the measured win. On the real local-stack repo that motivated this (org
    # 70d529e0 / repo 7b9583ee, ~1.1M raw case rows/30d), ROWS_AGGREGATED_FROM
    # tracks what PR 1 alone would still have had to materialize for this
    # signal; ROWS_FETCHED tracks what this query actually returns.
    DEV_HEALTH_TESTOPS_HISTORICAL_ROWS_FETCHED = _prometheus_client_module.Histogram(
        "devhealth_testops_historical_rows_fetched",
        "Distinct (repo_id, case_name) rows returned by the historical "
        "failed-case-names aggregate query per call (CHAOS-4350 PR 2).",
        buckets=(1, 10, 50, 100, 500, 1_000, 5_000, 10_000),
    )
    DEV_HEALTH_TESTOPS_HISTORICAL_ROWS_AGGREGATED_FROM = (
        _prometheus_client_module.Histogram(
            "devhealth_testops_historical_rows_aggregated_from",
            "Sum of per-case-name occurrence counts behind one historical "
            "failed-case-names aggregate call -- the raw test_case_results "
            "row volume this query's GROUP BY replaced (CHAOS-4350 PR 2).",
            buckets=(
                1,
                100,
                1_000,
                10_000,
                50_000,
                100_000,
                500_000,
                1_000_000,
                2_000_000,
            ),
        )
    )

else:
    # Graceful no-ops when prometheus_client is unavailable
    CELERY_TASKS_TOTAL = _noop_counter()
    CELERY_TASK_DURATION_SECONDS = _noop_histogram()
    REPORT_RUN_LEASE_EXPIRED_TOTAL = _noop_counter()
    SYNC_COVERAGE_DATASETS_EXCLUDED_BY_INTENT_TOTAL = _noop_counter()
    SYNC_TARGET_DATASET_DRIFT_REPAIRED_TOTAL = _noop_counter()
    CREDENTIAL_MAPPING_REJECTED_TOTAL = _noop_counter()
    DEV_HEALTH_TEAM_AUTOIMPORT_ROSTER_PRESERVATION_FAILED_TOTAL = _noop_counter()
    CLICKHOUSE_QUERY_DURATION_SECONDS = _noop_histogram()
    CLICKHOUSE_QUERIES_TOTAL = _noop_counter()
    CLICKHOUSE_ORG_SCOPE_ROWS_FILTERED_TOTAL = _noop_counter()
    LLM_REQUESTS_TOTAL = _noop_counter()
    LLM_TOKENS_TOTAL = _noop_counter()
    LLM_REQUEST_DURATION_SECONDS = _noop_histogram()
    BYO_LLM_BASE_URL_FALLBACK_TOTAL = _noop_counter()
    BYO_LLM_BASE_URL_FALLBACK_ALERT_TOTAL = _noop_counter()
    GITHUB_API_REQUESTS_TOTAL = _noop_counter()
    GITHUB_RATE_LIMIT_REMAINING = _noop_gauge()
    INVESTMENT_MEMBERSHIP_SCOPE_STALE_TOTAL = _noop_counter()
    INVESTMENT_MEMBERSHIP_SCOPE_LAG_SECONDS = _noop_gauge()
    ASK_DEV_UNREGISTERED_TERMINAL_CODE_TOTAL = _noop_counter()
    ASK_DEV_TOOL_EXECUTOR_FAULT_TOTAL = _noop_counter()
    ASK_DEV_UNHANDLED_RUN_FAULT_TOTAL = _noop_counter()
    ASK_DEV_INTERNAL_TOKEN_LEAK_TOTAL = _noop_counter()
    ASK_DEV_PLAN_REGISTRY_GAP_TOTAL = _noop_counter()
    ASK_DEV_NARRATIVE_FALLBACK_TOTAL = _noop_counter()
    ASK_DEV_QUA_SHADOW_TOTAL = _noop_counter()
    ASK_DEV_QUA_COMMIT_TOTAL = _noop_counter()
    ASK_DEV_QUA_SHADOW_LATENCY_SECONDS = _noop_histogram()
    ASK_DEV_PLATFORM_MODEL_UNPRICED_TOTAL = _noop_counter()
    ASK_DEV_QUA_SHADOW_FAULT_TOTAL = _noop_counter()
    ASK_DEV_QUA_SHADOW_CARDINALITY_UNCORROBORATED_TOTAL = _noop_counter()
    ASK_DEV_RETENTION_SWEEP_TOTAL = _noop_counter()
    ASK_DEV_RETENTION_SWEEP_PURGED_TOTAL = _noop_counter()
    ASK_DEV_RETENTION_SWEEP_LAST_SUCCESS_TIMESTAMP = _noop_gauge()
    RECOMMENDATIONS_READINESS_GATE_FAIL_OPEN_TOTAL = _noop_counter()
    INTEGRATION_CREDENTIAL_DECRYPT_FAILED_TOTAL = _noop_counter()
    DEV_HEALTH_METRICS_FAMILY_FAILURES_TOTAL = _noop_counter()
    WORK_ITEM_TEAM_ATTRIBUTIONS_WRITTEN_TOTAL = _noop_counter()
    TEAM_ATTRIBUTION_MEMBERSHIP_LAYER_TOTAL = _noop_counter()
    DEV_HEALTH_METRIC_COMPAT_RUNNER_RSS_BYTES = _noop_gauge()
    DEV_HEALTH_METRIC_COMPAT_PROCESS_EXITS_TOTAL = _noop_counter()
    DEV_HEALTH_METRIC_COMPAT_EXECUTION_DURATION_SECONDS = _noop_histogram()
    DEV_HEALTH_METRIC_COMPAT_RETRY_TOTAL = _noop_counter()
    DEV_HEALTH_TESTOPS_LOADER_ROWS_LOADED = _noop_histogram()
    DEV_HEALTH_TESTOPS_LOADER_ROW_CAP_EXCEEDED_TOTAL = _noop_counter()
    DEV_HEALTH_TESTOPS_HISTORICAL_ROWS_FETCHED = _noop_histogram()
    DEV_HEALTH_TESTOPS_HISTORICAL_ROWS_AGGREGATED_FROM = _noop_histogram()
    DEV_HEALTH_TEAM_METRICS_DAILY_REPO_COUNT = _noop_histogram()
    DEV_HEALTH_METRIC_COMPAT_LIVENESS_KILL_TOTAL = _noop_counter()
    DEV_HEALTH_METRIC_COMPAT_CHILD_SILENCE_SECONDS = _noop_histogram()
    DEV_HEALTH_METRIC_COMPAT_RUNNER_SLOTS_IN_USE = _noop_gauge()
    DEV_HEALTH_METRIC_COMPAT_PIDS_CURRENT = _noop_gauge()
    DEV_HEALTH_METRIC_COMPAT_PIDS_CEILING = _noop_gauge()
    DEV_HEALTH_METRIC_COMPAT_PIDS_WAIT_SECONDS = _noop_histogram()
    DEV_HEALTH_METRIC_COMPAT_CAPACITY_WAIT_EXHAUSTED_TOTAL = _noop_counter()
    DEV_HEALTH_OTEL_INIT_FAILURES_TOTAL = _noop_counter()


# ---------------------------------------------------------------------------
# Convenience helpers
# ---------------------------------------------------------------------------


def work_item_team_attribution_metric_source(source: str, evidence: str) -> str:
    """Map a written work_item_team_attributions row onto CHAOS-4244's
    coarser written-source vocabulary for WORK_ITEM_TEAM_ATTRIBUTIONS_WRITTEN_TOTAL.

    Deliberately coarser than the ClickHouse `source` enum
    (native_team/issue_project/project_ownership/repo_ownership/
    assignee_membership/linked_issue/author_membership/manual_fallback/
    unassigned): author_membership and assignee_membership are now separate
    stored sources (CHAOS-4244's precedence ruling gave the author its own
    rank 6, below linked_issue), so this collapses to a plain per-source
    label rather than the earlier evidence-prefix split. An `unassigned` row
    carrying `no_candidate:<reason>` (bot_author, ambiguous_membership --
    resolve_team_attribution's reporter-skip precision conditions) surfaces
    that reason as its own label instead of folding back into the generic
    "unassigned" bucket. Mirrors Go's
    githubWorkItemTeamAttributionMetricSource exactly; keep both in sync.
    """
    if source == "author_membership":
        return "author"
    if source == "assignee_membership":
        return "assignee"
    if source in ("project_ownership", "issue_project"):
        return "project"
    if source == "repo_ownership":
        return "repo"
    if source == "unassigned":
        prefix = "no_candidate:"
        if evidence.startswith(prefix) and len(evidence) > len(prefix):
            reason = evidence[len(prefix) :]
            # CHAOS-4321: an ambiguous-membership reason carries the
            # colliding team ids after a second ":" (e.g.
            # "ambiguous_admin_membership:team-ops,team-platform") so an
            # admin can act on the persisted evidence text -- but a
            # Prometheus label must stay bounded cardinality, so only the
            # reason NAME (before that ":") becomes the label value here.
            return reason.split(":", 1)[0]
        return "unassigned"
    return source


def record_celery_task(task_name: str, state: str, duration_seconds: float) -> None:
    """Record Celery task completion metrics."""
    CELERY_TASKS_TOTAL.labels(task_name=task_name, state=state).inc()
    if state == "success":
        CELERY_TASK_DURATION_SECONDS.labels(task_name=task_name).observe(
            duration_seconds
        )


def record_llm_call(
    provider: str,
    model: str,
    status: str,
    duration_seconds: float,
    prompt_tokens: int = 0,
    completion_tokens: int = 0,
) -> None:
    """Record an LLM API call with token usage."""
    LLM_REQUESTS_TOTAL.labels(provider=provider, model=model, status=status).inc()
    LLM_REQUEST_DURATION_SECONDS.labels(provider=provider, model=model).observe(
        duration_seconds
    )
    if prompt_tokens > 0:
        LLM_TOKENS_TOTAL.labels(
            provider=provider, model=model, token_type="prompt"
        ).inc(prompt_tokens)
    if completion_tokens > 0:
        LLM_TOKENS_TOTAL.labels(
            provider=provider, model=model, token_type="completion"
        ).inc(completion_tokens)


def record_byo_llm_base_url_fallback(
    *, provider: str, reason_code: str, audit_inserted: str
) -> None:
    BYO_LLM_BASE_URL_FALLBACK_TOTAL.labels(
        provider=provider,
        reason_code=reason_code,
        audit_inserted=audit_inserted,
    ).inc()


def record_byo_llm_base_url_fallback_alert(
    *, provider: str, reason_code: str, threshold: str, window_seconds: str
) -> None:
    _ = (threshold, window_seconds)
    BYO_LLM_BASE_URL_FALLBACK_ALERT_TOTAL.labels(
        provider=provider,
        reason_code=reason_code,
    ).inc()


def record_credential_mapping_rejected(*, provider: str, missing_field: str) -> None:
    """Record a stored credential a resolver could not build.

    ``missing_field`` is drawn from a fixed per-provider vocabulary by the
    caller, never from credential contents -- an unbounded label here would
    be both a cardinality problem and a way for credential material to
    reach the metrics endpoint.
    """
    CREDENTIAL_MAPPING_REJECTED_TOTAL.labels(
        provider=provider,
        missing_field=missing_field,
    ).inc()


def record_team_autoimport_roster_preservation_failed(*, provider: str) -> None:
    """Record a members-off team-autoimport run that could not confirm a
    team's current roster and skipped the team-dimension write (CHAOS-4323).

    Called from each provider's team_autoimport_<provider>.py wherever
    ``roster_write_safe`` becomes False -- see
    ``_existing_team_members``'s docstring in team_autoimport_github.py for
    the full data-loss rationale this skip (and this counter) exist to
    close.
    """
    DEV_HEALTH_TEAM_AUTOIMPORT_ROSTER_PRESERVATION_FAILED_TOTAL.labels(
        provider=provider,
    ).inc()


def record_github_api_request(endpoint: str, status_code: str) -> None:
    """Record a GitHub API request with endpoint and HTTP status code."""
    GITHUB_API_REQUESTS_TOTAL.labels(endpoint=endpoint, status_code=status_code).inc()


def record_github_rate_limit(resource: str, remaining: int) -> None:
    """Update the GitHub rate limit remaining gauge for a resource type."""
    GITHUB_RATE_LIMIT_REMAINING.labels(resource=resource).set(remaining)


def record_investment_membership_scope_stale(
    *, lag_seconds: int, scope_mode: str
) -> None:
    INVESTMENT_MEMBERSHIP_SCOPE_STALE_TOTAL.labels(scope_mode=scope_mode).inc()
    INVESTMENT_MEMBERSHIP_SCOPE_LAG_SECONDS.labels(scope_mode=scope_mode).set(
        lag_seconds
    )


def record_team_metrics_daily_repo_rows(rows: list[Any]) -> None:
    """Observe the distinct repo_id count per team_id in one team_metrics_daily
    write (CHAOS-4329). ``rows`` is the exact list passed to
    ``write_team_metrics`` -- every row must carry ``team_id``/``repo_id``
    attributes (``TeamMetricsDailyRecord`` does). Call once per write, after
    the sink call, never per-sink (a dual-sink write would otherwise double
    every observation).
    """
    if not rows:
        return
    repos_by_team: dict[str, set[str]] = {}
    for row in rows:
        repos_by_team.setdefault(row.team_id, set()).add(row.repo_id)
    for repo_ids in repos_by_team.values():
        DEV_HEALTH_TEAM_METRICS_DAILY_REPO_COUNT.observe(len(repo_ids))


def record_metrics_family_zero_rows(*, family: str, cause: str) -> None:
    """Record a metrics.daily_partition sub-family that computed zero rows.

    ``cause`` is drawn from a fixed vocabulary by the caller (e.g.
    "no_rows_computed"), never from row contents.
    """
    DEV_HEALTH_METRICS_FAMILY_FAILURES_TOTAL.labels(
        family=family,
        cause=cause,
    ).inc()


@contextmanager
def clickhouse_query_timer(query_type: str = "query") -> Generator[None, None, None]:
    """Context manager that records ClickHouse query latency."""
    start = time.perf_counter()
    status = "success"
    try:
        yield
    except Exception:
        status = "error"
        raise
    finally:
        duration = time.perf_counter() - start
        CLICKHOUSE_QUERY_DURATION_SECONDS.labels(query_type=query_type).observe(
            duration
        )
        CLICKHOUSE_QUERIES_TOTAL.labels(query_type=query_type, status=status).inc()
