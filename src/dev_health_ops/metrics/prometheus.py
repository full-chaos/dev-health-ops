"""Custom Prometheus metrics for dev-health-ops.

Defines application-level counters, histograms, and gauges for:
  - Celery task execution
  - ClickHouse query latency
  - LLM API calls (OpenAI / Anthropic)
  - GitHub API calls (requests by endpoint/status, rate limit remaining)
  - Context Fabric graph arm: embedded-surface document indexing/removal

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
        record_context_fabric_documents_indexed,
        record_context_fabric_document_removed,
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

    ASK_DEV_GRAPH_ROUTING_OUTCOME_TOTAL = _prometheus_client_module.Counter(
        "devhealth_ask_dev_graph_routing_outcome_total",
        "DISCOVERED_COHORT Ask Dev requests routed through the graph-assisted "
        "seam, by GraphQueryOutcome (CHAOS-3502, beta gate 10 observability "
        "hook). Every value other than 'completed' falls back to the legacy "
        "model-tool-choice loop -- content-safe: the label is the closed "
        "GraphQueryOutcome vocabulary, never question or entity text.",
        ["outcome"],
    )

    # CHAOS-3502/3650: a SEPARATE axis from the routing-outcome counter above.
    # That counter measures the transport/investigation call
    # (GraphQueryOutcome); this one measures what the ORCHESTRATOR did with a
    # COMPLETED result once it tried to assemble a real answer from it. The
    # two must stay distinct counters -- collapsing them would conflate "the
    # graph route completed" with "the graph route produced a usable
    # answer", which is exactly the state-conflation CHAOS-3502's ruling
    # forbids. Only incremented when assembly is actually attempted (i.e.
    # canonical_enrichment is wired and the packet's own outcome asserts a
    # judgment) -- a gate-skip (collaborator not wired, packet not
    # supported) has no assembly outcome to report and is observable through
    # the existing log line instead.
    ASK_DEV_GRAPH_ASSEMBLY_OUTCOME_TOTAL = _prometheus_client_module.Counter(
        "devhealth_ask_dev_graph_assembly_outcome_total",
        "Attempts to assemble a graph-grounded DevAnswer from a COMPLETED "
        "graph-routing packet (CHAOS-3502/3650), by outcome: 'assembled_clean' "
        "(no evidence refused, no enrichment gap), "
        "'assembled_degraded_evidence_refused' (the canonical evidence "
        "service refused at least one candidate -- drop-and-disclose, never "
        "an abort), 'assembled_degraded_enrichment_gap' (a canonical "
        "enrichment source was unavailable), 'assembled_degraded_both', "
        "'no_material' (admission + enrichment together produced nothing to "
        "ground an answer on -- falls through to the legacy loop, same as "
        "today), or 'assembly_raised' (an unexpected exception during "
        "admission/enrichment -- caught, logged at ERROR, never surfaced as "
        "RunState.FAILED). Content-safe: the label is this closed outcome "
        "vocabulary, never question or entity text.",
        ["outcome"],
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

    # ---------------------------------------------------------------------------
    # Ask Dev retention sweep (CHAOS-3404): DevPersistenceService.cleanup_expired
    # previously had no production caller, so the 0/30-day retention policy
    # never actually ran. Now beat-scheduled (workers/ask_dev_retention.py).
    # ---------------------------------------------------------------------------
    ASK_DEV_RETENTION_SWEEP_TOTAL = _prometheus_client_module.Counter(
        "devhealth_ask_dev_retention_sweep_total",
        "Ask Dev retention sweep (cleanup_expired) completions by status "
        "(completed/partial/failed -- partial means it hit its batch cap "
        "without draining the backlog, not that it errored). A dashboard/"
        "alert should page on status=failed and, more importantly, on this "
        "counter's own absence -- see "
        "devhealth_ask_dev_retention_sweep_last_success_timestamp for the "
        "skipped-or-falling-behind-sweep (dead-man's-switch) signal.",
        ["status"],
    )

    ASK_DEV_RETENTION_SWEEP_PURGED_TOTAL = _prometheus_client_module.Counter(
        "devhealth_ask_dev_retention_sweep_purged_total",
        "Ask Dev conversations purged by the retention sweep, cumulative. "
        "Incremented for every batch that actually committed a purge, "
        "regardless of the sweep run's overall status (completed/partial/"
        "failed) -- a purge that committed is real and durable even if a "
        "later batch in the same run then failed.",
    )

    ASK_DEV_RETENTION_SWEEP_LAST_SUCCESS_TIMESTAMP = _prometheus_client_module.Gauge(
        "devhealth_ask_dev_retention_sweep_last_success_timestamp",
        "Unix timestamp (seconds) of the last time the Ask Dev retention "
        "sweep completed successfully. Only ever advances on success -- "
        "never set on a failed or skipped run -- so "
        "`time() - this` growing unbounded is the receipt that a sweep "
        "was silently skipped (beat never fired, worker down, queue "
        "starved), not just that one run failed.",
    )

    # ---------------------------------------------------------------------------
    # Context Fabric graph arm: embedded-surface document lifecycle (issue 3632)
    #
    # Content-safe by construction: neither counter's labels ever carry a
    # title, body or canonical id -- only a closed reason-code vocabulary
    # (context_fabric.graph_arm.store.DocumentRemovalReason's own values).
    # ---------------------------------------------------------------------------
    CONTEXT_FABRIC_DOCUMENTS_INDEXED_TOTAL = _prometheus_client_module.Counter(
        "devhealth_context_fabric_documents_indexed_total",
        "Documents written as nodes to the context-fabric graph trial "
        "store, cumulative across all partitions. Incremented once per "
        "document per successful projection write -- a write that raises "
        "increments nothing, since no node was actually written.",
    )

    CONTEXT_FABRIC_DOCUMENTS_REMOVED_TOTAL = _prometheus_client_module.Counter(
        "devhealth_context_fabric_documents_removed_total",
        "Documents removed from the context-fabric graph trial store via "
        "GraphArmStore.remove_document, by reason. Incremented only when a "
        "node was actually deleted -- a no-op removal (already absent, or "
        "never approved in the first place) increments nothing, so this "
        "counter measures real index shrinkage, not call volume.",
        ["reason"],
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
else:
    # Graceful no-ops when prometheus_client is unavailable
    CELERY_TASKS_TOTAL = _noop_counter()
    CELERY_TASK_DURATION_SECONDS = _noop_histogram()
    CLICKHOUSE_QUERY_DURATION_SECONDS = _noop_histogram()
    CLICKHOUSE_QUERIES_TOTAL = _noop_counter()
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
    ASK_DEV_GRAPH_ROUTING_OUTCOME_TOTAL = _noop_counter()
    ASK_DEV_GRAPH_ASSEMBLY_OUTCOME_TOTAL = _noop_counter()
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
    CONTEXT_FABRIC_DOCUMENTS_INDEXED_TOTAL = _noop_counter()
    CONTEXT_FABRIC_DOCUMENTS_REMOVED_TOTAL = _noop_counter()
    INTEGRATION_CREDENTIAL_DECRYPT_FAILED_TOTAL = _noop_counter()


# ---------------------------------------------------------------------------
# Convenience helpers
# ---------------------------------------------------------------------------


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


def record_ask_dev_retention_sweep(
    *, status: str, purged: int = 0, timestamp: float | None = None
) -> None:
    """Record one Ask Dev retention sweep completion (CHAOS-3404).

    ``status`` is one of:

    * ``"completed"`` -- the sweep drained its backlog with no error.
    * ``"partial"`` -- the sweep ran error-free but hit its batch cap
      before draining the backlog (a real, growing backlog, not a failure).
    * ``"failed"`` -- the sweep raised. ``purged`` may still be > 0 (earlier
      batches in the same run committed before a later one failed).

    ``purged`` is recorded for every status, since a purge that committed is
    real and durable regardless of how the overall run ended. The
    last-success gauge, however, is deliberately advanced ONLY on
    ``"completed"`` (Codex adversarial-review round 1, CHAOS-3404, HIGH,
    confirmed: a "partial" run that still advanced it let retention debt
    persist while the dead-man alert reported a healthy sweep) -- a failed,
    partial, or never-run sweep must leave it stale so an alert on
    ``time() - devhealth_ask_dev_retention_sweep_last_success_timestamp``
    detects a failing, falling-behind, or silently skipped sweep alike.
    """
    ASK_DEV_RETENTION_SWEEP_TOTAL.labels(status=status).inc()
    if purged:
        ASK_DEV_RETENTION_SWEEP_PURGED_TOTAL.inc(purged)
    if status == "completed":
        ASK_DEV_RETENTION_SWEEP_LAST_SUCCESS_TIMESTAMP.set(
            timestamp if timestamp is not None else time.time()
        )


def record_context_fabric_documents_indexed(count: int) -> None:
    """Record ``count`` documents written as nodes in one projection write.

    ``count`` rather than a one-at-a-time call: the write side embeds and
    writes an entire batch per ``write_projection`` call, and recording once
    per call (with the real count) avoids a metrics-loop for something the
    caller already has as a single number.
    """

    if count:
        CONTEXT_FABRIC_DOCUMENTS_INDEXED_TOTAL.inc(count)


def record_context_fabric_document_removed(reason: str) -> None:
    """Record one document actually removed from the graph store.

    ``reason`` is expected to be one of
    ``context_fabric.graph_arm.store.DocumentRemovalReason``'s values --
    passed as a plain ``str`` here (not the enum type) so this module stays
    free of a dependency on the optional graph arm, matching every other
    entry in this file.
    """

    CONTEXT_FABRIC_DOCUMENTS_REMOVED_TOTAL.labels(reason=reason).inc()


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
