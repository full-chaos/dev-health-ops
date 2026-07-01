"""Materialize work unit investment categorization into sinks."""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
import uuid
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from time import monotonic
from typing import Any

from sqlalchemy.exc import IntegrityError

from dev_health_ops.llm import (
    LLMAuthError,
    LLMContextLengthError,
    LLMError,
    LLMOutputError,
    LLMRateLimitError,
    LLMServerError,
    classify_provider_error,
    get_provider,
    resolve_model_name,
    resolve_provider_name,
)
from dev_health_ops.llm.providers.batch import (
    BatchItemRequest,
    BatchItemResult,
    BatchItemStatus,
    BatchJobStatus,
    batch_capability_for,
)
from dev_health_ops.llm.providers.none import NoneProvider
from dev_health_ops.metrics.llm_token_usage import write_llm_token_usage
from dev_health_ops.metrics.schemas import (
    WorkUnitInvestmentEvidenceQuoteRecord,
    WorkUnitInvestmentRecord,
    WorkUnitRepoEffortRecord,
)
from dev_health_ops.metrics.sinks.base import BaseMetricsSink
from dev_health_ops.metrics.sinks.factory import create_sink
from dev_health_ops.work_graph.ids import parse_commit_from_id, parse_pr_from_id
from dev_health_ops.work_graph.investment.batch_store import (
    InvestmentBatchItemSpec,
    InvestmentBatchStore,
)
from dev_health_ops.work_graph.investment.categorize import (
    PROMPT_VERSION,
    TAXONOMY_VERSION,
    build_categorization_prompt,
    categorize_text_bundle,
    categorize_text_bundle_completion,
    fallback_outcome,
)
from dev_health_ops.work_graph.investment.constants import (
    MEMBERSHIP_WEIGHT_THRESHOLD,
    MIN_EVIDENCE_CHARS,
)
from dev_health_ops.work_graph.investment.evidence import (
    TimeBounds,
    build_text_bundle,
    compute_evidence_quality,
    compute_time_bounds,
)
from dev_health_ops.work_graph.investment.queries import (
    fetch_commit_churn,
    fetch_commits,
    fetch_parent_titles,
    fetch_pull_requests,
    fetch_work_graph_edges,
    fetch_work_item_active_hours,
    fetch_work_items,
    resolve_repo_ids_for_teams,
)
from dev_health_ops.work_graph.investment.utils import (
    evidence_quality_band,
    rollup_subcategories_to_themes,
    work_unit_id,
)

logger = logging.getLogger(__name__)

_MAX_LLM_CONCURRENCY = 32

NodeKey = tuple[str, str]


def _classify_llm_exception(
    exc: BaseException,
    *,
    provider: str,
    model: str,
) -> LLMError:
    if isinstance(exc, LLMError):
        return exc
    return classify_provider_error(exc, provider=provider, model=model)


def _llm_failure_class(exc: BaseException) -> str:
    text = str(exc).lower()
    if "insufficient_quota" in text or "quota exhausted" in text:
        return "quota_exceeded"
    if "model_not_found" in text or "model not found" in text:
        return "model_not_found"
    if "missing" in text and "api key" in text:
        return "missing_key"
    if "invalid" in text and "api key" in text:
        return "invalid_api_key"
    if isinstance(exc, LLMAuthError):
        return "auth"
    if isinstance(exc, LLMRateLimitError):
        return "rate_limit"
    if isinstance(exc, LLMServerError):
        return "server_error"
    if isinstance(exc, LLMContextLengthError):
        return "context_length"
    if isinstance(exc, LLMOutputError):
        return "output_error"
    return "llm_error"


def _is_deterministic_llm_failure(exc: BaseException) -> bool:
    if isinstance(exc, LLMAuthError):
        return True
    return _llm_failure_class(exc) in {
        "invalid_api_key",
        "missing_key",
        "model_not_found",
        "quota_exceeded",
    }


def _format_llm_summary(ok: int, failure_counts: Counter[str]) -> str:
    parts = [f"{ok} ok"]
    parts.extend(
        f"{count} {failure_class}"
        for failure_class, count in sorted(failure_counts.items())
        if count
    )
    return "llm: " + ", ".join(parts)


def _effective_model_version(provider: str, model: str | None) -> str:
    resolved_model = resolve_model_name(provider, model) or model or provider
    return (
        f"provider={provider};model={resolved_model};"
        f"taxonomy={TAXONOMY_VERSION};prompt={PROMPT_VERSION}"
    )


def _batch_contract_version() -> str:
    return f"taxonomy={TAXONOMY_VERSION};prompt={PROMPT_VERSION};batch=v1"


def _batch_error_label(exc: Exception) -> str:
    return type(exc).__name__


def _batch_correlation_id(run_id: str, chunk_index: int | None) -> str:
    if chunk_index is None:
        return run_id
    return f"{run_id}:chunk:{chunk_index}"


def _batch_custom_id(batch_correlation_id: str, component_index: int) -> str:
    return f"{batch_correlation_id}-{component_index}"


def _create_batch_job(
    *,
    org_id: str,
    provider: str,
    model: str,
    run_id: str,
    local_correlation_id: str,
    specs: list[InvestmentBatchItemSpec],
) -> str:
    from dev_health_ops.db import get_postgres_session_sync

    with get_postgres_session_sync() as session:
        store = InvestmentBatchStore(session, org_id)
        existing = store.get_job_by_correlation(local_correlation_id)
        if existing is not None:
            return str(existing.id)
        try:
            job = store.create_job(
                provider=provider,
                model=model,
                run_id=run_id,
                prompt_version=PROMPT_VERSION,
                contract_version=_batch_contract_version(),
                items=specs,
                local_correlation_id=local_correlation_id,
            )
        except IntegrityError:
            session.rollback()
            store = InvestmentBatchStore(session, org_id)
            existing = store.get_job_by_correlation(local_correlation_id)
            if existing is None:
                raise
            return str(existing.id)
        return str(job.id)


def _transition_batch_job(
    *,
    org_id: str,
    job_id: str,
    status: str,
    provider_job_id: str | None = None,
    error: str | None = None,
    provider_metadata: dict[str, Any] | None = None,
) -> None:
    from dev_health_ops.db import get_postgres_session_sync

    with get_postgres_session_sync() as session:
        store = InvestmentBatchStore(session, org_id)
        job = store.get_job(job_id)
        if job is not None:
            store.transition_job(
                job,
                status,
                provider_job_id=provider_job_id,
                error=error,
                provider_metadata=provider_metadata,
            )


def _transition_batch_item(
    *,
    org_id: str,
    job_id: str,
    custom_id: str,
    status: str,
    provider_response: dict[str, Any] | None = None,
    provider_error: dict[str, Any] | None = None,
    audit: dict[str, Any] | None = None,
) -> None:
    from dev_health_ops.db import get_postgres_session_sync

    with get_postgres_session_sync() as session:
        store = InvestmentBatchStore(session, org_id)
        item = store.get_item_by_custom_id(job_id=job_id, custom_id=custom_id)
        if item is not None:
            store.transition_item(
                item,
                status,
                provider_response=provider_response,
                provider_error=provider_error,
                audit=audit,
            )


def _update_batch_counts(*, org_id: str, job_id: str) -> None:
    from dev_health_ops.db import get_postgres_session_sync

    with get_postgres_session_sync() as session:
        store = InvestmentBatchStore(session, org_id)
        job = store.get_job(job_id)
        if job is not None:
            completed, failed = store.terminal_counts(job)
            terminal_status = job.status
            if job.status not in {
                BatchJobStatus.FAILED.value,
                BatchJobStatus.CANCELLED.value,
                BatchJobStatus.EXPIRED.value,
            } and completed >= int(job.total_items or 0):
                terminal_status = BatchJobStatus.SUCCEEDED.value
            store.transition_job(job, terminal_status)
            job.completed_items = completed
            job.failed_items = failed
            session.flush()


async def _fallback_unresolved_batch_items(
    *,
    org_id: str,
    job_id: str,
    pending_llm: list[tuple[int, Any]],
    run_id: str,
    batch_correlation_id: str,
    audit_reason: str,
    provider_error: dict[str, Any] | None = None,
) -> dict[int, Any]:
    outcomes: dict[int, Any] = {}
    for idx, _bundle in pending_llm:
        outcomes[idx] = fallback_outcome("llm_task_failed")
        await asyncio.to_thread(
            _transition_batch_item,
            org_id=org_id,
            job_id=job_id,
            custom_id=_batch_custom_id(batch_correlation_id, idx),
            status=BatchItemStatus.FALLBACK.value,
            provider_error=provider_error,
            audit={"reason": audit_reason},
        )
    await asyncio.to_thread(_update_batch_counts, org_id=org_id, job_id=job_id)
    return outcomes


async def _categorize_with_provider_batch(
    *,
    config: MaterializeConfig,
    provider_instance: Any,
    resolved_llm_provider: str,
    model_version: str,
    run_id: str,
    pending_llm: list[tuple[int, Any]],
    preprocessed: dict[int, PreprocessedComponent],
) -> dict[int, Any] | None:
    if not pending_llm:
        return {}
    capability = batch_capability_for(provider_instance, config.llm_model)
    if not capability.supported:
        if config.llm_batch_mode == "provider_batch":
            raise ValueError(
                f"LLM provider '{resolved_llm_provider}' does not support provider_batch"
            )
        logger.info(
            "LLM batch mode auto fell back to sync: provider=%s reason=%s",
            resolved_llm_provider,
            capability.reason or "unsupported",
        )
        return None
    if (
        config.llm_batch_mode == "auto"
        and len(pending_llm) < config.llm_batch_min_items
    ):
        logger.info(
            "LLM batch mode auto fell back to sync: pending=%d threshold=%d",
            len(pending_llm),
            config.llm_batch_min_items,
        )
        return None

    org_id = config.org_id or ""
    batch_correlation_id = _batch_correlation_id(run_id, config.chunk_index)
    specs = [
        InvestmentBatchItemSpec(
            work_unit_id=preprocessed[idx].unit_id,
            component_index=idx,
            custom_id=_batch_custom_id(batch_correlation_id, idx),
            input_hash=bundle.input_hash,
        )
        for idx, bundle in pending_llm
    ]
    job_id = await asyncio.to_thread(
        _create_batch_job,
        org_id=org_id,
        provider=resolved_llm_provider,
        model=model_version,
        run_id=run_id,
        local_correlation_id=batch_correlation_id,
        specs=specs,
    )
    await asyncio.to_thread(
        _transition_batch_job,
        org_id=org_id,
        job_id=job_id,
        status=BatchJobStatus.SUBMITTING.value,
    )

    requests = [
        BatchItemRequest(
            custom_id=_batch_custom_id(batch_correlation_id, idx),
            prompt=build_categorization_prompt(bundle),
            metadata={
                "org_id": org_id,
                "work_unit_id": preprocessed[idx].unit_id,
                "component_index": idx,
                "input_hash": bundle.input_hash,
            },
        )
        for idx, bundle in pending_llm
    ]
    try:
        submission = await provider_instance.submit_batch(requests)
    except Exception as exc:
        await asyncio.to_thread(
            _transition_batch_job,
            org_id=org_id,
            job_id=job_id,
            status=BatchJobStatus.FAILED.value,
            error=_batch_error_label(exc),
        )
        raise

    await asyncio.to_thread(
        _transition_batch_job,
        org_id=org_id,
        job_id=job_id,
        status=BatchJobStatus.SUBMITTED.value,
        provider_job_id=submission.provider_job_id,
        provider_metadata=submission.metadata,
    )
    for request in requests:
        await asyncio.to_thread(
            _transition_batch_item,
            org_id=org_id,
            job_id=job_id,
            custom_id=request.custom_id,
            status=BatchItemStatus.SUBMITTED.value,
        )

    started = monotonic()
    final_state = None
    while monotonic() - started < config.llm_batch_timeout_seconds:
        final_state = await provider_instance.poll_batch(submission.provider_job_id)
        await asyncio.to_thread(
            _transition_batch_job,
            org_id=org_id,
            job_id=job_id,
            status=final_state.status.value,
            provider_metadata=final_state.metadata,
        )
        if final_state.status in {
            BatchJobStatus.SUCCEEDED,
            BatchJobStatus.FAILED,
            BatchJobStatus.CANCELLED,
            BatchJobStatus.EXPIRED,
        }:
            break
        await asyncio.sleep(config.llm_batch_poll_interval_seconds)

    if final_state is None or final_state.status not in {
        BatchJobStatus.SUCCEEDED,
        BatchJobStatus.FAILED,
        BatchJobStatus.CANCELLED,
        BatchJobStatus.EXPIRED,
    }:
        cancel_batch = getattr(provider_instance, "cancel_batch", None)
        if callable(cancel_batch):
            try:
                maybe_cancelled = cancel_batch(submission.provider_job_id)
                if inspect.isawaitable(maybe_cancelled):
                    await maybe_cancelled
            except Exception as exc:
                logger.warning(
                    "Provider batch cancellation failed: provider=%s job_id=%s error_type=%s",
                    resolved_llm_provider,
                    submission.provider_job_id,
                    _batch_error_label(exc),
                )
        await asyncio.to_thread(
            _transition_batch_job,
            org_id=org_id,
            job_id=job_id,
            status=BatchJobStatus.EXPIRED.value,
            error="provider batch timed out",
        )
        return await _fallback_unresolved_batch_items(
            org_id=org_id,
            job_id=job_id,
            pending_llm=pending_llm,
            run_id=run_id,
            batch_correlation_id=batch_correlation_id,
            audit_reason="provider_batch_timeout",
        )

    if final_state.status != BatchJobStatus.SUCCEEDED:
        return await _fallback_unresolved_batch_items(
            org_id=org_id,
            job_id=job_id,
            pending_llm=pending_llm,
            run_id=run_id,
            batch_correlation_id=batch_correlation_id,
            audit_reason=f"provider_batch_{final_state.status.value}",
            provider_error={"status": final_state.status.value},
        )

    try:
        raw_results = await provider_instance.fetch_batch_results(
            submission.provider_job_id
        )
    except Exception as exc:
        await asyncio.to_thread(
            _transition_batch_job,
            org_id=org_id,
            job_id=job_id,
            status=BatchJobStatus.FAILED.value,
            error=_batch_error_label(exc),
        )
        return await _fallback_unresolved_batch_items(
            org_id=org_id,
            job_id=job_id,
            pending_llm=pending_llm,
            run_id=run_id,
            batch_correlation_id=batch_correlation_id,
            audit_reason="provider_batch_fetch_failed",
            provider_error={"error_type": _batch_error_label(exc)},
        )
    results_by_custom_id: dict[str, BatchItemResult] = {
        result.custom_id: result for result in raw_results if result.custom_id
    }
    outcomes: dict[int, Any] = {}
    for idx, bundle in pending_llm:
        custom_id = _batch_custom_id(batch_correlation_id, idx)
        item_result = results_by_custom_id.get(custom_id)
        if item_result is None:
            outcome = fallback_outcome("llm_task_failed")
            await asyncio.to_thread(
                _transition_batch_item,
                org_id=org_id,
                job_id=job_id,
                custom_id=custom_id,
                status=BatchItemStatus.FALLBACK.value,
                audit={"reason": "missing_batch_result"},
            )
        elif item_result.succeeded and item_result.raw_response is not None:
            try:
                outcome = await categorize_text_bundle_completion(
                    bundle,
                    item_result.raw_response,
                    llm_provider=resolved_llm_provider,
                    llm_model=config.llm_model,
                    provider=provider_instance,
                    input_tokens=int(
                        item_result.provider_metadata.get("input_tokens") or 0
                    ),
                    output_tokens=int(
                        item_result.provider_metadata.get("output_tokens") or 0
                    ),
                    llm_calls=1,
                    resolved_model=model_version,
                )
            except Exception as exc:
                outcome = fallback_outcome("llm_task_failed")
                await asyncio.to_thread(
                    _transition_batch_item,
                    org_id=org_id,
                    job_id=job_id,
                    custom_id=custom_id,
                    status=BatchItemStatus.FALLBACK.value,
                    provider_error={"error_type": _batch_error_label(exc)},
                    audit={"reason": "batch_result_validation_failed"},
                )
                outcomes[idx] = outcome
                continue
            status = (
                BatchItemStatus.VALIDATED.value
                if outcome.status == "ok"
                else BatchItemStatus.REPAIRED.value
                if outcome.status == "repaired"
                else BatchItemStatus.FALLBACK.value
            )
            await asyncio.to_thread(
                _transition_batch_item,
                org_id=org_id,
                job_id=job_id,
                custom_id=custom_id,
                status=status,
                provider_response={"metadata": item_result.provider_metadata},
                audit={"status": outcome.status, "errors": outcome.errors},
            )
        else:
            outcome = fallback_outcome("llm_task_failed")
            await asyncio.to_thread(
                _transition_batch_item,
                org_id=org_id,
                job_id=job_id,
                custom_id=custom_id,
                status=BatchItemStatus.FALLBACK.value,
                provider_error={
                    "code": item_result.error_code,
                    "message": item_result.error_message,
                    "metadata": item_result.provider_metadata,
                },
            )
        outcomes[idx] = outcome
    await asyncio.to_thread(_update_batch_counts, org_id=org_id, job_id=job_id)
    logger.info(
        "Completed provider batch categorization: provider=%s items=%d job_id=%s",
        resolved_llm_provider,
        len(outcomes),
        job_id,
    )
    return outcomes


def _fetch_existing_investment_keys(
    sink: BaseMetricsSink,
    *,
    org_id: str,
    keys: Iterable[tuple[str, str]],
    model_version: str,
) -> set[tuple[str, str]]:
    key_list = list(dict.fromkeys(keys))
    if not key_list or not hasattr(sink, "query_dicts"):
        return set()
    work_unit_ids = sorted({work_unit_id for work_unit_id, _ in key_list})
    input_hashes = sorted({input_hash for _, input_hash in key_list})
    query = """
        SELECT work_unit_id, categorization_input_hash
        FROM (
            SELECT
                work_unit_id,
                categorization_input_hash,
                argMax(categorization_status, computed_at) AS latest_status
            FROM work_unit_investments
            WHERE org_id = %(org_id)s
              AND work_unit_id IN %(work_unit_ids)s
              AND categorization_input_hash IN %(input_hashes)s
              AND categorization_model_version = %(model_version)s
            GROUP BY work_unit_id, categorization_input_hash
        )
        WHERE latest_status IN %(valid_statuses)s
    """
    rows = sink.query_dicts(
        query,
        {
            "org_id": org_id,
            "work_unit_ids": work_unit_ids,
            "input_hashes": input_hashes,
            "model_version": model_version,
            "valid_statuses": ["ok", "repaired"],
        },
    )
    return {
        (
            str(row.get("work_unit_id") or ""),
            str(row.get("categorization_input_hash") or ""),
        )
        for row in rows
        if row.get("work_unit_id") and row.get("categorization_input_hash")
    }


@dataclass(frozen=True)
class PreprocessedComponent:
    unit_id: str
    unit_nodes: list[NodeKey]
    issue_node_ids: list[str]
    pr_node_ids: list[str]
    commit_node_ids: list[str]
    bounds: TimeBounds
    bundle: Any
    component_edges: list[dict[str, object]]


def _float_value(value: object) -> float:
    if isinstance(value, bool):
        return 0.0
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return 0.0
    return 0.0


def _lexical_argmax(distribution: dict[str, float]) -> str:
    """Return the key with the highest value; break ties lexically (smallest key wins).

    An empty distribution returns "unknown". The lexical tie-break ensures
    deterministic output across Python runs regardless of dict insertion order.
    """
    if not distribution:
        return "unknown"
    # Smallest-key-wins on ties: order by (-value, key) and take the min, so the
    # highest weight comes first and the lexically smallest key breaks ties.
    return min(distribution, key=lambda k: (-_float_value(distribution[k]), k))


def _membership_categories(
    distribution: dict[str, float],
) -> list[tuple[str, float, int]]:
    """Return (category, weight, is_dominant) rows to emit for one distribution.

    Multi-membership: every category with weight >= MEMBERSHIP_WEIGHT_THRESHOLD
    is emitted, so a mixed unit is findable under each significant category. The
    argmax category (lexical tie-break) is ALWAYS included even when below the
    threshold and is flagged is_dominant=1, so every node is findable under at
    least its dominant category. Returns at least one row whenever the
    distribution is non-empty.
    """
    if not distribution:
        return []
    dominant = _lexical_argmax(distribution)
    out: list[tuple[str, float, int]] = []
    seen: set[str] = set()
    for category, raw_weight in distribution.items():
        weight = _float_value(raw_weight)
        is_dominant = 1 if category == dominant else 0
        if weight >= MEMBERSHIP_WEIGHT_THRESHOLD or is_dominant:
            out.append((category, weight, is_dominant))
            seen.add(category)
    # Defensive: ensure the dominant row is present even if it was filtered
    # (e.g. dominant key absent from the dict view due to mutation).
    if dominant not in seen:
        out.append((dominant, _float_value(distribution.get(dominant, 0.0)), 1))
    return out


_LLM_BATCH_MODES = {"sync", "auto", "provider_batch"}


def resolve_llm_batch_mode(value: object | None = None) -> str:
    raw = value if value is not None else os.getenv("INVESTMENT_LLM_BATCH_MODE", "sync")
    mode = str(raw or "sync").strip().lower().replace("-", "_")
    if mode not in _LLM_BATCH_MODES:
        raise ValueError("llm_batch_mode must be one of: sync, auto, provider_batch")
    return mode


def resolve_llm_batch_min_items(value: object | None = None) -> int:
    raw = (
        value
        if value is not None
        else os.getenv("INVESTMENT_LLM_BATCH_MIN_ITEMS", "25")
    )
    try:
        return max(1, int(str(raw)))
    except (TypeError, ValueError):
        return 25


def resolve_llm_batch_poll_interval_seconds(value: object | None = None) -> float:
    raw = (
        value
        if value is not None
        else os.getenv("INVESTMENT_LLM_BATCH_POLL_INTERVAL_SECONDS", "30")
    )
    try:
        return max(0.001, float(str(raw)))
    except (TypeError, ValueError):
        return 30.0


def resolve_llm_batch_timeout_seconds(value: object | None = None) -> float:
    raw = (
        value
        if value is not None
        else os.getenv("INVESTMENT_LLM_BATCH_TIMEOUT_SECONDS", "3000")
    )
    try:
        return max(0.001, float(str(raw)))
    except (TypeError, ValueError):
        return 3000.0


@dataclass(frozen=True)
class MaterializeConfig:
    dsn: str
    from_ts: datetime
    to_ts: datetime
    repo_ids: list[str] | None
    llm_provider: str
    persist_evidence_snippets: bool
    llm_model: str | None
    llm_api_key: str = field(default="", repr=False)
    llm_base_url: str = ""
    force: bool = False
    team_ids: list[str] | None = None
    llm_concurrency: int = 5
    org_id: str | None = None
    run_id: str | None = None
    computed_at: datetime | None = None
    component_indexes: list[int] | None = None
    chunk_index: int | None = None
    allow_unscoped: bool = False
    llm_batch_mode: str = "sync"
    llm_batch_min_items: int = 25
    llm_batch_poll_interval_seconds: float = 30.0
    llm_batch_timeout_seconds: float = 3000.0

    def __post_init__(self) -> None:
        if self.llm_batch_mode not in _LLM_BATCH_MODES:
            raise ValueError(
                "llm_batch_mode must be one of: sync, auto, provider_batch"
            )
        if self.llm_batch_min_items < 1:
            raise ValueError("llm_batch_min_items must be >= 1")
        if self.llm_batch_poll_interval_seconds <= 0:
            raise ValueError("llm_batch_poll_interval_seconds must be > 0")
        if self.llm_batch_timeout_seconds <= 0:
            raise ValueError("llm_batch_timeout_seconds must be > 0")


def _build_components(
    edges: list[dict[str, object]],
) -> list[tuple[list[NodeKey], list[dict[str, object]]]]:
    adjacency: dict[NodeKey, list[NodeKey]] = {}
    edges_by_node: dict[NodeKey, list[dict[str, object]]] = {}

    for edge in edges:
        source = (str(edge.get("source_type")), str(edge.get("source_id")))
        target = (str(edge.get("target_type")), str(edge.get("target_id")))
        adjacency.setdefault(source, []).append(target)
        adjacency.setdefault(target, []).append(source)
        edges_by_node.setdefault(source, []).append(edge)
        edges_by_node.setdefault(target, []).append(edge)

    visited: set[NodeKey] = set()
    components: list[tuple[list[NodeKey], list[dict[str, object]]]] = []

    for node in adjacency:
        if node in visited:
            continue
        stack = [node]
        visited.add(node)
        component_nodes: list[NodeKey] = []
        component_edges: dict[str, dict[str, object]] = {}
        while stack:
            current = stack.pop()
            component_nodes.append(current)
            for edge in edges_by_node.get(current, []):
                edge_id = str(edge.get("edge_id") or "")
                if edge_id and edge_id not in component_edges:
                    component_edges[edge_id] = edge
            for neighbor in adjacency.get(current, []):
                if neighbor in visited:
                    continue
                visited.add(neighbor)
                stack.append(neighbor)
        components.append((component_nodes, list(component_edges.values())))
    return components


def _flatten_nodes(
    components: list[tuple[list[NodeKey], list[dict[str, object]]]],
) -> list[NodeKey]:
    nodes: list[NodeKey] = []
    for node_list, _ in components:
        nodes.extend(node_list)
    return nodes


def _group_prs_by_repo(pr_ids: Iterable[str]) -> dict[str, list[int]]:
    repo_map: dict[str, list[int]] = {}
    for pr_id in pr_ids:
        repo_id, number = parse_pr_from_id(pr_id)
        if repo_id and number is not None:
            repo_map.setdefault(str(repo_id), []).append(number)
    return repo_map


def _group_commits_by_repo(commit_ids: Iterable[str]) -> dict[str, list[str]]:
    repo_map: dict[str, list[str]] = {}
    for commit_id in commit_ids:
        repo_id, commit_hash = parse_commit_from_id(commit_id)
        if repo_id and commit_hash:
            repo_map.setdefault(str(repo_id), []).append(commit_hash)
    return repo_map


def _map_prs(prs: Iterable[dict[str, object]]) -> dict[str, dict[str, object]]:
    mapped: dict[str, dict[str, object]] = {}
    for pr in prs:
        repo_id = str(pr.get("repo_id") or "")
        number = pr.get("number")
        if not repo_id or number is None:
            continue
        pr_id = f"{repo_id}#pr{number}"
        mapped[pr_id] = pr
    return mapped


def _map_commits(commits: Iterable[dict[str, object]]) -> dict[str, dict[str, object]]:
    mapped: dict[str, dict[str, object]] = {}
    for commit in commits:
        repo_id = str(commit.get("repo_id") or "")
        commit_hash = str(commit.get("hash") or "")
        if not repo_id or not commit_hash:
            continue
        commit_id = f"{repo_id}@{commit_hash}"
        mapped[commit_id] = commit
    return mapped


def _pr_churn_map(prs: Iterable[dict[str, object]]) -> dict[str, float]:
    churn: dict[str, float] = {}
    for pr in prs:
        repo_id = str(pr.get("repo_id") or "")
        number = pr.get("number")
        if not repo_id or number is None:
            continue
        pr_id = f"{repo_id}#pr{number}"
        additions = _float_value(pr.get("additions"))
        deletions = _float_value(pr.get("deletions"))
        churn[pr_id] = additions + deletions
    return churn


def _effort_from_work_unit(
    *,
    issue_ids: Iterable[str],
    pr_ids: Iterable[str],
    commit_ids: Iterable[str],
    pr_churn: dict[str, float],
    commit_churn: dict[str, float],
    active_hours: dict[str, float],
) -> tuple[str, float]:
    commit_total = sum(commit_churn.get(cid, 0.0) for cid in commit_ids)
    if commit_total > 0:
        return "churn_loc", float(commit_total)
    pr_total = sum(pr_churn.get(pid, 0.0) for pid in pr_ids)
    if pr_total > 0:
        return "churn_loc", float(pr_total)
    active_total = sum(active_hours.get(wid, 0.0) for wid in issue_ids)
    if active_total > 0:
        return "active_hours", float(active_total)
    return "churn_loc", 0.0


def _allocate_repo_effort(
    *,
    issue_ids: Iterable[str],
    pr_ids: Iterable[str],
    commit_ids: Iterable[str],
    pr_churn: dict[str, float],
    commit_churn: dict[str, float],
    active_hours: dict[str, float],
    effort_metric: str,
    effort_value: float,
) -> list[tuple[uuid.UUID | None, float, float, str]]:
    commit_effort_by_repo: dict[str, float] = {}
    commit_total = 0.0
    for commit_id in commit_ids:
        churn = float(commit_churn.get(commit_id, 0.0))
        commit_total += churn
        if churn <= 0:
            continue
        repo_id, _ = parse_commit_from_id(commit_id)
        repo_key = str(repo_id) if repo_id else ""
        commit_effort_by_repo[repo_key] = (
            commit_effort_by_repo.get(repo_key, 0.0) + churn
        )
    if commit_total > 0:
        return [
            (
                _parse_repo_id(repo_key or None),
                repo_effort,
                repo_effort / commit_total,
                "commit_churn",
            )
            for repo_key, repo_effort in sorted(commit_effort_by_repo.items())
        ]

    pr_effort_by_repo: dict[str, float] = {}
    pr_total = 0.0
    for pr_id in pr_ids:
        churn = float(pr_churn.get(pr_id, 0.0))
        pr_total += churn
        if churn <= 0:
            continue
        repo_id, _ = parse_pr_from_id(pr_id)
        repo_key = str(repo_id) if repo_id else ""
        pr_effort_by_repo[repo_key] = pr_effort_by_repo.get(repo_key, 0.0) + churn
    if pr_total > 0:
        return [
            (
                _parse_repo_id(repo_key or None),
                repo_effort,
                repo_effort / pr_total,
                "pr_churn",
            )
            for repo_key, repo_effort in sorted(pr_effort_by_repo.items())
        ]

    if effort_metric == "active_hours" and effort_value > 0:
        return [(None, effort_value, 1.0, "active_hours_unassigned")]

    return [(None, 0.0, 0.0, "empty")]


def _collect_repo_ids(edges: list[dict[str, object]]) -> list[str]:
    repo_ids = {str(edge.get("repo_id") or "") for edge in edges if edge.get("repo_id")}
    return sorted(repo_id for repo_id in repo_ids if repo_id)


def _parse_repo_id(repo_id: str | None) -> uuid.UUID | None:
    if not repo_id:
        return None
    try:
        return uuid.UUID(str(repo_id))
    except Exception:
        return None


def _collect_provider(
    work_item_ids: Iterable[str],
    work_item_map: dict[str, dict[str, object]],
) -> str | None:
    providers = {
        str(work_item_map.get(item_id, {}).get("provider") or "")
        for item_id in work_item_ids
        if work_item_map.get(item_id, {}).get("provider")
    }
    providers = {provider for provider in providers if provider}
    if len(providers) == 1:
        return next(iter(providers))
    return None


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _first_line(text: str) -> str:
    for line in text.splitlines():
        stripped = line.strip()
        if stripped:
            return stripped
    return text.strip()


def _resolve_work_unit_label(
    *,
    issue_ids: Iterable[str],
    pr_ids: Iterable[str],
    commit_ids: Iterable[str],
    work_item_map: dict[str, dict[str, object]],
    pr_map: dict[str, dict[str, object]],
    commit_map: dict[str, dict[str, object]],
) -> tuple[str | None, str | None]:
    for issue_id in sorted(issue_ids):
        item = work_item_map.get(issue_id) or {}
        title = _clean_text(item.get("title"))
        if title:
            item_type = _clean_text(item.get("type")) or "issue"
            return item_type, title

    for pr_id in sorted(pr_ids):
        pr = pr_map.get(pr_id) or {}
        title = _clean_text(pr.get("title"))
        if title:
            return "pr", title

    for commit_id in sorted(commit_ids):
        commit = commit_map.get(commit_id) or {}
        message = _clean_text(commit.get("message"))
        if message:
            return "commit", _first_line(message)

    if issue_ids:
        for issue_id in sorted(issue_ids):
            item = work_item_map.get(issue_id) or {}
            item_type = _clean_text(item.get("type"))
            return (item_type or "issue"), None
        return "issue", None
    if pr_ids:
        return "pr", None
    if commit_ids:
        return "commit", None
    return None, None


def _resolve_repo_ids(
    sink: BaseMetricsSink,
    repo_ids: list[str] | None,
    team_ids: list[str] | None,
    config_org_id: str = "",
) -> list[str] | None:
    if repo_ids:
        return repo_ids
    if team_ids:
        return resolve_repo_ids_for_teams(sink, team_ids=team_ids, org_id=config_org_id)
    return None


async def materialize_investments(config: MaterializeConfig) -> dict[str, Any]:
    sink = create_sink(config.dsn)
    provider_instance = None
    try:
        sink.ensure_schema()

        # Initialize LLM provider once (reusing connection pool)
        resolved_llm_provider = resolve_provider_name(
            config.llm_provider, org_id=config.org_id or None
        )
        if (
            not (config.org_id or "").strip()
            and resolved_llm_provider not in {"mock", "none"}
            and not config.allow_unscoped
        ):
            raise ValueError(
                "Investment materialize requires a non-empty org for real LLM "
                "providers. Pass --org <org_id> or --allow-unscoped to write "
                "empty-org rows intentionally."
            )
        provider_instance = get_provider(
            resolved_llm_provider,
            org_id=config.org_id or None,
            model=config.llm_model,
            api_key=config.llm_api_key or None,
            base_url=config.llm_base_url or None,
        )
        if isinstance(provider_instance, NoneProvider):
            raise LLMAuthError(
                "LLM provider 'none' cannot materialize investment categorizations; "
                "configure a real LLM provider or use --llm-provider mock for tests.",
                provider=resolved_llm_provider,
                model="none",
            )

        repo_ids = _resolve_repo_ids(
            sink, config.repo_ids, config.team_ids, config_org_id=config.org_id or ""
        )
        edges = fetch_work_graph_edges(
            sink, repo_ids=repo_ids, org_id=config.org_id or ""
        )
        components = _build_components(edges)
        total_components = len(components)
        if not components:
            logger.info(
                "No work graph components found for investment materialization."
            )
            return {"components": 0, "records": 0, "quotes": 0}
        if config.component_indexes is not None:
            wanted_indexes = set(config.component_indexes)
            components = [
                component
                for component_index, component in enumerate(components)
                if component_index in wanted_indexes
            ]

        issue_ids = {
            node_id
            for node_type, node_id in _flatten_nodes(components)
            if node_type == "issue"
        }
        pr_ids = {
            node_id
            for node_type, node_id in _flatten_nodes(components)
            if node_type == "pr"
        }
        commit_ids = {
            node_id
            for node_type, node_id in _flatten_nodes(components)
            if node_type == "commit"
        }

        work_items = fetch_work_items(
            sink, work_item_ids=issue_ids, org_id=config.org_id or ""
        )
        active_hours = fetch_work_item_active_hours(
            sink, work_item_ids=issue_ids, org_id=config.org_id or ""
        )
        repo_prs = _group_prs_by_repo(pr_ids)
        prs = fetch_pull_requests(
            sink, repo_numbers=repo_prs, org_id=config.org_id or ""
        )
        repo_commits = _group_commits_by_repo(commit_ids)
        commits = fetch_commits(
            sink, repo_commits=repo_commits, org_id=config.org_id or ""
        )
        commit_churn = fetch_commit_churn(
            sink, repo_commits=repo_commits, org_id=config.org_id or ""
        )

        work_item_map = {str(item.get("work_item_id")): item for item in work_items}
        pr_map = _map_prs(prs)
        commit_map = _map_commits(commits)
        pr_churn = _pr_churn_map(prs)

        parent_ids = {
            str(item.get("parent_id") or "")
            for item in work_items
            if item.get("parent_id")
        }
        epic_ids = {
            str(item.get("epic_id") or "") for item in work_items if item.get("epic_id")
        }
        parent_titles = fetch_parent_titles(
            sink, work_item_ids=parent_ids, org_id=config.org_id or ""
        )
        epic_titles = fetch_parent_titles(
            sink, work_item_ids=epic_ids, org_id=config.org_id or ""
        )

        records: list[WorkUnitInvestmentRecord] = []
        repo_effort_records: list[WorkUnitRepoEffortRecord] = []
        quote_records: list[WorkUnitInvestmentEvidenceQuoteRecord] = []
        run_id = config.run_id or uuid.uuid4().hex
        computed_at = config.computed_at or datetime.now(timezone.utc)
        model_version = _effective_model_version(
            resolved_llm_provider, config.llm_model
        )

        logger.info(
            "Materializing investments for %d components (run_id=%s)",
            len(components),
            run_id,
        )

        # Pre-process all components: build bundles and separate into LLM vs fallback
        pending_llm: list[tuple[int, Any]] = []
        fallback_results: list[tuple[int, Any]] = []
        preprocessed: dict[int, PreprocessedComponent] = {}

        for idx, (nodes, component_edges) in enumerate(components):
            unit_nodes = list(dict.fromkeys(nodes))
            issue_node_ids = [
                node_id for node_type, node_id in unit_nodes if node_type == "issue"
            ]
            pr_node_ids = [
                node_id for node_type, node_id in unit_nodes if node_type == "pr"
            ]
            commit_node_ids = [
                node_id for node_type, node_id in unit_nodes if node_type == "commit"
            ]

            bounds = compute_time_bounds(unit_nodes, work_item_map, pr_map, commit_map)
            if bounds is None:
                continue
            if bounds.end < config.from_ts or bounds.start >= config.to_ts:
                continue

            unit_id = work_unit_id(unit_nodes)
            bundle = build_text_bundle(
                issue_ids=issue_node_ids,
                pr_ids=pr_node_ids,
                commit_ids=commit_node_ids,
                work_item_map=work_item_map,
                pr_map=pr_map,
                commit_map=commit_map,
                parent_titles=parent_titles,
                epic_titles=epic_titles,
                work_unit_id=unit_id,
            )

            data = PreprocessedComponent(
                unit_id=unit_id,
                unit_nodes=unit_nodes,
                issue_node_ids=issue_node_ids,
                pr_node_ids=pr_node_ids,
                commit_node_ids=commit_node_ids,
                bounds=bounds,
                bundle=bundle,
                component_edges=component_edges,
            )
            preprocessed[idx] = data

            if bundle.text_char_count < MIN_EVIDENCE_CHARS:
                fallback_results.append(
                    (idx, fallback_outcome("insufficient_evidence"))
                )
            elif bundle.text_source_count == 0:
                fallback_results.append((idx, fallback_outcome("no_text_sources")))
            else:
                pending_llm.append((idx, bundle))

        logger.info(
            "Pre-processed %d components: %d need LLM, %d use fallback",
            len(preprocessed),
            len(pending_llm),
            len(fallback_results),
        )

        skipped_existing: set[int] = set()
        if pending_llm and not config.force:
            pending_keys = {
                idx: (preprocessed[idx].unit_id, bundle.input_hash)
                for idx, bundle in pending_llm
            }
            existing_keys = await asyncio.to_thread(
                _fetch_existing_investment_keys,
                sink,
                org_id=config.org_id or "",
                keys=pending_keys.values(),
                model_version=model_version,
            )
            skipped_existing = {
                idx for idx, key in pending_keys.items() if key in existing_keys
            }
            if skipped_existing:
                pending_llm = [
                    (idx, bundle)
                    for idx, bundle in pending_llm
                    if idx not in skipped_existing
                ]
                logger.info(
                    "Skipping %d unchanged investment bundle(s) with fresh categorization",
                    len(skipped_existing),
                )

        from dev_health_ops.llm.credentials import resolve_llm_org_settings_concurrency

        requested_concurrency = (
            resolve_llm_org_settings_concurrency(org_id=config.org_id or None)
            or config.llm_concurrency
        )
        configured_concurrency = min(
            max(1, requested_concurrency), _MAX_LLM_CONCURRENCY
        )
        if configured_concurrency != requested_concurrency:
            logger.warning(
                "LLM concurrency %d exceeds maximum %d; clamping to %d",
                requested_concurrency,
                _MAX_LLM_CONCURRENCY,
                configured_concurrency,
            )

        class _AdaptiveLLMConcurrency:
            def __init__(self, limit: int) -> None:
                self.base_limit = max(1, int(limit))
                self.effective_limit = self.base_limit
                self.active = 0
                self.rate_limit_streak = 0
                self.success_streak = 0
                self.condition = asyncio.Condition()

            async def acquire(self) -> None:
                async with self.condition:
                    while self.active >= self.effective_limit:
                        await self.condition.wait()
                    self.active += 1

            async def release(self) -> None:
                async with self.condition:
                    self.active = max(0, self.active - 1)
                    self.condition.notify_all()

            async def record_success(self) -> None:
                async with self.condition:
                    self.rate_limit_streak = 0
                    if self.effective_limit < self.base_limit:
                        self.success_streak += 1
                        if self.success_streak >= 2:
                            self.effective_limit = min(
                                self.base_limit, self.effective_limit + 1
                            )
                            self.success_streak = 0
                            logger.info(
                                "LLM adaptive concurrency recovered to %d/%d",
                                self.effective_limit,
                                self.base_limit,
                            )
                            self.condition.notify_all()

            async def record_failure(self, failure_class: str) -> None:
                async with self.condition:
                    if failure_class == "rate_limit":
                        self.rate_limit_streak += 1
                        self.success_streak = 0
                        if self.rate_limit_streak >= 2 and self.effective_limit > 1:
                            self.effective_limit = max(1, self.effective_limit // 2)
                            self.rate_limit_streak = 0
                            logger.warning(
                                "LLM adaptive concurrency reduced to %d/%d after sustained rate limits",
                                self.effective_limit,
                                self.base_limit,
                            )
                            self.condition.notify_all()
                    else:
                        self.rate_limit_streak = 0

        adaptive_concurrency = _AdaptiveLLMConcurrency(configured_concurrency)
        llm_results: dict[int, Any] = {}
        llm_failure_counts: Counter[str] = Counter()
        llm_token_usage_flushed = False

        def flush_llm_token_usage() -> tuple[int, int, int]:
            nonlocal llm_token_usage_flushed
            llm_calls = sum(
                int(getattr(outcome, "llm_calls", 0))
                for outcome in llm_results.values()
            )
            llm_input_tokens = sum(
                int(getattr(outcome, "input_tokens", 0))
                for outcome in llm_results.values()
            )
            llm_output_tokens = sum(
                int(getattr(outcome, "output_tokens", 0))
                for outcome in llm_results.values()
            )
            if not llm_token_usage_flushed:
                write_llm_token_usage(
                    sink,
                    org_id=config.org_id or "",
                    provider=resolved_llm_provider,
                    model=model_version,
                    source="investment_materialize",
                    input_tokens=llm_input_tokens,
                    output_tokens=llm_output_tokens,
                    calls=llm_calls,
                    computed_at=computed_at,
                )
                llm_token_usage_flushed = True
            return llm_calls, llm_input_tokens, llm_output_tokens

        def record_completed_llm_task_result(result: Any) -> None:
            if not isinstance(result, tuple) or len(result) != 2:
                logger.warning("Unexpected LLM task result: %r", result)
                return
            idx, outcome = result
            llm_results[idx] = outcome

        async def categorize_with_limit(idx: int, bundle: Any) -> tuple[int, Any]:
            await adaptive_concurrency.acquire()
            try:
                outcome = await categorize_text_bundle(
                    bundle,
                    llm_provider=resolved_llm_provider,
                    llm_model=config.llm_model,
                    provider=provider_instance,
                )
                return (idx, outcome)
            finally:
                await adaptive_concurrency.release()

        batch_results: dict[int, Any] | None = None
        if pending_llm and config.llm_batch_mode != "sync":
            batch_results = await _categorize_with_provider_batch(
                config=config,
                provider_instance=provider_instance,
                resolved_llm_provider=resolved_llm_provider,
                model_version=model_version,
                run_id=run_id,
                pending_llm=pending_llm,
                preprocessed=preprocessed,
            )
            if batch_results is not None:
                llm_results.update(batch_results)

        if pending_llm and batch_results is None:
            logger.info(
                "Starting parallel LLM categorization (%d tasks, concurrency=%d)",
                len(pending_llm),
                configured_concurrency,
            )
            tasks = [
                asyncio.create_task(categorize_with_limit(idx, bundle))
                for idx, bundle in pending_llm
            ]
            for task in asyncio.as_completed(tasks):
                try:
                    result = await task
                except Exception as exc:
                    classified = _classify_llm_exception(
                        exc,
                        provider=config.llm_provider,
                        model=config.llm_model or config.llm_provider,
                    )
                    failure_class = _llm_failure_class(classified)
                    llm_failure_counts[failure_class] += 1
                    await adaptive_concurrency.record_failure(failure_class)
                    if _is_deterministic_llm_failure(classified):
                        for pending_task in tasks:
                            if not pending_task.done():
                                pending_task.cancel()
                        settled_results = await asyncio.gather(
                            *tasks, return_exceptions=True
                        )
                        for settled_result in settled_results:
                            if isinstance(settled_result, BaseException):
                                continue
                            record_completed_llm_task_result(settled_result)
                        flush_llm_token_usage()
                        verdict = _format_llm_summary(
                            len(llm_results), llm_failure_counts
                        )
                        logger.error(
                            "Investment materialization stopped on fatal LLM failure: %s",
                            verdict,
                        )
                        raise classified
                    logger.warning(
                        "LLM task failed (%s): %s", failure_class, classified
                    )
                    continue
                record_completed_llm_task_result(result)
                await adaptive_concurrency.record_success()
            logger.info(
                "Completed LLM categorizations: %s",
                _format_llm_summary(len(llm_results), llm_failure_counts),
            )

        # Merge fallback results
        for idx, outcome in fallback_results:
            llm_results[idx] = outcome

        llm_calls, llm_input_tokens, llm_output_tokens = flush_llm_token_usage()
        if llm_calls or llm_failure_counts:
            logger.info(
                "LLM usage summary: calls=%d input_tokens=%d output_tokens=%d %s",
                llm_calls,
                llm_input_tokens,
                llm_output_tokens,
                _format_llm_summary(
                    len(llm_results) - len(fallback_results), llm_failure_counts
                ),
            )
        # Post-process: create records from outcomes
        for idx, data in preprocessed.items():
            if idx in skipped_existing:
                # Categorization is unchanged, so skip re-writing the LLM
                # investment record -- but repo-effort allocation is derived
                # from structural churn (no LLM), so it MUST still be written
                # here or unchanged units would never get repo-allocation rows,
                # leaving the investment repo Sankey empty in steady state.
                skipped_metric, skipped_value = _effort_from_work_unit(
                    issue_ids=data.issue_node_ids,
                    pr_ids=data.pr_node_ids,
                    commit_ids=data.commit_node_ids,
                    pr_churn=pr_churn,
                    commit_churn=commit_churn,
                    active_hours=active_hours,
                )
                repo_effort_records.extend(
                    WorkUnitRepoEffortRecord(
                        work_unit_id=data.unit_id,
                        repo_id=allocated_repo_id,
                        effort_metric=skipped_metric,
                        effort_value=allocated_effort_value,
                        allocation_weight=allocation_weight,
                        allocation_source=allocation_source,
                        categorization_run_id=run_id,
                        computed_at=computed_at,
                        org_id=config.org_id or "",
                    )
                    for (
                        allocated_repo_id,
                        allocated_effort_value,
                        allocation_weight,
                        allocation_source,
                    ) in _allocate_repo_effort(
                        issue_ids=data.issue_node_ids,
                        pr_ids=data.pr_node_ids,
                        commit_ids=data.commit_node_ids,
                        pr_churn=pr_churn,
                        commit_churn=commit_churn,
                        active_hours=active_hours,
                        effort_metric=skipped_metric,
                        effort_value=skipped_value,
                    )
                )
                continue
            outcome = llm_results.get(idx)
            if outcome is None:
                outcome = fallback_outcome("llm_task_failed")

            unit_id = data.unit_id
            unit_nodes = data.unit_nodes
            issue_node_ids = data.issue_node_ids
            pr_node_ids = data.pr_node_ids
            commit_node_ids = data.commit_node_ids
            bounds = data.bounds
            bundle = data.bundle
            component_edges = data.component_edges

            theme_distribution = rollup_subcategories_to_themes(outcome.subcategories)
            evidence_quality_value = compute_evidence_quality(
                text_bundle=bundle,
                nodes_count=len(unit_nodes),
                edges=component_edges,
            )
            if outcome.status == "invalid_llm_output":
                evidence_quality_value = min(float(evidence_quality_value), 0.3)
            evidence_band = evidence_quality_band(float(evidence_quality_value))
            categorization_audit = [*outcome.errors, *outcome.warnings]

            effort_metric, effort_value = _effort_from_work_unit(
                issue_ids=issue_node_ids,
                pr_ids=pr_node_ids,
                commit_ids=commit_node_ids,
                pr_churn=pr_churn,
                commit_churn=commit_churn,
                active_hours=active_hours,
            )

            structural_evidence = {
                "issues": sorted(issue_node_ids),
                "prs": sorted(pr_node_ids),
                "commits": sorted(commit_node_ids),
                "edges": sorted(
                    edge_id
                    for edge_id in (
                        str(edge.get("edge_id") or "") for edge in component_edges
                    )
                    if edge_id
                ),
            }

            repo_id = None
            repo_candidates = _collect_repo_ids(component_edges)
            if len(repo_candidates) == 1:
                repo_id = _parse_repo_id(repo_candidates[0])

            provider = _collect_provider(issue_node_ids, work_item_map)
            work_unit_type, work_unit_name = _resolve_work_unit_label(
                issue_ids=issue_node_ids,
                pr_ids=pr_node_ids,
                commit_ids=commit_node_ids,
                work_item_map=work_item_map,
                pr_map=pr_map,
                commit_map=commit_map,
            )

            records.append(
                WorkUnitInvestmentRecord(
                    work_unit_id=unit_id,
                    work_unit_type=work_unit_type,
                    work_unit_name=work_unit_name,
                    from_ts=bounds.start,
                    to_ts=bounds.end,
                    repo_id=repo_id,
                    provider=provider,
                    effort_metric=effort_metric,
                    effort_value=effort_value,
                    theme_distribution_json=theme_distribution,
                    subcategory_distribution_json=outcome.subcategories,
                    structural_evidence_json=json.dumps(structural_evidence),
                    evidence_quality=evidence_quality_value,
                    evidence_quality_band=evidence_band,
                    categorization_status=outcome.status,
                    categorization_errors_json=json.dumps(categorization_audit),
                    categorization_model_version=model_version,
                    categorization_input_hash=bundle.input_hash,
                    categorization_run_id=run_id,
                    computed_at=computed_at,
                    org_id=config.org_id or "",
                )
            )
            repo_effort_records.extend(
                WorkUnitRepoEffortRecord(
                    work_unit_id=unit_id,
                    repo_id=allocated_repo_id,
                    effort_metric=effort_metric,
                    effort_value=allocated_effort_value,
                    allocation_weight=allocation_weight,
                    allocation_source=allocation_source,
                    categorization_run_id=run_id,
                    computed_at=computed_at,
                    org_id=config.org_id or "",
                )
                for (
                    allocated_repo_id,
                    allocated_effort_value,
                    allocation_weight,
                    allocation_source,
                ) in _allocate_repo_effort(
                    issue_ids=issue_node_ids,
                    pr_ids=pr_node_ids,
                    commit_ids=commit_node_ids,
                    pr_churn=pr_churn,
                    commit_churn=commit_churn,
                    active_hours=active_hours,
                    effort_metric=effort_metric,
                    effort_value=effort_value,
                )
            )

            # NOTE (CHAOS-2433 round-3 finding #2): the materializer NO LONGER
            # writes work_unit_membership rows or completion markers.  It only
            # categorizes and persists work_unit_investments here.  Membership is
            # written EXCLUSIVELY by the no-LLM projection (backfill.py), which
            # iterates the FULL current work graph (not this run's time window)
            # and so publishes a legitimately FULL-COVERAGE org-wide marker.
            #
            # This unification fixes the partial-coverage blanking bug: a
            # date-windowed materialize only processes in-window components, so an
            # org-wide marker from it would blank current components OUTSIDE the
            # window.  By making the full-coverage projection the SOLE membership
            # writer, the windowed materializer can never publish partial coverage.
            # The post-sync chain (build -> materialize -> project-membership)
            # runs the projection right after materialize persists the new
            # investments, so membership stays fresh.

            if config.persist_evidence_snippets and outcome.evidence_quotes:
                for quote in outcome.evidence_quotes:
                    quote_records.append(
                        WorkUnitInvestmentEvidenceQuoteRecord(
                            work_unit_id=unit_id,
                            quote=quote.quote,
                            source_type=quote.source_type,
                            source_id=quote.source_id,
                            computed_at=computed_at,
                            categorization_run_id=run_id,
                            org_id=config.org_id or "",
                        )
                    )

        logger.info("Finished component loop, writing %d records to sink", len(records))
        if records:
            sink.write_work_unit_investments(records)
        if repo_effort_records:
            sink.write_work_unit_repo_effort(repo_effort_records)
        if quote_records:
            sink.write_work_unit_investment_quotes(quote_records)
        # CHAOS-2433 round-3 finding #2: membership rows + completion markers are
        # written EXCLUSIVELY by the no-LLM projection (backfill.py).  The
        # materializer persists work_unit_investments only.  The post-sync chain
        # runs the projection (build -> materialize -> project-membership) right
        # after this, so membership is refreshed from the new investments with
        # FULL current-component coverage (never the partial coverage a windowed
        # materialize would produce).
        logger.info("Sink write complete")

        return {
            "components": len(components),
            "total_components": total_components,
            "records": len(records),
            "repo_effort_records": len(repo_effort_records),
            "quotes": len(quote_records),
            "skipped_existing": len(skipped_existing),
            "llm_calls": llm_calls,
            "llm_input_tokens": llm_input_tokens,
            "llm_output_tokens": llm_output_tokens,
            "llm_failures": sum(llm_failure_counts.values()),
            "llm_failure_counts": dict(llm_failure_counts),
        }
    finally:
        sink.close()
        if provider_instance:
            await provider_instance.aclose()
