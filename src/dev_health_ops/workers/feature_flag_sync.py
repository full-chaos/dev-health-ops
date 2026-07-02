from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Any

from dev_health_ops.workers.async_runner import run_async

logger = logging.getLogger(__name__)


def _ch_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _load_pr_ids_by_repo_path(
    sink: Any,
    *,
    org_id: str,
    repo_paths: set[tuple[str, str]],
) -> dict[tuple[str, str], set[str]]:
    if not repo_paths:
        return {}

    from dev_health_ops.work_graph.ids import generate_pr_id

    repo_ids = sorted({repo_id for repo_id, _path in repo_paths})
    paths = sorted({path for _repo_id, path in repo_paths})
    query = f"""
        SELECT DISTINCT
            p.repo_id AS repo_id,
            p.pr_number AS pr_number,
            s.file_path AS file_path
        FROM work_graph_pr_commit AS p
        INNER JOIN git_commit_stats AS s ON (
            toString(p.repo_id) = toString(s.repo_id)
            AND p.commit_hash = s.commit_hash
            AND toString(p.org_id) = toString(s.org_id)
        )
        WHERE p.org_id = {_ch_string(org_id)}
          AND toString(p.repo_id) IN ({", ".join(_ch_string(v) for v in repo_ids)})
          AND s.file_path IN ({", ".join(_ch_string(v) for v in paths)})
    """
    rows = sink.query_dicts(query, {})
    matches: dict[tuple[str, str], set[str]] = {}
    for row in rows:
        repo_id = str(row.get("repo_id") or "")
        file_path = str(row.get("file_path") or "")
        pr_number = row.get("pr_number")
        if not repo_id or not file_path or pr_number is None:
            continue
        matches.setdefault((repo_id, file_path), set()).add(
            generate_pr_id(uuid.UUID(repo_id), int(pr_number))
        )
    return matches


def _sync_launchdarkly_feature_flags(
    *,
    db_url: str,
    org_id: str,
    credentials: dict[str, Any],
    sync_options: dict[str, Any],
    since_dt: datetime | None,
) -> dict[str, Any]:
    from dev_health_ops.connectors.exceptions import ConnectorException
    from dev_health_ops.metrics.job_work_items import (
        attach_work_item_partial_observations,
    )
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
    from dev_health_ops.processors.launchdarkly import (
        normalize_audit_events,
        normalize_flags,
    )
    from dev_health_ops.providers.launchdarkly.client import LaunchDarklyClient
    from dev_health_ops.providers.launchdarkly.code_refs import (
        LD_CODE_REFERENCE_CONFIDENCE,
        LaunchDarklyCodeReferencesClient,
        build_code_reference_links,
        index_repo_rows,
        resolve_repo_id,
    )
    from dev_health_ops.providers.usage import drain_provider_usage
    from dev_health_ops.work_graph.builder import BuildConfig, WorkGraphBuilder
    from dev_health_ops.work_graph.ids import generate_feature_flag_id
    from dev_health_ops.work_graph.models import EdgeType, NodeType, Provenance

    api_key = str(credentials.get("api_key") or "")
    project_key = str(
        credentials.get("project_key") or sync_options.get("project_key") or ""
    )
    environment = str(
        credentials.get("environment") or sync_options.get("environment") or ""
    )
    if not api_key or not project_key:
        raise ValueError(
            "LaunchDarkly feature-flag sync requires api_key and project_key"
        )
    if not db_url.startswith("clickhouse://"):
        raise ValueError(
            "Feature-flag sync requires CLICKHOUSE_URI / ClickHouse analytics sink"
        )

    async def _run() -> dict[str, Any]:
        # Real per-request actuals drained through the shared CHAOS-2754
        # recorder for all 3 currently-emitted LaunchDarkly route families
        # (flags, audit_log, code_refs) -- CHAOS-2761.
        provider_usage_observations: list[dict[str, Any]] = []

        connector: LaunchDarklyClient | None = None
        try:
            async with LaunchDarklyClient(
                api_key=api_key, project_key=project_key
            ) as connector:
                raw_flags = await connector.get_flags(project_key)
                raw_events = await connector.get_audit_log(since=since_dt, limit=1000)
        except Exception as exc:
            # Preserve actuals gathered before the raise (e.g. flags succeeded,
            # audit_log then hit a rate limit) so the worker's deferral/failure
            # stamp can still persist them (CHAOS-2754 contract, reused here
            # verbatim -- `attach_work_item_partial_observations` is
            # provider-neutral despite its name; `_merge_partial_observations_
            # into_result` in workers/sync_units.py reads it regardless of
            # dataset/provider).
            if connector is not None:
                provider_usage_observations.extend(drain_provider_usage(connector))
            if provider_usage_observations:
                attach_work_item_partial_observations(
                    exc, {"provider_usage": provider_usage_observations}
                )
            raise
        provider_usage_observations.extend(drain_provider_usage(connector))

        code_refs_client: LaunchDarklyCodeReferencesClient | None = None
        try:
            async with LaunchDarklyCodeReferencesClient(
                api_key=api_key
            ) as code_refs_client:
                raw_code_refs = await code_refs_client.list_default_branch_references(
                    project_key=project_key
                )
            code_references_error = None
        except ConnectorException as exc:
            logger.warning(
                "Skipping LaunchDarkly code references for project %s: %s",
                project_key,
                exc,
            )
            raw_code_refs = []
            code_references_error = str(exc)
        finally:
            # Code references are best-effort (errors are swallowed above), but
            # any requests that DID complete before the failure still count as
            # real actuals -- drain regardless of outcome.
            if code_refs_client is not None:
                provider_usage_observations.extend(
                    drain_provider_usage(code_refs_client)
                )

        flags = normalize_flags(raw_flags, org_id)
        if environment:
            flags = [
                flag.__class__(
                    provider=flag.provider,
                    flag_key=flag.flag_key,
                    project_key=flag.project_key,
                    repo_id=flag.repo_id,
                    environment=environment,
                    flag_type=flag.flag_type,
                    created_at=flag.created_at,
                    archived_at=flag.archived_at,
                    last_synced=flag.last_synced,
                    org_id=flag.org_id,
                )
                for flag in flags
            ]
        events = normalize_audit_events(raw_events, org_id)
        if environment:
            events = [
                event.__class__(
                    event_type=event.event_type,
                    flag_key=event.flag_key,
                    environment=event.environment or environment,
                    repo_id=event.repo_id,
                    actor_type=event.actor_type,
                    prev_state=event.prev_state,
                    next_state=event.next_state,
                    event_ts=event.event_ts,
                    ingested_at=event.ingested_at,
                    source_event_id=event.source_event_id,
                    dedupe_key=event.dedupe_key,
                    org_id=event.org_id,
                )
                for event in events
            ]

        sink = ClickHouseMetricsSink(db_url)
        setattr(sink, "org_id", org_id)
        builder = WorkGraphBuilder(BuildConfig(dsn=db_url, org_id=org_id))
        try:
            sink.write_feature_flags(flags)
            sink.write_feature_flag_events(events)

            repo_rows = sink.query_dicts(
                "SELECT id, repo FROM repos WHERE org_id = " + _ch_string(org_id),
                {},
            )
            repo_index = index_repo_rows(repo_rows)
            repo_paths = {
                (str(repo_id), ref.file_path)
                for ref in raw_code_refs
                if (repo_id := resolve_repo_id(ref, repo_index)) is not None
            }
            pr_ids_by_repo_path = _load_pr_ids_by_repo_path(
                sink,
                org_id=org_id,
                repo_paths=repo_paths,
            )
            code_ref_links, code_ref_edges = build_code_reference_links(
                raw_code_refs,
                org_id=org_id,
                repo_index=repo_index,
                pr_ids_by_repo_path=pr_ids_by_repo_path,
            )
            sink.write_feature_flag_links(code_ref_links)
            for edge in code_ref_edges:
                target_type = (
                    NodeType.FILE if edge["target_type"] == "file" else NodeType.PR
                )
                builder.add_feature_flag_edge(
                    flag_id=str(edge["flag_id"]),
                    target_type=target_type,
                    target_id=str(edge["target_id"]),
                    edge_type=EdgeType.GUARDS,
                    confidence=LD_CODE_REFERENCE_CONFIDENCE,
                    evidence=str(edge["evidence"]),
                    provenance=Provenance.NATIVE,
                    repo_id=edge["repo_id"],
                    provider="launchdarkly",
                )

            latest_events: dict[str, Any] = {}
            for event in events:
                existing = latest_events.get(event.flag_key)
                if existing is None or event.event_ts > existing.event_ts:
                    latest_events[event.flag_key] = event

            for flag in flags:
                builder.add_feature_flag_node(
                    flag_key=flag.flag_key,
                    provider=flag.provider,
                    project_key=flag.project_key or project_key,
                    repo_id=flag.repo_id,
                    event_ts=flag.created_at,
                )
                latest_event = latest_events.get(flag.flag_key)
                if latest_event is None:
                    continue
                flag_id = generate_feature_flag_id(
                    org_id,
                    flag.provider,
                    flag.project_key or project_key,
                    flag.flag_key,
                )
                builder.add_feature_flag_edge(
                    flag_id=flag_id,
                    target_type=NodeType.FEATURE_FLAG,
                    target_id=flag_id,
                    edge_type=EdgeType.CONFIG_CHANGED_BY,
                    confidence=1.0,
                    evidence=(
                        f"{latest_event.event_ts.isoformat()}"
                        f"|{latest_event.event_type}"
                        f"|{latest_event.next_state or ''}"
                    ),
                    provenance=Provenance.NATIVE,
                    provider=flag.provider,
                    event_ts=latest_event.event_ts,
                )
        finally:
            builder.close()
            sink.close()

        result: dict[str, Any] = {
            "flags_synced": len(flags),
            "events_synced": len(events),
            "code_references_synced": len(raw_code_refs),
            "code_reference_links_synced": len(code_ref_links),
            "code_reference_edges_synced": len(code_ref_edges),
            "code_references_error": code_references_error,
            "project_key": project_key,
            "environment": environment or None,
        }
        if provider_usage_observations:
            result["observations"] = {"provider_usage": provider_usage_observations}
        return result

    return run_async(_run())


def _sync_gitlab_feature_flags(
    *,
    db_url: str,
    org_id: str,
    credentials: dict[str, Any],
    sync_options: dict[str, Any],
) -> dict[str, Any]:
    from dev_health_ops.connectors.gitlab import GitLabConnector
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
    from dev_health_ops.processors.gitlab_feature_flags import (
        normalize_gitlab_feature_flags,
        snapshot_gitlab_feature_flag_events,
    )
    from dev_health_ops.work_graph.builder import BuildConfig, WorkGraphBuilder
    from dev_health_ops.work_graph.ids import generate_feature_flag_id
    from dev_health_ops.work_graph.models import EdgeType, NodeType, Provenance

    token = str(credentials.get("token") or "")
    gitlab_url = str(
        credentials.get("url")
        or credentials.get("base_url")
        or sync_options.get("gitlab_url")
        or "https://gitlab.com"
    )
    project_id_or_path = (
        sync_options.get("project_id")
        or sync_options.get("repo")
        or sync_options.get("project_key")
    )
    if not token or not project_id_or_path:
        raise ValueError(
            "GitLab feature-flag sync requires token and project_id/project path"
        )
    if not db_url.startswith("clickhouse://"):
        raise ValueError(
            "Feature-flag sync requires CLICKHOUSE_URI / ClickHouse analytics sink"
        )

    connector = GitLabConnector(url=gitlab_url, private_token=token)
    raw_flags = connector.get_feature_flags(project_id_or_path)
    project_key = connector.get_project_name(project_id_or_path)
    repo_id = None

    flags = normalize_gitlab_feature_flags(
        raw_flags,
        project_key=project_key,
        org_id=org_id,
        repo_id=repo_id,
    )
    events = snapshot_gitlab_feature_flag_events(
        raw_flags,
        project_key=project_key,
        org_id=org_id,
        repo_id=repo_id,
    )

    sink = ClickHouseMetricsSink(db_url)
    setattr(sink, "org_id", org_id)
    builder = WorkGraphBuilder(BuildConfig(dsn=db_url, org_id=org_id))
    try:
        sink.write_feature_flags(flags)
        sink.write_feature_flag_events(events)

        latest_events: dict[str, Any] = {}
        for event in events:
            existing = latest_events.get(event.flag_key)
            if existing is None or event.event_ts > existing.event_ts:
                latest_events[event.flag_key] = event

        for flag in flags:
            builder.add_feature_flag_node(
                flag_key=flag.flag_key,
                provider=flag.provider,
                project_key=flag.project_key or project_key,
                repo_id=flag.repo_id,
                event_ts=flag.created_at,
            )
            latest_event = latest_events.get(flag.flag_key)
            if latest_event is None:
                continue
            flag_id = generate_feature_flag_id(
                org_id,
                flag.provider,
                flag.project_key or project_key,
                flag.flag_key,
            )
            builder.add_feature_flag_edge(
                flag_id=flag_id,
                target_type=NodeType.FEATURE_FLAG,
                target_id=flag_id,
                edge_type=EdgeType.CONFIG_CHANGED_BY,
                confidence=1.0,
                evidence=(
                    f"{latest_event.event_ts.isoformat()}"
                    f"|{latest_event.event_type}"
                    f"|{latest_event.next_state or ''}"
                ),
                provenance=Provenance.NATIVE,
                provider=flag.provider,
                event_ts=latest_event.event_ts,
            )
    finally:
        builder.close()
        sink.close()

    return {
        "flags_synced": len(flags),
        "events_synced": len(events),
        "project_key": project_key,
        "gitlab_url": gitlab_url,
    }
