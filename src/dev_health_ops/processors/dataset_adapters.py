from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import date, datetime, time, timezone
from typing import Any

from dev_health_ops.credentials.resolver import (
    github_credentials_from_mapping,
    gitlab_credentials_from_mapping,
    jira_credentials_from_mapping,
    pagerduty_credentials_from_mapping,
    resolve_gitlab_url,
)
from dev_health_ops.metrics.sinks.ingestion import IngestionSink
from dev_health_ops.providers.usage import (
    PROVIDER_USAGE_OBSERVATION_KEY,
    attach_partial_observations,
)
from dev_health_ops.storage import resolve_db_type, run_with_store
from dev_health_ops.sync.datasets import DatasetKey
from dev_health_ops.workers.async_runner import run_async
from dev_health_ops.workers.sync_bootstrap import ProviderRuntime, SyncTaskContext

_PROCESSOR_FLAG_NAMES = (
    "sync_git",
    "sync_prs",
    "sync_cicd",
    "sync_deployments",
    "sync_incidents",
    "sync_security",
    "sync_tests",
    "blame_only",
    "sync_commits",
    "sync_commit_stats",
    "sync_files",
    "sync_blame",
)

_CODE_DATASETS = frozenset(
    {
        DatasetKey.REPO_METADATA.value,
        DatasetKey.COMMITS.value,
        DatasetKey.COMMIT_STATS.value,
        DatasetKey.FILES.value,
        DatasetKey.BLAME.value,
        DatasetKey.PRS.value,
        DatasetKey.PR_REVIEWS.value,
        DatasetKey.PR_COMMENTS.value,
        DatasetKey.CICD.value,
        DatasetKey.TESTS.value,
        DatasetKey.DEPLOYMENTS.value,
        DatasetKey.INCIDENTS.value,
        DatasetKey.SECURITY.value,
    }
)

_WORK_ITEM_DATASETS = frozenset(
    {
        DatasetKey.WORK_ITEMS.value,
        DatasetKey.WORK_ITEM_LABELS.value,
        DatasetKey.WORK_ITEM_PROJECTS.value,
        DatasetKey.WORK_ITEM_HISTORY.value,
        DatasetKey.WORK_ITEM_COMMENTS.value,
    }
)

_PAGERDUTY_DATASETS = frozenset(
    {
        DatasetKey.SERVICES.value,
        DatasetKey.BUSINESS_SERVICES.value,
        DatasetKey.ESCALATION_POLICIES.value,
        DatasetKey.SCHEDULES.value,
        DatasetKey.ON_CALLS.value,
        DatasetKey.USERS.value,
        DatasetKey.TEAMS.value,
        DatasetKey.INCIDENTS.value,
        DatasetKey.INCIDENT_ALERTS.value,
        DatasetKey.INCIDENT_LOG_ENTRIES.value,
        DatasetKey.INCIDENT_NOTES.value,
    }
)


async def _run_with_reused_or_new_store(
    context: SyncTaskContext,
    runtime: ProviderRuntime,
    handler: Callable[[Any], Awaitable[Any]],
) -> Any:
    if runtime.store is not None:
        if context.org_id:
            setattr(runtime.store, "org_id", context.org_id)
        return await handler(runtime.store)

    db_type = resolve_db_type(context.db_url, None)
    return await run_with_store(
        context.db_url,
        db_type,
        handler,
        org_id=context.org_id,
    )


def _credentials_mapping(context: SyncTaskContext) -> dict[str, Any]:
    credentials = context.decrypted_credentials or {}
    if isinstance(credentials, dict):
        return credentials
    return {}


def _explicit_flags(context: SyncTaskContext) -> dict[str, bool]:
    flags = {name: False for name in _PROCESSOR_FLAG_NAMES}
    flags.update(
        {
            str(name): bool(value)
            for name, value in (context.processor_flags or {}).items()
            if str(name) in flags
        }
    )
    return flags


def _github_repo_parts(source_external_id: str) -> tuple[str, str]:
    parts = (source_external_id or "").split("/", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(
            "GitHub dataset unit requires source_external_id in 'owner/repo' form"
        )
    return parts[0], parts[1]


def _github_credentials(context: SyncTaskContext) -> Any:
    credentials = context.decrypted_credentials
    if not isinstance(credentials, dict):
        return credentials
    resolved = github_credentials_from_mapping(credentials)
    if resolved is None:
        raise ValueError("Missing GitHub credentials for dataset unit")
    return resolved


def _gitlab_credentials(context: SyncTaskContext) -> tuple[str, str]:
    mapping = _credentials_mapping(context)
    resolved = gitlab_credentials_from_mapping(mapping)
    if resolved is None:
        raise ValueError("Missing GitLab token for dataset unit")
    gitlab_url = resolve_gitlab_url(mapping, resolved)
    return resolved.token, gitlab_url


def _gitlab_project_id(source_external_id: str) -> int:
    try:
        return int(source_external_id)
    except (TypeError, ValueError):
        pass
    raise ValueError(
        "GitLab dataset unit requires numeric source_external_id project id"
    )


def _pagerduty_client(context: SyncTaskContext) -> tuple[Any, str]:
    from dev_health_ops.providers.pagerduty.auth import (
        ApiTokenAuth,
        OAuthBearerAuth,
        PagerDutyAuth,
    )
    from dev_health_ops.providers.pagerduty.client import PagerDutyClient

    credentials = pagerduty_credentials_from_mapping(_credentials_mapping(context))
    if credentials is None:
        raise ValueError("Missing PagerDuty credentials for dataset unit")
    auth: PagerDutyAuth
    match credentials.auth_mode:
        case "oauth" | "client_credentials" as auth_mode:
            access_token = credentials.access_token
            if not access_token:
                raise ValueError(
                    f"PagerDuty {auth_mode} credential is missing a hydrated access token"
                )
            auth = OAuthBearerAuth(access_token)
        case "api_token":
            api_token = credentials.api_token
            if not api_token:
                raise ValueError(
                    "PagerDuty api_token credential is missing an API token"
                )
            auth = ApiTokenAuth(api_token)
        case None:
            if credentials.access_token:
                auth = OAuthBearerAuth(credentials.access_token)
            elif credentials.api_token:
                auth = ApiTokenAuth(credentials.api_token)
            else:
                raise ValueError(
                    "PagerDuty dataset unit requires an access token or API token"
                )
        case auth_mode:
            raise ValueError(f"Unsupported PagerDuty auth mode: {auth_mode}")
    provider_instance_id = (
        credentials.subdomain.strip() if credentials.subdomain else ""
    )
    if not provider_instance_id:
        raise ValueError("PagerDuty dataset unit requires an account subdomain")
    return PagerDutyClient(auth, region=credentials.region), provider_instance_id


def _jira_client(context: SyncTaskContext) -> tuple[Any, str, str]:
    from dev_health_ops.providers.jira.client import JiraAuth, JiraClient

    credentials = jira_credentials_from_mapping(_credentials_mapping(context))
    if credentials is None:
        raise ValueError("Missing Jira credentials for incident dataset unit")
    credential_mapping = _credentials_mapping(context)
    cloud_id = str(
        credential_mapping.get("cloud_id") or credential_mapping.get("cloudId") or ""
    ).strip()
    if not cloud_id:
        raise ValueError("Jira incident dataset unit requires a cloud_id")
    site_url = credentials.base_url.rstrip("/")
    return (
        JiraClient(
            auth=JiraAuth(
                base_url=site_url,
                email=credentials.email,
                api_token=credentials.api_token,
                cloud_id=cloud_id,
            ),
            org_id=context.org_id,
        ),
        cloud_id,
        site_url,
    )


def _require_jira_incident_entitlement(context: SyncTaskContext) -> None:
    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.sync.canonical_incident_gate import (
        require_canonical_incident_feature_for_update_sync,
    )

    with get_postgres_session_sync() as session:
        require_canonical_incident_feature_for_update_sync(session, context.org_id)


def _window_backfill_days(context: SyncTaskContext) -> int:
    if context.window_start is None or context.window_end is None:
        return 1
    start_day = context.window_start.date()
    end_day = context.window_end.date()
    return max(1, (end_day - start_day).days + 1)


def _window_day(context: SyncTaskContext) -> date:
    window_end = context.window_end
    if window_end is not None:
        return window_end.date()
    window_start = context.window_start
    if window_start is not None:
        return window_start.date()
    return datetime.now(timezone.utc).date()


def _window_start_from_work_item_args(context: SyncTaskContext) -> datetime | None:
    if context.window_start is not None:
        return context.window_start
    day = _window_day(context)
    return datetime.combine(day, time.min, tzinfo=timezone.utc)


def _work_item_kwargs(context: SyncTaskContext) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "db_url": context.db_url,
        "day": _window_day(context),
        "backfill_days": _window_backfill_days(context),
        "provider": context.provider,
        "org_id": context.org_id,
        "credentials": _credentials_mapping(context) or None,
        "require_source": True,
    }
    if context.provider in {"github", "gitlab", "linear"}:
        repo_name: str | None = context.source_external_id
        if context.provider == "linear" and context.source_is_org_wide_placeholder:
            repo_name = None
            kwargs["require_source"] = False
        kwargs["repo_name"] = repo_name
    if context.provider == "jira":
        kwargs["jira_project_keys"] = [context.source_external_id]
    if context.provider == "gitlab" and kwargs["credentials"]:
        token, gitlab_url = _gitlab_credentials(context)
        kwargs["credentials"] = {
            **kwargs["credentials"],
            "token": token,
            "gitlab_url": gitlab_url,
        }
    if context.provider == "github":
        flags = _explicit_flags(context)
        kwargs["include_issues"] = context.dataset_key in _WORK_ITEM_DATASETS
        # CHAOS-646: only ingest PRs as work items when the PRS dataset is also
        # enabled for this config. The planner stamps ``sync_prs`` on the github
        # work-items unit (False when PRs are not selected); None would let the
        # provider fall back to the GITHUB_INCLUDE_PRS env default (PRs ON).
        kwargs["include_pull_requests"] = flags["sync_prs"]
    return kwargs


def _run_github_dataset(
    context: SyncTaskContext, runtime: ProviderRuntime
) -> dict[str, Any]:
    from dev_health_ops.processors.github import process_github_repo

    owner, repo_name = _github_repo_parts(context.source_external_id)
    flags = _explicit_flags(context)
    token = _github_credentials(context)
    # CHAOS-2803/CS2: adapter-owned sink. process_github_repo drains every
    # instrumented client it constructs (currently: the PR review-batch's
    # local GitHubWorkClient) into this list in a `finally:` block, on both
    # the success AND failure path -- the list is mutated in place, so
    # whatever was drained before a mid-sync raise is still visible here even
    # though the raise unwinds through run_async below.
    usage_sink: list[dict[str, Any]] = []

    async def _handler(store: Any) -> None:
        await process_github_repo(
            store=store,
            owner=owner,
            repo_name=repo_name,
            token=token,
            fetch_blame=False,
            blame_only=flags["blame_only"],
            max_commits=None,
            sync_git=flags["sync_git"],
            sync_prs=flags["sync_prs"],
            sync_cicd=flags["sync_cicd"],
            sync_deployments=flags["sync_deployments"],
            sync_incidents=flags["sync_incidents"],
            sync_security=flags["sync_security"],
            sync_tests=flags["sync_tests"],
            backfill_missing=False,
            since=context.window_start,
            until=context.window_end,
            sync_commits=flags["sync_commits"],
            sync_commit_stats=flags["sync_commit_stats"],
            sync_files=flags["sync_files"],
            sync_blame=flags["sync_blame"],
            usage_sink=usage_sink,
        )

    try:
        run_async(_run_with_reused_or_new_store(context, runtime, _handler))
    except Exception as exc:
        _attach_usage_sink_to_exception(exc, usage_sink)
        raise

    result: dict[str, Any] = {
        "provider": context.provider,
        "dataset": context.dataset_key,
        "source": context.source_external_id,
        "owner": owner,
        "repo": repo_name,
        "flags": flags,
        "window_start": context.window_start.isoformat()
        if context.window_start is not None
        else None,
        "window_end": context.window_end.isoformat()
        if context.window_end is not None
        else None,
    }
    if usage_sink:
        result["observations"] = {PROVIDER_USAGE_OBSERVATION_KEY: list(usage_sink)}
    return result


def _run_gitlab_dataset(
    context: SyncTaskContext, runtime: ProviderRuntime
) -> dict[str, Any]:
    from dev_health_ops.processors.gitlab import process_gitlab_project

    project_id = _gitlab_project_id(context.source_external_id)
    token, gitlab_url = _gitlab_credentials(context)
    flags = _explicit_flags(context)
    # CHAOS-2803/CS2: adapter-owned sink (see _run_github_dataset). No
    # instrumented client is constructed by process_gitlab_project yet (GitLab
    # code-dataset fetch stays on the frozen connector until CHAOS-2773 Wave
    # B), so this sink is inert today -- the plumbing is wired now so a
    # future GitLab code client only has to start draining into it.
    usage_sink: list[dict[str, Any]] = []

    async def _handler(store: Any) -> None:
        await process_gitlab_project(
            store=store,
            project_id=project_id,
            token=token,
            gitlab_url=gitlab_url,
            fetch_blame=False,
            blame_only=flags["blame_only"],
            max_commits=None,
            sync_git=flags["sync_git"],
            sync_prs=flags["sync_prs"],
            sync_cicd=flags["sync_cicd"],
            sync_deployments=flags["sync_deployments"],
            sync_incidents=flags["sync_incidents"],
            sync_security=flags["sync_security"],
            sync_tests=flags["sync_tests"],
            backfill_missing=False,
            since=context.window_start,
            until=context.window_end,
            sync_commits=flags["sync_commits"],
            sync_commit_stats=flags["sync_commit_stats"],
            sync_files=flags["sync_files"],
            sync_blame=flags["sync_blame"],
            usage_sink=usage_sink,
        )

    try:
        run_async(_run_with_reused_or_new_store(context, runtime, _handler))
    except Exception as exc:
        _attach_usage_sink_to_exception(exc, usage_sink)
        raise

    result: dict[str, Any] = {
        "provider": context.provider,
        "dataset": context.dataset_key,
        "source": context.source_external_id,
        "project_id": project_id,
        "gitlab_url": gitlab_url,
        "flags": flags,
        "window_start": context.window_start.isoformat()
        if context.window_start is not None
        else None,
        "window_end": context.window_end.isoformat()
        if context.window_end is not None
        else None,
    }
    if usage_sink:
        result["observations"] = {PROVIDER_USAGE_OBSERVATION_KEY: list(usage_sink)}
    return result


def _run_pagerduty_dataset(
    context: SyncTaskContext, runtime: ProviderRuntime
) -> dict[str, Any]:
    from dev_health_ops.providers.pagerduty.enrichment import PagerDutyEnrichmentToggles
    from dev_health_ops.providers.pagerduty.normalize import PagerDutyNormalizer
    from dev_health_ops.providers.pagerduty.service_repository_mapping import (
        PagerDutyServiceRepositoryMappingInputs,
    )
    from dev_health_ops.providers.pagerduty.sync import (
        PagerDutyOperationalSync,
        PagerDutySyncOptions,
    )

    client, provider_instance_id = _pagerduty_client(context)
    usage_sink: list[dict[str, Any]] = []
    enrichment_cap = context.dataset_options.get("enrichment_cap", 100)
    if not isinstance(enrichment_cap, int) or isinstance(enrichment_cap, bool):
        enrichment_cap = 100
    enrichment = PagerDutyEnrichmentToggles.from_dataset_options(
        context.dataset_key, context.dataset_options
    )
    mapping_inputs = PagerDutyServiceRepositoryMappingInputs.from_dataset_options(
        context.dataset_options
    )

    async def _handler(store: Any) -> dict[str, Any]:
        result = await PagerDutyOperationalSync(
            client=client,
            store=store,
            normalizer=PagerDutyNormalizer(
                org_id=context.org_id,
                provider_instance_id=provider_instance_id,
                observed_at=context.window_end
                or context.window_start
                or datetime.now(timezone.utc),
            ),
            mapping_inputs=mapping_inputs,
        ).run(
            PagerDutySyncOptions(
                dataset_key=context.dataset_key,
                window_start=context.window_start,
                window_end=context.window_end,
                resume_after=context.resume_cursor,
                enrichment_cap=enrichment_cap,
                enrichment=enrichment,
            )
        )
        usage_sink.extend(result.observations)
        return {
            "persisted": result.persisted,
            "degraded": result.degraded,
            "watermark_at": result.watermark_at.isoformat()
            if result.watermark_at is not None
            else None,
        }

    sync_error: Exception | None = None
    try:
        sync_result = run_async(
            _run_with_reused_or_new_store(context, runtime, _handler)
        )
    except Exception as exc:
        sync_error = exc
        usage_sink.extend(client.drain_usage_observations())
        _attach_usage_sink_to_exception(exc, usage_sink)
        raise
    finally:
        try:
            run_async(client.close())
        except Exception:
            if sync_error is None:
                raise
    result: dict[str, Any] = {
        "provider": context.provider,
        "dataset": context.dataset_key,
        "source": context.source_external_id,
        "provider_instance_id": provider_instance_id,
        "window_start": context.window_start.isoformat()
        if context.window_start is not None
        else None,
        "window_end": context.window_end.isoformat()
        if context.window_end is not None
        else None,
        **sync_result,
    }
    if usage_sink:
        result["observations"] = {PROVIDER_USAGE_OBSERVATION_KEY: usage_sink}
    return result


def _run_jira_incident_dataset(
    context: SyncTaskContext, runtime: ProviderRuntime
) -> dict[str, Any]:
    from dev_health_ops.providers.jira.jsm_incidents import JsmIncidentProducer

    _require_jira_incident_entitlement(context)
    if context.window_start is None or context.window_end is None:
        raise ValueError("Jira incident dataset unit requires a bounded window")
    if context.window_start >= context.window_end:
        raise ValueError(
            "Jira incident dataset unit requires a non-empty bounded window"
        )
    client, cloud_id, site_url = _jira_client(context)
    usage_sink: list[dict[str, Any]] = []
    sync_error: Exception | None = None

    async def _handler(store: Any) -> int:
        batch = await JsmIncidentProducer(
            client=client,
            org_id=context.org_id,
            provider_instance_id=cloud_id,
            base_url=site_url,
            window_start=context.window_start,
            window_end=context.window_end,
            observed_at=context.window_end
            or context.window_start
            or datetime.now(timezone.utc),
            allowed_project_keys=(context.source_external_id,),
        ).collect()
        _require_jira_incident_entitlement(context)
        await IngestionSink(store).insert_operational_batch(batch)
        return len(batch.incidents)

    try:
        persisted = run_async(_run_with_reused_or_new_store(context, runtime, _handler))
    except Exception as exc:
        sync_error = exc
        raise
    finally:
        drain_error: Exception | None = None
        try:
            usage_sink.extend(client.drain_usage_observations())
            if sync_error is not None:
                _attach_usage_sink_to_exception(sync_error, usage_sink)
        except Exception as exc:
            drain_error = exc
            raise
        finally:
            try:
                client.close()
            except Exception:
                if sync_error is None and drain_error is None:
                    raise

    result: dict[str, Any] = {
        "provider": context.provider,
        "dataset": context.dataset_key,
        "source": context.source_external_id,
        "provider_instance_id": cloud_id,
        "persisted": persisted,
        "window_start": context.window_start.isoformat()
        if context.window_start is not None
        else None,
        "window_end": context.window_end.isoformat()
        if context.window_end is not None
        else None,
    }
    if usage_sink:
        result["observations"] = {PROVIDER_USAGE_OBSERVATION_KEY: usage_sink}
    return result


def _attach_usage_sink_to_exception(
    exc: BaseException, usage_sink: list[dict[str, Any]]
) -> None:
    """Preserve actuals drained before a mid-sync raise (CHAOS-2754) so the
    worker's rate-limit deferral / failure stamp can still persist them --
    the code-dataset twin of ``job_work_items.attach_work_item_partial_observations``,
    via the provider-neutral alias (CHAOS-2803/CS2). No-ops when the sink is
    empty. Never suppresses the error."""

    if usage_sink:
        attach_partial_observations(
            exc, {PROVIDER_USAGE_OBSERVATION_KEY: list(usage_sink)}
        )


def _run_work_item_dataset(context: SyncTaskContext) -> dict[str, Any]:
    from dev_health_ops.metrics.job_work_items import run_work_items_sync_job

    kwargs = _work_item_kwargs(context)
    sync_result = run_work_items_sync_job(**kwargs)
    window_start = _window_start_from_work_item_args(context)
    result: dict[str, Any] = {
        "provider": context.provider,
        "dataset": context.dataset_key,
        "source": context.source_external_id,
        "work_items_synced": True,
        "day": kwargs["day"].isoformat(),
        "backfill_days": kwargs["backfill_days"],
        "window_start": window_start.isoformat() if window_start is not None else None,
        "window_end": context.window_end.isoformat()
        if context.window_end is not None
        else None,
    }
    if isinstance(sync_result, dict) and isinstance(
        sync_result.get("observations"), dict
    ):
        result["observations"] = sync_result["observations"]
    return result


def _run_feature_flags_dataset(context: SyncTaskContext) -> dict[str, Any]:
    from dev_health_ops.workers.feature_flag_sync import (
        _sync_gitlab_feature_flags,
        _sync_launchdarkly_feature_flags,
    )

    credentials = _credentials_mapping(context)
    sync_options: dict[str, Any] = {
        "project_id": context.source_external_id,
        "project_key": context.source_external_id,
    }
    if context.provider == "gitlab":
        result = _sync_gitlab_feature_flags(
            db_url=context.db_url,
            org_id=context.org_id,
            credentials=credentials,
            sync_options=sync_options,
        )
    elif context.provider == "launchdarkly":
        result = _sync_launchdarkly_feature_flags(
            db_url=context.db_url,
            org_id=context.org_id,
            credentials=credentials,
            sync_options=sync_options,
            since_dt=context.window_start,
        )
    else:
        raise ValueError(
            f"Unsupported feature-flags provider: provider={context.provider!r}"
        )

    dataset_result: dict[str, Any] = {
        "provider": context.provider,
        "dataset": context.dataset_key,
        "source": context.source_external_id,
        "feature_flags": result,
        "window_start": context.window_start.isoformat()
        if context.window_start is not None
        else None,
        "window_end": context.window_end.isoformat()
        if context.window_end is not None
        else None,
    }
    if isinstance(result, dict) and isinstance(result.get("observations"), dict):
        dataset_result["observations"] = result["observations"]
    return dataset_result


def run_dataset_unit(
    context: SyncTaskContext, runtime: ProviderRuntime
) -> dict[str, Any]:
    provider = context.provider.lower()
    dataset_key = context.dataset_key

    if provider == "pagerduty" and dataset_key in _PAGERDUTY_DATASETS:
        return _run_pagerduty_dataset(context, runtime)

    if provider == "jira" and dataset_key == DatasetKey.INCIDENTS.value:
        return _run_jira_incident_dataset(context, runtime)

    if dataset_key in _CODE_DATASETS:
        if provider == "github":
            return _run_github_dataset(context, runtime)
        if provider == "gitlab":
            return _run_gitlab_dataset(context, runtime)

    if dataset_key in _WORK_ITEM_DATASETS and provider in {
        "github",
        "gitlab",
        "jira",
        "linear",
    }:
        return _run_work_item_dataset(context)

    if dataset_key == DatasetKey.FEATURE_FLAGS.value:
        return _run_feature_flags_dataset(context)

    raise ValueError(
        "Unsupported provider dataset unit: "
        f"provider={context.provider!r} dataset={context.dataset_key!r}"
    )
