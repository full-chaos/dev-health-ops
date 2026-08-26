import argparse
import asyncio
import os
from datetime import datetime, timedelta, timezone

from dev_health_ops.credentials import (
    CredentialResolutionError,
    CredentialSource,
    GitHubCredentials,
    resolve_credentials_sync,
)
from dev_health_ops.db import resolve_sink_uri
from dev_health_ops.fixtures.demo_identity import DEFAULT_DEMO_REPO_NAME
from dev_health_ops.metrics.sinks.ingestion import IngestionSink
from dev_health_ops.processors.github import (
    process_github_repo,
    process_github_repos_batch,
)
from dev_health_ops.processors.gitlab import (
    process_gitlab_project,
    process_gitlab_projects_batch,
)
from dev_health_ops.processors.local import process_local_blame, process_local_repo
from dev_health_ops.providers.operational_migration import (
    IssueIncidentSource,
    map_issue_incidents,
    write_operational_batch,
)
from dev_health_ops.storage import (
    ClickHouseStore,
    SQLAlchemyStore,
    detect_db_type,
    run_with_store,
)
from dev_health_ops.sync.datasets import processor_sync_targets
from dev_health_ops.utils.cli import (
    add_date_range_args,
    add_sink_arg,
    resolve_date_range,
    resolve_max_commits,
    resolve_since_datetime,
    validate_sink,
)

# Real provider registered in the Go-native post-sync capability matrix
# (internal/providersync/capabilities.go) for cicd/deployments/incidents/tests
# with the exact legacy targets native_post_sync.go's loadPostSyncPlan needs
# (hasCICD/hasDeployments/hasIncidents). "synthetic" is NOT a registered
# provider there -- a unit tagged with it would silently fail
# providersync.Capability's lookup and never reach those flags at all, so the
# post-sync fanout this exists to exercise would just never fire. "gitlab" is
# used (not "github") because only gitlab's entry in that table registers an
# "incidents" dataset; github has none.
_SYNTHETIC_SYNC_RUN_PROVIDER = "gitlab"


def _sync_flags_for_target(target: str) -> dict:
    return {
        "sync_git": target == "git",
        "sync_prs": target == "prs",
        "sync_cicd": target == "cicd",
        "sync_deployments": target == "deployments",
        "sync_incidents": target == "incidents",
        "sync_security": target == "security",
        "sync_tests": target == "tests",
        "blame_only": target == "blame",
    }


def _resolve_synthetic_repo_name(ns: argparse.Namespace) -> str:
    if ns.repo_name:
        return ns.repo_name
    if ns.owner and ns.repo:
        return f"{ns.owner}/{ns.repo}"
    if ns.search:
        if "*" in ns.search or "?" in ns.search:
            raise SystemExit(
                "Synthetic provider does not support pattern search; use --repo-name."
            )
        return ns.search
    return DEFAULT_DEMO_REPO_NAME


def _read_github_app_private_key(path: str) -> str:
    try:
        with open(path, encoding="utf-8") as key_file:
            return key_file.read()
    except OSError as exc:
        raise SystemExit(f"Unable to read GitHub App private key file: {path}") from exc


def _build_github_cli_or_env_credentials(
    *,
    token: str | None,
    app_id: str | None,
    private_key_path: str | None,
    installation_id: str | None,
    base_url: str | None = None,
    credential_name: str = "default",
) -> GitHubCredentials | None:
    has_token = bool(token)
    app_values = [app_id, private_key_path, installation_id]
    has_any_app = any(app_values)
    has_all_app = all(app_values)

    if has_token and has_any_app:
        raise SystemExit(
            "GitHub auth must use exactly one mode: PAT (--auth/GITHUB_TOKEN) XOR GitHub App."
        )
    if has_any_app and not has_all_app:
        raise SystemExit(
            "GitHub App auth requires app id, private key path, and installation id."
        )
    if has_token:
        return GitHubCredentials(
            token=token,
            base_url=base_url,
            source=CredentialSource.ENVIRONMENT,
            credential_name=credential_name,
        )
    if has_all_app:
        assert app_id is not None
        assert private_key_path is not None
        assert installation_id is not None
        return GitHubCredentials(
            app_id=app_id,
            private_key=_read_github_app_private_key(private_key_path),
            installation_id=installation_id,
            base_url=base_url,
            source=CredentialSource.ENVIRONMENT,
            credential_name=credential_name,
        )
    return None


def _resolve_github_sync_credentials(ns: argparse.Namespace) -> GitHubCredentials:
    """Resolve GitHub sync auth with precedence CLI > env > DB."""
    cli_credentials = _build_github_cli_or_env_credentials(
        token=getattr(ns, "auth", None),
        app_id=getattr(ns, "github_app_id", None),
        private_key_path=getattr(ns, "github_app_key_path", None),
        installation_id=getattr(ns, "github_app_installation_id", None),
        credential_name="cli",
    )
    if cli_credentials is not None:
        return cli_credentials

    env_credentials = _build_github_cli_or_env_credentials(
        token=os.getenv("GITHUB_TOKEN"),
        app_id=os.getenv("GITHUB_APP_ID"),
        private_key_path=os.getenv("GITHUB_APP_PRIVATE_KEY_PATH"),
        installation_id=os.getenv("GITHUB_APP_INSTALLATION_ID"),
        base_url=os.getenv("GITHUB_URL") or os.getenv("GITHUB_BASE_URL"),
        credential_name="environment",
    )
    if env_credentials is not None:
        return env_credentials

    db_url = (
        getattr(ns, "db", None)
        or os.getenv("POSTGRES_URI")
        or os.getenv("DATABASE_URI")
    )
    org_id = getattr(ns, "org", None)
    if db_url and org_id:
        try:
            credentials = resolve_credentials_sync(
                "github",
                org_id=org_id,
                db_url=db_url,
                allow_env_fallback=False,
            )
        except CredentialResolutionError:
            credentials = None
        if isinstance(credentials, GitHubCredentials):
            return credentials

    raise SystemExit(
        "Missing GitHub credentials (pass --auth, set GITHUB_TOKEN, configure GitHub App flags/env vars, or configure DB credentials)."
    )


async def sync_local_target(ns: argparse.Namespace, target: str) -> int:
    if target not in {"git", "prs", "blame"}:
        raise SystemExit("Local provider supports only git, prs, or blame targets.")

    db_uri = resolve_sink_uri(ns)
    validate_sink(ns)
    db_type = detect_db_type(db_uri)
    since = resolve_since_datetime(ns)

    async def _handler(store):
        if target == "blame":
            await process_local_blame(
                store=store,
                repo_path=ns.repo_path,
                since=since,
            )
            return

        await process_local_repo(
            store=store,
            repo_path=ns.repo_path,
            since=since,
            sync_git=(target == "git"),
            sync_prs=(target == "prs"),
            sync_blame=False,
        )

    await run_with_store(db_uri, db_type, _handler, org_id=getattr(ns, "org", None))
    return 0


async def sync_github_target(ns: argparse.Namespace, target: str) -> int:
    credentials = _resolve_github_sync_credentials(ns)

    db_uri = resolve_sink_uri(ns)
    validate_sink(ns)
    db_type = detect_db_type(db_uri)
    since = resolve_since_datetime(ns)
    max_commits = resolve_max_commits(ns)
    flags = _sync_flags_for_target(target)

    async def _handler(store):
        if ns.search:
            org_name = ns.group
            user_name = str(ns.owner or "") if not ns.group else ""
            batch_kwargs = {
                "store": store,
                "token": credentials,
                "org_name": org_name,
                "user_name": user_name,
                "pattern": ns.search,
                "batch_size": ns.batch_size,
                "max_concurrent": ns.max_concurrent,
                "rate_limit_delay": ns.rate_limit_delay,
                "max_repos": ns.max_repos,
                "use_async": ns.use_async,
                "sync_git": flags["sync_git"],
                "sync_prs": flags["sync_prs"],
                "sync_cicd": flags["sync_cicd"],
                "sync_deployments": flags["sync_deployments"],
                "sync_incidents": flags["sync_incidents"],
                "sync_security": flags["sync_security"],
                "sync_tests": flags["sync_tests"],
                "blame_only": flags["blame_only"],
                "backfill_missing": True,
                "since": since,
            }
            if max_commits is not None:
                batch_kwargs["max_commits_per_repo"] = max_commits
            await process_github_repos_batch(**batch_kwargs)
            return

        if not (ns.owner and ns.repo):
            raise SystemExit(
                "GitHub sync requires --owner and --repo (or --search for batch)."
            )
        await process_github_repo(
            store,
            ns.owner,
            ns.repo,
            credentials,
            blame_only=flags["blame_only"],
            max_commits=max_commits,
            sync_git=flags["sync_git"],
            sync_prs=flags["sync_prs"],
            sync_cicd=flags["sync_cicd"],
            sync_deployments=flags["sync_deployments"],
            sync_incidents=flags["sync_incidents"],
            sync_security=flags["sync_security"],
            sync_tests=flags["sync_tests"],
            since=since,
        )

    await run_with_store(db_uri, db_type, _handler, org_id=getattr(ns, "org", None))
    return 0


async def sync_gitlab_target(ns: argparse.Namespace, target: str) -> int:
    token = ns.auth or os.getenv("GITLAB_TOKEN") or ""
    if not token:
        raise SystemExit("Missing GitLab token (pass --auth or set GITLAB_TOKEN).")

    db_uri = resolve_sink_uri(ns)
    validate_sink(ns)
    db_type = detect_db_type(db_uri)
    since = resolve_since_datetime(ns)
    max_commits = resolve_max_commits(ns)
    flags = _sync_flags_for_target(target)

    async def _handler(store):
        if ns.search:
            batch_kwargs = {
                "store": store,
                "token": token,
                "gitlab_url": ns.gitlab_url,
                "group_name": ns.group,
                "pattern": ns.search,
                "batch_size": ns.batch_size,
                "max_concurrent": ns.max_concurrent,
                "rate_limit_delay": ns.rate_limit_delay,
                "max_projects": ns.max_repos,
                "use_async": ns.use_async,
                "sync_git": flags["sync_git"],
                "sync_prs": flags["sync_prs"],
                "sync_cicd": flags["sync_cicd"],
                "sync_deployments": flags["sync_deployments"],
                "sync_incidents": flags["sync_incidents"],
                "sync_security": flags["sync_security"],
                "sync_tests": flags["sync_tests"],
                "blame_only": flags["blame_only"],
                "backfill_missing": True,
                "since": since,
            }
            if max_commits is not None:
                batch_kwargs["max_commits_per_project"] = max_commits
            await process_gitlab_projects_batch(**batch_kwargs)
            return

        if ns.project_id is None:
            raise SystemExit(
                "GitLab sync requires --project-id (or --search for batch)."
            )
        await process_gitlab_project(
            store,
            ns.project_id,
            token,
            ns.gitlab_url,
            blame_only=flags["blame_only"],
            max_commits=max_commits,
            sync_git=flags["sync_git"],
            sync_prs=flags["sync_prs"],
            sync_cicd=flags["sync_cicd"],
            sync_deployments=flags["sync_deployments"],
            sync_incidents=flags["sync_incidents"],
            sync_security=flags["sync_security"],
            sync_tests=flags["sync_tests"],
            since=since,
        )

    await run_with_store(db_uri, db_type, _handler, org_id=getattr(ns, "org", None))
    return 0


_SYNC_RUN_BACKED_SYNTHETIC_TARGETS = frozenset(
    {"cicd", "deployments", "incidents", "tests"}
)


def _complete_synthetic_sync_run(
    *,
    org_id: str,
    repo_full_name: str,
    target: str,
    since_at: datetime,
    before_at: datetime,
) -> str:
    """Complete a real sync_run for a synthetic cicd/deployments/incidents/tests
    seed, through the SAME production finalize path a real provider sync uses
    (workers.sync_units.finalize_sync_run) -- so the sync_run_units row and
    the post_sync outbox row this leaves are byte-identical to what a real
    completed sync leaves, not a fixture shortcut wearing that costume
    (CHAOS-4266). Deliberately does NOT call
    workers.post_sync_dispatch._dispatch_post_sync_tasks directly -- that is
    the Celery-era fixture shortcut this exists to stop being mistaken for
    pipeline proof.

    Integration/IntegrationSource/IntegrationDataset rows are found-or-created
    (unique on org_id+integration_id+provider+external_id /
    org_id+integration_id+dataset_key) so repeated calls across the four
    targets in one seeding run share one integration, the way one real
    provider connection would.
    """
    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.models import (
        Integration,
        IntegrationDataset,
        IntegrationSource,
        SyncRun,
        SyncRunMode,
        SyncRunStatus,
        SyncRunUnit,
        SyncRunUnitStatus,
    )
    from dev_health_ops.sync.executed_proof_ledger import record_executed_proof_attempts
    from dev_health_ops.workers.sync_units import finalize_sync_run

    provider = _SYNTHETIC_SYNC_RUN_PROVIDER
    processor_flags = _sync_flags_for_target(target)
    synthetic_integration_name = f"{provider}-synthetic-seed"

    with get_postgres_session_sync() as session:
        # Matched on name too, not just org_id+provider (codex review,
        # CHAOS-4266): a real gitlab integration for this org must never be
        # found by this lookup and have synthetic sources/runs/rows attached
        # to it. Scoping to the marker name means this can only ever
        # find-or-create the synthetic integration this function itself
        # created, regardless of what real integrations the org has.
        integration = (
            session.query(Integration)
            .filter(
                Integration.org_id == org_id,
                Integration.provider == provider,
                Integration.name == synthetic_integration_name,
            )
            .one_or_none()
        )
        if integration is None:
            integration = Integration(
                org_id=org_id,
                provider=provider,
                name=synthetic_integration_name,
                config={},
                is_active=True,
            )
            session.add(integration)
            session.flush()

        source = (
            session.query(IntegrationSource)
            .filter(
                IntegrationSource.org_id == org_id,
                IntegrationSource.integration_id == integration.id,
                IntegrationSource.provider == provider,
                IntegrationSource.external_id == repo_full_name,
            )
            .one_or_none()
        )
        if source is None:
            source = IntegrationSource(
                org_id=org_id,
                integration_id=integration.id,
                provider=provider,
                source_type="repo",
                external_id=repo_full_name,
                name=repo_full_name.rsplit("/", 1)[-1],
                full_name=repo_full_name,
                metadata_={},
                is_enabled=True,
            )
            session.add(source)
            session.flush()

        dataset = (
            session.query(IntegrationDataset)
            .filter(
                IntegrationDataset.org_id == org_id,
                IntegrationDataset.integration_id == integration.id,
                IntegrationDataset.dataset_key == target,
            )
            .one_or_none()
        )
        if dataset is None:
            dataset = IntegrationDataset(
                org_id=org_id,
                integration_id=integration.id,
                dataset_key=target,
                is_enabled=True,
                options={},
            )
            session.add(dataset)
            session.flush()

        run = SyncRun(
            org_id=org_id,
            integration_id=integration.id,
            triggered_by="metrics-executed-proof-gate",
            mode=SyncRunMode.INCREMENTAL.value,
            status=SyncRunStatus.RUNNING.value,
            total_units=1,
            completed_units=0,
            failed_units=0,
            started_at=since_at,
        )
        session.add(run)
        session.flush()

        unit = SyncRunUnit(
            org_id=org_id,
            sync_run_id=run.id,
            integration_id=integration.id,
            source_id=source.id,
            provider=provider,
            dataset_key=target,
            cost_class="medium",
            mode=SyncRunMode.INCREMENTAL.value,
            since_at=since_at,
            before_at=before_at,
            status=SyncRunUnitStatus.SUCCESS.value,
            attempts=1,
            processor_flags=processor_flags,
        )
        session.add(unit)
        session.flush()
        # CHAOS-4114: this file now INSERTs sync_run_units directly (see
        # tests/test_executed_proof_ledger_write_path_audit.py), so it must
        # record the ATTEMPTED half the same way sync/planner.py's real
        # plan_sync_run does, in the same transaction as the insert.
        #
        # Deliberately NOT calling record_executed_proof_terminal (codex
        # review, CHAOS-4266): the (provider, dataset_key) ledger this feeds
        # is GLOBAL, not scoped to this synthetic org, and provider is
        # "gitlab" (required for the Go capability lookup -- see
        # _SYNTHETIC_SYNC_RUN_PROVIDER above). Marking a pair PROVEN here
        # would let CHAOS-4060's executed-proof gate treat a genuinely broken
        # real gitlab/<dataset> route as satisfied on the strength of fake
        # data -- exactly the failure the gate exists to catch (CHAOS-4048/
        # CHAOS-4049 shape). ATTEMPTED-only converges to the same "brand-new
        # route" bootstrap behavior the gate already tolerates (blocks
        # transiently until something proves it, never permanently passes a
        # route that never really worked); a real gitlab sync's own
        # finalize_sync_run still proves it correctly and unconditionally
        # when it happens. This is safe for this job's own throwaway
        # database either way -- but never run this CLI command against a
        # shared or production-adjacent database.
        record_executed_proof_attempts(
            session, [(unit.provider, unit.dataset_key)], now=before_at
        )
        run_id = str(run.id)

    finalize_sync_run(run_id)
    return run_id


async def sync_synthetic_target(ns: argparse.Namespace, target: str) -> int:
    from dev_health_ops.fixtures.generator import SyntheticDataGenerator

    repo_name = _resolve_synthetic_repo_name(ns)
    db_uri = resolve_sink_uri(ns)
    validate_sink(ns)
    db_type = detect_db_type(db_uri)
    _, backfill_days = resolve_date_range(ns)
    days = backfill_days
    org_id = getattr(ns, "org", None)

    if target in _SYNC_RUN_BACKED_SYNTHETIC_TARGETS and not org_id:
        raise SystemExit(
            f"--provider synthetic --target {target} requires a resolved org "
            "(--org or ORG_ID env): it completes a real sync_run scoped to "
            "that org, unlike git/prs/blame which write analytics rows only."
        )

    # _complete_synthetic_sync_run records ATTEMPTED evidence in the CHAOS-4114
    # executed-proof ledger under the REAL provider identity
    # (_SYNTHETIC_SYNC_RUN_PROVIDER = "gitlab"), and that ledger is keyed
    # globally by (provider, dataset_key), not by org. Run against a shared or
    # production-adjacent database, this could make a currently-unproven real
    # gitlab route look attempted-but-unproven from fake data (codex review,
    # CHAOS-4266 round 3: a code comment alone is not a guard). Requiring an
    # explicit env var -- set by this repo's only two legitimate callers,
    # ci/run_metrics_executed_proof.sh and nothing else -- makes any other
    # invocation fail closed instead of silently touching that ledger.
    if (
        target in _SYNC_RUN_BACKED_SYNTHETIC_TARGETS
        and os.environ.get("DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN") != "1"
    ):
        raise SystemExit(
            f"--provider synthetic --target {target} writes to the GLOBAL "
            "CHAOS-4114 executed-proof ledger under a real provider identity "
            "and must never run against a shared or production-adjacent "
            "database. Set DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN=1 explicitly "
            "if this really is a throwaway CI/test database."
        )

    async def _handler(store):
        ingestion_sink = IngestionSink(store)
        generator = SyntheticDataGenerator(repo_name=repo_name)
        repo = generator.generate_repo()
        await ingestion_sink.insert_repo(repo)

        if target == "git":
            commits = generator.generate_commits(days=days)
            await ingestion_sink.insert_git_commit_data(commits)
            stats = generator.generate_commit_stats(commits)
            await ingestion_sink.insert_git_commit_stats(stats)
            return

        if target == "prs":
            pr_data = generator.generate_prs()
            prs = [p["pr"] for p in pr_data]
            await ingestion_sink.insert_git_pull_requests(prs)

            reviews = []
            for p in pr_data:
                reviews.extend(p["reviews"])
            if reviews:
                await ingestion_sink.insert_git_pull_request_reviews(reviews)
            return

        if target == "blame":
            commits = generator.generate_commits(days=days)
            files = generator.generate_files()
            await ingestion_sink.insert_git_file_data(files)
            blame_data = generator.generate_blame(commits)
            if blame_data:
                await ingestion_sink.insert_blame_data(blame_data)
            return

        if target == "cicd":
            assert org_id is not None  # guarded above, this target requires it
            pipeline_runs = generator.generate_ci_pipeline_runs(days=days)
            # Mirrors fixtures/runner.py's exact ClickHouse-vs-Postgres branch:
            # ClickHouse gets the single enriched insert_testops_pipeline_runs
            # call (one MV event per run_id); Postgres/SQLite use the plain
            # ORM insert_ci_pipeline_runs.
            ch_pipeline_insert = (
                getattr(store, "insert_testops_pipeline_runs", None)
                if not isinstance(store, SQLAlchemyStore)
                else None
            )
            if ch_pipeline_insert is not None:
                extended_rows = generator.generate_pipeline_run_extended_rows(
                    pipeline_runs=pipeline_runs, org_id=org_id
                )
                await ch_pipeline_insert(extended_rows)
            else:
                await store.insert_ci_pipeline_runs(pipeline_runs)
            return

        if target == "deployments":
            deployments = generator.generate_deployments(
                days=days, release_refs=generator._default_release_refs(days)
            )
            await store.insert_deployments(deployments)
            return

        if target == "incidents":
            assert org_id is not None  # guarded above, this target requires it
            incidents = generator.generate_incidents(days=days)
            if not incidents:
                return
            if isinstance(store, ClickHouseStore):
                # The real canonical incident write path (CHAOS-4269 traced
                # this exact call chain): repository-derived incidents go
                # through the operational entity model, not a legacy
                # incidents table -- ClickHouseStore has no insert_incidents.
                incident_sources = [
                    IssueIncidentSource(
                        org_id=org_id,
                        provider="synthetic",
                        provider_instance_id="cli-sync-synthetic",
                        repo_id=generator.repo_id,
                        repo_full_name=repo_name,
                        external_id=incident.incident_id,
                        issue_number=None,
                        source_url=None,
                        labels=(),
                        raw_status=incident.status,
                        title=incident.incident_id,
                        description=None,
                        created_at=incident.started_at,
                        resolved_at=incident.resolved_at,
                        source_version_at=incident.resolved_at or incident.started_at,
                    )
                    for incident in incidents
                ]
                await write_operational_batch(
                    store, map_issue_incidents(incident_sources)
                )
            else:
                await store.insert_incidents(incidents)
            return

        if target == "tests":
            assert org_id is not None  # guarded above, this target requires it
            pipeline_runs = generator.generate_ci_pipeline_runs(days=days)
            insert_ci_job_runs = getattr(store, "insert_ci_job_runs", None)
            if insert_ci_job_runs is None:
                raise SystemExit(
                    "--target tests --provider synthetic requires a "
                    "ClickHouse sink (this store has no insert_ci_job_runs)."
                )
            job_runs = generator.generate_ci_job_runs(pipeline_runs, org_id=org_id)
            await insert_ci_job_runs(job_runs)
            test_data = generator.generate_test_executions(
                job_runs, days=days, org_id=org_id
            )
            if hasattr(store, "insert_test_suite_results"):
                await store.insert_test_suite_results(test_data["suite_results"])
            if hasattr(store, "insert_test_case_results"):
                await store.insert_test_case_results(test_data["case_results"])
            return

    await run_with_store(db_uri, db_type, _handler, org_id=org_id)

    # --defer-finalize (CHAOS-4266): finalizing here, right after this one
    # target's own rows land, is what raced dora in the metrics-executed-proof
    # gate -- NativePostSyncService.Fanout triggers a remaining-metric family
    # off ANY qualifying dataset (dora: git/deployments/cicd/incidents) as
    # soon as ITS sync_run finalizes, without waiting for a caller's OTHER
    # targets to also land. Seeding cicd/deployments/incidents/tests in a
    # loop that finalizes each immediately meant dora's dispatch fired off
    # whichever target happened to be seeded first -- before the other two
    # dora inputs existed -- and it never gets a second chance (idempotent
    # per-day dispatch; no retry when the rest of the data later shows up).
    # A caller seeding multiple dora-relevant targets for the same org/window
    # must pass --defer-finalize for every one of them, then call the
    # `finalize-synthetic-sync` verb for each only after ALL of them have
    # written their rows -- matching how the real pipeline never fans out
    # before a provider's sync actually completes.
    if target in _SYNC_RUN_BACKED_SYNTHETIC_TARGETS and not getattr(
        ns, "defer_finalize", False
    ):
        assert org_id is not None  # guarded above, this target requires it
        before_at = datetime.now(timezone.utc)
        since_at = before_at - timedelta(days=days)
        _complete_synthetic_sync_run(
            org_id=org_id,
            repo_full_name=repo_name,
            target=target,
            since_at=since_at,
            before_at=before_at,
        )
    return 0


def finalize_synthetic_sync_run(ns: argparse.Namespace, target: str) -> int:
    """Finalize a synthetic sync run seeded earlier with ``--defer-finalize``.

    Writes no analytics rows -- only completes the durable SyncRun/
    SyncRunUnit and triggers the real post_sync fanout, exactly like the
    non-deferred path in `sync_synthetic_target` does, just decoupled from
    the row-write step so a caller can seed several dora-relevant targets
    before any of their fanouts run (CHAOS-4266; see the comment above the
    call site in `sync_synthetic_target`).

    NOT idempotent (codex review): every call mints a new `sync_runs` row via
    `_complete_synthetic_sync_run`, exactly as the non-deferred path already
    did -- calling this (or the non-deferred seed path) twice for the same
    target has always produced two independent sync_run/post_sync
    generations, not a merge. What decoupling changes is only WHEN
    `since_at`/`before_at` get computed: always fresh at call time (`now` and
    `now - backfill days`), same as before, just later than the seed step by
    however long the caller takes to finalize its other deferred targets.
    Call this at most once per seeded target, immediately after all of a
    run's dora-relevant targets have finished seeding -- exactly what
    `ci/run_metrics_executed_proof.sh` does.
    """
    if target not in _SYNC_RUN_BACKED_SYNTHETIC_TARGETS:
        raise SystemExit(
            "finalize-synthetic-sync --target "
            f"{target}: only valid for "
            f"{', '.join(sorted(_SYNC_RUN_BACKED_SYNTHETIC_TARGETS))}."
        )
    if os.environ.get("DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN") != "1":
        raise SystemExit(
            "finalize-synthetic-sync writes to the GLOBAL CHAOS-4114 "
            "executed-proof ledger under a real provider identity and must "
            "never run against a shared or production-adjacent database. "
            "Set DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN=1 explicitly if this "
            "really is a throwaway CI/test database."
        )
    org_id = getattr(ns, "org", None)
    if not org_id:
        raise SystemExit(
            "finalize-synthetic-sync --target "
            f"{target} requires a resolved org (--org or ORG_ID env)."
        )
    repo_name = _resolve_synthetic_repo_name(ns)
    _, backfill_days = resolve_date_range(ns)
    before_at = datetime.now(timezone.utc)
    since_at = before_at - timedelta(days=backfill_days)
    run_id = _complete_synthetic_sync_run(
        org_id=org_id,
        repo_full_name=repo_name,
        target=target,
        since_at=since_at,
        before_at=before_at,
    )
    print(run_id)
    return 0


def run_sync_target(ns: argparse.Namespace) -> int:
    target = ns.sync_target
    provider = (ns.provider or "").lower()
    if provider not in {"local", "github", "gitlab", "synthetic"}:
        raise SystemExit("Provider must be one of: local, github, gitlab, synthetic.")

    target_choices = processor_sync_targets()
    if target not in target_choices:
        raise SystemExit(
            "Sync target must be one of: " + ", ".join(target_choices) + "."
        )

    if provider == "local":
        return asyncio.run(sync_local_target(ns, target))
    if provider == "github":
        return asyncio.run(sync_github_target(ns, target))
    if provider == "gitlab":
        return asyncio.run(sync_gitlab_target(ns, target))
    return asyncio.run(sync_synthetic_target(ns, target))


def _add_sync_target_args(parser: argparse.ArgumentParser) -> None:
    add_sink_arg(parser)
    parser.add_argument(
        "--provider",
        choices=["local", "github", "gitlab", "synthetic"],
        required=True,
        help="Source provider for the sync job.",
    )
    parser.add_argument("--auth", help="Provider token override (GitHub/GitLab).")
    parser.add_argument("--github-app-id", help="GitHub App ID (GitHub provider).")
    parser.add_argument(
        "--github-app-key-path",
        help="Path to GitHub App private key PEM (GitHub provider).",
    )
    parser.add_argument(
        "--github-app-installation-id",
        help="GitHub App installation ID (GitHub provider).",
    )
    parser.add_argument(
        "--repo-path", default=".", help="Local git repo path (local provider)."
    )
    parser.add_argument("--owner", help="GitHub owner/org (single repo mode).")
    parser.add_argument("--repo", help="GitHub repo name (single repo mode).")
    parser.add_argument(
        "--project-id", type=int, help="GitLab project ID (single project mode)."
    )
    parser.add_argument(
        "--gitlab-url",
        default=os.getenv("GITLAB_URL", "https://gitlab.com"),
        help="GitLab instance URL.",
    )
    parser.add_argument("--group", help="Batch mode org/group name.")
    parser.add_argument(
        "-s",
        "--search",
        help="Batch mode pattern (e.g. 'org/*').",
    )
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--max-concurrent", type=int, default=4)
    parser.add_argument("--rate-limit-delay", type=float, default=1.0)
    parser.add_argument("--max-repos", type=int)
    parser.add_argument("--use-async", action="store_true")
    parser.add_argument("--max-commits-per-repo", type=int)
    parser.add_argument(
        "--repo-name",
        help=f"Synthetic repo name (default: {DEFAULT_DEMO_REPO_NAME}).",
    )
    parser.add_argument(
        "--defer-finalize",
        action="store_true",
        help=(
            "Synthetic provider, sync-run-backed targets only (cicd/"
            "deployments/incidents/tests): write analytics rows but do NOT "
            "complete the sync_run or trigger the post_sync fanout yet. Use "
            "the `finalize-synthetic-sync` verb afterward, once every "
            "dora-relevant target (cicd/deployments/incidents) has been "
            "seeded, so a remaining-metric family that spans multiple "
            "targets never fans out against a partially-seeded org "
            "(CHAOS-4266)."
        ),
    )
    add_date_range_args(parser)


def _add_finalize_synthetic_sync_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--target",
        required=True,
        choices=sorted(_SYNC_RUN_BACKED_SYNTHETIC_TARGETS),
        help="The synthetic target seeded earlier with --defer-finalize.",
    )
    parser.add_argument(
        "--repo-name",
        help=f"Synthetic repo name (default: {DEFAULT_DEMO_REPO_NAME}); must "
        "match the value passed when seeding.",
    )
    add_date_range_args(parser)


def run_finalize_synthetic_sync(ns: argparse.Namespace) -> int:
    return finalize_synthetic_sync_run(ns, ns.target)


def register_commands(subparsers: argparse._SubParsersAction) -> None:
    target_help = {
        "git": "Sync commits and commit stats.",
        "prs": "Sync pull/merge requests.",
        "blame": "Sync blame data only.",
        "cicd": "Sync CI/CD runs and pipelines.",
        "deployments": "Sync deployments.",
        "incidents": "Sync incidents.",
        "security": "Sync security and dependency alerts.",
        "tests": "Sync CI test results and coverage (TestOps).",
    }

    for target in processor_sync_targets():
        help_text = target_help[target]
        target_parser = subparsers.add_parser(target, help=help_text)
        _add_sync_target_args(target_parser)
        target_parser.set_defaults(func=run_sync_target, sync_target=target)

    finalize_parser = subparsers.add_parser(
        "finalize-synthetic-sync",
        help=(
            "Complete a synthetic sync run seeded earlier with "
            "--defer-finalize (CHAOS-4266)."
        ),
    )
    _add_finalize_synthetic_sync_args(finalize_parser)
    finalize_parser.set_defaults(func=run_finalize_synthetic_sync)

    # Note: 'teams' and 'work-items' are also sync subcommands but handled in their own modules.
