#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import subprocess
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import List, Optional

from processors.github import process_github_repo, process_github_repos_batch
from processors.gitlab import process_gitlab_project, process_gitlab_projects_batch
from processors.local import process_local_blame, process_local_repo
from storage import create_store, detect_db_type
from utils import _parse_since, BATCH_SIZE, MAX_WORKERS

REPO_ROOT = Path(__file__).resolve().parent


def _load_dotenv(path: Path) -> int:
    """
    Load a .env file into process environment (without overriding existing vars).

    Keeps dependencies minimal (avoids python-dotenv).
    """
    if not path.exists():
        return 0
    loaded = 0
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or key in os.environ:
            continue
        if (len(value) >= 2) and ((value[0] == value[-1]) and value[0] in {"'", '"'}):
            value = value[1:-1]
        os.environ[key] = value
        loaded += 1
    return loaded


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date '{value}', expected YYYY-MM-DD"
        ) from exc


def _since_from_date_backfill(day: date, backfill_days: int) -> datetime:
    backfill = max(1, int(backfill_days))
    start_day = day - timedelta(days=backfill - 1)
    return datetime(
        start_day.year,
        start_day.month,
        start_day.day,
        tzinfo=timezone.utc,
    )


def _resolve_db_type(db_url: str, db_type: Optional[str]) -> str:
    if db_type:
        resolved = db_type.lower()
    else:
        try:
            resolved = detect_db_type(db_url)
        except ValueError as exc:
            raise SystemExit(str(exc)) from exc

    if resolved not in {"postgres", "mongo", "sqlite", "clickhouse"}:
        raise SystemExit(
            "DB_TYPE must be 'postgres', 'mongo', 'sqlite', or 'clickhouse'"
        )
    return resolved


async def _run_with_store(db_url: str, db_type: str, handler) -> None:
    store = create_store(db_url, db_type)
    async with store:
        await handler(store)


def _cmd_sync_teams(ns: argparse.Namespace) -> int:
    from models.teams import Team

    provider = (ns.provider or "config").lower()
    teams_data: List[Team] = []

    if provider == "config":
        from providers.teams import DEFAULT_TEAM_MAPPING_PATH
        import yaml

        path = Path(ns.path) if ns.path else DEFAULT_TEAM_MAPPING_PATH
        if not path.exists():
            logging.error(f"Teams config file not found at {path}")
            return 1

        try:
            with path.open("r", encoding="utf-8") as handle:
                payload = yaml.safe_load(handle) or {}
        except Exception as e:
            logging.error(f"Failed to parse teams config: {e}")
            return 1

        for entry in payload.get("teams") or []:
            team_id = str(entry.get("team_id") or "").strip()
            team_name = str(entry.get("team_name") or team_id).strip()
            description = entry.get("description")
            members = entry.get("members") or []
            if team_id:
                teams_data.append(
                    Team(
                        id=team_id,
                        name=team_name,
                        description=str(description) if description else None,
                        members=[str(m) for m in members],
                    )
                )

    elif provider == "jira":
        from providers.jira.client import JiraClient

        try:
            client = JiraClient.from_env()
        except ValueError as e:
            logging.error(f"Jira configuration error: {e}")
            return 1

        try:
            logging.info("Fetching projects from Jira...")
            projects = client.get_all_projects()
            for p in projects:
                # Use project Key as ID (stable), Name as Name
                key = p.get("key")
                name = p.get("name")
                desc = p.get("description")
                lead = p.get("lead", {})

                members = []
                if lead and lead.get("accountId"):
                    members.append(lead.get("accountId"))

                if key and name:
                    teams_data.append(
                        Team(
                            id=key,
                            name=name,
                            description=str(desc) if desc else f"Jira Project {key}",
                            members=members,
                        )
                    )
            logging.info(f"Fetched {len(teams_data)} projects from Jira.")
        except Exception as e:
            logging.error(f"Failed to fetch Jira projects: {e}")
            return 1
        finally:
            client.close()

    elif provider == "synthetic":
        from fixtures.generator import SyntheticDataGenerator

        generator = SyntheticDataGenerator()
        # Use 8 teams as requested for better visualization
        teams_data = generator.generate_teams(count=8)
        logging.info(f"Generated {len(teams_data)} synthetic teams.")

    else:
        logging.error(f"Unknown provider: {provider}")
        return 1

    if not teams_data:
        logging.warning("No teams found/generated.")
        return 0

    db_type = _resolve_db_type(ns.db, ns.db_type)

    async def _handler(store):
        # Ensure table exists (for SQL stores)
        if hasattr(store, "ensure_tables"):
            await store.ensure_tables()
        await store.insert_teams(teams_data)
        logging.info(f"Synced {len(teams_data)} teams to DB.")

    asyncio.run(_run_with_store(ns.db, db_type, _handler))
    return 0


def _resolve_since(ns: argparse.Namespace) -> Optional[datetime]:
    if ns.date is not None:
        return _since_from_date_backfill(ns.date, ns.backfill)
    if int(ns.backfill) != 1:
        raise SystemExit("--backfill requires --date")
    return _parse_since(getattr(ns, "since", None))


def _resolve_max_commits(ns: argparse.Namespace) -> Optional[int]:
    if ns.date is not None or getattr(ns, "since", None):
        return ns.max_commits_per_repo if ns.max_commits_per_repo is not None else None
    return ns.max_commits_per_repo or 100


def _sync_flags_for_target(target: str) -> dict:
    return {
        "sync_git": target == "git",
        "sync_prs": target == "prs",
        "sync_cicd": target == "cicd",
        "sync_deployments": target == "deployments",
        "sync_incidents": target == "incidents",
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
    return "acme/demo-app"


def _cmd_sync_target(ns: argparse.Namespace) -> int:
    target = ns.sync_target
    provider = (ns.provider or "").lower()
    if provider not in {"local", "github", "gitlab", "synthetic"}:
        raise SystemExit("Provider must be one of: local, github, gitlab, synthetic.")

    if target not in {"git", "prs", "blame", "cicd", "deployments", "incidents"}:
        raise SystemExit(
            "Sync target must be git, prs, blame, cicd, deployments, or incidents."
        )

    # TODO: Add Jira Ops/Service Desk incident ingestion once project-to-repo or deployment mapping is defined.
    if provider == "local":
        return _sync_local_target(ns, target)
    if provider == "github":
        return _sync_github_target(ns, target)
    if provider == "gitlab":
        return _sync_gitlab_target(ns, target)
    return _sync_synthetic_target(ns, target)


def _sync_local_target(ns: argparse.Namespace, target: str) -> int:
    if target not in {"git", "prs", "blame"}:
        raise SystemExit("Local provider supports only git, prs, or blame targets.")

    db_type = _resolve_db_type(ns.db, ns.db_type)
    since = _resolve_since(ns)

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

    asyncio.run(_run_with_store(ns.db, db_type, _handler))
    return 0


def _sync_github_target(ns: argparse.Namespace, target: str) -> int:
    token = ns.auth or os.getenv("GITHUB_TOKEN") or ""
    if not token:
        raise SystemExit("Missing GitHub token (pass --auth or set GITHUB_TOKEN).")

    db_type = _resolve_db_type(ns.db, ns.db_type)
    since = _resolve_since(ns)
    max_commits = _resolve_max_commits(ns)
    flags = _sync_flags_for_target(target)

    async def _handler(store):
        if ns.search:
            org_name = ns.group
            user_name = ns.owner if not ns.group else None
            await process_github_repos_batch(
                store=store,
                token=token,
                org_name=org_name,
                user_name=user_name,
                pattern=ns.search,
                batch_size=ns.batch_size,
                max_concurrent=ns.max_concurrent,
                rate_limit_delay=ns.rate_limit_delay,
                max_commits_per_repo=max_commits,
                max_repos=ns.max_repos,
                use_async=ns.use_async,
                sync_git=flags["sync_git"],
                sync_prs=flags["sync_prs"],
                sync_cicd=flags["sync_cicd"],
                sync_deployments=flags["sync_deployments"],
                sync_incidents=flags["sync_incidents"],
                blame_only=flags["blame_only"],
                backfill_missing=False,
                since=since,
            )
            return

        if not (ns.owner and ns.repo):
            raise SystemExit(
                "GitHub sync requires --owner and --repo (or --search for batch)."
            )
        await process_github_repo(
            store,
            ns.owner,
            ns.repo,
            token,
            blame_only=flags["blame_only"],
            max_commits=max_commits,
            sync_git=flags["sync_git"],
            sync_prs=flags["sync_prs"],
            sync_cicd=flags["sync_cicd"],
            sync_deployments=flags["sync_deployments"],
            sync_incidents=flags["sync_incidents"],
            since=since,
        )

    asyncio.run(_run_with_store(ns.db, db_type, _handler))
    return 0


def _sync_gitlab_target(ns: argparse.Namespace, target: str) -> int:
    token = ns.auth or os.getenv("GITLAB_TOKEN") or ""
    if not token:
        raise SystemExit("Missing GitLab token (pass --auth or set GITLAB_TOKEN).")

    db_type = _resolve_db_type(ns.db, ns.db_type)
    since = _resolve_since(ns)
    max_commits = _resolve_max_commits(ns)
    flags = _sync_flags_for_target(target)

    async def _handler(store):
        if ns.search:
            await process_gitlab_projects_batch(
                store=store,
                token=token,
                gitlab_url=ns.gitlab_url,
                group_name=ns.group,
                pattern=ns.search,
                batch_size=ns.batch_size,
                max_concurrent=ns.max_concurrent,
                rate_limit_delay=ns.rate_limit_delay,
                max_commits_per_project=max_commits,
                max_projects=ns.max_repos,
                use_async=ns.use_async,
                sync_git=flags["sync_git"],
                sync_prs=flags["sync_prs"],
                sync_cicd=flags["sync_cicd"],
                sync_deployments=flags["sync_deployments"],
                sync_incidents=flags["sync_incidents"],
                blame_only=flags["blame_only"],
                backfill_missing=False,
                since=since,
            )
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
            since=since,
        )

    asyncio.run(_run_with_store(ns.db, db_type, _handler))
    return 0


def _sync_synthetic_target(ns: argparse.Namespace, target: str) -> int:
    from fixtures.generator import SyntheticDataGenerator

    repo_name = _resolve_synthetic_repo_name(ns)
    db_type = _resolve_db_type(ns.db, ns.db_type)
    days = max(1, int(ns.backfill))

    async def _handler(store):
        generator = SyntheticDataGenerator(repo_name=repo_name)
        repo = generator.generate_repo()
        await store.insert_repo(repo)

        if target == "git":
            commits = generator.generate_commits(days=days)
            await store.insert_git_commit_data(commits)
            stats = generator.generate_commit_stats(commits)
            await store.insert_git_commit_stats(stats)
            return

        if target == "prs":
            pr_data = generator.generate_prs()
            prs = [p["pr"] for p in pr_data]
            await store.insert_git_pull_requests(prs)

            reviews = []
            for p in pr_data:
                reviews.extend(p["reviews"])
            if reviews:
                await store.insert_git_pull_request_reviews(reviews)
            return

        if target == "blame":
            commits = generator.generate_commits(days=days)
            files = generator.generate_files()
            await store.insert_git_file_data(files)
            blame_data = generator.generate_blame(commits)
            if blame_data:
                await store.insert_blame_data(blame_data)
            return

        if target == "cicd":
            pipeline_runs = generator.generate_ci_pipeline_runs(days=days)
            if pipeline_runs:
                await store.insert_ci_pipeline_runs(pipeline_runs)
            return

        if target == "deployments":
            deployments = generator.generate_deployments(days=days)
            if deployments:
                await store.insert_deployments(deployments)
            return

        if target == "incidents":
            incidents = generator.generate_incidents(days=days)
            if incidents:
                await store.insert_incidents(incidents)
            return

    asyncio.run(_run_with_store(ns.db, db_type, _handler))
    return 0


def _cmd_sync_work_items(ns: argparse.Namespace) -> int:
    from metrics.job_work_items import run_work_items_sync_job

    if ns.auth:
        provider = (ns.provider or "").strip().lower()
        if provider == "gitlab":
            os.environ["GITLAB_TOKEN"] = ns.auth
        elif provider == "github":
            os.environ["GITHUB_TOKEN"] = ns.auth
        elif provider == "jira":
            raise SystemExit(
                "--auth is not supported for Jira; use JIRA_BASE_URL/JIRA_EMAIL/JIRA_API_TOKEN."
            )
        elif provider in {"all", "*"}:
            raise SystemExit(
                "--auth is ambiguous with --provider all; set GITHUB_TOKEN and GITLAB_TOKEN env vars."
            )

    run_work_items_sync_job(
        db_url=ns.db,
        day=ns.date,
        backfill_days=max(1, int(ns.backfill)),
        provider=ns.provider,
        sink=ns.sink,
        repo_id=ns.repo_id,
        repo_name=getattr(ns, "repo_name", None),
        search_pattern=getattr(ns, "search", None),
    )
    return 0


def _cmd_metrics_daily(ns: argparse.Namespace) -> int:
    # Import lazily to keep CLI startup fast and avoid optional deps at import time.
    from metrics.job_daily import run_daily_metrics_job

    run_daily_metrics_job(
        db_url=ns.db,
        day=ns.date,
        backfill_days=max(1, int(ns.backfill)),
        repo_id=ns.repo_id,
        repo_name=getattr(ns, "repo_name", None),
        include_commit_metrics=not ns.skip_commit_metrics,
        sink=ns.sink,
        provider=ns.provider,
    )
    return 0


def _cmd_metrics_complexity(ns: argparse.Namespace) -> int:
    from metrics.job_complexity_db import run_complexity_db_job

    if ns.repo_path and ns.repo_path != ".":
        logging.warning(
            "--repo-path is ignored; complexity metrics are computed from the DB."
        )
    if ns.ref and ns.ref != "HEAD":
        logging.warning(
            "--ref is ignored; complexity metrics are computed from the DB."
        )

    try:
        return run_complexity_db_job(
            repo_id=ns.repo_id,
            db_url=ns.db,
            date=ns.date,
            backfill_days=ns.backfill,
            language_globs=getattr(ns, "include_glob", None),
            max_files=getattr(ns, "max_files", None),
            search_pattern=getattr(ns, "search", None),
            exclude_globs=getattr(ns, "exclude_glob", None),
        )
    except Exception as exc:
        logging.error(f"Complexity DB job failed: {exc}")
        return 1


def _cmd_audit_completeness(ns: argparse.Namespace) -> int:
    db_url = ns.db
    if not db_url:
        logging.error("Database URI is required (pass --db).")
        return 2
    try:
        backend = detect_db_type(db_url)
    except ValueError as exc:
        logging.error(str(exc))
        return 2
    if backend != "clickhouse":
        print("unsupported for now")
        return 2

    from audit.completeness import (
        completeness_failed,
        format_completeness_json,
        format_completeness_table,
        run_completeness_audit,
    )

    report = run_completeness_audit(db_url=db_url, days=ns.days)
    if ns.format == "json":
        print(format_completeness_json(report))
    else:
        print(format_completeness_table(report))

    if ns.fail_on_missing and completeness_failed(report):
        return 1
    return 0


def _cmd_audit_schema(ns: argparse.Namespace) -> int:
    db_url = ns.db
    if not db_url:
        logging.error("Database URI is required (pass --db).")
        return 2

    from audit.schema import format_schema_report, run_schema_audit

    report = run_schema_audit(db_url=db_url)
    if report.get("status") == "unchecked":
        print("unchecked")
        return 2

    print(format_schema_report(report))
    if ns.fail_on_missing and report.get("status") == "missing":
        return 1
    return 0


def _cmd_audit_perf(ns: argparse.Namespace) -> int:
    db_url = ns.db
    if not db_url:
        logging.error("Database URI is required (pass --db).")
        return 2

    from audit.perf import format_perf_report, run_perf_audit

    report = run_perf_audit(
        db_url=db_url,
        threshold_ms=ns.threshold_ms,
        lookback_minutes=ns.lookback_minutes,
        limit=ns.limit,
    )
    print(format_perf_report(report))
    if report.get("status") == "unchecked":
        return 2
    return 0


def _cmd_audit_coverage(ns: argparse.Namespace) -> int:
    db_url = ns.db
    if not db_url:
        logging.error("Database URI is required (pass --db).")
        return 2

    from audit.coverage import (
        coverage_failed,
        format_coverage_json,
        format_coverage_table,
        parse_provider_list,
        run_coverage_audit,
    )

    try:
        providers = parse_provider_list(ns.providers)
    except ValueError as exc:
        logging.error(str(exc))
        return 2

    report = run_coverage_audit(db_url=db_url, providers=providers)
    if ns.format == "json":
        print(format_coverage_json(report))
    else:
        print(format_coverage_table(report))

    if ns.fail_on_missing and coverage_failed(report):
        return 1
    return 0


def _cmd_grafana_up(_ns: argparse.Namespace) -> int:
    cmd = [
        "docker",
        "compose",
        "-f",
        str(REPO_ROOT / "compose.yml"),
        "up",
        "-d",
    ]
    return subprocess.run(cmd, check=False).returncode


def _cmd_grafana_down(_ns: argparse.Namespace) -> int:
    cmd = [
        "docker",
        "compose",
        "-f",
        str(REPO_ROOT / "compose.yml"),
        "down",
    ]
    return subprocess.run(cmd, check=False).returncode


def _add_sync_target_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--db", required=True, help="Database connection string.")
    parser.add_argument(
        "--db-type",
        choices=["postgres", "mongo", "sqlite", "clickhouse"],
        help="Optional DB backend override.",
    )
    parser.add_argument(
        "--provider",
        choices=["local", "github", "gitlab", "synthetic"],
        required=True,
        help="Source provider for the sync job.",
    )
    parser.add_argument("--auth", help="Provider token override (GitHub/GitLab).")
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
        "--repo-name", help="Synthetic repo name (default: acme/demo-app)."
    )
    time_group = parser.add_mutually_exclusive_group()
    time_group.add_argument("--since", help="Lower-bound ISO date/time (UTC).")
    time_group.add_argument(
        "--date",
        type=_parse_date,
        help="Target day (UTC) as YYYY-MM-DD (use with --backfill).",
    )
    parser.add_argument(
        "--backfill",
        type=int,
        default=1,
        help="Sync N days ending at --date (inclusive). Requires --date.",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dev-health-ops",
        description="Sync git data and compute developer health metrics.",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        help="Logging level (DEBUG, INFO, WARNING). Defaults to env LOG_LEVEL or INFO.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # ---- sync ----
    # TODO: ship an installed binary (e.g. `dho sync ...`) instead of `python cli.py`.
    sync = sub.add_parser("sync", help="Sync git facts into a DB backend.")
    sync_sub = sync.add_subparsers(dest="sync_command", required=True)

    target_parsers = {
        "git": "Sync commits and commit stats.",
        "prs": "Sync pull/merge requests.",
        "blame": "Sync blame data only.",
        "cicd": "Sync CI/CD runs and pipelines.",
        "deployments": "Sync deployments.",
        "incidents": "Sync incidents.",
    }

    for target, help_text in target_parsers.items():
        target_parser = sync_sub.add_parser(target, help=help_text)
        _add_sync_target_args(target_parser)
        target_parser.set_defaults(func=_cmd_sync_target, sync_target=target)

    teams = sync_sub.add_parser(
        "teams", help="Sync teams from config/teams.yaml, Jira, or Synthetic."
    )
    teams.add_argument("--db", required=True, help="Database connection string.")
    teams.add_argument(
        "--db-type",
        choices=["postgres", "mongo", "sqlite", "clickhouse"],
        help="Optional DB backend override.",
    )
    teams.add_argument(
        "--provider",
        choices=["config", "jira", "synthetic"],
        default="config",
        help="Source of team data (default: config).",
    )
    teams.add_argument(
        "--path", help="Path to teams.yaml config (used if provider=config)."
    )
    teams.set_defaults(func=_cmd_sync_teams)

    wi = sync_sub.add_parser(
        "work-items",
        help="Sync work tracking data from provider APIs (and write derived work item tables).",
    )
    wi.add_argument("--db", required=True, help="Database connection string.")
    wi.add_argument(
        "--provider",
        choices=["all", "jira", "github", "gitlab", "synthetic"],
        required=True,
        help="Which work item providers to sync.",
    )
    wi.add_argument(
        "--auth",
        help="Provider token override (GitHub/GitLab). Sets GITHUB_TOKEN or GITLAB_TOKEN.",
    )
    wi.add_argument(
        "--sink",
        choices=["auto", "clickhouse", "mongo", "sqlite", "postgres", "both"],
        default="auto",
        help="Where to write derived work item metrics.",
    )
    wi.add_argument(
        "--date",
        required=True,
        type=_parse_date,
        help="Target day (UTC) as YYYY-MM-DD.",
    )
    wi.add_argument(
        "--backfill",
        type=int,
        default=1,
        help="Sync and compute N days ending at --date (inclusive).",
    )
    wi.add_argument(
        "--repo-id",
        type=lambda s: __import__("uuid").UUID(s),
        help="Optional repo_id UUID filter (affects GitHub/GitLab repo selection).",
    )
    wi.add_argument(
        "--repo-name",
        help="Optional repo name filter (e.g. 'owner/repo') (affects GitHub/GitLab repo selection).",
    )
    wi.add_argument(
        "-s",
        "--search",
        help="Filter repos by name (glob pattern, e.g. 'org/*').",
    )
    wi.set_defaults(func=_cmd_sync_work_items)

    # ---- metrics ----
    metrics = sub.add_parser("metrics", help="Compute and write derived metrics.")
    metrics_sub = metrics.add_subparsers(dest="metrics_command", required=True)

    daily = metrics_sub.add_parser(
        "daily", help="Compute daily metrics (optionally backfill)."
    )
    daily.add_argument(
        "--date",
        required=True,
        type=_parse_date,
        help="Target day (UTC) as YYYY-MM-DD.",
    )
    daily.add_argument(
        "--backfill",
        type=int,
        default=1,
        help="Compute N days ending at --date (inclusive).",
    )
    daily.add_argument(
        "--db",
        default=os.getenv("DATABASE_URI") or os.getenv("DATABASE_URL"),
        help="Source DB URI (and default sink).",
    )
    daily.add_argument(
        "--repo-id",
        type=lambda s: __import__("uuid").UUID(s),
        help="Optional repo_id UUID filter.",
    )
    daily.add_argument(
        "--repo-name",
        help="Optional repo name filter (e.g. 'owner/repo').",
    )
    daily.add_argument(
        "--provider",
        choices=["all", "jira", "github", "gitlab", "synthetic", "none"],
        default="none",
        help="Which work item providers to include.",
    )
    daily.add_argument(
        "--sink",
        choices=["auto", "clickhouse", "mongo", "sqlite", "postgres", "both"],
        default="auto",
        help="Where to write derived metrics.",
    )
    daily.add_argument(
        "--skip-commit-metrics",
        action="store_true",
        help="Skip per-commit metrics output.",
    )
    daily.set_defaults(func=_cmd_metrics_daily)

    complexity = metrics_sub.add_parser(
        "complexity", help="Scan and compute complexity metrics."
    )
    complexity.add_argument(
        "--repo-path",
        default=".",
        help="Ignored (complexity is computed from DB). Kept for backward compatibility.",
    )
    complexity.add_argument(
        "--repo-id",
        type=lambda s: __import__("uuid").UUID(s),
        help="Repo UUID. If omitted, scans dir for repos.",
    )
    complexity.add_argument(
        "-s", "--search", help="Filter repos by name (glob pattern, e.g. 'org/*')."
    )
    complexity.add_argument(
        "--date",
        type=_parse_date,
        default=date.today().isoformat(),
        help="Date of snapshot (YYYY-MM-DD).",
    )
    complexity.add_argument(
        "--ref",
        default="HEAD",
        help="Ignored (complexity is computed from DB). Kept for backward compatibility.",
    )
    complexity.add_argument(
        "--backfill",
        type=int,
        default=1,
        help="Compute N days ending at --date (inclusive).",
    )
    complexity.add_argument(
        "--max-files",
        type=int,
        help="Optional limit on number of files scanned per repo.",
    )
    complexity.add_argument(
        "--include-glob",
        action="append",
        help="Glob patterns to include when scanning (repeatable).",
    )
    complexity.add_argument(
        "--exclude-glob",
        action="append",
        help="Glob patterns to exclude when scanning (repeatable).",
    )
    complexity.add_argument(
        "--db",
        default=os.getenv("DATABASE_URI") or os.getenv("DATABASE_URL"),
        help="Database URI.",
    )
    complexity.set_defaults(func=_cmd_metrics_complexity)

    # ---- grafana ----
    graf = sub.add_parser(
        "grafana", help="Start/stop the Grafana + ClickHouse dev stack."
    )
    graf_sub = graf.add_subparsers(dest="grafana_command", required=True)
    graf_up = graf_sub.add_parser(
        "up", help="docker compose up -d for grafana/docker-compose.yml"
    )
    graf_up.set_defaults(func=_cmd_grafana_up)
    graf_down = graf_sub.add_parser(
        "down", help="docker compose down for grafana/docker-compose.yml"
    )
    graf_down.set_defaults(func=_cmd_grafana_down)

    # ---- fixtures ----
    fix = sub.add_parser("fixtures", help="Data simulation and fixtures.")
    fix_sub = fix.add_subparsers(dest="fixtures_command", required=True)
    fix_gen = fix_sub.add_parser("generate", help="Generate synthetic data.")
    fix_gen.add_argument(
        "--db",
        default=os.getenv("DATABASE_URI") or os.getenv("DATABASE_URL"),
        help="Target DB URI.",
    )
    fix_gen.add_argument(
        "--db-type", help="Explicit DB type (postgres, clickhouse, etc)."
    )
    fix_gen.add_argument("--repo-name", default="acme/demo-app", help="Repo name.")
    fix_gen.add_argument(
        "--repo-count", type=int, default=1, help="Number of repos to generate."
    )
    fix_gen.add_argument("--days", type=int, default=30, help="Number of days of data.")
    fix_gen.add_argument(
        "--commits-per-day", type=int, default=5, help="Avg commits per day."
    )
    fix_gen.add_argument("--pr-count", type=int, default=20, help="Total PRs.")
    fix_gen.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Deterministic seed for fixtures (repeatable runs).",
    )
    fix_gen.add_argument(
        "--provider",
        default="synthetic",
        choices=["synthetic", "github", "gitlab", "jira"],
        help="Provider label to use for generated work items.",
    )
    fix_gen.add_argument(
        "--with-work-graph",
        action="store_true",
        help="Build work graph edges after fixture generation (ClickHouse only).",
    )
    fix_gen.add_argument(
        "--with-metrics", action="store_true", help="Also generate derived metrics."
    )
    fix_gen.set_defaults(func=_cmd_fixtures_generate)

    fix_val = fix_sub.add_parser("validate", help="Validate fixture data quality.")
    fix_val.add_argument(
        "--db",
        required=True,
        help="Database URI.",
    )
    fix_val.set_defaults(func=_cmd_fixtures_validate)

    # ---- audit ----
    audit = sub.add_parser("audit", help="Audit data completeness.")
    audit_sub = audit.add_subparsers(dest="audit_command", required=True)
    completeness = audit_sub.add_parser(
        "completeness", help="Audit source data completeness."
    )
    completeness.add_argument("--db", required=True, help="Database connection string.")
    completeness.add_argument(
        "--days",
        type=int,
        default=30,
        help="Lookback window in days.",
    )
    completeness.add_argument(
        "--fail-on-missing",
        action="store_true",
        help="Exit non-zero if required data is missing or stale.",
    )
    completeness.add_argument(
        "--format",
        choices=["table", "json"],
        default="table",
        help="Output format.",
    )
    completeness.set_defaults(func=_cmd_audit_completeness)

    coverage = audit_sub.add_parser(
        "coverage", help="Audit provider coverage (implementation checks)."
    )
    coverage.add_argument("--db", required=True, help="Database connection string.")
    coverage.add_argument(
        "--providers",
        default="jira,github,gitlab,synthetic",
        help="Comma-separated providers to audit.",
    )
    coverage.add_argument(
        "--fail-on-missing",
        action="store_true",
        help="Exit non-zero if coverage checks are missing.",
    )
    coverage.add_argument(
        "--format",
        choices=["table", "json"],
        default="table",
        help="Output format.",
    )
    coverage.set_defaults(func=_cmd_audit_coverage)

    schema = audit_sub.add_parser(
        "schema", help="Audit database schema against migrations."
    )
    schema.add_argument("--db", required=True, help="Database connection string.")
    schema.add_argument(
        "--fail-on-missing",
        action="store_true",
        help="Exit non-zero if required schema objects are missing.",
    )
    schema.set_defaults(func=_cmd_audit_schema)

    perf = audit_sub.add_parser(
        "perf", help="Audit ClickHouse slow queries via system.query_log."
    )
    perf.add_argument("--db", required=True, help="Database connection string.")
    perf.add_argument(
        "--threshold-ms",
        type=int,
        default=1000,
        help="Slow query threshold in milliseconds.",
    )
    perf.add_argument(
        "--lookback-minutes",
        type=int,
        default=60,
        help="Lookback window in minutes.",
    )
    perf.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Max number of slow queries to return.",
    )
    perf.set_defaults(func=_cmd_audit_perf)

    # ---- api ----
    api = sub.add_parser("api", help="Run the Dev Health Ops API server.")
    api.add_argument(
        "--db",
        default=os.getenv("DATABASE_URI") or os.getenv("DATABASE_URL"),
        help="Database URI.",
    )
    api.add_argument("--host", default="127.0.0.1", help="Bind host.")
    api.add_argument("--port", type=int, default=8000, help="Bind port.")
    api.add_argument(
        "--reload",
        action="store_true",
        help="Enable auto-reload for local development.",
    )
    api.set_defaults(func=_cmd_api)

    # ---- work-graph ----
    wg = sub.add_parser("work-graph", help="Work graph operations.")
    wg_sub = wg.add_subparsers(dest="work_graph_command", required=True)

    wg_build = wg_sub.add_parser("build", help="Build work graph edges from raw data.")
    wg_build.add_argument(
        "--db",
        required=True,
        help="ClickHouse connection string (clickhouse://user:pass@host:port/db).",
    )
    wg_build.add_argument(
        "--from",
        dest="from_date",
        type=str,
        help="Start date (YYYY-MM-DD). Defaults to 30 days ago.",
    )
    wg_build.add_argument(
        "--to",
        dest="to_date",
        type=str,
        help="End date (YYYY-MM-DD). Defaults to today.",
    )
    wg_build.add_argument(
        "--repo-id",
        type=str,
        help="Filter to specific repository UUID.",
    )
    wg_build.add_argument(
        "--heuristic-window",
        type=int,
        default=7,
        help="Days window for heuristic issue->PR matching (default: 7).",
    )
    wg_build.add_argument(
        "--heuristic-confidence",
        type=float,
        default=0.3,
        help="Confidence score for heuristic matches (default: 0.3).",
    )
    wg_build.add_argument(
        "--allow-degenerate",
        action="store_true",
        help="Allow single connected-component graphs (default: fail).",
    )
    wg_build.set_defaults(func=_cmd_work_graph_build)

    # ---- investment ----
    investment = sub.add_parser(
        "investment", help="Investment materialization operations."
    )
    investment_sub = investment.add_subparsers(dest="investment_command", required=True)
    investment_materialize = investment_sub.add_parser(
        "materialize",
        help="Materialize work unit investment categorization into sinks.",
    )
    investment_materialize.add_argument(
        "--db",
        required=True,
        help="ClickHouse connection string (clickhouse://user:pass@host:port/db).",
    )
    investment_materialize.add_argument(
        "--from",
        dest="from_date",
        type=str,
        help="Start date (YYYY-MM-DD). Defaults to window-days before --to.",
    )
    investment_materialize.add_argument(
        "--to",
        dest="to_date",
        type=str,
        help="End date (YYYY-MM-DD). Defaults to now.",
    )
    investment_materialize.add_argument(
        "--window-days",
        type=int,
        default=30,
        help="Window size in days when --from is not set (default: 30).",
    )
    investment_materialize.add_argument(
        "--repo-id",
        action="append",
        default=[],
        help="Filter to specific repository UUID(s).",
    )
    investment_materialize.add_argument(
        "--team-id",
        action="append",
        default=[],
        help="Filter to specific team identifier(s).",
    )
    investment_materialize.add_argument(
        "--llm-provider",
        default="auto",
        help="LLM provider (auto, openai, anthropic, local, mock).",
    )
    investment_materialize.add_argument(
        "--persist-evidence-snippets",
        action="store_true",
        help="Persist extractive evidence quotes for work units.",
    )
    investment_materialize.add_argument(
        "--model",
        default=None,
        help="Override model version label for audit fields.",
    )
    investment_materialize.set_defaults(func=_cmd_investment_materialize)

    return parser


def _cmd_fixtures_generate(ns: argparse.Namespace) -> int:
    from fixtures.generator import SyntheticDataGenerator

    db_type = _resolve_db_type(ns.db, ns.db_type)

    async def _handler(store):
        from datetime import datetime, timedelta, timezone

        repo_count = max(1, ns.repo_count)
        base_name = ns.repo_name
        team_assignment = SyntheticDataGenerator(
            repo_name=base_name, seed=ns.seed
        ).get_team_assignment(count=8)
        if hasattr(store, "insert_teams") and team_assignment.get("teams"):
            await store.insert_teams(team_assignment["teams"])
            logging.info("Inserted %d synthetic teams.", len(team_assignment["teams"]))
        from storage import SQLAlchemyStore

        allow_parallel_inserts = not isinstance(store, SQLAlchemyStore)
        insert_semaphore = asyncio.Semaphore(MAX_WORKERS)

        async def _insert_batches(
            insert_fn, items, batch_size: int = BATCH_SIZE
        ) -> None:
            if not items:
                return
            batches = [
                items[i : i + batch_size] for i in range(0, len(items), batch_size)
            ]
            if not allow_parallel_inserts or MAX_WORKERS <= 1 or len(batches) == 1:
                for batch in batches:
                    await insert_fn(batch)
                return

            async def _run(batch):
                async with insert_semaphore:
                    await insert_fn(batch)

            await asyncio.gather(*(_run(batch) for batch in batches))

        for i in range(repo_count):
            r_name = base_name
            if repo_count > 1:
                r_name = f"{base_name}-{i + 1}"

            logging.info(
                f"Generating fixture data for repo {i + 1}/{repo_count}: {r_name}"
            )
            seed_value = (int(ns.seed) + i) if ns.seed is not None else None
            generator = SyntheticDataGenerator(
                repo_name=r_name, provider=ns.provider, seed=seed_value
            )

            # 1. Repo
            repo = generator.generate_repo()
            await store.insert_repo(repo)

            # 2. Files
            files = generator.generate_files()
            await _insert_batches(store.insert_git_file_data, files)

            # 3. Commits & Stats
            commits = generator.generate_commits(
                days=ns.days, commits_per_day=ns.commits_per_day
            )
            await _insert_batches(store.insert_git_commit_data, commits)
            stats = generator.generate_commit_stats(commits)
            await _insert_batches(store.insert_git_commit_stats, stats)

            # 4. Work Items (Raw)
            work_items = generator.generate_work_items(
                days=ns.days, provider=ns.provider
            )
            transitions = generator.generate_work_item_transitions(work_items)

            if hasattr(store, "insert_work_items"):
                await _insert_batches(store.insert_work_items, work_items)
            if hasattr(store, "insert_work_item_transitions"):
                await _insert_batches(store.insert_work_item_transitions, transitions)

            dependencies = generator.generate_work_item_dependencies(work_items)
            if hasattr(store, "insert_work_item_dependencies"):
                await _insert_batches(store.insert_work_item_dependencies, dependencies)

            issue_numbers = []
            for item in work_items:
                raw_id = str(getattr(item, "work_item_id", "") or "")
                if "#" in raw_id:
                    tail = raw_id.split("#")[-1]
                    if tail.isdigit():
                        issue_numbers.append(int(tail))
                        continue
                if "-" in raw_id:
                    tail = raw_id.split("-")[-1]
                    if tail.isdigit():
                        issue_numbers.append(int(tail))

            # 5. PRs & Reviews
            pr_data = generator.generate_prs(
                count=ns.pr_count, issue_numbers=issue_numbers
            )
            prs = [p["pr"] for p in pr_data]
            await _insert_batches(store.insert_git_pull_requests, prs)

            all_reviews = []
            for p in pr_data:
                all_reviews.extend(p["reviews"])
            await _insert_batches(store.insert_git_pull_request_reviews, all_reviews)

            pr_commit_links = generator.generate_pr_commits(prs, commits)
            if hasattr(store, "insert_work_graph_pr_commit"):
                await _insert_batches(
                    store.insert_work_graph_pr_commit, pr_commit_links
                )

            issue_pr_links = generator.generate_issue_pr_links(
                work_items, prs, min_coverage=0.7
            )
            if hasattr(store, "insert_work_graph_issue_pr"):
                await _insert_batches(store.insert_work_graph_issue_pr, issue_pr_links)

            # 6. CI/CD + Deployments + Incidents
            pr_numbers = [pr.number for pr in prs]
            pipeline_runs = generator.generate_ci_pipeline_runs(days=ns.days)
            deployments = generator.generate_deployments(
                days=ns.days, pr_numbers=pr_numbers
            )
            incidents = generator.generate_incidents(days=ns.days)
            await _insert_batches(store.insert_ci_pipeline_runs, pipeline_runs)
            await _insert_batches(store.insert_deployments, deployments)
            await _insert_batches(store.insert_incidents, incidents)

            # 7. Blame Data
            blame_data = generator.generate_blame(commits)
            await _insert_batches(store.insert_blame_data, blame_data)

            logging.info(f"Generated synthetic data for {r_name}")
            logging.info(f"- Commits: {len(commits)}")
            logging.info(f"- PRs: {len(prs)}")
            logging.info(f"- Work Items: {len(work_items)}")

            # 8. Complexity Metrics (Always generate snapshots if sink available)
            # We need a sink for complexity snapshots. If metrics are disabled, we might not have one initialized.
            # But the user requested "complexity" be added.

            # If with_metrics is on, we do full daily metrics.
            # If off, we only do raw data. Complexity snapshots are border-line.
            # Let's include them if with_metrics is True OR if we initialize a temporary sink just for them.
            # For simplicity, we'll keep them in with_metrics block for now, as they are "metrics".
            # BUT the user said "isn't adding... complexity".
            # I will assume they run with --with-metrics.
            # If they want complexity without daily metrics, that's a different feature request (scan job).

            if ns.with_metrics:
                from metrics.job_daily import (
                    ClickHouseMetricsSink,
                    MongoMetricsSink,
                    PostgresMetricsSink,
                    SQLiteMetricsSink,
                )

                sink = None
                if db_type == "clickhouse":
                    sink = ClickHouseMetricsSink(ns.db)
                elif db_type == "sqlite":
                    from metrics.job_daily import _normalize_sqlite_url

                    sink = SQLiteMetricsSink(_normalize_sqlite_url(ns.db))
                elif db_type == "mongo":
                    sink = MongoMetricsSink(ns.db)
                elif db_type == "postgres":
                    sink = PostgresMetricsSink(ns.db)

                if sink:
                    if isinstance(sink, MongoMetricsSink):
                        sink.ensure_indexes()
                    else:
                        sink.ensure_tables()

                    # Generate and write complexity snapshots
                    comp_data = generator.generate_complexity_metrics(days=ns.days)
                    complexity_by_day = {}
                    for snapshot in comp_data["snapshots"]:
                        complexity_by_day.setdefault(snapshot.as_of_day, {})[
                            snapshot.file_path
                        ] = snapshot
                    if hasattr(sink, "write_file_complexity_snapshots"):
                        if comp_data["snapshots"]:
                            sink.write_file_complexity_snapshots(comp_data["snapshots"])
                        if comp_data["dailies"]:
                            sink.write_repo_complexity_daily(comp_data["dailies"])
                        logging.info(
                            f"- Complexity snapshots: {len(comp_data['snapshots'])}"
                        )

                    blame_concentration = {}
                    if blame_data:
                        blame_by_file = {}
                        for row in blame_data:
                            author = (
                                getattr(row, "author_email", None)
                                or getattr(row, "author_name", None)
                                or "unknown"
                            )
                            path = getattr(row, "path", None)
                            if not path:
                                continue
                            blame_by_file.setdefault(path, {})
                            blame_by_file[path][author] = (
                                blame_by_file[path].get(author, 0) + 1
                            )
                        for path, counts in blame_by_file.items():
                            total = sum(counts.values())
                            if total:
                                blame_concentration[path] = max(
                                    counts.values()
                                ) / float(total)

                    import uuid

                    from analytics.investment import InvestmentClassifier
                    from analytics.issue_types import IssueTypeNormalizer
                    from metrics.compute import compute_daily_metrics
                    from metrics.compute_cicd import compute_cicd_metrics_daily
                    from metrics.compute_deployments import compute_deploy_metrics_daily
                    from metrics.compute_incidents import compute_incident_metrics_daily
                    from metrics.compute_wellbeing import (
                        compute_team_wellbeing_metrics_daily,
                    )
                    from metrics.compute_work_item_state_durations import (
                        compute_work_item_state_durations_daily,
                    )
                    from metrics.compute_work_items import (
                        compute_work_item_metrics_daily,
                    )
                    from metrics.compute_ic import (
                        compute_ic_metrics_daily,
                        compute_ic_landscape_rolling,
                    )
                    from metrics.hotspots import (
                        compute_file_hotspots,
                        compute_file_risk_hotspots,
                    )
                    from metrics.knowledge import (
                        compute_bus_factor,
                        compute_code_ownership_gini,
                    )
                    from metrics.quality import (
                        compute_rework_churn_ratio,
                        compute_single_owner_file_ratio,
                    )
                    from metrics.reviews import compute_review_edges_daily
                    from metrics.schemas import (
                        InvestmentClassificationRecord,
                        InvestmentMetricsRecord,
                        IssueTypeMetricsRecord,
                    )
                    from providers.identity import load_identity_resolver
                    from providers.teams import TeamResolver

                    investment_classifier = InvestmentClassifier(
                        REPO_ROOT / "config/investment_areas.yaml"
                    )
                    issue_type_normalizer = IssueTypeNormalizer(
                        REPO_ROOT / "config/issue_type_mapping.yaml"
                    )
                    identity_resolver = load_identity_resolver()

                    commit_by_hash = {c.hash: c for c in commits}
                    commit_stat_rows = []
                    for stat in stats:
                        commit = commit_by_hash.get(stat.commit_hash)
                        if not commit:
                            continue
                        commit_stat_rows.append({
                            "repo_id": stat.repo_id,
                            "commit_hash": stat.commit_hash,
                            "author_email": commit.author_email,
                            "author_name": commit.author_name,
                            "committer_when": commit.committer_when,
                            "file_path": stat.file_path,
                            "additions": stat.additions,
                            "deletions": stat.deletions,
                        })

                    pull_request_rows = []
                    for pr in prs:
                        pull_request_rows.append({
                            "repo_id": pr.repo_id,
                            "number": pr.number,
                            "author_email": pr.author_email,
                            "author_name": pr.author_name,
                            "created_at": pr.created_at,
                            "merged_at": pr.merged_at,
                            "first_review_at": pr.first_review_at,
                            "first_comment_at": pr.first_comment_at,
                            "reviews_count": pr.reviews_count,
                            "comments_count": pr.comments_count,
                            "changes_requested_count": pr.changes_requested_count,
                            "additions": pr.additions,
                            "deletions": pr.deletions,
                            "changed_files": pr.changed_files,
                        })

                    review_rows = []
                    for review in all_reviews:
                        review_rows.append({
                            "repo_id": review.repo_id,
                            "number": review.number,
                            "reviewer": review.reviewer,
                            "submitted_at": review.submitted_at,
                            "state": review.state,
                        })

                    pipeline_rows = []
                    for run in pipeline_runs:
                        pipeline_rows.append({
                            "repo_id": run.repo_id,
                            "run_id": run.run_id,
                            "status": run.status,
                            "queued_at": run.queued_at,
                            "started_at": run.started_at,
                            "finished_at": run.finished_at,
                        })

                    deployment_rows = []
                    for deployment in deployments:
                        deployment_rows.append({
                            "repo_id": deployment.repo_id,
                            "deployment_id": deployment.deployment_id,
                            "status": deployment.status,
                            "environment": deployment.environment,
                            "started_at": deployment.started_at,
                            "finished_at": deployment.finished_at,
                            "deployed_at": deployment.deployed_at,
                            "merged_at": deployment.merged_at,
                            "pull_request_number": deployment.pull_request_number,
                        })

                    incident_rows = []
                    for incident in incidents:
                        incident_rows.append({
                            "repo_id": incident.repo_id,
                            "incident_id": incident.incident_id,
                            "status": incident.status,
                            "started_at": incident.started_at,
                            "resolved_at": incident.resolved_at,
                        })

                    # Provide stable team IDs for dashboards without requiring config.
                    team_resolver = TeamResolver(
                        member_to_team=team_assignment["member_map"]
                    )
                    team_map = {
                        k: v[0] for k, v in team_assignment["member_map"].items()
                    }

                    computed_at = datetime.now(timezone.utc)

                    end_day = computed_at.date()
                    start_day = end_day - timedelta(days=max(1, int(ns.days)) - 1)

                    for day_index in range(max(1, int(ns.days))):
                        day = start_day + timedelta(days=day_index)

                        start_dt = datetime.combine(day, time.min, tzinfo=timezone.utc)
                        end_dt = start_dt + timedelta(days=1)

                        mttr_by_repo = {}
                        bug_times = {}
                        for item in work_items:
                            if (
                                item.type == "bug"
                                and item.completed_at
                                and item.started_at
                            ):
                                start_item = item.started_at
                                end_item = item.completed_at
                                if start_item is None or end_item is None:
                                    continue
                                if start_item < end_dt and end_item >= start_dt:
                                    r_id = getattr(item, "repo_id", None)
                                    if r_id:
                                        hours = (
                                            end_item - start_item
                                        ).total_seconds() / 3600.0
                                        bug_times.setdefault(r_id, []).append(hours)
                        for r_id, times in bug_times.items():
                            mttr_by_repo[r_id] = sum(times) / len(times)

                        window_days = 30
                        h_start = datetime.combine(
                            day - timedelta(days=window_days - 1),
                            time.min,
                            tzinfo=timezone.utc,
                        )
                        window_stats = [
                            row
                            for row in commit_stat_rows
                            if h_start <= row["committer_when"] < end_dt
                        ]
                        day_commit_stats = [
                            row
                            for row in commit_stat_rows
                            if start_dt <= row["committer_when"] < end_dt
                        ]
                        rework_ratio_by_repo = {
                            repo.id: compute_rework_churn_ratio(
                                repo_id=str(repo.id), window_stats=window_stats
                            )
                        }
                        single_owner_ratio_by_repo = {
                            repo.id: compute_single_owner_file_ratio(
                                repo_id=str(repo.id), window_stats=window_stats
                            )
                        }
                        bus_factor_by_repo = {
                            repo.id: compute_bus_factor(
                                repo_id=str(repo.id), window_stats=window_stats
                            )
                        }
                        gini_by_repo = {
                            repo.id: compute_code_ownership_gini(
                                repo_id=str(repo.id), window_stats=window_stats
                            )
                        }

                        repo_result = compute_daily_metrics(
                            day=day,
                            commit_stat_rows=day_commit_stats,
                            pull_request_rows=pull_request_rows,
                            pull_request_review_rows=review_rows,
                            computed_at=computed_at,
                            include_commit_metrics=True,
                            team_resolver=team_resolver,
                            identity_resolver=identity_resolver,
                            mttr_by_repo=mttr_by_repo,
                            rework_churn_ratio_by_repo=rework_ratio_by_repo,
                            single_owner_file_ratio_by_repo=single_owner_ratio_by_repo,
                            bus_factor_by_repo=bus_factor_by_repo,
                            code_ownership_gini_by_repo=gini_by_repo,
                        )
                        team_metrics = compute_team_wellbeing_metrics_daily(
                            day=day,
                            commit_stat_rows=day_commit_stats,
                            team_resolver=team_resolver,
                            computed_at=computed_at,
                        )

                        wi_rows, wi_user_rows, cycle_rows = (
                            compute_work_item_metrics_daily(
                                day=day,
                                work_items=work_items,
                                transitions=transitions,
                                computed_at=computed_at,
                                team_resolver=team_resolver,
                            )
                        )

                        if not repo_result.user_metrics:
                            repo_result.user_metrics = (
                                generator.generate_user_metrics_daily(
                                    day=day,
                                    member_map=team_assignment["member_map"],
                                )
                            )
                        if not wi_user_rows:
                            wi_user_rows = (
                                generator.generate_work_item_user_metrics_daily(
                                    day=day,
                                    member_map=team_assignment["member_map"],
                                )
                            )

                        # Enrich User Metrics with IC fields
                        ic_metrics = compute_ic_metrics_daily(
                            git_metrics=repo_result.user_metrics,
                            wi_metrics=wi_user_rows,
                            team_map=team_map,
                        )
                        # Use the enriched list for writing
                        repo_result.user_metrics[:] = ic_metrics

                        if wi_rows:
                            sink.write_work_item_metrics(wi_rows)
                        if wi_user_rows:
                            sink.write_work_item_user_metrics(wi_user_rows)
                        if cycle_rows:
                            sink.write_work_item_cycle_times(cycle_rows)

                        state_rows = compute_work_item_state_durations_daily(
                            day=day,
                            work_items=work_items,
                            transitions=transitions,
                            computed_at=computed_at,
                            team_resolver=team_resolver,
                        )
                        if state_rows:
                            sink.write_work_item_state_durations(state_rows)

                        issue_type_stats = {}
                        investment_classifications = []
                        investment_metrics_rows = []
                        inv_metrics_map = {}

                        def _get_team(wi):
                            if getattr(wi, "assignees", None):
                                t_id, _ = team_resolver.resolve(wi.assignees[0])
                                if t_id:
                                    return t_id
                            return "unassigned"

                        def _normalize_investment_team_id(team_id):
                            if not team_id or team_id == "unassigned":
                                return None
                            return team_id

                        for item in work_items:
                            r_id = getattr(item, "repo_id", None) or uuid.UUID(int=0)
                            prov = item.provider
                            team_id = _get_team(item)
                            norm_type = issue_type_normalizer.normalize(
                                prov, item.type, getattr(item, "labels", [])
                            )

                            key = (r_id, prov, team_id, norm_type)
                            if key not in issue_type_stats:
                                issue_type_stats[key] = {
                                    "created": 0,
                                    "completed": 0,
                                    "active": 0,
                                    "cycle_hours": [],
                                }

                            stats = issue_type_stats[key]
                            created = item.created_at
                            if start_dt <= created < end_dt:
                                stats["created"] += 1

                            if item.completed_at:
                                completed = item.completed_at
                                if start_dt <= completed < end_dt:
                                    stats["completed"] += 1
                                    if item.started_at:
                                        hours = (
                                            completed - item.started_at
                                        ).total_seconds() / 3600.0
                                        if hours >= 0:
                                            stats["cycle_hours"].append(hours)

                            if created < end_dt and (
                                not item.completed_at or item.completed_at >= start_dt
                            ):
                                stats["active"] += 1

                            if created < end_dt and (
                                not item.completed_at or item.completed_at >= start_dt
                            ):
                                cls = investment_classifier.classify({
                                    "labels": getattr(item, "labels", []),
                                    "component": getattr(item, "component", ""),
                                    "title": item.title,
                                    "provider": item.provider,
                                })
                                investment_classifications.append(
                                    InvestmentClassificationRecord(
                                        repo_id=r_id if r_id.int != 0 else None,
                                        day=day,
                                        artifact_type="work_item",
                                        artifact_id=item.work_item_id,
                                        provider=item.provider,
                                        investment_area=cls.investment_area,
                                        project_stream=cls.project_stream or "",
                                        confidence=cls.confidence,
                                        rule_id=cls.rule_id,
                                        computed_at=computed_at,
                                    )
                                )

                                if item.completed_at:
                                    completed = item.completed_at
                                    if start_dt <= completed < end_dt:
                                        team_id = _normalize_investment_team_id(
                                            _get_team(item)
                                        )
                                        key = (
                                            r_id,
                                            team_id,
                                            cls.investment_area,
                                            cls.project_stream or "",
                                        )
                                        if key not in inv_metrics_map:
                                            inv_metrics_map[key] = {
                                                "units": 0,
                                                "completed": 0,
                                                "churn": 0,
                                                "cycles": [],
                                            }
                                        inv_metrics_map[key]["completed"] += 1
                                        points = getattr(item, "story_points", 1) or 1
                                        inv_metrics_map[key]["units"] += int(points)
                                        if item.started_at:
                                            hours = (
                                                completed - item.started_at
                                            ).total_seconds() / 3600.0
                                            if hours >= 0:
                                                inv_metrics_map[key]["cycles"].append(
                                                    hours
                                                )

                        issue_type_metrics_rows = []
                        for (
                            r_id,
                            prov,
                            team_id,
                            norm_type,
                        ), stat in issue_type_stats.items():
                            cycles = sorted(stat["cycle_hours"])
                            p50 = cycles[len(cycles) // 2] if cycles else 0.0
                            p90 = cycles[int(len(cycles) * 0.9)] if cycles else 0.0
                            issue_type_metrics_rows.append(
                                IssueTypeMetricsRecord(
                                    repo_id=r_id if r_id.int != 0 else None,
                                    day=day,
                                    provider=prov,
                                    team_id=team_id,
                                    issue_type_norm=norm_type,
                                    created_count=stat["created"],
                                    completed_count=stat["completed"],
                                    active_count=stat["active"],
                                    cycle_p50_hours=p50,
                                    cycle_p90_hours=p90,
                                    lead_p50_hours=0.0,
                                    computed_at=computed_at,
                                )
                            )

                        for c in day_commit_stats:
                            r_id = c["repo_id"]
                            path = c["file_path"]
                            if not path:
                                continue
                            cls = investment_classifier.classify({
                                "paths": [path],
                                "labels": [],
                                "component": "",
                            })
                            author_email = c["author_email"] or ""
                            t_id, _ = team_resolver.resolve(author_email)
                            team_id = _normalize_investment_team_id(t_id)
                            key = (
                                r_id,
                                team_id,
                                cls.investment_area,
                                cls.project_stream or "",
                            )
                            if key not in inv_metrics_map:
                                inv_metrics_map[key] = {
                                    "units": 0,
                                    "completed": 0,
                                    "churn": 0,
                                    "cycles": [],
                                }
                            inv_metrics_map[key]["churn"] += (
                                c["additions"] + c["deletions"]
                            )

                        for (
                            r_id,
                            team_id,
                            area,
                            stream,
                        ), data in inv_metrics_map.items():
                            cycles = sorted(data["cycles"])
                            p50 = cycles[len(cycles) // 2] if cycles else 0.0
                            investment_metrics_rows.append(
                                InvestmentMetricsRecord(
                                    repo_id=r_id if r_id.int != 0 else None,
                                    day=day,
                                    team_id=team_id,
                                    investment_area=area,
                                    project_stream=stream,
                                    delivery_units=data["units"],
                                    work_items_completed=data["completed"],
                                    prs_merged=0,
                                    churn_loc=data["churn"],
                                    cycle_p50_hours=p50,
                                    computed_at=computed_at,
                                )
                            )

                        file_metrics = compute_file_hotspots(
                            repo_id=repo.id,
                            day=day,
                            window_stats=window_stats,
                            computed_at=computed_at,
                        )
                        if file_metrics:
                            sink.write_file_metrics(file_metrics)

                        risk_hotspots = compute_file_risk_hotspots(
                            repo_id=repo.id,
                            day=day,
                            window_stats=window_stats,
                            complexity_map=complexity_by_day.get(day, {}),
                            blame_map=blame_concentration,
                            computed_at=computed_at,
                        )
                        if risk_hotspots and hasattr(sink, "write_file_hotspot_daily"):
                            sink.write_file_hotspot_daily(risk_hotspots)

                        review_edges = compute_review_edges_daily(
                            day=day,
                            pull_request_rows=pull_request_rows,
                            pull_request_review_rows=review_rows,
                            computed_at=computed_at,
                        )
                        if review_edges:
                            sink.write_review_edges(review_edges)

                        if repo_result.repo_metrics:
                            sink.write_repo_metrics(repo_result.repo_metrics)
                        if repo_result.user_metrics:
                            sink.write_user_metrics(repo_result.user_metrics)
                        if repo_result.commit_metrics:
                            sink.write_commit_metrics(repo_result.commit_metrics)
                        if team_metrics:
                            sink.write_team_metrics(team_metrics)
                        if (
                            hasattr(sink, "write_issue_type_metrics")
                            and issue_type_metrics_rows
                        ):
                            sink.write_issue_type_metrics(issue_type_metrics_rows)
                        if (
                            hasattr(sink, "write_investment_classifications")
                            and investment_classifications
                        ):
                            sink.write_investment_classifications(
                                investment_classifications
                            )
                        if (
                            hasattr(sink, "write_investment_metrics")
                            and investment_metrics_rows
                        ):
                            sink.write_investment_metrics(investment_metrics_rows)

                        cicd_metrics = compute_cicd_metrics_daily(
                            day=day,
                            pipeline_runs=pipeline_rows,
                            computed_at=computed_at,
                        )
                        if cicd_metrics:
                            sink.write_cicd_metrics(cicd_metrics)

                        deploy_metrics = compute_deploy_metrics_daily(
                            day=day,
                            deployments=deployment_rows,
                            computed_at=computed_at,
                        )
                        if deploy_metrics:
                            sink.write_deploy_metrics(deploy_metrics)

                        incident_metrics = compute_incident_metrics_daily(
                            day=day,
                            incidents=incident_rows,
                            computed_at=computed_at,
                        )
                        if incident_metrics:
                            sink.write_incident_metrics(incident_metrics)

                        # Landscape rolling metrics
                        try:
                            if hasattr(sink, "get_rolling_30d_user_stats") and hasattr(
                                sink, "write_ic_landscape_rolling"
                            ):
                                rolling_stats = sink.get_rolling_30d_user_stats(
                                    as_of_day=day, repo_id=repo.id
                                )
                                landscape_recs = compute_ic_landscape_rolling(
                                    as_of_day=day,
                                    rolling_stats=rolling_stats,
                                    team_map=team_map,
                                )
                                sink.write_ic_landscape_rolling(landscape_recs)
                        except Exception as e:
                            logging.warning(
                                "Failed to compute/write fixture landscape metrics: %s",
                                e,
                            )

                    logging.info("Generated fixtures metrics for %s", r_name)

    asyncio.run(_run_with_store(ns.db, db_type, _handler))
    if ns.with_work_graph:
        if db_type != "clickhouse":
            logging.warning("Skipping work graph build (requires ClickHouse).")
        else:
            from work_graph.builder import BuildConfig, WorkGraphBuilder

            now = datetime.now(timezone.utc)
            config = BuildConfig(
                dsn=ns.db,
                from_date=now - timedelta(days=ns.days),
                to_date=now,
            )
            builder = WorkGraphBuilder(config)
            try:
                result = builder.build()
                total_edges = sum(result.values())
                if total_edges == 0:
                    logging.error("FAIL: Work graph build produced zero edges.")
                    return 1

                client = getattr(builder, "client", None)
                if client is None:
                    logging.error(
                        "FAIL: Work graph builder did not expose a ClickHouse client."
                    )
                    return 1

                edge_count = client.query(
                    "SELECT count() FROM work_graph_edges"
                ).result_rows[0][0]
                if int(edge_count or 0) == 0:
                    logging.error(
                        "FAIL: work_graph_edges is empty after fixture build."
                    )
                    return 1

                nodes = client.query(
                    """
                    SELECT count(DISTINCT id) FROM (
                        SELECT source_id as id FROM work_graph_edges
                        UNION ALL
                        SELECT target_id as id FROM work_graph_edges
                    )
                    """
                ).result_rows[0][0]
                if int(nodes or 0) <= 1:
                    logging.error("FAIL: Work graph is degenerate (nodes<=1).")
                    return 1

                logging.info("Built work graph edges for fixtures.")
            finally:
                builder.close()
    return 0


def _cmd_api(ns: argparse.Namespace) -> int:
    import uvicorn

    if ns.db:
        os.environ["DATABASE_URI"] = ns.db

    log_level = str(getattr(ns, "log_level", "") or "INFO").upper()
    log_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {"format": "%(asctime)s %(levelname)s %(name)s: %(message)s"},
            "access": {"format": "%(message)s"},
        },
        "handlers": {
            "default": {
                "class": "logging.StreamHandler",
                "formatter": "default",
                "stream": "ext://sys.stdout",
            },
            "access": {
                "class": "logging.StreamHandler",
                "formatter": "access",
                "stream": "ext://sys.stdout",
            },
        },
        "loggers": {
            "uvicorn": {
                "handlers": ["default"],
                "level": log_level,
                "propagate": False,
            },
            "uvicorn.error": {
                "handlers": ["default"],
                "level": log_level,
                "propagate": False,
            },
            "uvicorn.access": {
                "handlers": ["access"],
                "level": log_level,
                "propagate": False,
            },
        },
        "root": {"handlers": ["default"], "level": log_level},
    }

    uvicorn.run(
        "api.main:app",
        host=ns.host,
        port=ns.port,
        reload=ns.reload,
        log_level=log_level.lower(),
        access_log=True,
        log_config=log_config,
    )
    return 0


def _cmd_work_graph_build(ns: argparse.Namespace) -> int:
    """Build work graph edges from raw data."""
    from datetime import datetime, timedelta, timezone

    from work_graph.builder import BuildConfig, WorkGraphBuilder

    def _component_count(edges: list[tuple[str, str, str, str]]) -> int:
        adjacency: dict[tuple[str, str], list[tuple[str, str]]] = {}
        for source_type, source_id, target_type, target_id in edges:
            source = (str(source_type), str(source_id))
            target = (str(target_type), str(target_id))
            adjacency.setdefault(source, []).append(target)
            adjacency.setdefault(target, []).append(source)

        visited: set[tuple[str, str]] = set()
        components = 0
        for node in adjacency:
            if node in visited:
                continue
            components += 1
            stack = [node]
            visited.add(node)
            while stack:
                current = stack.pop()
                for neighbor in adjacency.get(current, []):
                    if neighbor in visited:
                        continue
                    visited.add(neighbor)
                    stack.append(neighbor)
        return components

    # Parse dates
    from_date = None
    to_date = None

    if ns.from_date:
        from_date = datetime.fromisoformat(ns.from_date).replace(tzinfo=timezone.utc)
    else:
        from_date = datetime.now(timezone.utc) - timedelta(days=30)

    if ns.to_date:
        to_date = datetime.fromisoformat(ns.to_date).replace(tzinfo=timezone.utc)
    else:
        to_date = datetime.now(timezone.utc)

    # Parse repo_id if provided
    repo_id = None
    if ns.repo_id:
        import uuid

        repo_id = uuid.UUID(ns.repo_id)

    config = BuildConfig(
        dsn=ns.db,
        from_date=from_date,
        to_date=to_date,
        repo_id=repo_id,
        heuristic_days_window=ns.heuristic_window,
        heuristic_confidence=ns.heuristic_confidence,
    )

    builder = WorkGraphBuilder(config)
    try:
        result = builder.build()

        total_edges = sum(result.values())
        logging.info("Work graph build complete. Total edges: %d", total_edges)
        logging.info("  issue_issue_edges: %d", result.get("issue_issue_edges", 0))
        logging.info("  issue_pr_edges: %d", result.get("issue_pr_edges", 0))
        logging.info("  pr_commit_edges: %d", result.get("pr_commit_edges", 0))
        logging.info("  commit_file_edges: %d", result.get("commit_file_edges", 0))
        logging.info("  heuristic_edges: %d", result.get("heuristic_edges", 0))

        client = getattr(builder, "client", None)
        if client is None:
            logging.error(
                "FAIL: Work graph builder did not expose a ClickHouse client."
            )
            return 1

        where_parts = [
            f"event_ts >= '{from_date.strftime('%Y-%m-%d %H:%M:%S')}'",
            f"event_ts <= '{to_date.strftime('%Y-%m-%d %H:%M:%S')}'",
        ]
        if repo_id:
            where_parts.append(f"repo_id = '{repo_id}'")
        where_sql = " AND ".join(where_parts)

        edge_count = client.query(
            f"SELECT count() FROM work_graph_edges WHERE {where_sql}"
        ).result_rows[0][0]
        if int(edge_count or 0) == 0:
            # Debug logging
            try:
                bounds = client.query(
                    "SELECT min(event_ts), max(event_ts) FROM work_graph_edges"
                ).result_rows[0]
                logging.info(
                    f"DEBUG: work_graph_edges event_ts range: {bounds[0]} to {bounds[1]}"
                )
                logging.info(f"DEBUG: Filter window: {from_date} to {to_date}")
            except Exception as e:
                logging.debug(
                    "DEBUG: Failed to query work_graph_edges bounds for diagnostics: %s",
                    e,
                )

            logging.error(
                "FAIL: work_graph_edges is empty for the selected window. "
                "Prerequisites missing or build produced no edges."
            )
            return 1

        edge_rows = (
            client.query(
                f"""
            SELECT source_type, source_id, target_type, target_id
            FROM work_graph_edges
            WHERE {where_sql}
            """
            ).result_rows
            or []
        )
        components = _component_count(edge_rows)
        if components == 1 and not ns.allow_degenerate:
            logging.error(
                "FAIL: Work graph is degenerate (connected_components=1). "
                "Re-run with --allow-degenerate to override."
            )
            return 1

        logging.info("Connected components in window: %d", components)
        return 0
    finally:
        builder.close()


def _cmd_investment_materialize(ns: argparse.Namespace) -> int:
    from datetime import datetime, time, timedelta, timezone

    from work_graph.investment.materialize import (
        MaterializeConfig,
        materialize_investments,
    )

    now = datetime.now(timezone.utc)
    if ns.to_date:
        to_day = date.fromisoformat(ns.to_date)
        to_ts = datetime.combine(
            to_day + timedelta(days=1), time.min, tzinfo=timezone.utc
        )
    else:
        to_ts = now

    if ns.from_date:
        from_day = date.fromisoformat(ns.from_date)
        from_ts = datetime.combine(from_day, time.min, tzinfo=timezone.utc)
    else:
        window_days = max(1, int(ns.window_days or 30))
        from_ts = to_ts - timedelta(days=window_days)

    if from_ts >= to_ts:
        logging.error("--from must be before --to")
        return 2

    repo_ids = [repo_id for repo_id in (ns.repo_id or []) if repo_id]
    team_ids = [team_id for team_id in (ns.team_id or []) if team_id]

    config = MaterializeConfig(
        dsn=ns.db,
        from_ts=from_ts,
        to_ts=to_ts,
        repo_ids=repo_ids or None,
        llm_provider=ns.llm_provider,
        persist_evidence_snippets=ns.persist_evidence_snippets,
        llm_model=ns.model,
        team_ids=team_ids or None,
    )

    stats = asyncio.run(materialize_investments(config))
    logging.info(
        "Investment materialization complete. Components=%d Records=%d Quotes=%d",
        stats.get("components", 0),
        stats.get("records", 0),
        stats.get("quotes", 0),
    )
    return 0


def _cmd_fixtures_validate(ns: argparse.Namespace) -> int:
    """Validate that fixture data is sufficient for work graph and investment."""
    import clickhouse_connect
    from work_graph.investment.constants import MIN_EVIDENCE_CHARS
    from work_graph.investment.evidence import build_text_bundle
    from work_graph.investment.queries import (
        fetch_commits,
        fetch_parent_titles,
        fetch_pull_requests,
        fetch_work_graph_edges,
        fetch_work_items,
    )
    from work_graph.ids import parse_commit_from_id, parse_pr_from_id

    db_url = ns.db
    if not db_url.startswith("clickhouse://"):
        logging.error("Validation only supported for ClickHouse currently.")
        return 1

    try:
        client = clickhouse_connect.get_client(dsn=db_url)
    except Exception as e:
        logging.error(f"Failed to connect to DB: {e}")
        return 1

    logging.info("Running fixture validation...")

    # 1. Check raw data counts
    try:
        wi_count = int(client.query("SELECT count() FROM work_items").result_rows[0][0])
        non_epic_wi_count = int(
            client.query(
                "SELECT count() FROM work_items WHERE type != 'epic'"
            ).result_rows[0][0]
        )
        pr_count = int(
            client.query("SELECT count() FROM git_pull_requests").result_rows[0][0]
        )
        commit_count = int(
            client.query("SELECT count() FROM git_commits").result_rows[0][0]
        )
        logging.info(
            f"Raw Counts: WI={wi_count}, PR={pr_count}, Commits={commit_count}"
        )

        if wi_count < 10 or pr_count < 5 or commit_count < 20:
            logging.error("FAIL: Insufficient raw data.")
            return 1
    except Exception as e:
        logging.error(f"FAIL: Could not query raw tables: {e}")
        return 1

    # 2. Check prerequisites
    try:
        pr_commit_count = int(
            client.query("SELECT count() FROM work_graph_pr_commit").result_rows[0][0]
        )
        issue_pr_count = int(
            client.query("SELECT count() FROM work_graph_issue_pr").result_rows[0][0]
        )
        logging.info(
            f"Prereqs: work_graph_pr_commit={pr_commit_count}, work_graph_issue_pr={issue_pr_count}"
        )
        if pr_commit_count == 0:
            logging.error(
                "FAIL: work_graph_pr_commit is empty (fixtures missing PR->commit prerequisites)."
            )
            return 1
        if issue_pr_count == 0:
            logging.error(
                "FAIL: work_graph_issue_pr is empty (fixtures missing issue->PR prerequisites)."
            )
            return 1

        linked_non_epic = int(
            client.query(
                """
                SELECT count(DISTINCT wi.repo_id, wi.work_item_id)
                FROM work_items wi
                INNER JOIN work_graph_issue_pr l
                  ON wi.repo_id = l.repo_id AND wi.work_item_id = l.work_item_id
                WHERE wi.type != 'epic'
                """
            ).result_rows[0][0]
        )
        coverage = (linked_non_epic / non_epic_wi_count) if non_epic_wi_count else 0.0
        if coverage < 0.7:
            logging.error(
                "FAIL: Issue->PR coverage too low (linked=%.1f%%, target>=70%%).",
                coverage * 100.0,
            )
            return 1

        prs_with_commits = int(
            client.query(
                "SELECT count(DISTINCT repo_id, pr_number) FROM work_graph_pr_commit"
            ).result_rows[0][0]
        )
        if prs_with_commits < pr_count:
            logging.error(
                "FAIL: Not all PRs have commits in work_graph_pr_commit (prs_with_commits=%d, prs=%d).",
                prs_with_commits,
                pr_count,
            )
            return 1
    except Exception as e:
        logging.error(f"FAIL: Could not validate prerequisites: {e}")
        return 1

    # 3. Check work_graph_edges + components (Python, deterministic)
    try:
        edges = fetch_work_graph_edges(client)
        if not edges:
            logging.error(
                "FAIL: work_graph_edges is empty (run `cli.py work-graph build`)."
            )
            return 1

        adjacency: dict[tuple[str, str], list[tuple[str, str]]] = {}
        for edge in edges:
            source = (str(edge.get("source_type")), str(edge.get("source_id")))
            target = (str(edge.get("target_type")), str(edge.get("target_id")))
            adjacency.setdefault(source, []).append(target)
            adjacency.setdefault(target, []).append(source)

        visited: set[tuple[str, str]] = set()
        components: list[list[tuple[str, str]]] = []
        for node in adjacency:
            if node in visited:
                continue
            stack = [node]
            visited.add(node)
            group: list[tuple[str, str]] = []
            while stack:
                current = stack.pop()
                group.append(current)
                for neighbor in adjacency.get(current, []):
                    if neighbor in visited:
                        continue
                    visited.add(neighbor)
                    stack.append(neighbor)
            components.append(group)

        component_count = len(components)
        min_components = min(50, max(2, non_epic_wi_count // 2))
        logging.info(
            "WorkUnits (connected components): %d (min_required=%d)",
            component_count,
            min_components,
        )
        if component_count < min_components:
            logging.error(
                "FAIL: WorkUnits too low (components=%d, required>=%d).",
                component_count,
                min_components,
            )
            return 1

    except Exception as e:
        logging.error(f"FAIL: Could not validate work graph edges/components: {e}")
        return 1

    # 4. Evidence sanity (sample bundles)
    try:
        sample_needed = 10
        eligible = []
        for node_list in components:
            has_issue = any(nt == "issue" for nt, _ in node_list)
            has_pr = any(nt == "pr" for nt, _ in node_list)
            if has_issue and has_pr:
                eligible.append(node_list)
            if len(eligible) >= sample_needed:
                break

        if not eligible:
            logging.error(
                "FAIL: No WorkUnits with both issues and PRs; evidence bundles will be empty."
            )
            return 1

        for idx, node_list in enumerate(eligible, start=1):
            issue_ids = [
                node_id for node_type, node_id in node_list if node_type == "issue"
            ]
            pr_ids = [node_id for node_type, node_id in node_list if node_type == "pr"]
            commit_ids = [
                node_id for node_type, node_id in node_list if node_type == "commit"
            ]

            work_items = fetch_work_items(client, work_item_ids=issue_ids)
            work_item_map = {
                str(item.get("work_item_id")): item
                for item in work_items
                if item.get("work_item_id")
            }

            pr_repo_numbers: dict[str, list[int]] = {}
            for pr_id in pr_ids:
                repo_id, number = parse_pr_from_id(pr_id)
                if repo_id and number is not None:
                    pr_repo_numbers.setdefault(str(repo_id), []).append(int(number))
            prs = fetch_pull_requests(client, repo_numbers=pr_repo_numbers)
            pr_map: dict[str, dict[str, object]] = {}
            for pr in prs:
                repo = str(pr.get("repo_id") or "")
                number = pr.get("number")
                if repo and number is not None:
                    pr_map[f"{repo}#pr{int(number)}"] = pr

            commit_repo_hashes: dict[str, list[str]] = {}
            for commit_id in commit_ids:
                repo_id, commit_hash = parse_commit_from_id(commit_id)
                if repo_id and commit_hash:
                    commit_repo_hashes.setdefault(str(repo_id), []).append(
                        str(commit_hash)
                    )
            commits = fetch_commits(client, repo_commits=commit_repo_hashes)
            commit_map: dict[str, dict[str, object]] = {}
            for commit in commits:
                repo = str(commit.get("repo_id") or "")
                commit_hash = str(commit.get("hash") or "")
                if repo and commit_hash:
                    commit_map[f"{repo}@{commit_hash}"] = commit

            parent_ids = {
                str(item.get("parent_id") or "")
                for item in work_items
                if item.get("parent_id")
            }
            epic_ids = {
                str(item.get("epic_id") or "")
                for item in work_items
                if item.get("epic_id")
            }
            parent_titles = fetch_parent_titles(client, work_item_ids=parent_ids)
            epic_titles = fetch_parent_titles(client, work_item_ids=epic_ids)

            bundle = build_text_bundle(
                issue_ids=issue_ids,
                pr_ids=pr_ids,
                commit_ids=commit_ids,
                work_item_map=work_item_map,
                pr_map=pr_map,
                commit_map=commit_map,
                parent_titles=parent_titles,
                epic_titles=epic_titles,
                work_unit_id=f"validate:{idx}",
            )
            if bundle.text_char_count < MIN_EVIDENCE_CHARS:
                logging.error(
                    "FAIL: Evidence bundle too small for WorkUnit sample %d (chars=%d, required>=%d).",
                    idx,
                    bundle.text_char_count,
                    MIN_EVIDENCE_CHARS,
                )
                return 1

        logging.info(
            "Evidence sanity: PASS (sampled %d work units, min_chars=%d)",
            len(eligible),
            MIN_EVIDENCE_CHARS,
        )
    except Exception as e:
        logging.error(f"FAIL: Evidence sanity check failed: {e}")
        return 1

    logging.info("PASS: Fixture validation successful.")
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    if os.getenv("DISABLE_DOTENV", "").strip().lower() not in {
        "1",
        "true",
        "yes",
        "on",
    }:
        _load_dotenv(REPO_ROOT / ".env")

    parser = build_parser()
    ns = parser.parse_args(argv)

    level_name = str(getattr(ns, "log_level", "") or "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    func = getattr(ns, "func", None)
    if func is None:
        parser.print_help()
        return 2
    return int(func(ns))


if __name__ == "__main__":
    raise SystemExit(main())
