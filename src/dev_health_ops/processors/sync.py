import argparse
import asyncio
import os

from dev_health_ops.storage import resolve_db_type, run_with_store
from dev_health_ops.utils import _resolve_since, _resolve_max_commits
from dev_health_ops.processors.local import process_local_blame, process_local_repo
from dev_health_ops.processors.github import (
    process_github_repo,
    process_github_repos_batch,
)
from dev_health_ops.processors.gitlab import (
    process_gitlab_project,
    process_gitlab_projects_batch,
)


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


async def sync_local_target(ns: argparse.Namespace, target: str) -> int:
    if target not in {"git", "prs", "blame"}:
        raise SystemExit("Local provider supports only git, prs, or blame targets.")

    db_type = resolve_db_type(ns.db, ns.db_type)
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

    await run_with_store(ns.db, db_type, _handler)
    return 0


async def sync_github_target(ns: argparse.Namespace, target: str) -> int:
    token = ns.auth or os.getenv("GITHUB_TOKEN") or ""
    if not token:
        raise SystemExit("Missing GitHub token (pass --auth or set GITHUB_TOKEN).")

    db_type = resolve_db_type(ns.db, ns.db_type)
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

    await run_with_store(ns.db, db_type, _handler)
    return 0


async def sync_gitlab_target(ns: argparse.Namespace, target: str) -> int:
    token = ns.auth or os.getenv("GITLAB_TOKEN") or ""
    if not token:
        raise SystemExit("Missing GitLab token (pass --auth or set GITLAB_TOKEN).")

    db_type = resolve_db_type(ns.db, ns.db_type)
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

    await run_with_store(ns.db, db_type, _handler)
    return 0


async def sync_synthetic_target(ns: argparse.Namespace, target: str) -> int:
    from dev_health_ops.fixtures.generator import SyntheticDataGenerator

    repo_name = _resolve_synthetic_repo_name(ns)
    db_type = resolve_db_type(ns.db, ns.db_type)
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

    await run_with_store(ns.db, db_type, _handler)
    return 0


def run_sync_target(ns: argparse.Namespace) -> int:
    target = ns.sync_target
    provider = (ns.provider or "").lower()
    if provider not in {"local", "github", "gitlab", "synthetic"}:
        raise SystemExit("Provider must be one of: local, github, gitlab, synthetic.")

    if target not in {"git", "prs", "blame", "cicd", "deployments", "incidents"}:
        raise SystemExit(
            "Sync target must be git, prs, blame, cicd, deployments, or incidents."
        )

    if provider == "local":
        return asyncio.run(sync_local_target(ns, target))
    if provider == "github":
        return asyncio.run(sync_github_target(ns, target))
    if provider == "gitlab":
        return asyncio.run(sync_gitlab_target(ns, target))
    return asyncio.run(sync_synthetic_target(ns, target))


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
        help="Target day (UTC) as YYYY-MM-DD (use with --backfill).",
    )
    parser.add_argument(
        "--backfill",
        type=int,
        default=1,
        help="Sync N days ending at --date (inclusive). Requires --date.",
    )


def register_commands(subparsers: argparse._SubParsersAction) -> None:
    target_parsers = {
        "git": "Sync commits and commit stats.",
        "prs": "Sync pull/merge requests.",
        "blame": "Sync blame data only.",
        "cicd": "Sync CI/CD runs and pipelines.",
        "deployments": "Sync deployments.",
        "incidents": "Sync incidents.",
    }

    for target, help_text in target_parsers.items():
        target_parser = subparsers.add_parser(target, help=help_text)
        _add_sync_target_args(target_parser)
        target_parser.set_defaults(func=run_sync_target, sync_target=target)

    # Note: 'teams' and 'work-items' are also sync subcommands but handled in their own modules.
