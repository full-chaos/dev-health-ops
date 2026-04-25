import argparse
import asyncio
import os

from dev_health_ops.credentials import (
    CredentialResolutionError,
    CredentialSource,
    GitHubCredentials,
    resolve_credentials_sync,
)
from dev_health_ops.db import resolve_sink_uri
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
from dev_health_ops.storage import detect_db_type, run_with_store
from dev_health_ops.utils.cli import (
    add_date_range_args,
    add_sink_arg,
    resolve_date_range,
    resolve_max_commits,
    resolve_since_datetime,
    validate_sink,
)


def _sync_flags_for_target(target: str) -> dict:
    return {
        "sync_git": target == "git",
        "sync_prs": target == "prs",
        "sync_cicd": target == "cicd",
        "sync_deployments": target == "deployments",
        "sync_incidents": target == "incidents",
        "sync_security": target == "security",
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
        token=ns.auth,
        app_id=ns.github_app_id,
        private_key_path=ns.github_app_key_path,
        installation_id=ns.github_app_installation_id,
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
                "blame_only": flags["blame_only"],
                "backfill_missing": False,
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
                "blame_only": flags["blame_only"],
                "backfill_missing": False,
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
            since=since,
        )

    await run_with_store(db_uri, db_type, _handler, org_id=getattr(ns, "org", None))
    return 0


async def sync_synthetic_target(ns: argparse.Namespace, target: str) -> int:
    from dev_health_ops.fixtures.generator import SyntheticDataGenerator

    repo_name = _resolve_synthetic_repo_name(ns)
    db_uri = resolve_sink_uri(ns)
    validate_sink(ns)
    db_type = detect_db_type(db_uri)
    _, backfill_days = resolve_date_range(ns)
    days = backfill_days

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

    await run_with_store(db_uri, db_type, _handler, org_id=getattr(ns, "org", None))
    return 0


def run_sync_target(ns: argparse.Namespace) -> int:
    target = ns.sync_target
    provider = (ns.provider or "").lower()
    if provider not in {"local", "github", "gitlab", "synthetic"}:
        raise SystemExit("Provider must be one of: local, github, gitlab, synthetic.")

    if target not in {
        "git",
        "prs",
        "blame",
        "cicd",
        "deployments",
        "incidents",
        "security",
    }:
        raise SystemExit(
            "Sync target must be git, prs, blame, cicd, deployments, incidents, or security."
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
        "--repo-name", help="Synthetic repo name (default: acme/demo-app)."
    )
    add_date_range_args(parser)


def register_commands(subparsers: argparse._SubParsersAction) -> None:
    target_parsers = {
        "git": "Sync commits and commit stats.",
        "prs": "Sync pull/merge requests.",
        "blame": "Sync blame data only.",
        "cicd": "Sync CI/CD runs and pipelines.",
        "deployments": "Sync deployments.",
        "incidents": "Sync incidents.",
        "security": "Sync security and dependency alerts.",
    }

    for target, help_text in target_parsers.items():
        target_parser = subparsers.add_parser(target, help=help_text)
        _add_sync_target_args(target_parser)
        target_parser.set_defaults(func=run_sync_target, sync_target=target)

    # Note: 'teams' and 'work-items' are also sync subcommands but handled in their own modules.
