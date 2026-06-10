from __future__ import annotations

import fnmatch
from typing import Any


def discover_repos_for_config(
    config, credentials: dict[str, Any]
) -> list[tuple[str, ...]]:
    provider = (config.provider or "").lower()
    sync_options = dict(config.sync_options or {})

    if provider == "github":
        token = _github_token_from_credentials(credentials)
        if not token:
            return []
        return discover_github_repos(sync_options, token)
    if provider == "gitlab":
        token = str(credentials.get("token") or "")
        return discover_gitlab_repos(sync_options, token)
    return []


def _github_token_from_credentials(credentials: dict[str, Any]) -> str:
    """Resolve a usable GitHub token from a credentials mapping.

    Supports both PAT (``token``) and GitHub App auth; for App auth an
    installation token is minted via the GitHub connector.
    """
    from dev_health_ops.credentials.resolver import github_credentials_from_mapping

    gh_credentials = github_credentials_from_mapping(credentials)
    if gh_credentials is None:
        return ""
    if gh_credentials.is_app_auth:
        from dev_health_ops.connectors.github import GitHubConnector

        return GitHubConnector(credentials=gh_credentials).token
    return gh_credentials.token or ""


def discover_github_repos(
    sync_options: dict[str, Any], token: str
) -> list[tuple[str, ...]]:
    from github import Github

    search = sync_options.get("search", "")
    owner = sync_options.get("owner", "")

    if isinstance(search, str) and "/" in search:
        parts = search.split("/", 1)
        owner = parts[0]
        repo_pattern = parts[1]
    else:
        repo_pattern = "*"

    if not owner:
        return []

    g = Github(token)
    try:
        org = g.get_organization(owner)
        repos = org.get_repos()
    except Exception:
        try:
            user = g.get_user(owner)
            repos = user.get_repos()
        except Exception:
            return []

    result: list[tuple[str, ...]] = []
    for repo in repos:
        if fnmatch.fnmatch(repo.name, repo_pattern):
            result.append((owner, repo.name))

    return result


def discover_gitlab_repos(
    sync_options: dict[str, Any], token: str
) -> list[tuple[str, ...]]:
    import gitlab as gitlab_lib

    gitlab_url = str(sync_options.get("gitlab_url", "https://gitlab.com"))
    search = sync_options.get("search", "")
    group_path = sync_options.get("group", "")

    if isinstance(search, str) and "/" in search:
        parts = search.split("/", 1)
        group_path = parts[0]
        project_pattern = parts[1]
    else:
        project_pattern = "*"

    if not group_path:
        return []

    gl = gitlab_lib.Gitlab(gitlab_url, private_token=token)
    try:
        grp = gl.groups.get(group_path)
        projects = grp.projects.list(all=True)
    except Exception:
        return []

    result: list[tuple[str, ...]] = []
    for project in projects:
        name = getattr(project, "name", "") or ""
        project_id = getattr(project, "id", None)
        if project_id is not None and fnmatch.fnmatch(name, project_pattern):
            result.append((str(project_id),))

    return result
