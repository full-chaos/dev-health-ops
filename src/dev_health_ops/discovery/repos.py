from __future__ import annotations

import fnmatch
from typing import Any


def discover_repos_for_config(
    config, credentials: dict[str, Any]
) -> list[tuple[str, ...]]:
    provider = (config.provider or "").lower()
    sync_options = dict(config.sync_options or {})

    if provider == "github":
        from dev_health_ops.credentials.resolver import github_credentials_from_mapping

        gh_credentials = github_credentials_from_mapping(credentials)
        if (
            gh_credentials is not None
            and gh_credentials.is_app_auth
            and sync_options.get("all_repos") is True
        ):
            token = ""
        else:
            token = _github_token_from_resolved_credentials(gh_credentials)
        # No token => attempt anonymous public-repo discovery (rate-limited,
        # public-only). Authenticated configs pass their token through.
        return discover_github_repos(
            sync_options, token, github_credentials=gh_credentials
        )
    if provider == "gitlab":
        from dev_health_ops.credentials.resolver import (
            gitlab_credentials_from_mapping,
            resolve_gitlab_url,
        )

        gl_credentials = gitlab_credentials_from_mapping(credentials)
        token = gl_credentials.token if gl_credentials is not None else ""
        gitlab_url = resolve_gitlab_url(sync_options, gl_credentials)
        return discover_gitlab_repos(sync_options, token, gitlab_url=gitlab_url)
    return []


def _github_token_from_credentials(credentials: dict[str, Any]) -> str:
    """Resolve a usable GitHub token from a credentials mapping.

    Supports both PAT (``token``) and GitHub App auth; for App auth an
    installation token is minted via ``providers.github.app_auth`` (no
    dependency on the ``connectors`` package).
    """
    from dev_health_ops.credentials.resolver import github_credentials_from_mapping

    gh_credentials = github_credentials_from_mapping(credentials)
    return _github_token_from_resolved_credentials(gh_credentials)


def _github_token_from_resolved_credentials(gh_credentials: Any | None) -> str:
    if gh_credentials is None:
        return ""
    if gh_credentials.is_app_auth:
        from dev_health_ops.providers.github.app_auth import mint_installation_token

        return mint_installation_token(
            app_id=gh_credentials.app_id,
            private_key=gh_credentials.private_key,
            installation_id=gh_credentials.installation_id,
            base_url=gh_credentials.base_url,
        )
    return gh_credentials.token or ""


def discover_github_repos(
    sync_options: dict[str, Any], token: str, github_credentials: Any | None = None
) -> list[tuple[str, ...]]:
    from github import Github

    search = sync_options.get("search", "")
    owner = sync_options.get("owner", "")
    all_repos = sync_options.get("all_repos") is True
    namespace = owner.strip() if all_repos and isinstance(owner, str) else ""

    if all_repos and isinstance(search, str) and search.strip():
        if "/" in search:
            parts = search.split("/", 1)
            if not namespace:
                namespace = parts[0].strip()
            repo_pattern = parts[1]
        else:
            repo_pattern = search.strip()
    elif isinstance(search, str) and "/" in search:
        parts = search.split("/", 1)
        owner = parts[0]
        repo_pattern = parts[1]
    elif isinstance(search, str) and search.strip() and not owner:
        owner = search.strip()
        repo_pattern = "*"
    else:
        repo_pattern = "*"

    if all_repos:
        if getattr(github_credentials, "is_app_auth", False):
            return _discover_github_app_installation_repos(
                github_credentials, namespace=namespace, repo_pattern=repo_pattern
            )

        g = Github(token) if token else Github()
        repos = g.get_user().get_repos()

        result: list[tuple[str, ...]] = []
        for repo in repos:
            repo_name = getattr(repo, "name", "") or ""
            if not fnmatch.fnmatch(repo_name, repo_pattern):
                continue
            repo_owner = getattr(getattr(repo, "owner", None), "login", "") or ""
            if not repo_owner:
                full_name = getattr(repo, "full_name", "") or ""
                if "/" in full_name:
                    repo_owner = full_name.split("/", 1)[0]
            if namespace and repo_owner.lower() != namespace.lower():
                continue
            if repo_owner:
                result.append((repo_owner, repo_name))

        return result

    if not owner:
        return []

    g = Github(token) if token else Github()
    try:
        org = g.get_organization(owner)
        repos = org.get_repos()
    except Exception:
        try:
            user = g.get_user(owner)
            repos = user.get_repos()
        except Exception:
            return []

    result = []
    for repo in repos:
        if fnmatch.fnmatch(repo.name, repo_pattern):
            result.append((owner, repo.name))

    return result


def _discover_github_app_installation_repos(
    github_credentials: Any, *, namespace: str, repo_pattern: str
) -> list[tuple[str, ...]]:
    import requests

    from dev_health_ops.providers.github.app_auth import mint_installation_token

    app_id = getattr(github_credentials, "app_id", None)
    private_key = getattr(github_credentials, "private_key", None)
    installation_id = getattr(github_credentials, "installation_id", None)
    if not app_id or not private_key or not installation_id:
        raise ValueError(
            "GitHub App credentials require app_id, private_key, and installation_id"
        )

    base_url = str(
        getattr(github_credentials, "base_url", None) or "https://api.github.com"
    ).rstrip("/")
    token = mint_installation_token(
        app_id=str(app_id),
        private_key=str(private_key),
        installation_id=str(installation_id),
        base_url=base_url,
    )

    result: list[tuple[str, ...]] = []
    page = 1
    per_page = 100
    while True:
        response = requests.get(
            f"{base_url}/installation/repositories",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            params={"per_page": per_page, "page": page},
            timeout=30,
        )
        if response.status_code < 200 or response.status_code >= 300:
            raise RuntimeError(
                "GitHub App installation repositories request failed: "
                f"HTTP {response.status_code}"
            )

        repositories = response.json().get("repositories") or []
        for repo in repositories:
            repo_name = str(repo.get("name") or "")
            if not fnmatch.fnmatch(repo_name, repo_pattern):
                continue
            owner = repo.get("owner") or {}
            repo_owner = str(owner.get("login") or "")
            if not repo_owner:
                full_name = str(repo.get("full_name") or "")
                if "/" in full_name:
                    repo_owner = full_name.split("/", 1)[0]
            if namespace and repo_owner.lower() != namespace.lower():
                continue
            if repo_owner:
                result.append((repo_owner, repo_name))

        if len(repositories) < per_page:
            break
        page += 1

    return result


def discover_gitlab_repos(
    sync_options: dict[str, Any],
    token: str,
    gitlab_url: str | None = None,
) -> list[tuple[str, ...]]:
    import gitlab as gitlab_lib

    if not gitlab_url:
        gitlab_url = str(sync_options.get("gitlab_url") or "https://gitlab.com")
    search = sync_options.get("search", "")
    group_path = sync_options.get("group", "")
    owner = sync_options.get("owner", "")
    all_repos = sync_options.get("all_repos") is True
    namespace = ""
    if all_repos:
        if isinstance(group_path, str) and group_path.strip():
            namespace = group_path.strip()
        elif isinstance(owner, str) and owner.strip():
            namespace = owner.strip()

    if all_repos and isinstance(search, str) and search.strip():
        if "/" in search:
            # Split on the LAST slash so nested GitLab namespaces (e.g.
            # "group/subgroup/*") resolve namespace="group/subgroup",
            # pattern="*" instead of treating "subgroup/*" as a project name
            # pattern (project names contain no slashes).
            ns_part, _, pattern_part = search.rpartition("/")
            if not namespace:
                namespace = ns_part.strip()
            project_pattern = pattern_part
        else:
            project_pattern = search.strip()
    elif isinstance(search, str) and "/" in search:
        parts = search.split("/", 1)
        group_path = parts[0]
        project_pattern = parts[1]
    elif isinstance(search, str) and search.strip() and not group_path:
        group_path = search.strip()
        project_pattern = "*"
    else:
        project_pattern = "*"

    gl = gitlab_lib.Gitlab(gitlab_url, private_token=token)
    if all_repos:
        projects = gl.projects.list(all=True, membership=True)

        result: list[tuple[str, ...]] = []
        for project in projects:
            name = getattr(project, "name", "") or ""
            project_id = getattr(project, "id", None)
            path_with_namespace = getattr(project, "path_with_namespace", "") or ""
            normalized_path = path_with_namespace.lower()
            normalized_namespace = namespace.lower()
            if normalized_namespace and not (
                normalized_path == normalized_namespace
                or normalized_path.startswith(f"{normalized_namespace}/")
            ):
                continue
            if project_id is not None and fnmatch.fnmatch(name, project_pattern):
                result.append((str(project_id), path_with_namespace))

        return result

    if not group_path:
        return []

    try:
        grp = gl.groups.get(group_path)
        group_projects = grp.projects.list(all=True)
    except Exception:
        return []

    result = []
    for group_project in group_projects:
        name = getattr(group_project, "name", "") or ""
        project_id = getattr(group_project, "id", None)
        if project_id is not None and fnmatch.fnmatch(name, project_pattern):
            # Prefer the canonical path_with_namespace, then the URL *path*
            # slug. The display name can differ from the slug stored as the
            # repo full_name downstream, so it is only a last resort for
            # objects that expose neither path_with_namespace nor path. Leave
            # empty if nothing usable is available; callers guard
            # against an empty scope.
            path_with_namespace = (
                getattr(group_project, "path_with_namespace", "") or ""
            )
            if not path_with_namespace:
                slug = getattr(group_project, "path", "") or name
                path_with_namespace = f"{group_path}/{slug}" if slug else ""
            result.append((str(project_id), path_with_namespace))

    return result
