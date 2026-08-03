"""Pure GitLab repository-row construction shared by production and parity."""

from __future__ import annotations

from typing import Any

from dev_health_ops.providers.gitlab.instance import normalize_gitlab_instance


def build_gitlab_repository_values(
    project: object,
    gitlab_url: str,
    *,
    batch_processed: bool = False,
) -> dict[str, Any]:
    """Build the exact kwargs used to construct the persisted ``Repo`` model."""
    full_name = (
        getattr(project, "path_with_namespace", None)
        or getattr(project, "full_name", None)
        or getattr(project, "name", None)
    )
    project_id = getattr(project, "id", None)
    if not full_name or project_id is None:
        raise ValueError("GitLab project requires id and path_with_namespace")
    settings: dict[str, Any] = {
        "source": "gitlab",
        "project_id": project_id,
        "url": getattr(project, "web_url", None) or getattr(project, "url", None),
        "default_branch": getattr(project, "default_branch", "main") or "main",
    }
    if batch_processed:
        settings["batch_processed"] = True
    instance = normalize_gitlab_instance(gitlab_url)
    if instance is not None:
        settings["gitlab_instance_url"] = instance
    return {
        "repo": str(full_name),
        "provider": "gitlab",
        "settings": settings,
        "tags": ["gitlab"],
    }
