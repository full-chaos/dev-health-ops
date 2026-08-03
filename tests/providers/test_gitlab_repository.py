from types import SimpleNamespace

from dev_health_ops.providers.gitlab.repository import build_gitlab_repository_values


def test_build_gitlab_repository_values_preserves_instance_and_order() -> None:
    values = build_gitlab_repository_values(
        SimpleNamespace(
            id=123,
            path_with_namespace="Acme/API",
            web_url="https://gitlab.example/Acme/API",
            default_branch="main",
        ),
        "HTTPS://GITLAB.EXAMPLE:443/api/v4",
    )

    assert values == {
        "repo": "Acme/API",
        "provider": "gitlab",
        "settings": {
            "source": "gitlab",
            "project_id": 123,
            "url": "https://gitlab.example/Acme/API",
            "default_branch": "main",
            "gitlab_instance_url": "https://gitlab.example",
        },
        "tags": ["gitlab"],
    }


def test_build_gitlab_repository_values_marks_batch_and_defaults_branch() -> None:
    values = build_gitlab_repository_values(
        SimpleNamespace(
            id=123,
            path_with_namespace="Acme/API",
            web_url=None,
            default_branch=None,
        ),
        "https://gitlab.example:8443",
        batch_processed=True,
    )

    assert values["settings"] == {
        "source": "gitlab",
        "project_id": 123,
        "url": None,
        "default_branch": "main",
        "batch_processed": True,
        "gitlab_instance_url": "https://gitlab.example:8443",
    }
