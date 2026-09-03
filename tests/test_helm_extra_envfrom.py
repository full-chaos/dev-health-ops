"""CHAOS-4984: api.extraEnvFrom / goWorkers.extraEnvFrom ordering.

Before this hook, api-deployment.yaml and go-workers.yaml both hardcoded
envFrom to exactly the chart's own configMapRef + secretRef, with no
values-driven way to add a source in front of them. An operator who needed
a third source (e.g. a local dotenv Secret) to win over the chart's own
config/secret had to kubectl patch the rendered Deployment out-of-band --
a patch a subsequent `helm upgrade` silently drops, since nothing in the
chart's own values re-creates it.

Kubernetes resolves a key present in more than one envFrom source in favour
of the LAST source that sets it, so this is an ordering assertion, not just
a presence assertion: extraEnvFrom entries must render BEFORE the chart's
own configMapRef/secretRef, in both templates, and the chart's own sources
must still be present and last so they still take precedence over anything
extraEnvFrom does NOT set.
"""

from pathlib import Path
from subprocess import run

import yaml

_CHART = Path(__file__).parents[1] / "deploy/helm/dev-health"


def _render(show_only: str, extra_set: list[str]) -> dict:
    """First Deployment document from one rendered template file.

    go-workers.yaml renders one Deployment (+ optional HPA) per worker
    group, so --show-only that template still yields multiple `---`
    separated documents -- safe_load_all, not safe_load.
    """
    args = [
        "helm",
        "template",
        "extra-envfrom-test",
        str(_CHART),
        "--show-only",
        show_only,
    ]
    for value in extra_set:
        args += ["--set", value]
    rendered = run(args, check=True, capture_output=True, text=True)
    documents = [d for d in yaml.safe_load_all(rendered.stdout) if d]
    for document in documents:
        if document.get("kind") == "Deployment":
            return document
    raise AssertionError(f"no Deployment document rendered from {show_only}")


def _envfrom_names(container: dict) -> list[tuple[str, str]]:
    """[(kind, name), ...] in rendered order, kind in {configMapRef, secretRef}."""
    out = []
    for source in container.get("envFrom") or []:
        for kind in ("configMapRef", "secretRef"):
            if kind in source:
                out.append((kind, source[kind]["name"]))
    return out


def test_api_extra_envfrom_renders_before_chart_config_and_secret() -> None:
    deployment = _render(
        "templates/api-deployment.yaml",
        ["api.extraEnvFrom[0].secretRef.name=local-dotenv"],
    )
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    refs = _envfrom_names(container)

    assert refs[0] == ("secretRef", "local-dotenv"), (
        "api.extraEnvFrom must render first so its keys lose to the chart's "
        f"own config/secret, not the other way around; got {refs}"
    )
    # Chart's own sources still present, still last, still in their existing
    # relative order (configMapRef before secretRef).
    assert ("configMapRef", "extra-envfrom-test-dev-health-config") in refs
    assert ("secretRef", "extra-envfrom-test-dev-health-secrets") in refs
    assert refs.index(
        ("configMapRef", "extra-envfrom-test-dev-health-config")
    ) < refs.index(("secretRef", "extra-envfrom-test-dev-health-secrets"))


def test_api_envfrom_is_unchanged_when_extra_envfrom_is_unset() -> None:
    deployment = _render("templates/api-deployment.yaml", [])
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    refs = _envfrom_names(container)

    assert refs == [
        ("configMapRef", "extra-envfrom-test-dev-health-config"),
        ("secretRef", "extra-envfrom-test-dev-health-secrets"),
    ], "default envFrom must be exactly the chart's own config + secret, in that order"


def test_go_worker_extra_envfrom_renders_before_chart_config_and_secret() -> None:
    deployment = _render(
        "templates/go-workers.yaml",
        [
            "goWorkers.enabled=true",
            "goWorkers.pgbouncer.postgres.host=postgres.example.com",
            "goWorkers.extraEnvFrom[0].secretRef.name=local-dotenv",
        ],
    )
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    refs = _envfrom_names(container)

    assert refs[0] == ("secretRef", "local-dotenv"), (
        "goWorkers.extraEnvFrom must render first, same rule as api.extraEnvFrom "
        f"and for the same reason; got {refs}"
    )
    assert ("configMapRef", "extra-envfrom-test-dev-health-config") in refs
    assert ("secretRef", "extra-envfrom-test-dev-health-secrets") in refs
    assert refs.index(
        ("configMapRef", "extra-envfrom-test-dev-health-config")
    ) < refs.index(("secretRef", "extra-envfrom-test-dev-health-secrets"))


def test_go_worker_extra_envfrom_is_shared_across_every_group() -> None:
    """extraEnvFrom is a single goWorkers-level list, not per-group."""
    rendered = run(
        [
            "helm",
            "template",
            "extra-envfrom-test",
            str(_CHART),
            "--set",
            "goWorkers.enabled=true",
            "--set",
            "goWorkers.pgbouncer.postgres.host=postgres.example.com",
            "--set",
            "goWorkers.extraEnvFrom[0].secretRef.name=local-dotenv",
            "--show-only",
            "templates/go-workers.yaml",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    documents = [d for d in yaml.safe_load_all(rendered.stdout) if d]
    deployments = [d for d in documents if d.get("kind") == "Deployment"]
    assert len(deployments) >= 2, (
        "expected more than one worker-group Deployment rendered"
    )

    for deployment in deployments:
        container = deployment["spec"]["template"]["spec"]["containers"][0]
        refs = _envfrom_names(container)
        assert refs[0] == ("secretRef", "local-dotenv"), (
            f"{deployment['metadata']['name']} did not get the shared extraEnvFrom entry first"
        )
