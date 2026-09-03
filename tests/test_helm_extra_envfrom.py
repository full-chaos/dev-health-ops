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

_DEFAULT_ENVFROM = [
    {"configMapRef": {"name": "extra-envfrom-test-dev-health-config"}},
    {"secretRef": {"name": "extra-envfrom-test-dev-health-secrets"}},
]


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


def test_go_worker_envfrom_is_unchanged_when_extra_envfrom_is_unset_for_every_group() -> (
    None
):
    """Semantic equality on the parsed envFrom object, for every group's Deployment.

    Not just the two chart-owned (kind, name) pairs (that's the name-tuple
    check the other tests use) -- the full rendered envFrom list, as parsed
    YAML objects, must equal the expected list exactly. Block vs flow
    mapping style in the rendered YAML is fine either way (both parse to the
    same dict); an extra/missing key on either source, or an extra list
    entry, is not.
    """
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
        assert container.get("envFrom") == _DEFAULT_ENVFROM, (
            f"{deployment['metadata']['name']}: default envFrom (extraEnvFrom "
            f"unset) must be exactly the chart's own config + secret, in "
            f"that order, as objects -- got {container.get('envFrom')!r}"
        )


def test_api_extra_envfrom_rejects_unknown_field_on_a_ref() -> None:
    """values.schema.json pins each entry's shape to what the templates build.

    An extraEnvFrom entry the chart's own dict-builder wouldn't recognize
    (a typo'd field, e.g.) must fail schema validation at `helm template`
    time -- not render silently with the typo'd field dropped by toYaml, or
    worse, passed through into a Kubernetes envFrom source where the API
    server rejects or ignores it far from where the operator set it.
    """
    result = run(
        [
            "helm",
            "template",
            "extra-envfrom-test",
            str(_CHART),
            "--set",
            "api.extraEnvFrom[0].secretRef.name=local-dotenv",
            "--set",
            "api.extraEnvFrom[0].secretRef.badfield=oops",
            "--show-only",
            "templates/api-deployment.yaml",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0, (
        "an extraEnvFrom entry with an unknown field must fail schema "
        f"validation, not render; stdout={result.stdout!r}"
    )
    assert "badfield" in result.stderr


def test_api_extra_envfrom_rejects_a_non_map_entry() -> None:
    result = run(
        [
            "helm",
            "template",
            "extra-envfrom-test",
            str(_CHART),
            "--set",
            "api.extraEnvFrom[0]=notamap",
            "--show-only",
            "templates/api-deployment.yaml",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0, (
        "a non-map extraEnvFrom entry must fail schema validation, not "
        f"render; stdout={result.stdout!r}"
    )


def test_go_worker_extra_envfrom_rejects_unknown_field_on_a_ref() -> None:
    result = run(
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
            "--set",
            "goWorkers.extraEnvFrom[0].secretRef.badfield=oops",
            "--show-only",
            "templates/go-workers.yaml",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0, (
        "goWorkers.extraEnvFrom must be schema-constrained the same way "
        f"api.extraEnvFrom is; stdout={result.stdout!r}"
    )
    assert "badfield" in result.stderr


def test_go_worker_extra_envfrom_rejects_a_non_map_entry() -> None:
    result = run(
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
            "goWorkers.extraEnvFrom[0]=notamap",
            "--show-only",
            "templates/go-workers.yaml",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0, (
        "a non-map goWorkers.extraEnvFrom entry must fail schema "
        f"validation, not render; stdout={result.stdout!r}"
    )
