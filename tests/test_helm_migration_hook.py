from pathlib import Path
from subprocess import run

import yaml

CHART_PATH = Path(__file__).parents[1] / "deploy/helm/dev-health"
QUICKSTART_VALUES = CHART_PATH / "values-quickstart.yaml"


def _render(*args: str):
    return run(
        ["helm", "template", "migration-contract", CHART_PATH, *args],
        capture_output=True,
        text=True,
    )


def _migration_job(rendered: str) -> dict:
    resources = yaml.safe_load_all(rendered)
    return next(
        resource
        for resource in resources
        if resource
        and resource["kind"] == "Job"
        and resource["metadata"]["labels"]["app.kubernetes.io/component"] == "migrate"
    )


def test_default_migration_hook_stays_pre_install_and_pre_upgrade() -> None:
    rendered = _render()

    assert rendered.returncode == 0, rendered.stderr
    job = _migration_job(rendered.stdout)
    assert job["metadata"]["annotations"]["helm.sh/hook"] == "pre-install,pre-upgrade"
    assert "initContainers" not in job["spec"]["template"]["spec"]


def test_quickstart_uses_post_install_and_waits_for_bundled_postgres() -> None:
    rendered = _render("-f", str(QUICKSTART_VALUES))

    assert rendered.returncode == 0, rendered.stderr
    job = _migration_job(rendered.stdout)
    assert job["metadata"]["annotations"]["helm.sh/hook"] == "post-install,pre-upgrade"
    init_container = job["spec"]["template"]["spec"]["initContainers"][0]
    assert init_container["name"] == "wait-for-postgresql"
    assert init_container["securityContext"]["runAsUser"] == 70
    assert init_container["securityContext"]["runAsGroup"] == 70
    assert "pg_isready" in init_container["command"][-1]


def test_post_install_is_rejected_without_explicit_local_bundled_mode() -> None:
    rendered = _render(
        "--set-json",
        'migrations.hook.events=["post-install","pre-upgrade"]',
        "--set",
        "postgresql.enabled=true",
    )

    assert rendered.returncode != 0
    assert "localBundledPostgres" in rendered.stderr


def test_local_post_install_requires_bundled_postgres() -> None:
    rendered = _render(
        "--set",
        "migrations.hook.localBundledPostgres=true",
        "--set-json",
        'migrations.hook.events=["post-install","pre-upgrade"]',
    )

    assert rendered.returncode != 0
    # Helm renders the offending schema path as a JSON Pointer
    # ("/postgresql/enabled") in some versions and a dotted path
    # ("postgresql.enabled") in others, and this repo pins no helm version --
    # neither CI nor any ci/ script installs one, so the test would otherwise
    # track whichever build happened to be on the author's PATH. Accept either
    # rendering of the same pointer; the contract being asserted is that the
    # chart refuses the combination AND names the field, which the sibling test
    # above already checks in the version-agnostic bare-name form.
    assert any(
        form in rendered.stderr
        for form in ("/postgresql/enabled", "postgresql.enabled")
    ), rendered.stderr
