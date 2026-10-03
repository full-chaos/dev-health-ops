"""CHAOS-6188: web.extraVolumes / web.extraVolumeMounts render outside localDevelopment.

Prod web reads its ACR web-assertion Ed25519 signing key ONLY from a file
(ACR_WEB_ASSERTION_KEY_FILE), which means it needs a Secret volume mounted
into the web Deployment. Before this change, web-deployment.yaml only ever
rendered volumeMounts/volumes when web.localDevelopment.enabled was true, so
production web (localDevelopment always off) had no values-driven way to get
a Secret mounted in -- the env vars already pass through web.extraEnv, but
nothing carried a volume.

web.extraVolumes / web.extraVolumeMounts render in every environment. When
web.localDevelopment.enabled is also true, both sources must merge under a
single volumeMounts:/volumes: key -- never two separate keys, which would be
invalid Kubernetes YAML (a mapping can't repeat a key) and, depending on
YAML-merge behavior, silently drop one source's entries.
"""

import os
from pathlib import Path
from subprocess import run

import yaml

_CHART = Path(__file__).parents[1] / "deploy/helm/dev-health"

_KEY_VOLUME_MOUNT = {
    "name": "acr-web-assertion-key",
    "mountPath": "/etc/dev-health/acr",
    "readOnly": True,
}
_KEY_VOLUME = {
    "name": "acr-web-assertion-key",
    "secret": {"secretName": "acr-web-assertion-key", "defaultMode": 0o400},
}
_LOCALDEV_VOLUME_MOUNT = {"name": "localdev", "mountPath": "/app/local"}
_LOCALDEV_VOLUME = {"name": "localdev", "hostPath": {"path": "/host/local"}}


def _render_web(extra_set_json: list[str], extra_set: list[str] | None = None) -> dict:
    args = ["helm", "template", "extra-volumes-test", str(_CHART)]
    for value in extra_set_json:
        args += ["--set-json", value]
    for value in extra_set or []:
        args += ["--set", value]
    args += ["--show-only", "templates/web-deployment.yaml"]
    rendered = run(args, check=True, capture_output=True, text=True)
    documents = [d for d in yaml.safe_load_all(rendered.stdout) if d]
    for document in documents:
        if document.get("kind") == "Deployment":
            return document
    raise AssertionError("no Deployment document rendered from web-deployment.yaml")


def test_extra_volumes_render_with_localdevelopment_off() -> None:
    """A Secret volume + readOnly mount appear with localDevelopment untouched."""
    deployment = _render_web(
        [
            f"web.extraVolumeMounts=[{_json(_KEY_VOLUME_MOUNT)}]",
            f"web.extraVolumes=[{_json(_KEY_VOLUME)}]",
        ]
    )
    pod_spec = deployment["spec"]["template"]["spec"]
    container = pod_spec["containers"][0]

    assert container["volumeMounts"] == [_KEY_VOLUME_MOUNT]
    assert pod_spec["volumes"] == [_KEY_VOLUME]


def test_extra_volumes_absent_when_unset() -> None:
    """Default render: no volumeMounts/volumes keys at all (matches main)."""
    deployment = _render_web([])
    container = deployment["spec"]["template"]["spec"]["containers"][0]

    assert "volumeMounts" not in container
    assert "volumes" not in deployment["spec"]["template"]["spec"]


def test_extra_volumes_merge_with_local_development_under_one_key() -> None:
    """web.extra* + web.localDevelopment.extra* combine into ONE volumeMounts/volumes key."""
    deployment = _render_web(
        [
            f"web.extraVolumeMounts=[{_json(_KEY_VOLUME_MOUNT)}]",
            f"web.extraVolumes=[{_json(_KEY_VOLUME)}]",
            f"web.localDevelopment.extraVolumeMounts=[{_json(_LOCALDEV_VOLUME_MOUNT)}]",
            f"web.localDevelopment.extraVolumes=[{_json(_LOCALDEV_VOLUME)}]",
        ],
        extra_set=["web.localDevelopment.enabled=true"],
    )
    pod_spec = deployment["spec"]["template"]["spec"]
    container = pod_spec["containers"][0]

    # Exactly one volumeMounts/volumes list each (a YAML mapping can't repeat
    # a key, but confirm the merged CONTENT rather than just parse success).
    assert container["volumeMounts"] == [_KEY_VOLUME_MOUNT, _LOCALDEV_VOLUME_MOUNT]
    assert pod_spec["volumes"] == [_KEY_VOLUME, _LOCALDEV_VOLUME]


def test_local_development_only_is_unchanged() -> None:
    """web.extra* unset + localDevelopment.enabled: identical to the pre-existing shape."""
    deployment = _render_web(
        [
            f"web.localDevelopment.extraVolumeMounts=[{_json(_LOCALDEV_VOLUME_MOUNT)}]",
            f"web.localDevelopment.extraVolumes=[{_json(_LOCALDEV_VOLUME)}]",
        ],
        extra_set=["web.localDevelopment.enabled=true"],
    )
    pod_spec = deployment["spec"]["template"]["spec"]
    container = pod_spec["containers"][0]

    assert container["volumeMounts"] == [_LOCALDEV_VOLUME_MOUNT]
    assert pod_spec["volumes"] == [_LOCALDEV_VOLUME]


def _json(obj: dict) -> str:
    import json

    return json.dumps(obj)


def test_web_backend_url_is_required() -> None:
    """CHAOS-8310: web.env.BACKEND_URL has no chart default. With nothing set the render fails and the
    message names the key (the suite's helm wrapper is bypassed with HELM_SHIM_OFF=1); web.enabled=false
    needs none; an explicit value reaches the Deployment verbatim."""
    env = {**os.environ, "HELM_SHIM_OFF": "1"}
    refused = run(
        ["helm", "template", "t", str(_CHART)], capture_output=True, text=True, env=env
    )
    assert refused.returncode != 0, "a render without web.env.BACKEND_URL succeeded"
    assert "web.env.BACKEND_URL is required whenever web.enabled=true" in refused.stderr
    assert "web.backendFromRelease=true" in refused.stderr
    off = run(
        ["helm", "template", "t", str(_CHART), "--set", "web.enabled=false"],
        capture_output=True,
        text=True,
        env=env,
    )
    assert off.returncode == 0, off.stderr
    explicit = run(
        [
            "helm",
            "template",
            "t",
            str(_CHART),
            "--set",
            "web.env.BACKEND_URL=http://explicit.example:9000",
        ],
        capture_output=True,
        text=True,
        env=env,
    )
    assert explicit.returncode == 0, explicit.stderr
    assert 'value: "http://explicit.example:9000"' in explicit.stdout


def test_quickstart_profile_carries_its_own_backend_url() -> None:
    """CHAOS-8310: the suite's helm wrapper (--set) would hide a values FILE's own value, so the
    quickstart profile is rendered with the wrapper off, as its usage line says. The release form is
    release-relative: the value the chart rendered before CHAOS-8310 for each release name."""
    env = {**os.environ, "HELM_SHIM_OFF": "1"}
    for release, want in (
        ("dev-health", "http://dev-health-api:8000"),
        ("lane-a", "http://lane-a-dev-health-api:8000"),
    ):
        done = run(
            [
                "helm",
                "template",
                release,
                str(_CHART),
                "-f",
                str(_CHART / "values-quickstart.yaml"),
            ],
            capture_output=True,
            text=True,
            env=env,
        )
        assert done.returncode == 0, done.stderr
        assert f'value: "{want}"' in done.stdout, release


def test_web_backend_url_and_backend_from_release_are_exclusive() -> None:
    """CHAOS-8310: exactly one of web.env.BACKEND_URL and web.backendFromRelease; both is refused,
    naming both; a misspelt opt-in key leaves the render refused as unset."""
    env = {**os.environ, "HELM_SHIM_OFF": "1"}
    both = run(
        [
            "helm",
            "template",
            "t",
            str(_CHART),
            "--set",
            "web.env.BACKEND_URL=http://x:1",
            "--set",
            "web.backendFromRelease=true",
        ],
        capture_output=True,
        text=True,
        env=env,
    )
    assert both.returncode != 0
    assert (
        "web.env.BACKEND_URL and web.backendFromRelease=true are both set"
        in both.stderr
    )
    typo = run(
        ["helm", "template", "t", str(_CHART), "--set", "web.backendFromRelese=true"],
        capture_output=True,
        text=True,
        env=env,
    )
    assert typo.returncode != 0
    assert "web.env.BACKEND_URL is required" in typo.stderr
