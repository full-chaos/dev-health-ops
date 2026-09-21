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
