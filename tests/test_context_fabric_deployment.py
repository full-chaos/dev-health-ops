from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path

import yaml


_REPO_ROOT = Path(__file__).resolve().parents[1]
_SCRIPT = _REPO_ROOT / "scripts" / "context-fabric-kubernetes.sh"
_VALUES = _REPO_ROOT / "deploy" / "context-fabric" / "helm-values.yaml"
_VALID_IMAGE = "registry.example/acr-api@sha256:" + ("a" * 64)


def _write_executable(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def _fake_acr_checkout(tmp_path: Path) -> Path:
    root = tmp_path / "dev-health-acr"
    chart = root / "deploy" / "helm" / "acr"
    chart.mkdir(parents=True)
    (chart / "Chart.yaml").write_text("apiVersion: v2\nname: acr\n", encoding="utf-8")
    (chart / "values.schema.json").write_text("{}\n", encoding="utf-8")
    return root


def _run(
    tmp_path: Path,
    *args: str,
    helm: str | None = None,
    kubectl: str | None = None,
) -> subprocess.CompletedProcess[str]:
    acr_root = _fake_acr_checkout(tmp_path)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    if helm is not None:
        _write_executable(bin_dir / "helm", helm)
    if kubectl is not None:
        _write_executable(bin_dir / "kubectl", kubectl)

    env = os.environ.copy()
    env["DEV_HEALTH_ACR_DIR"] = str(acr_root)
    env["PATH"] = f"{bin_dir}{os.pathsep}{env.get('PATH', '')}"
    return subprocess.run(
        ["bash", str(_SCRIPT), *args],
        cwd=_REPO_ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def test_context_fabric_values_reference_existing_secrets_only() -> None:
    values = yaml.safe_load(_VALUES.read_text(encoding="utf-8"))

    assert values["fullnameOverride"] == "context-fabric"
    assert values["image"]["reference"] == ""
    assert values["config"]["entitlement"]["url"] == ""
    assert values["config"]["requireBackingStores"] is True
    assert values["config"]["enableEpisodeWriteback"] is False
    assert values["credentials"]["runtime"]["existingSecret"] == (
        "acr-runtime-credentials"
    )
    assert values["credentials"]["migration"]["existingSecret"] == (
        "acr-migration-credentials"
    )
    assert values["credentials"]["entitlementToken"]["existingSecret"] == (
        "acr-entitlement-token"
    )

    raw = _VALUES.read_text(encoding="utf-8")
    forbidden_values = (
        "postgres://",
        "postgresql://",
        "clickhouse://",
        "fcacr_",
        "svc_acr_",
    )
    for forbidden in forbidden_values:
        assert forbidden not in raw


def test_context_fabric_render_uses_canonical_acr_chart(tmp_path: Path) -> None:
    result = _run(
        tmp_path,
        "render",
        "--image",
        _VALID_IMAGE,
        "--entitlement-url",
        "https://ops.dev-health.test",
        helm='#!/usr/bin/env bash\nprintf "%s\\n" "$@"\n',
    )

    assert result.returncode == 0, result.stderr
    assert "template" in result.stdout
    assert "deploy/helm/acr" in result.stdout
    assert f"image.reference={_VALID_IMAGE}" in result.stdout
    assert "config.entitlement.url=https://ops.dev-health.test" in result.stdout
    assert "networkPolicy.egress.entitlementPort=443" in result.stdout
    assert str(_VALUES) in result.stdout


def test_context_fabric_render_aligns_custom_entitlement_port(tmp_path: Path) -> None:
    result = _run(
        tmp_path,
        "render",
        "--image",
        _VALID_IMAGE,
        "--entitlement-url",
        "https://ops.dev-health.test:8443",
        helm='#!/usr/bin/env bash\nprintf "%s\\n" "$@"\n',
    )

    assert result.returncode == 0, result.stderr
    assert "config.entitlement.url=https://ops.dev-health.test:8443" in result.stdout
    assert "networkPolicy.egress.entitlementPort=8443" in result.stdout


def test_context_fabric_render_rejects_mutable_image(tmp_path: Path) -> None:
    result = _run(
        tmp_path,
        "render",
        "--image",
        "registry.example/acr-api:latest",
        "--entitlement-url",
        "https://ops.dev-health.test",
        helm='#!/usr/bin/env bash\nexit 99\n',
    )

    assert result.returncode == 1
    assert "immutable image@sha256" in result.stderr


def test_context_fabric_render_rejects_plain_http_entitlement(tmp_path: Path) -> None:
    result = _run(
        tmp_path,
        "render",
        "--image",
        _VALID_IMAGE,
        "--entitlement-url",
        "http://dev-health-api:8000",
        helm='#!/usr/bin/env bash\nexit 99\n',
    )

    assert result.returncode == 1
    assert "HTTPS origin" in result.stderr


def test_context_fabric_credential_is_written_mode_0600(tmp_path: Path) -> None:
    output = tmp_path / "client" / "acr-token"
    result = _run(
        tmp_path,
        "create-credential",
        "--org-id",
        "12345678-1234-1234-1234-123456789abc",
        "--repository",
        "acme/repository",
        "--output",
        str(output),
        kubectl='#!/usr/bin/env bash\nprintf "%s" "fcacr_test_token"\n',
    )

    assert result.returncode == 0, result.stderr
    assert output.read_text(encoding="utf-8") == "fcacr_test_token"
    assert stat.S_IMODE(output.stat().st_mode) == 0o600
