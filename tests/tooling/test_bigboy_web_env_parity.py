"""CHAOS-6987 (D2736): check-web-env-parity.py fails loud on every web env drift shape."""

from __future__ import annotations

import copy
import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "ci" / "bigboy" / "check-web-env-parity.py"
OVERLAY = ROOT / "ci" / "bigboy" / "compose.bigboy.router.yml"

PROD: dict[str, Any] = {
    "ops": {
        "web": {
            "env": {"BACKEND_URL": "http://ingress"},
            "extraEnv": [
                {"name": "AUTH_URL", "value": "https://prod.example"},
                {"name": "TRUST_PROXY", "value": "true"},
                {"name": "TRUSTED_PROXY_HOPS", "value": "1"},
                {"name": "ACR_API_ORIGIN", "value": "x"},
                {"name": "ACR_WEB_ASSERTION_ISSUER", "value": "x"},
                {"name": "ACR_WEB_ASSERTION_AUDIENCE", "value": "x"},
                {"name": "ACR_WEB_ASSERTION_KID", "value": "x"},
                {"name": "ACR_WEB_ASSERTION_KEY_FILE", "value": "x"},
            ],
        }
    }
}


def _mod() -> ModuleType:
    spec = importlib.util.spec_from_file_location("check_web_env_parity", SCRIPT)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _write(tmp_path: Path, name: str, doc: Any) -> str:
    p = tmp_path / name
    p.write_text(yaml.safe_dump(doc), encoding="utf-8")
    return str(p)


def test_tracked_overlay_is_in_parity(tmp_path: Path) -> None:
    assert _mod().check(_write(tmp_path, "v.yaml", PROD), str(OVERLAY), None) == []


def test_new_prod_name_fails_named(tmp_path: Path) -> None:
    prod = copy.deepcopy(PROD)
    prod["ops"]["web"]["extraEnv"].append({"name": "NEW_WEB_KNOB", "value": "1"})
    findings = _mod().check(_write(tmp_path, "v.yaml", prod), str(OVERLAY), None)
    assert any("NEW_WEB_KNOB" in f for f in findings)


def test_missing_overlay_auth_url_fails(tmp_path: Path) -> None:
    overlay = _mod().load_compose_yaml(OVERLAY.read_text(encoding="utf-8"))
    del overlay["services"]["web"]["environment"]["AUTH_URL"]
    findings = _mod().check(
        _write(tmp_path, "v.yaml", PROD), _write(tmp_path, "o.yml", overlay), None
    )
    assert any("AUTH_URL" in f and "missing" in f for f in findings)


def test_wrong_overlay_value_fails(tmp_path: Path) -> None:
    overlay = _mod().load_compose_yaml(OVERLAY.read_text(encoding="utf-8"))
    overlay["services"]["web"]["environment"]["BACKEND_URL"] = "http://api:8000"
    findings = _mod().check(
        _write(tmp_path, "v.yaml", PROD), _write(tmp_path, "o.yml", overlay), None
    )
    assert any("BACKEND_URL" in f for f in findings)


def test_stale_classification_fails(tmp_path: Path) -> None:
    prod = copy.deepcopy(PROD)
    prod["ops"]["web"]["extraEnv"] = [
        e for e in prod["ops"]["web"]["extraEnv"] if e["name"] != "ACR_API_ORIGIN"
    ]
    findings = _mod().check(_write(tmp_path, "v.yaml", prod), str(OVERLAY), None)
    assert any("ACR_API_ORIGIN" in f and "no longer" in f for f in findings)


def test_live_names_missing_fails(tmp_path: Path) -> None:
    live = {"BACKEND_URL", "ACR_API_ORIGIN"}
    findings = _mod().check(_write(tmp_path, "v.yaml", PROD), str(OVERLAY), live)
    assert any(
        "running web container has no env var named AUTH_URL" in f for f in findings
    )


def test_empty_live_names_file_is_not_a_pass(tmp_path: Path) -> None:
    empty = tmp_path / "names"
    empty.write_text("", encoding="utf-8")
    rc = _mod().main(
        [_write(tmp_path, "v.yaml", PROD), str(OVERLAY), "--live-names", str(empty)]
    )
    assert rc == 2


def test_empty_prod_web_env_is_not_a_pass(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        _mod().check(
            _write(tmp_path, "v.yaml", {"ops": {"web": {}}}), str(OVERLAY), None
        )


@pytest.mark.parametrize("name", ["TRUST_PROXY", "TRUSTED_PROXY_HOPS"])
def test_missing_overlay_trust_proxy_name_fails(tmp_path: Path, name: str) -> None:
    overlay = _mod().load_compose_yaml(OVERLAY.read_text(encoding="utf-8"))
    del overlay["services"]["web"]["environment"][name]
    findings = _mod().check(
        _write(tmp_path, "v.yaml", PROD), _write(tmp_path, "o.yml", overlay), None
    )
    assert any(name in f and "missing" in f for f in findings)


@pytest.mark.parametrize(
    ("name", "wrong"), [("TRUST_PROXY", "false"), ("TRUSTED_PROXY_HOPS", "2")]
)
def test_wrong_overlay_trust_proxy_value_fails(
    tmp_path: Path, name: str, wrong: str
) -> None:
    overlay = _mod().load_compose_yaml(OVERLAY.read_text(encoding="utf-8"))
    overlay["services"]["web"]["environment"][name] = wrong
    findings = _mod().check(
        _write(tmp_path, "v.yaml", PROD), _write(tmp_path, "o.yml", overlay), None
    )
    assert any(name in f and "expected" in f for f in findings)


def test_unclassified_trust_proxy_name_is_the_drift_cut_e_saw(tmp_path: Path) -> None:
    """Prod carrying the two names while the checker did not classify them was rc=1."""
    prod = copy.deepcopy(PROD)
    prod["ops"]["web"]["extraEnv"].append({"name": "TRUST_PROXY_NEW", "value": "1"})
    findings = _mod().check(_write(tmp_path, "v.yaml", prod), str(OVERLAY), None)
    assert any("TRUST_PROXY_NEW" in f and "not classified" in f for f in findings)
