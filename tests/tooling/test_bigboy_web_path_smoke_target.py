"""CHAOS-6987: ci/bigboy/web-path-smoke.py connects only to an allowlisted target.

DHO_SMOKE_BASE_URL comes from the environment. The smoke parses it once and refuses
(exit 2, named reason) any scheme other than http, any host other than the internal router
allowlist, and any URL carrying more than scheme://host[:port]. No connection is
opened for a refused target.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "ci" / "bigboy" / "web-path-smoke.py"


@pytest.fixture()
def smoke() -> ModuleType:
    spec = importlib.util.spec_from_file_location("web_path_smoke_target", SCRIPT)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.mark.parametrize(
    ("raw", "scheme", "host", "port"),
    [
        ("http://traefik:3000", "http", "traefik", 3000),
        ("http://traefik/", "http", "traefik", 80),
    ],
)
def test_allowed_targets_parse(
    smoke: ModuleType, raw: str, scheme: str, host: str, port: int
) -> None:
    t = smoke.parse_base_url(raw)
    assert (t.scheme, t.host, t.port) == (scheme, host, port)


@pytest.mark.parametrize(
    ("raw", "reason"),
    [
        ("file:///etc/passwd", "base_url_scheme_not_allowed=file"),
        ("https://traefik:3000", "base_url_scheme_not_allowed=https"),
        (
            "http://www.commanderkeen.dev",
            "base_url_host_not_allowed=www.commanderkeen.dev",
        ),
        ("ftp://traefik", "base_url_scheme_not_allowed=ftp"),
        ("traefik:3000", "base_url_scheme_not_allowed"),
        ("http://evil.example:3000", "base_url_host_not_allowed=evil.example"),
        ("http://169.254.169.254", "base_url_host_not_allowed=169.254.169.254"),
        (
            "http://traefik.evil.example",
            "base_url_host_not_allowed=traefik.evil.example",
        ),
        ("http://user:pw@traefik:3000", "base_url_must_be_scheme_host_port_only"),
        ("http://traefik:3000/api", "base_url_must_be_scheme_host_port_only"),
        ("http://traefik:3000?x=1", "base_url_must_be_scheme_host_port_only"),
        ("http://traefik:notaport", "base_url_port_invalid"),
    ],
)
def test_refused_targets_are_named(smoke: ModuleType, raw: str, reason: str) -> None:
    with pytest.raises(smoke.ConfigError) as exc:
        smoke.parse_base_url(raw)
    assert str(exc.value).startswith(reason)


def test_main_refuses_before_any_connection(
    smoke: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    opened: list[object] = []
    monkeypatch.setattr(
        smoke.http.client, "HTTPConnection", lambda *a, **k: opened.append(a)
    )
    monkeypatch.setattr(
        smoke.http.client, "HTTPSConnection", lambda *a, **k: opened.append(a)
    )
    monkeypatch.setenv("DHO_SMOKE_BASE_URL", "http://evil.example")
    assert smoke.main() == 2
    assert opened == []
    assert "base_url_host_not_allowed=evil.example" in capsys.readouterr().err


def test_send_without_parsed_target_refuses(
    smoke: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(smoke, "TARGET", None)
    with pytest.raises(smoke.ConfigError):
        smoke._send("GET", "/health", None, {})


def test_main_refuses_https_base_url(
    smoke: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("DHO_SMOKE_BASE_URL", "https://www.commanderkeen.dev")
    assert smoke.main() == 2
    assert "base_url_scheme_not_allowed=https" in capsys.readouterr().err


def test_no_tls_client_in_the_smoke() -> None:
    text = SCRIPT.read_text(encoding="utf-8")
    assert "HTTPSConnection" not in text and "import ssl" not in text
