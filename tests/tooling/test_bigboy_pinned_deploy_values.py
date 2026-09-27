"""R467: the bigboy router is generated from a PINNED deploy sha whose ops vendor pin is the running
build; and every routed path must have a handler in that build (ROUTEPROBE 405/401, never 404)."""

from __future__ import annotations

import http.server
import importlib.util
import subprocess
import sys
import threading
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
OPS_A = "a" * 40
OPS_B = "b" * 40
VALUES = {
    "ingress": {
        "goApiPaths": [
            {"path": "/api/v1/known"},
            {"path": "/api/v1/things/{thing_id}"},
        ],
        "queryApiPaths": [{"path": "/api/v1/q", "pathType": "Exact"}],
    }
}


def _load(name: str, rel: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / rel)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True
    ).stdout.strip()


@pytest.fixture()
def deploy_repo(tmp_path: Path) -> tuple[Path, str]:
    repo = tmp_path / "deploy"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "t@example.invalid")
    _git(repo, "config", "user.name", "t")
    (repo / "values.prod.yaml").write_text(yaml.safe_dump(VALUES), encoding="utf-8")
    _git(repo, "add", "values.prod.yaml")
    _git(
        repo,
        "update-index",
        "--add",
        "--cacheinfo",
        f"160000,{OPS_A},vendor/dev-health-ops",
    )
    _git(repo, "commit", "-q", "-m", "pin a")
    return repo, _git(repo, "rev-parse", "HEAD")


def test_pinned_mode_matches_vendor_and_writes_values(
    deploy_repo: tuple[Path, str], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    gen = _load("gen_pinned", "ci/bigboy/generate-plane-split-router.py")
    repo, sha = deploy_repo
    out = tmp_path / "v.yaml"
    rc = gen.main(
        [
            "--deploy-repo",
            str(repo),
            "--deploy-sha",
            sha[:8],
            "--expect-ops-sha",
            OPS_A[:12],
            "--values-out",
            str(out),
            "--format",
            "dynamic",
        ]
    )
    cap = capsys.readouterr()
    assert rc == 0
    assert f"deploy_sha={sha} ops_vendor={OPS_A}" in cap.err
    assert (
        "Host(`traefik`) && PathRegexp(`^(/api/v1/known|/api/v1/things/[^/]+)$`)"
        in cap.out
    )
    assert yaml.safe_load(out.read_text(encoding="utf-8")) == VALUES


def test_pinned_mode_refuses_other_build(
    deploy_repo: tuple[Path, str], capsys: pytest.CaptureFixture[str]
) -> None:
    gen = _load("gen_pinned2", "ci/bigboy/generate-plane-split-router.py")
    repo, sha = deploy_repo
    rc = gen.main(
        [
            "--deploy-repo",
            str(repo),
            "--deploy-sha",
            sha,
            "--expect-ops-sha",
            OPS_B,
            "--format",
            "dynamic",
        ]
    )
    cap = capsys.readouterr()
    assert rc == 3
    assert (
        "REFUSED" in cap.err
        and f"vendors ops {OPS_A[:12]}" in cap.err
        and OPS_B[:12] in cap.err
    )
    assert cap.out == ""


@pytest.mark.parametrize("expect", ["", "abc", "zzzzzzzz"])
def test_pinned_mode_refuses_malformed_expectation(
    deploy_repo: tuple[Path, str], expect: str
) -> None:
    gen = _load("gen_pinned3", "ci/bigboy/generate-plane-split-router.py")
    repo, sha = deploy_repo
    assert gen.main(
        ["--deploy-repo", str(repo), "--deploy-sha", sha, "--expect-ops-sha", expect]
    ) in (2, 3)


def test_unknown_deploy_sha_refused(deploy_repo: tuple[Path, str]) -> None:
    gen = _load("gen_pinned4", "ci/bigboy/generate-plane-split-router.py")
    repo, _ = deploy_repo
    assert (
        gen.main(
            [
                "--deploy-repo",
                str(repo),
                "--deploy-sha",
                "f" * 40,
                "--expect-ops-sha",
                OPS_A,
            ]
        )
        == 3
    )


def test_modes_are_exclusive(deploy_repo: tuple[Path, str], tmp_path: Path) -> None:
    gen = _load("gen_pinned5", "ci/bigboy/generate-plane-split-router.py")
    repo, sha = deploy_repo
    f = tmp_path / "v.yaml"
    f.write_text(yaml.safe_dump(VALUES), encoding="utf-8")
    assert gen.main([str(f), "--deploy-sha", sha]) == 2
    assert gen.main([]) == 2


# --- route coverage -----------------------------------------------------------------------


class _Mux(http.server.BaseHTTPRequestHandler):
    registered: dict[str, int] = {}

    def do_ROUTEPROBE(self) -> None:
        status = self.registered.get(self.path, 404)
        self.send_response(status)
        self.end_headers()

    def log_message(self, *_a: object) -> None:
        return


@pytest.fixture()
def plane() -> Iterator[int]:
    srv = http.server.HTTPServer(("127.0.0.1", 0), _Mux)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    yield srv.server_address[1]
    srv.shutdown()


def _values_file(tmp_path: Path) -> str:
    f = tmp_path / "v.yaml"
    f.write_text(yaml.safe_dump(VALUES), encoding="utf-8")
    return str(f)


def test_coverage_passes_when_every_path_registered(
    plane: int, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    cov = _load("cov1", "ci/bigboy/check-route-coverage.py")
    _Mux.registered = {
        "/api/v1/known": 405,
        "/api/v1/things/routeprobe": 405,
        "/api/v1/q": 401,
    }
    url = f"http://127.0.0.1:{plane}"
    assert (
        cov.main([_values_file(tmp_path), "--go-api-url", url, "--query-api-url", url])
        == 0
    )
    assert "plane=go-api routed=2 registered=2 missing=0" in capsys.readouterr().out


def test_coverage_names_a_missing_handler(
    plane: int, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    cov = _load("cov2", "ci/bigboy/check-route-coverage.py")
    _Mux.registered = {"/api/v1/known": 405, "/api/v1/q": 405}
    url = f"http://127.0.0.1:{plane}"
    assert (
        cov.main([_values_file(tmp_path), "--go-api-url", url, "--query-api-url", url])
        == 1
    )
    assert (
        "go-api has NO handler for routed path /api/v1/things/routeprobe"
        in capsys.readouterr().err
    )


def test_coverage_flags_unexpected_status(
    plane: int, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    cov = _load("cov3", "ci/bigboy/check-route-coverage.py")
    _Mux.registered = {
        "/api/v1/known": 405,
        "/api/v1/things/routeprobe": 500,
        "/api/v1/q": 405,
    }
    url = f"http://127.0.0.1:{plane}"
    assert (
        cov.main([_values_file(tmp_path), "--go-api-url", url, "--query-api-url", url])
        == 1
    )
    assert "/api/v1/things/routeprobe=500" in capsys.readouterr().err


@pytest.mark.parametrize(
    "url", ["https://127.0.0.1:1", "http://evil.example:1", "http://127.0.0.1:1/x"]
)
def test_coverage_refuses_non_local_targets(tmp_path: Path, url: str) -> None:
    cov = _load("cov4", "ci/bigboy/check-route-coverage.py")
    with pytest.raises(SystemExit):
        cov.main([_values_file(tmp_path), "--go-api-url", url])
