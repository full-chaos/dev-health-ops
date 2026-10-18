"""The authclient parsers must work from the BUILT WHEEL, outside the checkout.

Every parser loads its JSON Schema at runtime. A wheel that does not ship the
schemas passes every in-repo test (the checkout is on the path) and fails every
parser in production, with an error naming a missing directory. This test builds
the wheel from the real pyproject.toml, installs it into an isolated virtualenv,
and runs EVERY public ``parse_*`` the installed package exports (and the other
schema-loading entry points) on one accept fixture, from a working directory
outside the checkout.

A copy of the tree without ``.git`` is built, because setuptools_scm's git file
finder deadlocks in linked worktrees (see ops/AGENTS.md) and the build must not
depend on repository metadata.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
EXAMPLES = REPO / "contracts" / "auth" / "v1" / "examples"
SCHEMAS = REPO / "contracts" / "auth" / "v1" / "jsonschema"


def _run(args: list[str], *, cwd: Path, env: dict[str, str] | None = None) -> str:
    result = subprocess.run(args, cwd=cwd, env=env, capture_output=True, text=True)
    assert result.returncode == 0, (
        f"{' '.join(args)} failed ({result.returncode}):\n{result.stdout}\n{result.stderr}"
    )
    return result.stdout


# Everything the isolated interpreter needs, as data: one accept fixture per
# public parser. A parser without an entry fails the coverage assertion inside
# the isolated run, so adding a parser forces adding its fixture here.
PROBE = textwrap.dedent(
    r"""
    import json, sys
    from pathlib import Path

    examples = Path(sys.argv[1])
    import dev_health_ops.authclient as client

    def raw(directory, name):
        return (examples / directory / name).read_bytes()

    calls = {
        "parse_bytes": lambda: client.parse_bytes(raw("error", FIX["error"]), 403),
        "parse_decoded": lambda: client.parse_decoded(json.loads(raw("error", FIX["error"])), 403),
        "parse_discovery": lambda: client.parse_discovery(
            raw("discovery", "valid-minimal.json"), "https://auth.example.test"),
        "parse_provisioning_event": lambda: client.parse_provisioning_event(
            raw("provisioning-event", "valid-minimal.json")),
        "parse_audit_event": lambda: client.parse_audit_event(raw("audit-event", "valid-minimal.json")),
        "parse_decision_bytes": lambda: client.parse_decision_bytes(
            raw("authorization_decision", "decision-valid-deny.json")),
        "parse_batch_response_bytes": lambda: client.parse_batch_response_bytes(
            raw("authorization_batch_request", "batch-request-valid-two-items.json"),
            raw("authorization_batch_response", "batch-response-valid-items-disagree.json")),
        "parse_rfc3339": lambda: client.parse_rfc3339("2026-09-03T10:00:00Z", "probe.v1", "at"),
    }
    exported = sorted(n for n in client.__all__ if n.startswith("parse_"))
    uncovered = [n for n in exported if n not in calls]
    if uncovered:
        print(json.dumps({"uncovered": uncovered}))
        sys.exit(3)
    out = {}
    for name in exported:
        calls[name]()
        out[name] = "ok"
    # Schema-loading entry points that are not parse_*.
    client.Principal.from_wire(json.loads(raw("principal", "valid-human-minimal.json")))
    out["Principal.from_wire"] = "ok"
    client.validate("jwks.v1", json.loads(raw("jwks", "valid-single-signing-key.json")))
    out["validate(jwks.v1)"] = "ok"
    # A schema that $refs another file must resolve from the installed tree too.
    client.validate(
        "endpoint-auth-profile.v1",
        json.loads(raw("endpoint-auth-profile", "valid-protected-jwt-single-class.json")),
    )
    out["validate(endpoint-auth-profile.v1)"] = "ok"
    print(json.dumps({"ran": out, "schemas_dir": str(client.contracts_dir() / "jsonschema")}))
    """
)


def test_every_public_parser_runs_from_the_built_wheel(tmp_path: Path) -> None:
    uv = shutil.which("uv")
    assert uv, "uv is required to build the wheel and create the isolated environment"

    # 1. Build from a copy with no .git.
    tree = tmp_path / "tree"
    tree.mkdir()
    shutil.copy(REPO / "pyproject.toml", tree)
    for name in ("README.md", "LICENSE.md"):
        shutil.copy(REPO / name, tree)
    shutil.copytree(
        REPO / "src",
        tree / "src",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    shutil.copytree(REPO / "contracts", tree / "contracts")
    dist = tmp_path / "dist"
    env = {
        "PATH": "/usr/bin:/bin:/usr/local/bin:" + str(Path(uv).parent),
        "HOME": str(tmp_path),
        "SETUPTOOLS_SCM_PRETEND_VERSION": "0.0.0",
        "UV_CACHE_DIR": str(tmp_path / "uv-cache"),
    }
    _run(
        [uv, "build", "--wheel", "--out-dir", str(dist), str(tree)],
        cwd=tmp_path,
        env=env,
    )
    wheels = list(dist.glob("*.whl"))
    assert len(wheels) == 1, wheels

    # 2. Isolated environment: the wheel (no dependency resolution of the whole
    #    project) plus exactly what the parsers import.
    venv = tmp_path / "venv"
    _run([uv, "venv", str(venv), "--python", sys.executable], cwd=tmp_path, env=env)
    python = venv / "bin" / "python"
    _run(
        [uv, "pip", "install", "--python", str(python), "--no-deps", str(wheels[0])],
        cwd=tmp_path,
        env=env,
    )
    _run(
        [
            uv,
            "pip",
            "install",
            "--python",
            str(python),
            "jsonschema",
            "rfc3339-validator",
        ],
        cwd=tmp_path,
        env=env,
    )

    # 3. Run from OUTSIDE the checkout, isolated (-I: no cwd, no user site, no
    #    PYTHON* variables), with the repository nowhere on the path.
    outside = tmp_path / "elsewhere"
    outside.mkdir()
    probe = textwrap.dedent("FIX = %r\n") % {"error": _an_error_fixture()} + PROBE
    out = _run(
        [str(python), "-I", "-c", probe, str(EXAMPLES)],
        cwd=outside,
        env={"PATH": "/usr/bin:/bin", "HOME": str(tmp_path)},
    )
    result = json.loads(out.strip().splitlines()[-1])
    assert "uncovered" not in result, result

    ran = result["ran"]
    for expected in (
        "parse_provisioning_event",
        "parse_audit_event",
        "parse_discovery",
        "parse_bytes",
    ):
        assert ran.get(expected) == "ok", (expected, ran)

    # The schemas the parsers loaded came from the installed data prefix, not
    # from the checkout, and every schema in the repository shipped.
    installed = Path(result["schemas_dir"])
    assert REPO not in installed.parents, f"resolved inside the checkout: {installed}"
    shipped = {p.name for p in installed.glob("*.json")}
    expected_names = {p.name for p in SCHEMAS.glob("*.json")}
    assert shipped == expected_names, (
        f"wheel schemas differ from the repository's: "
        f"missing {sorted(expected_names - shipped)}, extra {sorted(shipped - expected_names)}"
    )


def _an_error_fixture() -> str:
    manifest = json.loads((EXAMPLES / "error" / "manifest.json").read_text("utf-8"))
    for entry in manifest["accept"]:
        document = json.loads((EXAMPLES / "error" / entry["file"]).read_text("utf-8"))
        if document.get("status") == 403:
            return entry["file"]
    raise AssertionError("no accept fixture in the error corpus carries status 403")
