"""D2855: the chart's pre/post-upgrade routing carry/repoint hooks, run against the REAL
`dho` binary -- not the stub test_helm_routing_carry_hooks_behavior.py's other tests use.

Why this module exists, distinct from the stub-driven suite: that suite proves the CHART
SCRIPT's own shell control flow is real (a gutted fallback is caught because the stubbed
calls it expects simply never happen), but it can never catch a drift between what the
script ASSUMES the Go CLI accepts/emits and what the CLI ACTUALLY does -- a stub always
answers exactly what the test plan tells it to. That gap was real and already bit this
ticket once (r1's P1: the original grep pattern never matched the real Go error text) --
see test_helm_routing_carry_hooks_behavior.py's own docstring, and the file-level comment
on routing-carry-hooks.yaml documenting the CLI's real `reason` vocabulary.

Scope, deliberately narrow: flags parse (the real binary never answers "flag provided but
not defined" for anything this script passes) and -json's `reason` line is actually emitted
in the exact shape the script's own `carry_reason()` parses, letting the whole script reach
its own abort/proceed decision correctly. This does NOT stand up a live query-api or
Postgres -- the internal/goapiproof and internal/goapicli/routing packages already prove
the CLI's own carry/repoint/-json contract against a real database (routing_carry_test.go,
routing_carry_integration_test.go, carry_integration_test.go); this module's only job is
proving the CHART's assumption about that contract has not drifted from it.
"""

from __future__ import annotations

import shutil
import stat
import subprocess
import tempfile
from pathlib import Path

import pytest

# mypy has no path entry for this directory (pyproject.toml's mypy_path covers only
# src/), so it cannot resolve a sibling test module by bare name the way pytest's own
# rootdir-based sys.path insertion does at run time -- confirmed real at run time by
# every test in this file passing. Narrowly ignored rather than widening mypy_path
# repo-wide for one chart-hook test suite.
from test_helm_routing_carry_hooks import (  # type: ignore[import-not-found]
    _CARRY,
    _ENABLED,
    _REPOINT,
    _jobs,
    _script,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.skipif(
    shutil.which("helm") is None, reason="helm is not installed"
)


@pytest.fixture(scope="module")
def real_dho() -> Path:
    """Builds the real `dho` binary once for this module -- the CLI these hooks actually
    invoke in production, not a stand-in that always answers what a test plan says.

    tempfile.mkdtemp with no `dir=` already resolves TMPDIR/TEMP/TMP portably and falls
    back to a real, writable system temp dir on whatever host runs this -- a hardcoded
    bigboy-specific fallback path here (this module's own earlier draft) is exactly what
    breaks hosted CI runners, which have no /var/lib/oci-cache and no permission to
    create it (PermissionError: [Errno 13], caught live on #3384's own hosted CI run)."""
    binary = Path(tempfile.mkdtemp(prefix="real-dho-")) / "dho"
    completed = subprocess.run(
        ["go", "build", "-o", str(binary), "./cmd/dho"],
        cwd=_REPO_ROOT,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, (
        f"building the real dho binary failed:\n{completed.stderr}"
    )
    binary.chmod(binary.stat().st_mode | stat.S_IEXEC)
    return binary


def _mint_stub(bin_dir: Path, real_dho: Path) -> None:
    """`dho mint envelope` needs a real Ed25519 key file this test has no reason to
    provision -- only `mint envelope`'s own output (a bearer string) matters to the rest of
    the script, and it is never inspected once the carry/repoint call it feeds refuses on
    -postgres-uri first (see the module docstring: no live infra is being proven here).
    A stub NAMED `dho` would shadow the real binary for every OTHER call in the script, so
    this wraps the real one instead: any argv starting with "mint" is faked, everything
    else execs straight through to it.
    """
    stub = bin_dir / "dho"
    stub.write_text(
        "#!/usr/bin/env bash\n"
        "set -u\n"
        'if [ "$1 $2" = "mint envelope" ]; then echo "fake-bearer-for-real-dho-test"; exit 0; fi\n'
        f'exec "{real_dho}" "$@"\n'
    )
    stub.chmod(stub.stat().st_mode | stat.S_IEXEC)


def _run_real_script(
    job: dict, real_dho: Path, tmp_path: Path
) -> subprocess.CompletedProcess[str]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    _mint_stub(bin_dir, real_dho)

    script = _script(job)
    harness = tmp_path / "harness.sh"
    harness.write_text("#!/usr/bin/env bash\nset -u\n" + script + "\n")
    harness.chmod(harness.stat().st_mode | stat.S_IEXEC)

    env = {
        "PATH": f"{bin_dir}:/usr/bin:/bin",
        "MINT_ORG": "test-org",
        # Unreachable but syntactically valid -- carry/repoint refuse on the missing
        # -postgres-uri flag before ever attempting to dial either URL (see the module
        # docstring), so what this points at is irrelevant to what this test proves.
        "QUERY_API_URL": "http://127.0.0.1:1",
        "RELEASE_NAME": "test-release",
    }
    return subprocess.run(
        ["bash", str(harness)],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
        env=env,
    )


def test_pre_upgrade_carry_script_flags_parse_against_the_real_binary(
    real_dho: Path, tmp_path: Path
) -> None:
    proc = _run_real_script(_jobs(*_ENABLED)[_CARRY], real_dho, tmp_path)
    assert "flag provided but not defined" not in proc.stdout, proc.stdout
    assert "flag provided but not defined" not in proc.stderr, proc.stderr
    # The real binary's own -json line, not a stub's canned one -- proves the CLI actually
    # accepted -json and emitted the shape carry_reason() depends on (reason as the first
    # JSON key).
    assert 'GOAPI_ROUTING_JSON {"reason":"refused"' in proc.stdout, proc.stdout
    # The real refusal reason here is the missing -postgres-uri flag -- neither
    # digest_unchanged nor stale_build, so the script's own catch-all correctly aborts.
    # This is the shape the abort branch exists for; it is not itself the fix's target.
    assert "ABORTING upgrade" in proc.stdout, proc.stdout
    assert proc.returncode != 0


def test_post_upgrade_repoint_script_flags_parse_against_the_real_binary(
    real_dho: Path, tmp_path: Path
) -> None:
    proc = _run_real_script(_jobs(*_ENABLED)[_REPOINT], real_dho, tmp_path)
    assert "flag provided but not defined" not in proc.stdout, proc.stdout
    assert "flag provided but not defined" not in proc.stderr, proc.stderr
    # The post-upgrade repoint call passes no -json (see routing-carry-hooks.yaml): its
    # `set -eu` propagates the real binary's own refusal exit code directly, with no
    # branching logic of the script's own to exercise.
    assert proc.returncode != 0
    assert "required flag is missing: -postgres-uri" in proc.stdout + proc.stderr


def test_pre_upgrade_repoint_before_retry_flags_parse_against_the_real_binary(
    real_dho: Path, tmp_path: Path
) -> None:
    """The pre-upgrade hook's OWN repoint call, inside the stale_build branch, is a second,
    distinct invocation shape (-json -operations all-registered, not just -json) -- run
    directly, standalone, rather than only reachable by first forcing carry into the
    stale_build branch (which needs a real database)."""
    bin_dir = tmp_path / "bin2"
    bin_dir.mkdir(exist_ok=True)
    real_bin = bin_dir / "dho"
    shutil.copy(real_dho, real_bin)
    real_bin.chmod(real_bin.stat().st_mode | stat.S_IEXEC)
    proc = subprocess.run(
        [
            str(real_bin),
            "goapi",
            "routing",
            "repoint",
            "-registry-url",
            "http://127.0.0.1:1/registry",
            "-buildinfo-url",
            "http://127.0.0.1:1/buildinfo",
            "-json",
            "-operations",
            "all-registered",
            "-recorded-by",
            "test",
            "-review-evidence",
            "test",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert "flag provided but not defined" not in proc.stdout + proc.stderr
    assert 'GOAPI_ROUTING_JSON {"reason":"refused"' in proc.stdout
    assert proc.returncode != 0
