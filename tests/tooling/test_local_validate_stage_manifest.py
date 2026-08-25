"""Regression coverage for CHAOS-3571: a degraded ``ci/local_validate.sh`` run
must never print ``GATE PASSED``.

Observed 2026-08-07 while gating CHAOS-3552 (PR #1579), machine load ~27: the
docker probe inside ``ch_provision()`` failed (most likely ``docker ps`` timing
out under load -- the exact invocation was never captured), and the old code
rendered that FAILURE as the claim "container 'dev-health-clickhouse-1' not
running" and ``skip``'d ch-scratch-create, ch-migrate, and the argMax live-exec
proof. The gate still printed ``GATE PASSED. safe to push.`` with 3 of 8 real
stages silently missing -- the container had been up and healthy the entire
time, verified independently by two people minutes later.

This file proves two independent things now hold, matching the family rule "a
measurement that did not happen must FAIL loudly":

  (i)  A docker probe FAILURE (``docker ps`` itself erroring -- indeterminate
       container state) is a HARD gate failure with a message that names the
       true mechanism ("probe FAILED ... UNKNOWN"), never the old fabricated
       "not running" claim, and never a ``skip``.
  (ii) The stage manifest (declared vs. executed) is enforced structurally:
       a full run's executed-stage-id set equals its declared set, a
       SKIP_CLICKHOUSE=1 run's reduced declared set is met exactly, and
       ``verify_stage_manifest()`` itself refuses ANY declared-but-not-executed
       gap even when handed a scenario the current fix can no longer produce
       through normal execution.

Every test here drives the REAL functions in ``ci/local_validate.sh`` through
its `--ch-probe-only` / `--stage-manifest-probe` / `--stage-manifest-mismatch-
probe` test-only harness hooks (same precedent as `--lock-probe` in
test_local_validate_lock.py) -- never a reimplementation, and never the real
`bash ci/local_validate.sh` with no hook (which would run the full ~15-minute
gate, including a real docker probe against the shared, single-flight-locked
ClickHouse container these tests must not touch). No test here issues a single
real ``docker`` command: docker is always a PATH-shadowing stub script, or, for
the manifest-only tests, `ch_probe_docker` itself is stubbed by the harness
hook.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "ci" / "local_validate.sh"
_BASE_PATH = "/usr/bin:/bin"
_TIMEOUT = 30

# Every external command local_validate.sh's startup + ch_probe_docker() path
# can reach, EXCLUDING docker itself. Used only by the "docker absent" test
# below -- see its docstring for why `_BASE_PATH` alone is not safe for that
# one case.
_NON_DOCKER_TOOLS = (
    "bash",
    "dirname",
    "mktemp",
    "sed",
    "tr",
    "grep",
    "cat",
    "rm",
    "sha256sum",
    "shasum",
    "openssl",
    "cksum",
    "awk",
)


def _path_guaranteed_docker_free(bin_dir: Path) -> str:
    """A PATH that resolves every tool `local_validate.sh` needs up to and
    including `ch_probe_docker()`'s "docker missing" branch -- EXCEPT
    `docker` itself, which is guaranteed absent regardless of the host.

    `_BASE_PATH` ("/usr/bin:/bin") is NOT reliably docker-free: GitHub's
    `ubuntu-latest` Actions runners ship docker preinstalled at
    `/usr/bin/docker`, so a test that reused `_BASE_PATH` here (as an earlier
    version of this file did) found a REAL docker on that PATH and got
    rc=3 ("container confirmed NOT running") instead of the intended rc=1
    ("docker CLI not found") -- exactly what happened the first time this
    test ran in CI. This builds the allowed toolset by resolving each name in
    `_NON_DOCKER_TOOLS` via the REAL, live `PATH` (whatever it is on this
    host) and symlinking only those into an isolated directory, so the
    result is portable across hosts that do and do not ship docker in a
    standard location, without guessing at a fixed exclusion list.
    """
    bin_dir.mkdir(parents=True, exist_ok=True)
    for tool in _NON_DOCKER_TOOLS:
        found = shutil.which(tool)
        if found:
            (bin_dir / tool).symlink_to(found)
    assert shutil.which("docker", path=str(bin_dir)) is None, (
        "docker leaked into the supposedly docker-free PATH"
    )
    return str(bin_dir)


def _write_fake_docker(
    bin_dir: Path, *, ps_exit: int, ps_stdout: str, ps_stderr: str
) -> None:
    """A PATH-shadowing ``docker`` stub: only ``docker ps ...`` is faked."""
    script = bin_dir / "docker"
    script.write_text(
        "#!/bin/bash\n"
        'if [ "$1" = "ps" ]; then\n'
        f"  printf %s {_sh_quote(ps_stdout)}\n"
        f"  printf %s {_sh_quote(ps_stderr)} >&2\n"
        f"  exit {ps_exit}\n"
        "fi\n"
        "exit 0\n"
    )
    script.chmod(0o755)


def _sh_quote(value: str) -> str:
    return "'" + value.replace("'", "'\\''") + "'"


def _run_ch_probe_only(
    tmp_path: Path, *, extra_env: dict[str, str]
) -> subprocess.CompletedProcess:
    env = {"PATH": f"{tmp_path}:{_BASE_PATH}"}
    env.update(extra_env)
    return subprocess.run(
        ["bash", str(SCRIPT), "--ch-probe-only"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=_TIMEOUT,
    )


# --- (i) The exact CHAOS-3571 defect: a FAILED probe must never read as ------------
#         "not running", and must never let the gate pass. --------------------------


def test_docker_ps_failure_is_reported_as_a_failed_probe_not_a_confirmed_absence(
    tmp_path,
):
    """Plant the exact CHAOS-3571 mechanism: ``docker`` is on PATH and
    ``command -v docker`` succeeds, but ``docker ps`` itself exits nonzero
    (simulating the daemon being unreachable/overloaded, exactly as the ticket
    describes -- "most likely a docker CLI call timing out under load"). This
    must be reported as an INDETERMINATE probe failure, distinctly worded from
    "not running", and must be a hard failure (nonzero exit), never rc=0/skip.

    RED proof against the pre-fix code (not re-asserted here, see the PR body):
    sourcing origin/main's ``ch_provision()`` directly against this identical
    stub prints "container '...' not running" and returns rc=0 with FAILED=0
    -- the fabricated claim this test exists to forbid.
    """
    _write_fake_docker(
        tmp_path,
        ps_exit=1,
        ps_stdout="",
        ps_stderr="Cannot connect to the Docker daemon at unix:///var/run/docker.sock. Is the docker daemon running?\n",
    )
    result = _run_ch_probe_only(
        tmp_path, extra_env={"CH_CONTAINER": "dev-health-clickhouse-1"}
    )
    combined = result.stdout + result.stderr

    assert result.returncode == 2, combined
    assert "rc=2" in combined, combined
    assert "probe FAILED" in combined, combined
    assert "UNKNOWN" in combined, combined
    assert "not running" not in combined, (
        f"the FABRICATED claim from the CHAOS-3571 defect must never appear when "
        f"the probe itself failed.\n{combined}"
    )
    assert "Cannot connect to the Docker daemon" in combined, (
        f"the probe's real stderr must be surfaced verbatim, not swallowed.\n{combined}"
    )


def test_docker_absent_from_path_is_a_distinct_hard_failure(tmp_path):
    """No ``docker`` binary at all is its own, differently-worded case --
    never confused with a probe that ran and failed.

    Uses `_path_guaranteed_docker_free`, NOT the `_BASE_PATH`-based
    `_run_ch_probe_only` helper every other test in this file uses: this is
    the one case that specifically needs docker to be verifiably absent, and
    `_BASE_PATH` alone does not guarantee that on every host (see that
    helper's docstring).
    """
    result = subprocess.run(
        ["bash", str(SCRIPT), "--ch-probe-only"],
        cwd=ROOT,
        env={"PATH": _path_guaranteed_docker_free(tmp_path / "no-docker-bin")},
        capture_output=True,
        text=True,
        timeout=_TIMEOUT,
    )
    combined = result.stdout + result.stderr

    assert result.returncode == 1, combined
    assert "docker CLI not found on PATH" in combined, combined
    assert "probe FAILED" not in combined, combined


def test_container_confirmed_absent_is_still_a_hard_failure_not_a_silent_skip(
    tmp_path,
):
    """CHAOS-3571 (a): 'docker down' must HARD FAIL, never fall back to a
    silent skip -- even when ``docker ps`` genuinely, cleanly confirms the
    container is absent (as opposed to the probe itself failing, covered
    above). The only sanctioned escape hatch is the explicit SKIP_CLICKHOUSE=1
    opt-out (covered by test_skip_clickhouse_reduces_the_declared_set_exactly
    below), not a runtime probe result, however clean.
    """
    _write_fake_docker(
        tmp_path, ps_exit=0, ps_stdout="some-unrelated-container\n", ps_stderr=""
    )
    result = _run_ch_probe_only(
        tmp_path, extra_env={"CH_CONTAINER": "dev-health-clickhouse-1"}
    )
    combined = result.stdout + result.stderr

    assert result.returncode == 3, combined
    assert "confirmed NOT running" in combined, combined
    # Distinct from the probe-failure wording -- a human reading this must be
    # told the true mechanism, not a guess.
    assert "probe FAILED" not in combined, combined


def test_devhops_missing_is_a_distinct_hard_failure(tmp_path):
    """Container reachable, but the dev-hops CLI this stage shells out to is
    missing from the venv -- also a hard failure (CHAOS-3571 (a): 'missing
    dependency'), with its own distinguishing message."""
    _write_fake_docker(
        tmp_path, ps_exit=0, ps_stdout="dev-health-clickhouse-1\n", ps_stderr=""
    )
    result = _run_ch_probe_only(
        tmp_path,
        extra_env={
            "CH_CONTAINER": "dev-health-clickhouse-1",
            "DEVHOPS": str(tmp_path / "no-such-dev-hops"),
        },
    )
    combined = result.stdout + result.stderr

    assert result.returncode == 4, combined
    assert "dev-hops CLI missing" in combined, combined


def test_container_reachable_probe_succeeds(tmp_path):
    """Sanity check for the tests above: a genuinely healthy probe (container
    present, dev-hops present) returns 0 -- the stub harness itself is not
    what is forcing every other test's failure."""
    _write_fake_docker(
        tmp_path, ps_exit=0, ps_stdout="dev-health-clickhouse-1\n", ps_stderr=""
    )
    devhops = tmp_path / "dev-hops"
    devhops.write_text("#!/bin/bash\nexit 0\n")
    devhops.chmod(0o755)
    result = _run_ch_probe_only(
        tmp_path,
        extra_env={"CH_CONTAINER": "dev-health-clickhouse-1", "DEVHOPS": str(devhops)},
    )
    combined = result.stdout + result.stderr
    assert result.returncode == 0, combined
    assert "rc=0" in combined, combined


# --- (ii) The stage manifest: executed must equal declared, structurally. ----------


def test_full_run_executed_stages_equal_the_full_declared_set(tmp_path):
    """With every leaf gate stubbed to pass instantly (no lint/mypy/pytest/
    docker/ClickHouse actually run -- see the `--stage-manifest-probe` hook's
    own header comment in local_validate.sh), a clean run's executed-stage-id
    set must equal the full 9-stage declared set, and the verdict line +
    machine-readable manifest line must say so explicitly.
    """
    result = subprocess.run(
        ["bash", str(SCRIPT), "--stage-manifest-probe"],
        cwd=ROOT,
        env={"PATH": _BASE_PATH},
        capture_output=True,
        text=True,
        timeout=_TIMEOUT,
    )
    combined = result.stdout + result.stderr

    assert result.returncode == 0, combined
    assert "GATE PASSED." in combined, combined
    assert "GATE_STAGE_MANIFEST result=PASSED declared=9 executed=9" in combined, (
        combined
    )
    assert "[9/9:" in combined, combined
    for stage_id in (
        "lint_format",
        "lint_check",
        "typecheck",
        "ch_probe",
        "ch_scratch_create",
        "ch_migrate",
        "metrics_readback",
        "unit_suite",
        "ch_argmax_proof",
    ):
        assert stage_id in combined, (
            f"missing declared stage id {stage_id!r}.\n{combined}"
        )


def test_skip_clickhouse_reduces_the_declared_set_exactly(tmp_path):
    """SKIP_CLICKHOUSE=1 is the ONLY sanctioned way to shrink the declared set
    -- it must do so exactly (4/4, the 4 non-CH stage ids), never silently
    leaving a CH-dependent id in the declaration with nothing to execute it."""
    result = subprocess.run(
        ["bash", str(SCRIPT), "--stage-manifest-probe"],
        cwd=ROOT,
        env={"PATH": _BASE_PATH, "SKIP_CLICKHOUSE": "1"},
        capture_output=True,
        text=True,
        timeout=_TIMEOUT,
    )
    combined = result.stdout + result.stderr

    assert result.returncode == 0, combined
    assert "GATE_STAGE_MANIFEST result=PASSED declared=4 executed=4" in combined, (
        combined
    )
    assert "[4/4:" in combined, combined
    for stage_id in ("ch_probe", "ch_scratch_create", "ch_migrate", "ch_argmax_proof"):
        assert stage_id not in combined, (
            f"CH stage id {stage_id!r} must not appear in a SKIP_CLICKHOUSE=1 "
            f"manifest.\n{combined}"
        )


def test_verify_stage_manifest_refuses_a_declared_but_not_executed_gap(tmp_path):
    """The structural backstop itself: hand verify_stage_manifest() a declared
    set with one id that never made it into the executed set, and confirm it
    fails loudly by name -- even though nothing about run_stage()/
    ch_provision()'s pass/fail bookkeeping is exercised here at all. This is
    what actually makes "mismatch fails even if every executed stage passed"
    true, independent of every individual stage's own correctness.
    """
    result = subprocess.run(
        ["bash", str(SCRIPT), "--stage-manifest-mismatch-probe", "a,b,c", "a,b"],
        cwd=ROOT,
        env={"PATH": _BASE_PATH},
        capture_output=True,
        text=True,
        timeout=_TIMEOUT,
    )
    combined = result.stdout + result.stderr

    assert result.returncode == 1, combined
    assert "GATE FAILED." in combined, combined
    assert "stage-manifest self-check" in combined, combined
    assert "declared-but-not-executed=[c]" in combined, combined
    assert "GATE_STAGE_MANIFEST result=FAILED declared=3 executed=2" in combined, (
        combined
    )
    assert "no mismatch detected" not in combined, (
        f"the probe's own 'unexpected' fallback message must never print -- "
        f"verify_stage_manifest must have exited the process first.\n{combined}"
    )


def test_verify_stage_manifest_accepts_an_exact_match(tmp_path):
    """Sanity check for the mismatch test above: an exactly-matching
    declared/executed pair must NOT be flagged."""
    result = subprocess.run(
        ["bash", str(SCRIPT), "--stage-manifest-mismatch-probe", "a,b,c", "a,b,c"],
        cwd=ROOT,
        env={"PATH": _BASE_PATH},
        capture_output=True,
        text=True,
        timeout=_TIMEOUT,
    )
    combined = result.stdout + result.stderr
    assert result.returncode == 0, combined
    assert "no mismatch detected" in combined, combined
