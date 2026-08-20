"""Public command contract for the Go quality-gate wrapper."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

PUBLIC_VERBS = (
    "fmt",
    "vet",
    "test",
    "race",
    "live-python-oracles",
    "build",
    "contract",
    "multi-replica-workers",
    "integration-vet",
    "integration-coverage",
    "integration-shard-plan",
    "integration-prepull",
    "integration-shard",
    "integration",
    "fast",
    "ci",
    "all",
)

# CHAOS-3948: the composite verbs' exact step sequence, keyed to what `case
# "${1:-all}"` in ci/check_go.sh actually runs -- not just documented in
# usage(), asserted against the live source below. `ci` exists so
# go.yml's go-quality step keeps byte-for-byte the pre-CHAOS-3948 `all`
# coverage while `all` itself became the honest full local signal (it now
# runs check_integration for real instead of only planning it). The model
# below IS the parity table: `ci` must equal `fast` plus `check_race`, and
# `all` must equal `ci` with its two plan-only integration calls replaced by
# one real check_integration call -- anything else is a coverage regression
# in one verb or the other.
_FAST_STEPS = (
    "check_format",
    "check_vet",
    "check_test",
    "check_live_python_oracles",
    "check_build",
    "check_contract",
    "check_integration_vet",
    "plan_integration_shards",
    "plan_providersync_test_shards",
    "check_multi_replica_workers",
)
_CI_STEPS = (
    "check_format",
    "check_vet",
    "check_test",
    "check_race",
    "check_live_python_oracles",
    "check_build",
    "check_contract",
    "check_integration_vet",
    "plan_integration_shards",
    "plan_providersync_test_shards",
    "check_multi_replica_workers",
)
_ALL_STEPS = (
    "check_format",
    "check_vet",
    "check_test",
    "check_race",
    "check_live_python_oracles",
    "check_build",
    "check_contract",
    "check_integration_vet",
    "check_integration",
    "check_multi_replica_workers",
)
VERB_STEPS = {"fast": _FAST_STEPS, "ci": _CI_STEPS, "all": _ALL_STEPS}


def _case_block_body(source: str, verb: str) -> str:
    """The exact statement lines inside `  <verb>)` ... `    ;;` in the
    trailing `case "${1:-all}" in ... esac` dispatcher -- not the first
    match of the verb name anywhere in the file (a verb name can also appear
    in a comment or an earlier function body)."""

    dispatcher = source.split('case "${1:-all}" in', 1)[1]
    match = re.search(rf"\n  {re.escape(verb)}\)\n(.*?)\n    ;;", dispatcher, re.DOTALL)
    assert match, f"no `{verb})` case block found in the dispatcher"
    return match.group(1)


def _bare_call_lines(block: str) -> tuple[str, ...]:
    """Bare 4-space-indented `identifier` statement lines -- ignores
    comments, blank lines, and anything with arguments/braces so a stray
    guard clause doesn't get mistaken for a step."""

    return tuple(
        stripped
        for line in block.splitlines()
        if line.startswith("    ")
        and (stripped := line.strip())
        and re.fullmatch(r"[a-z_][a-z0-9_]*", stripped)
    )


def test_composite_verbs_match_the_checked_in_parity_table() -> None:
    source = (ROOT / "ci" / "check_go.sh").read_text(encoding="utf-8")
    for verb, expected_steps in VERB_STEPS.items():
        actual_steps = _bare_call_lines(_case_block_body(source, verb))
        assert actual_steps == expected_steps, (
            f"`{verb}` case block runs {actual_steps!r}, expected "
            f"{expected_steps!r} -- update VERB_STEPS deliberately if this "
            "verb's coverage is meant to change, don't just silence this"
        )


def test_ci_is_fast_plus_the_race_detector() -> None:
    # fast was NOT an equivalent stand-in for the pre-CHAOS-3948 `all` when
    # go.yml's go-quality step needed one: it silently drops the race
    # detector. Pinning the relationship directly, not just each verb's own
    # step list, so that invariant can't erode one verb at a time.
    assert set(_CI_STEPS) - set(_FAST_STEPS) == {"check_race"}
    assert set(_FAST_STEPS) - set(_CI_STEPS) == set()


def test_all_is_ci_with_integration_actually_executed() -> None:
    # `all`'s whole point (CHAOS-3948) is trading `ci`'s two plan-only calls
    # for one call that actually runs the suite -- everything else `ci`
    # covers must still be covered.
    assert set(_CI_STEPS) - set(_ALL_STEPS) == {
        "plan_integration_shards",
        "plan_providersync_test_shards",
    }
    assert set(_ALL_STEPS) - set(_CI_STEPS) == {"check_integration"}


def test_help_completes_and_documents_every_public_verb() -> None:
    """Help must not deadlock while Bash prepares its output transport."""

    result = subprocess.run(
        ["bash", "ci/check_go.sh", "--help"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    # The Go toolchain directive may fetch a newer patch release on a cold
    # cache and announces it on stderr ("go: downloading go1.25.9 ...").
    # That is environment noise, not help output — anything else on stderr
    # still fails.
    unexpected_stderr = [
        line
        for line in result.stderr.splitlines()
        if line.strip() and not line.startswith("go: downloading ")
    ]
    assert unexpected_stderr == []
    documented_verbs = {
        line.split(maxsplit=1)[0]
        for line in result.stdout.splitlines()
        if line.startswith("  ") and line.strip()
    }
    assert set(PUBLIC_VERBS) <= documented_verbs


def test_multi_replica_gate_is_cold_measured_and_required() -> None:
    source = (ROOT / "ci" / "check_go.sh").read_text(encoding="utf-8")
    function = source.split("check_multi_replica_workers() {", 1)[1].split("\n}", 1)[0]

    assert "-tags=integration -count=1" in function
    assert "DEV_HEALTH_MULTI_REPLICA_PROOF" in function
    assert "measured zero jobs" in function
    # One public verb plus the required fast, ci, and all paths (CHAOS-3948
    # added `ci`) -- also asserted per-verb in VERB_STEPS above, but this
    # catches a stray extra/missing call site anywhere else in the file.
    assert source.count("    check_multi_replica_workers\n") == 4
