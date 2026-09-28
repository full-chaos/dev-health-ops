"""CHAOS-7023 P3 (#3369 r1): the chart's pre-upgrade carry script actually EXECUTES its
own control flow -- proven by running it, not by inspecting its text.

The r1 reviewer found test_helm_routing_carry_hooks.py's carry-script tests only check
that certain substrings are PRESENT in the rendered script -- planting `if false && ...`
in front of the repoint-then-retry fallback still left `13 passed`, because a mutation
that makes a branch UNREACHABLE does not remove the TEXT of that branch from the script.

This module extracts the real rendered script (the same `_script()` helper the text-only
suite uses) and RUNS it under `sh` with a stubbed `dho` on PATH, asserting the actual
CONTROL FLOW: which `dho` subcommands actually get invoked, in what order, and what the
script's own exit code and stdout end up being. A gutted fallback is caught here because
the stubbed `dho repoint`/retry `dho carry` calls this module expects simply never happen.

Not real-dho-binary end to end (unlike CHAOS-7022's test_bigboy_cut_carry_refusal_text_
behavior.py) -- this module's job is proving the CHART SCRIPT's shell control flow is
real, not re-proving the Go CLI's own -json contract, which that sibling module (and
internal/goapicli/routing/carry_integration_test.go) already cover for the identical
verbs this script calls.
"""

from __future__ import annotations

import stat
import subprocess
import tempfile
from pathlib import Path

from test_helm_routing_carry_hooks import _ENABLED, _CARRY, _jobs, _script


def _run_script(
    tmp_path: Path, *, dho_plan: list[tuple[str, int, str]]
) -> subprocess.CompletedProcess[str]:
    """dho_plan: one (expected verb substring, exit_code, stdout) tuple per expected `dho`
    invocation, in order. The stub asserts each call's argv contains its expected verb
    substring (e.g. "routing carry", "routing repoint", "mint envelope") -- a wrong-order
    or extra/missing call fails loudly instead of silently returning the wrong canned line.
    """
    script = _script(_jobs(*_ENABLED)[_CARRY])
    stub_bin = tmp_path / "stubbin"
    stub_bin.mkdir(exist_ok=True)
    call_log = tmp_path / "calls.txt"
    idx_file = tmp_path / "idx"
    idx_file.write_text("0")

    plan_lines = []
    for i, (verb, rc, out) in enumerate(dho_plan, start=1):
        out_file = tmp_path / f"out{i}.txt"
        out_file.write_text(out)
        plan_lines.append(f"{verb}\t{rc}\t{out_file}")
    plan_file = tmp_path / "plan.txt"
    plan_file.write_text("\n".join(plan_lines) + "\n")

    dho_stub = stub_bin / "dho"
    dho_stub.write_text(
        "#!/usr/bin/env bash\n"
        "set -u\n"
        f'echo "$*" >> {call_log}\n'
        f"idx=$(cat {idx_file})\n"
        "idx=$((idx + 1))\n"
        f"echo $idx > {idx_file}\n"
        f'line=$(sed -n "${{idx}}p" {plan_file})\n'
        "IFS=$"
        "'"
        "\\t"
        "'"
        ' read -r want_verb rc outfile <<< "$line"\n'
        'case " $* " in\n'
        '  *" $want_verb "*) ;;\n'
        "  *) echo \"dho stub: call $idx expected a '$want_verb' invocation, got: $*\" >&2; exit 99 ;;\n"
        "esac\n"
        # `mint envelope` must print a bearer on stdout (the script captures it into $BEARER
        # via command substitution); every other verb prints its plan payload.
        'if [ "$want_verb" = "mint envelope" ]; then echo "fake-bearer-token"; exit "$rc"; fi\n'
        'cat "$outfile"\n'
        'exit "$rc"\n'
    )
    dho_stub.chmod(dho_stub.stat().st_mode | stat.S_IEXEC)

    harness = tmp_path / "harness.sh"
    # No trailing sentinel line here: unlike bigboy-cut.sh's carry BLOCK (a fragment
    # embedded in a longer script that falls through to later steps), this chart script
    # IS the whole container entrypoint and calls `exit 0`/`exit 1` explicitly on every
    # branch -- a line after it would be genuinely unreachable, not a completion signal.
    # The process's own return code is the real signal.
    harness.write_text("#!/usr/bin/env bash\nset -u\n" + script + "\n")
    harness.chmod(harness.stat().st_mode | stat.S_IEXEC)

    env = {
        "PATH": f"{stub_bin}:/usr/bin:/bin",
        "MINT_ORG": "test-org",
        "QUERY_API_URL": "http://query-api:8090",
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


def test_digest_unchanged_reason_does_not_abort_and_calls_dho_exactly_once() -> None:
    with tempfile.TemporaryDirectory(prefix="chaos7023-carry-script-") as tmp:
        proc = _run_script(
            Path(tmp),
            dho_plan=[
                ("mint envelope", 0, ""),
                (
                    "routing carry",
                    1,
                    'GOAPI_ROUTING_JSON {"reason":"digest_unchanged"}\n',
                ),
            ],
        )
    assert proc.returncode == 0, (
        f"a digest-unchanged refusal must not abort -- rc={proc.returncode}, "
        f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    )
    assert "no schema-digest change" in proc.stdout


def test_stale_build_reason_triggers_repoint_then_a_real_second_carry_call() -> None:
    """The exact shape the r1 reviewer's `if false && ...` mutation broke: gutting this
    fallback means the stubbed repoint/retry `dho` invocations below are simply never
    called -- the dho stub's own call-count/order assertion (via dho_plan) catches that
    directly, independent of anything this test itself asserts about stdout.

    The script mints its envelope bearer ONCE at the top and reuses it for every
    subsequent `dho` call (carry, repoint, the retry carry) -- only one "mint envelope"
    entry belongs in the plan below, not one per verb call.
    """
    with tempfile.TemporaryDirectory(prefix="chaos7023-carry-script-") as tmp:
        proc = _run_script(
            Path(tmp),
            dho_plan=[
                ("mint envelope", 0, ""),
                ("routing carry", 1, 'GOAPI_ROUTING_JSON {"reason":"stale_build"}\n'),
                ("routing repoint", 0, 'GOAPI_ROUTING_JSON {"reason":"repointed"}\n'),
                ("routing carry", 0, 'GOAPI_ROUTING_JSON {"reason":"carried"}\n'),
            ],
        )
    assert proc.returncode == 0, (
        f"stale-build, repoint, then a successful retry must not abort -- "
        f"rc={proc.returncode}, stdout={proc.stdout!r} stderr={proc.stderr!r}"
    )
    assert "rows lag the actually-running build" in proc.stdout
    assert "OK after repoint-then-retry" in proc.stdout


def test_an_unrecognized_reason_still_aborts_the_upgrade() -> None:
    """Negative control: proves the two tests above exercise the REAL branches, not a stub
    that always succeeds regardless of the script's own logic."""
    with tempfile.TemporaryDirectory(prefix="chaos7023-carry-script-") as tmp:
        proc = _run_script(
            Path(tmp),
            dho_plan=[
                ("mint envelope", 0, ""),
                ("routing carry", 1, 'GOAPI_ROUTING_JSON {"reason":"refused"}\n'),
            ],
        )
    assert proc.returncode != 0
    assert "ABORTING upgrade" in proc.stdout
