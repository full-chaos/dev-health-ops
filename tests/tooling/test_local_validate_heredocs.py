"""Regression coverage for the CHAOS-3362 here-document pipe wedge.

The budget scan covers **every** ``ci/*.sh`` script, not just the one that wedged
(CHAOS-3489). Two more sites of this exact class survived CHAOS-3362 because that
changeset was scoped to the gate script: ``ci/run_tests.sh`` fed its 800-byte
``usage()`` text through ``cat <<EOF``, and ``ci/run_live_backend_e2e.sh`` fed
seven programs to Python through ``run_python - <<PY`` (two of them over the
budget, at 2240 and 1201 bytes). Scanning the whole directory closes the class
instead of the sites: a new script, or a new stage in an existing one, is covered
the moment it lands.

``ci/local_validate.sh`` used to feed its argMax proof program to Python as a
1269-byte here-document. Bash delivers a here-document by writing it into a pipe
and only THEN forking the reader, so the writing shell transiently holds both
ends. If the document does not fit in the pipe buffer the write never completes
-- nothing is draining, and the writer owning the read end cannot even receive
EPIPE. The gate hung there forever, before ClickHouse or Python were ever
reached (CHAOS-3362 forensics: writer subshell in ``write()`` on 1802 of 1808
samples, fd 3 and fd 4 both on the same pipe, no Python child).

Measured on the affected host with ``cat >/dev/null <<EOF``::

     400 bytes -> completes in 0.3s
     512 bytes -> BLOCKS
    1024 bytes -> BLOCKS
    4000 bytes -> BLOCKS

``lsof`` reports those same pipes with the nominal 16384-byte capacity. Nominal
is not actual: macOS hands out a small pipe buffer and defers expansion under
kernel pipe-memory pressure, which a host running many concurrent agent sessions
sits in persistently. So "the here-document is small, the pipe is fine" is not a
defense at any size worth writing, which is why the budget below is 400 bytes --
the largest size actually measured to complete -- and not a comfortable-looking
round number derived from the nominal capacity.

Same class as CHAOS-3468 (lock-test probes blocking in ``write()`` at ~370 bytes
of output) and CHAOS-3348 (``ci/check_go.sh``, since converted away from the
here-string seam it used -- see ``test_check_go_integration_coverage.py``).
"""

from __future__ import annotations

import ast
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CI_DIR = ROOT / "ci"
SCRIPT = ROOT / "ci" / "local_validate.sh"

# Scripts the budget scan must have actually read. A bare `glob` that silently
# matched nothing -- moved directory, renamed extension -- would report zero
# oversized here-documents and read as coverage, so the scan asserts these are
# present rather than trusting whatever the glob returns.
_REQUIRED_SCRIPTS = (
    "check_go.sh",
    "check_go_containers.sh",
    "check_river_compat_static.sh",
    "local_validate.sh",
    "run_live_backend_e2e.sh",
    "run_tests.sh",
)

# Largest here-document size measured to complete on the affected host. Anything
# at or above this has been measured to block forever there.
_HEREDOC_BUDGET_BYTES = 400

# `cmd <<EOF`, `cmd <<'EOF'`, `cmd <<-"EOF"`. The negative lookahead keeps
# here-STRINGS (`<<<`) out: their content is a runtime expansion, not literal
# text this scanner could size.
_HEREDOC_START = re.compile(r"<<-?\s*(?!<)([\"']?)([A-Za-z_][A-Za-z0-9_]*)\1")


def _heredocs(text: str) -> list[tuple[int, str, bytes]]:
    """Every here-document in ``text`` as ``(line_number, delimiter, body)``."""

    lines = text.split("\n")
    found: list[tuple[int, str, bytes]] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        # Whole-line comments are prose, not redirections -- this very repo's
        # scripts document the hazard by quoting `cat >file <<EOF` in a comment.
        if line.lstrip().startswith("#"):
            index += 1
            continue
        match = _HEREDOC_START.search(line)
        if match is None:
            index += 1
            continue
        delimiter = match.group(2)
        body: list[str] = []
        cursor = index + 1
        while cursor < len(lines) and lines[cursor].strip() != delimiter:
            body.append(lines[cursor])
            cursor += 1
        # An unterminated here-document means this scanner mis-parsed the
        # script. Fail loudly: silently swallowing the rest of the file would
        # make every later here-document invisible to the budget check.
        assert cursor < len(lines), (
            f"unterminated here-document <<{delimiter} opened at line"
            f" {index + 1}; the scanner in this test needs fixing"
        )
        found.append((index + 1, delimiter, ("\n".join(body) + "\n").encode("utf-8")))
        index = cursor + 1
    return found


def _shell_string_programs(text: str, prefix: str) -> dict[str, str]:
    """Every ``PREFIX_NAME='...'`` single-quoted shell string in ``text``.

    A single-quoted shell string cannot contain a single quote, so the closing
    quote is unambiguous: the first ``'`` at the start of a line.
    """

    pattern = re.compile(rf"^({prefix}[A-Z0-9_]+)='(.*?)^'$", re.S | re.M)
    return {match.group(1): match.group(2) for match in pattern.finditer(text)}


def _argmax_program(text: str) -> str:
    match = re.search(r"^ARGMAX_PROOF_PY='(.*?)'\n", text, re.S | re.M)
    assert match is not None, (
        "ARGMAX_PROOF_PY assignment not found in ci/local_validate.sh. Either the"
        " argMax proof program was renamed, or -- the case this test exists for --"
        " it was converted back into a here-document. Do not delete this test to"
        " make it pass; see the module docstring."
    )
    return match.group(1)


def test_scanner_flags_an_oversized_heredoc_and_ignores_here_strings() -> None:
    """The scanner itself must still be able to fail (CHAOS-3489).

    A directory-wide scan whose regex has rotted reports "no findings" over
    every script and reads exactly like a clean bill of health. So plant the
    defect the scan exists to catch and observe it caught, and plant the two
    shapes it must NOT catch -- a here-STRING, whose content is a runtime
    expansion this scanner cannot size, and a here-document quoted inside a
    comment, which is prose rather than a redirection.
    """

    oversized_body = "x" * (_HEREDOC_BUDGET_BYTES + 1)
    planted = f"cat <<'EOF'\n{oversized_body}\nEOF\n"
    found = _heredocs(planted)
    assert [(line, delim) for line, delim, _ in found] == [(1, "EOF")], found
    assert len(found[0][2]) > _HEREDOC_BUDGET_BYTES

    # Here-STRING: same delivery mechanism, but the content is a runtime value
    # (ci/local_validate.sh:374 is a ~130-byte pid|lstart|cwd triple), so it is
    # deliberately out of scope -- see the module docstring.
    assert _heredocs('read -r a b c <<<"${target}"\n') == []
    # Prose, not a redirection: these scripts document the hazard by quoting it.
    assert _heredocs("  # Using `cat >file <<EOF` would reintroduce the pipe\n") == []


def _tracked_ci_scripts() -> list[Path]:
    """Every ``ci/*.sh`` git knows about, tracked or staged.

    NOT ``CI_DIR.glob("*.sh")``. That scans whatever happens to be sitting in
    the directory, so a developer's scratch copy of a script -- including a
    copy of a PRE-FIX script kept around to compare against -- turns this into
    a red gate about a file the repository does not contain. Adversarial review
    hit exactly that. ``git ls-files`` also covers the index, so a genuinely
    new script is scanned from the moment it is staged, which is before any
    commit hook or CI run can see it.
    """

    result = subprocess.run(  # noqa: S603
        ["git", "ls-files", "-z", "--", "ci/*.sh"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    # A failure here must not degrade into "nothing to scan": that reports zero
    # findings and reads exactly like a clean result.
    assert result.returncode == 0, (
        f"git ls-files failed ({result.returncode}) in {ROOT}: {result.stderr}."
        " The here-document budget scan cannot enumerate what to read, so it"
        " must fail rather than report no findings."
    )
    return [ROOT / name for name in result.stdout.split("\0") if name]


def test_no_ci_script_has_a_heredoc_over_the_measured_pipe_budget() -> None:
    """No here-document under ``ci/`` may exceed the measured budget.

    Guards the whole class over the whole directory, not the three call sites
    that were found wedged (CHAOS-3362, CHAOS-3489): any script that grows a
    here-document past ~400 bytes reintroduces the same hang, and every script
    in here runs on developer machines where the small pipe buffer is real.
    """

    scripts = sorted(_tracked_ci_scripts())
    names = {path.name for path in scripts}
    missing = [name for name in _REQUIRED_SCRIPTS if name not in names]
    assert not missing, (
        f"the ci/ here-document scan found no {missing} to scan (git ls-files"
        f" returned {sorted(names) or 'nothing'} under {CI_DIR}). A scan that"
        " reads nothing reports no findings and looks identical to a clean"
        " result -- fix the path or update _REQUIRED_SCRIPTS deliberately, do"
        " not let it pass."
    )

    oversized: list[tuple[str, int, str, int]] = []
    for path in scripts:
        text = path.read_text(encoding="utf-8")
        # Each script must have real content behind it, for the same reason.
        assert len(text) > 500, f"{path} is unexpectedly small ({len(text)} bytes)"
        oversized.extend(
            (path.name, line, delimiter, len(body))
            for line, delimiter, body in _heredocs(text)
            if len(body) >= _HEREDOC_BUDGET_BYTES
        )

    assert not oversized, (
        "here-document(s) at or above the measured pipe budget of "
        f"{_HEREDOC_BUDGET_BYTES} bytes: {oversized}. Bash writes a here-document"
        " into a pipe it also holds the read end of, so this hangs the script"
        " forever on hosts with a small effective pipe buffer (CHAOS-3362)."
        " Write the payload to a temp file with the printf BUILTIN instead --"
        " `cat >file <<EOF` is the same pipe and does NOT fix it."
    )


def test_argmax_proof_program_is_valid_python() -> None:
    """The program is now shell-string data, so nothing else type-checks it."""

    compile(
        _argmax_program(SCRIPT.read_text(encoding="utf-8")), "argmax_proof.py", "exec"
    )


def test_argmax_proof_program_awaits_the_loader_call() -> None:
    """The loader call must be AWAITED, not merely present in the text.

    Found by adversarial review and then reproduced: delete the ``await`` and
    the program builds a coroutine it never runs. No query reaches ClickHouse,
    Python exits 0, and the stage prints ``argMax live-exec OK -- context
    loaded (candidate buckets: coroutine)``. Measured directly against the real
    engine: the stage returned 0 with the ClickHouse query never executed. A
    stage that reports success without measuring anything is worse than a
    missing stage, because the green reads as proof.

    Checked through the AST rather than by searching for the substring
    ``await``: the substring appears elsewhere in the program, so a text match
    would keep passing with the await moved or removed from THIS call.
    """

    tree = ast.parse(_argmax_program(SCRIPT.read_text(encoding="utf-8")))
    awaited = {
        node.value.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Await)
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Attribute)
    }
    assert "load_team_attribution_context" in awaited, (
        "the argMax proof program does not AWAIT load_team_attribution_context."
        f" Awaited method calls found: {sorted(awaited) or 'none'}. Without the"
        " await the coroutine never runs, ClickHouse is never queried, and the"
        " stage reports OK anyway."
    )


def test_argmax_proof_program_contains_no_single_quote() -> None:
    """A single quote would silently truncate the single-quoted shell string.

    Bash would end the assignment early and try to execute the remainder as
    commands, so this is a syntax break rather than a subtle bug -- but it is
    cheap to catch here rather than in the middle of a gate run.
    """

    program = _argmax_program(SCRIPT.read_text(encoding="utf-8"))
    assert "'" not in program, (
        "ARGMAX_PROOF_PY is a single-quoted shell string; the Python program"
        " inside it must not contain a single-quote character"
    )


# --- ci/run_live_backend_e2e.sh (CHAOS-3489) -------------------------------

E2E_SCRIPT = ROOT / "ci" / "run_live_backend_e2e.sh"

# The seven programs the harness used to feed Python through `run_python - <<PY`.
# Named explicitly rather than counted: a rename or a quiet deletion should fail
# here and be re-checked, not pass because "seven of something" were found.
_E2E_PROGRAMS = {
    "PY_PROG_REDIS_PING": "redis_ping.py",
    "PY_PROG_AUTH_TOKEN": "auth_token.py",
    "PY_PROG_READY_HEALTH": "ready_health.py",
    "PY_PROG_JWT_SECRET": "jwt_secret.py",
    "PY_PROG_ASSERT_HEALTH": "assert_health.py",
    "PY_PROG_ASSERT_META": "assert_meta.py",
    "PY_PROG_ASSERT_HOME": "assert_home.py",
}


def test_e2e_harness_programs_are_all_present() -> None:
    """Every converted program is still a shell string, written, and used.

    Three separate things have to line up for a stage to work, and each fails
    silently on its own: the program string exists, ``write_py_programs``
    materializes it under ``PY_PROGRAM_DIR``, and some stage hands Python that
    path. A program that is defined but never written produces a "can't open
    file" at whatever hour that stage runs.
    """

    text = E2E_SCRIPT.read_text(encoding="utf-8")
    programs = _shell_string_programs(text, "PY_PROG_")
    assert set(programs) == set(_E2E_PROGRAMS), (
        f"expected shell-string programs {sorted(_E2E_PROGRAMS)}, found"
        f" {sorted(programs)} in {E2E_SCRIPT}. If a stage was legitimately"
        " added or removed, update _E2E_PROGRAMS deliberately -- do not let a"
        " program silently revert to a here-document."
    )
    for variable, filename in _E2E_PROGRAMS.items():
        assert f'write_py_program {filename} "${{{variable}}}"' in text, (
            f"{variable} is defined but write_py_programs() never writes it to"
            f" {filename}; the stage using it would fail to open the file"
        )
        assert f'"${{PY_PROGRAM_DIR}}/{filename}"' in text, (
            f"{filename} is written but no stage runs it; a materialized"
            " program nothing executes is dead weight, and more likely means"
            " the call site was missed"
        )


def test_e2e_harness_programs_are_valid_python() -> None:
    """They are shell-string data now, so nothing else parses them."""

    programs = _shell_string_programs(
        E2E_SCRIPT.read_text(encoding="utf-8"), "PY_PROG_"
    )
    assert programs, f"no PY_PROG_* shell strings found in {E2E_SCRIPT}"
    for variable, program in sorted(programs.items()):
        compile(program, f"{variable}.py", "exec")


def test_e2e_harness_programs_contain_no_single_quote() -> None:
    """A single quote would end the shell string early and break the script.

    This is why the users INSERT binds ``auth_provider`` as a parameter instead
    of carrying the SQL literal it used to: the literal cannot be spelled here.
    Verified against a live PostgreSQL before the swap -- both spellings produce
    the same row, same column, same type.
    """

    programs = _shell_string_programs(
        E2E_SCRIPT.read_text(encoding="utf-8"), "PY_PROG_"
    )
    assert programs, f"no PY_PROG_* shell strings found in {E2E_SCRIPT}"
    offenders = sorted(name for name, program in programs.items() if "'" in program)
    assert not offenders, (
        f"{offenders} are single-quoted shell strings containing a single-quote"
        " character; bash ends the assignment there and tries to run the rest"
        " as commands"
    )


# --- ci/run_tests.sh (CHAOS-3489) ------------------------------------------

RUN_TESTS_SCRIPT = ROOT / "ci" / "run_tests.sh"


def test_run_tests_usage_still_documents_every_tier() -> None:
    """usage() must still emit the text it used to, by running it.

    The conversion was verified byte-for-byte against the here-document output
    at the time. This guards the text from here on -- note it can only catch
    drift, NOT the wedge: on a host with a normal pipe buffer the here-document
    version passes this too. The budget scan above is what catches the wedge.
    """

    completed = subprocess.run(  # noqa: S603
        ["bash", str(RUN_TESTS_SCRIPT)],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    # No argument -> usage on stdout, EXIT_USAGE. Nothing on stderr.
    assert completed.returncode == 2, completed
    assert completed.stderr == "", completed.stderr
    output = completed.stdout
    assert output.startswith("Usage: ci/run_tests.sh <tier>\n"), output
    assert output.endswith("on one worker).\n"), output[-80:]
    for tier in ("unit", "integration", "e2e", "live-e2e", "ci"):
        assert f"\n  {tier} " in output, (tier, output)
    for knob in (
        "PYTEST_SINGLE_RETRY=1",
        "PYTEST_DURATIONS=25",
        "TEST_RESULTS_DIR=...",
        "PYTEST_XDIST_WORKERS=auto",
        "PYTEST_DIST_MODE=loadscope",
    ):
        assert knob in output, (knob, output)
