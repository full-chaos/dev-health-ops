"""Regression coverage for the CHAOS-3362 here-document pipe wedge.

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
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "ci" / "local_validate.sh"

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


def _argmax_program(text: str) -> str:
    match = re.search(r"^ARGMAX_PROOF_PY='(.*?)'\n", text, re.S | re.M)
    assert match is not None, (
        "ARGMAX_PROOF_PY assignment not found in ci/local_validate.sh. Either the"
        " argMax proof program was renamed, or -- the case this test exists for --"
        " it was converted back into a here-document. Do not delete this test to"
        " make it pass; see the module docstring."
    )
    return match.group(1)


def test_local_validate_has_no_heredoc_over_the_measured_pipe_budget() -> None:
    """No here-document in the gate script may exceed the measured budget.

    Guards the whole class, not just the one call site that wedged: any stage
    that grows a here-document past ~400 bytes reintroduces the same hang.
    """

    assert SCRIPT.is_file(), f"gate script missing at {SCRIPT}"
    text = SCRIPT.read_text(encoding="utf-8")
    # The scan must have something to scan: an empty/renamed script would
    # otherwise sail through with zero findings and read as coverage.
    assert len(text) > 10_000, f"{SCRIPT} is unexpectedly small ({len(text)} bytes)"

    oversized = [
        (line, delimiter, len(body))
        for line, delimiter, body in _heredocs(text)
        if len(body) >= _HEREDOC_BUDGET_BYTES
    ]
    assert not oversized, (
        "here-document(s) at or above the measured pipe budget of "
        f"{_HEREDOC_BUDGET_BYTES} bytes: {oversized}. Bash writes a here-document"
        " into a pipe it also holds the read end of, so this hangs the gate"
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
