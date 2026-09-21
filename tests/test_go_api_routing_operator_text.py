"""Every `go-api-routing enable` command an operator can read must parse.

The Go verb's flag set is the source of truth. A runbook, a checker hint or a
log line that names a flag the verb does not define (or passes operations as
separate words) sends an operator into an exit-2 refusal exactly when they are
recovering from an outage, so the text is checked against the verb's own flag
definitions rather than by wording.
"""

from __future__ import annotations

import re
import shlex
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
VERB_SOURCES = (
    ROOT / "cmd" / "go-api-routing" / "enable.go",
    ROOT / "cmd" / "go-api-routing" / "main.go",
)
FLAG_DEFINITION = re.compile(
    r"\.(?:String|Bool|Int|Duration)Var\(&[\w.]+,\s*\"([a-z][a-z-]*)\""
)
COMMAND = re.compile(
    r"^\s*[\"']?\s*(?:[A-Z_]+=\S+\s+)*(?:SDL:\s+)?(go-api-routing enable\b)"
)
BOOLEAN_FLAGS = {"dry-run"}


def _defined_flags() -> set[str]:
    flags: set[str] = set()
    for source in VERB_SOURCES:
        flags.update(FLAG_DEFINITION.findall(source.read_text(encoding="utf-8")))
    return flags


def _commands(text: str) -> list[str]:
    """Each `go-api-routing enable` invocation, continuation lines joined."""
    lines = text.splitlines()
    found: list[str] = []
    for index, line in enumerate(lines):
        match = COMMAND.search(line)
        if match is None:
            continue
        command = line[match.start(1) :]
        cursor = index
        while command.rstrip().endswith("\\") and cursor + 1 < len(lines):
            cursor += 1
            command = command.rstrip()[:-1] + " " + lines[cursor].strip()
        found.append(command)
    return found


def _sources() -> list[Path]:
    paths = sorted((ROOT / "docs").rglob("*.md"))
    paths.append(ROOT / "ci" / "check_go_api_routing_digest.py")
    return paths


def test_the_verb_defines_the_flags_the_text_relies_on() -> None:
    defined = _defined_flags()
    assert {
        "operations",
        "mode",
        "recorded-by",
        "review-evidence",
        "dry-run",
    } <= defined
    assert "apply" not in defined
    assert "candidate-build" not in defined


def test_the_scan_finds_the_commands_it_is_meant_to_check() -> None:
    scanned = {
        path.name for path in _sources() if _commands(path.read_text(encoding="utf-8"))
    }
    assert "query-api-bootstrap.md" in scanned
    assert "go-api-wave-0-proof-infrastructure.md" in scanned
    assert "check_go_api_routing_digest.py" in scanned


@pytest.mark.parametrize(
    "path", _sources(), ids=lambda path: str(path.relative_to(ROOT))
)
def test_every_enable_command_uses_only_flags_the_verb_defines(path: Path) -> None:
    defined = _defined_flags()
    for command in _commands(path.read_text(encoding="utf-8")):
        text = command.replace('\\n"', " ").replace('"\n', " ")
        tokens = shlex.split(text)[2:]
        used = [
            token.lstrip("-").split("=")[0] for token in tokens if token.startswith("-")
        ]
        unknown = sorted(set(used) - defined)
        assert not unknown, f"{path.name}: `{command}` uses undefined flag(s) {unknown}"
        # `enable` takes no positional operands: operations are ONE
        # comma-separated -operations value, and Go's flag parser stops at
        # the first stray word.
        index = 0
        while index < len(tokens):
            token = tokens[index]
            if not token.startswith("-"):
                pytest.fail(f"{path.name}: `{command}` has a stray word {token!r}")
            name = token.lstrip("-").split("=")[0]
            index += 1 if ("=" in token or name in BOOLEAN_FLAGS) else 2
