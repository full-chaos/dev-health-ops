#!/usr/bin/env python3
"""Strip agent-attribution trailers from a commit message file.

Invoked by the lefthook ``commit-msg`` hook on every ``git commit``.

The repo-root ``AGENTS.md`` rule states: "Never add contribution attribution
for agents in commits." Sisyphus, oh-my-openagent, Claude Code, and similar
AI coding agents inject "Ultraworked with [...]" and
"Co-authored-by: <agent>" trailers into commit messages despite the rule.

This hook removes them before the commit lands so ``main`` stays clean.

Usage::

    python3 scripts/strip_agent_attribution.py <commit-msg-file>

Behaviour:
* Idempotent: running on a clean message is a no-op.
* Preserves all other trailers (real ``Co-authored-by`` for humans,
  ``Signed-off-by``, ``Refs``, ``Closes``, etc.).
* Collapses runs of blank lines created by removal back to a single blank.

Exit codes:
* 0 — message rewritten (or unchanged); commit proceeds.
* non-zero — only if argv is malformed or the file is unreadable.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

# Patterns that match agent-attribution lines. Match the whole line including
# trailing newline so the surrounding blank-line collapse stays clean.
_AGENT_LINE_PATTERNS: tuple[re.Pattern[str], ...] = (
    # "Ultraworked with [Sisyphus](https://...)"
    re.compile(r"^Ultraworked with \[[^\]]+\]\([^)]*\)\s*$", re.MULTILINE),
    # "Co-authored-by: Sisyphus <clio-agent@sisyphuslabs.ai>"
    re.compile(r"^Co-authored-by:\s+Sisyphus\s+<[^>]*>\s*$", re.MULTILINE),
    # "Co-authored-by: Claude <noreply@anthropic.com>" or similar
    re.compile(r"^Co-authored-by:\s+Claude\s+<[^>]*>\s*$", re.MULTILINE),
    # "🤖 Generated with [Claude Code](https://claude.com/claude-code)"
    re.compile(r"^🤖 Generated with \[?[^\]\n]+\]?\([^)]*\)?\s*$", re.MULTILINE),
)


def strip_agent_attribution(message: str) -> str:
    """Return ``message`` with agent-attribution lines removed."""
    cleaned = message
    for pattern in _AGENT_LINE_PATTERNS:
        cleaned = pattern.sub("", cleaned)
    # Collapse runs of 3+ blank lines to a single blank line.
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    # Ensure exactly one trailing newline.
    return cleaned.rstrip() + "\n"


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(
            "usage: strip_agent_attribution.py <commit-msg-file>",
            file=sys.stderr,
        )
        return 2
    path = Path(argv[1])
    try:
        original = path.read_text()
    except OSError as exc:
        print(f"strip_agent_attribution: cannot read {path}: {exc}", file=sys.stderr)
        return 1
    cleaned = strip_agent_attribution(original)
    if cleaned != original:
        path.write_text(cleaned)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
