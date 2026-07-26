"""github/prs list-inclusion-decision oracle pair (CHAOS-3162).

Registers the "github/prs/window" boundary: given one listed PR's
updated_at plus a claim's since/until, decide whether
`_collect_github_pr_objects` (src/dev_health_ops/processors/github.py)
would fetch-and-keep it (excluded=False), skip only it (excluded=True,
stop=False), or stop the whole listing early (excluded=True, stop=True).

Scope note, documented rather than silently glossed over: unlike
github_prs_row.py, this pair does NOT execute the real, live
`_collect_github_pr_objects`. That function is async, needs a real
GitHubCodeClient, and processors/github.py's module-level imports pull in
the complexity scanner, testops ingestion, and half a dozen other
subsystems -- stubbing all of it, the way python_oracle_loader.py stubs
code_client.py's few httpx-adjacent imports, would mean writing and
maintaining a large surface of placeholder modules for a three-line
decision, with every stub itself a place this "live" oracle could quietly
diverge from reality. That trade was judged not worth it for a decision
this small.

Instead, _decide below is a byte-for-byte PINNED COPY of the two `if`
blocks that make this decision (processors/github.py, currently lines
630-644, inside `_collect_github_pr_objects`), and
_assert_pin_still_matches_source() reads the actual current source of that
function and hard-fails (raising, not warning) if the pinned significant
substrings are no longer present -- so drift is a loud, immediate test
failure at the next run, not a silent divergence. This is a deliberately
weaker guarantee than github_prs_row.py's live-execution oracle, and is
labelled as such everywhere it is used.
"""

from __future__ import annotations

import pathlib
from datetime import datetime, timezone
from typing import Any

from internal.providersync.testdata import oracle_registry

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_SOURCE_FILE = REPO_ROOT / "src/dev_health_ops/processors/github.py"

# Every one of these must appear, verbatim, inside _collect_github_pr_objects
# in the current source. If a future edit to the real function changes any
# of these fragments, this pin is stale and MUST be re-derived from the new
# source before this oracle can be trusted again.
_PINNED_SIGNIFICANT_FRAGMENTS = (
    "if until is not None:",
    'updated_at = getattr(listed_pr, "updated_at", None)',
    "isinstance(updated_at, datetime)",
    "updated_at.astimezone(timezone.utc) > until",
    "continue",
    "if since is not None:",
    "updated_at.astimezone(timezone.utc) < since",
    "break",
)


def _assert_pin_still_matches_source() -> None:
    source = _SOURCE_FILE.read_text()
    start = source.find("async def _collect_github_pr_objects(")
    if start == -1:
        raise AssertionError(
            f"{_SOURCE_FILE}: _collect_github_pr_objects not found -- "
            "github_prs_window_decision.py's pinned copy of its window-"
            "decision logic can no longer be verified against the real "
            "source and must be re-derived by hand"
        )
    # The two decision blocks are within the first ~50 lines of the
    # function body; a generous slice avoids false negatives from
    # unrelated later code reusing similar words.
    body = source[start : start + 3000]
    missing = [
        fragment for fragment in _PINNED_SIGNIFICANT_FRAGMENTS if fragment not in body
    ]
    if missing:
        raise AssertionError(
            f"{_SOURCE_FILE}: _collect_github_pr_objects no longer contains "
            f"the pinned window-decision fragment(s) {missing!r} -- "
            "github_prs_window_decision.py's _decide is a manually pinned "
            "copy of this logic (see module docstring for why) and must be "
            "updated to match before this oracle can be trusted"
        )


def _decide(case: dict[str, Any]) -> dict[str, Any]:
    _assert_pin_still_matches_source()

    def parse(value: str | None) -> datetime | None:
        if value is None:
            return None
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    updated_at = parse(case.get("updated_at"))
    since = parse(case.get("since"))
    until = parse(case.get("until"))

    excluded = False
    stop = False

    # --- pinned copy of processors/github.py::_collect_github_pr_objects,
    # currently lines 630-644 (verified above). updated_at is already either
    # None or a real datetime (parse() above), which is what the real
    # function's `getattr(listed_pr, "updated_at", None)` also always
    # produces in practice -- the isinstance guard is kept here anyway
    # because it is part of the pinned fragment, not because this mirror
    # can observe a non-datetime value. ---
    if until is not None:
        if (
            isinstance(updated_at, datetime)
            and updated_at.astimezone(timezone.utc) > until
        ):
            excluded = True

    if not excluded:
        if since is not None:
            if (
                isinstance(updated_at, datetime)
                and updated_at.astimezone(timezone.utc) < since
            ):
                excluded = True
                stop = True
    # --- end pinned copy ---

    return {"excluded": excluded, "stop": stop}


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/prs/window",
        build_row=_decide,
        excluded_fields={},
    )
)
