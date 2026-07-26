"""github/prs list-inclusion-decision oracle pair (CHAOS-3162).

Registers the "github/prs/window" boundary: given one listed PR's
updated_at plus a claim's since/until, decide whether
`_collect_github_pr_objects` (src/dev_health_ops/processors/github.py)
would fetch-and-keep it (excluded=False), skip only it (excluded=True,
stop=False), or stop the whole listing early (excluded=True, stop=True).

CODEX FINDING #5 (CHAOS-3162, third adversarial review): this pair
previously did NOT execute the real, live `_collect_github_pr_objects` --
it ran a hand-pinned COPY of the two `if` blocks that make the decision,
guarded by an unordered substring-presence check over a ~3,000-byte slice
of the real source. That guard was too weak to trust: deleting the SECOND
`isinstance(updated_at, datetime)` guard (a real, plausible regression --
it is what H3's original bug looked like) would still leave every pinned
substring present elsewhere in the slice, so the pin would report "still
matches" while actually being stale.

This version executes the REAL, unmodified `_collect_github_pr_objects`
instead, via the SAME isolation technique python_oracle_loader.py already
uses for code_client.py/base_git.py: stub every module-level import the
file needs to satisfy at LOAD time (python_oracle_loader.py's
_target_github_processor), then monkeypatch the one MODULE-LEVEL
dependency seam the function actually calls through at RUN time
(`_github_code_client_from_connector`) with a fake client whose
`get_pull_detail` raises a distinguishing sentinel the INSTANT it is
called. Since the real function's `try/finally` has no `except` clause,
the sentinel propagates cleanly out through `finally` (which the fake
client's `drain_usage_observations`/`close` satisfy) to this harness,
which reads off exactly how far the real loop's `continue`/`break`
control flow got:

  - sentinel raised for item A (the item under test) -> included
    (excluded=False, stop=False): the loop reached `get_pull_detail` for
    it directly, meaning NEITHER window check fired.
  - sentinel raised for item B (a second, fixed, unconditionally-included
    marker item placed right after item A -- its own `updated_at=None`
    means neither comparison can ever fire for it, regardless of the
    case's own since/until) -> item A was excluded via `continue` (skip
    this one, keep going): the loop reached item B, meaning item A's
    `until` check fired but its `since` check did not (or there was no
    `since`).
  - no sentinel at all (the call returns normally) -> item A triggered
    `break`: the loop never reached item B, so `get_pull_detail` was never
    called for anyone.

This is genuinely live execution, not a copy: the two `if` blocks that
decide inclusion are the REAL function's own bytecode running, with only
its external I/O boundary (the client) faked -- editing that logic in
processors/github.py changes what this oracle observes, with no separate
pin to keep in sync and no way for a change to go unnoticed the way a
substring-presence check could miss one.

REQUIREMENT, not a caveat: any Go test that exercises this pair MUST be
run with `go test -count=1`, never a bare `go test`. processors/github.py
lives under src/dev_health_ops/, outside
internal/providersync/testdata/ -- `//go:embed` cannot reach across that
package-directory boundary (Go rejects a `../` pattern outright), so
oracle_compare_test.go's cache-busting fix cannot see edits to it. A bare
`go test` can then return a stale cached PASS for a real regression in
the live function this pair exists to catch. See the recipe doc's
defect-class list for the full explanation.
"""

from __future__ import annotations

import asyncio
import pathlib
from datetime import datetime
from types import SimpleNamespace
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import (
    RETURN_LITERAL,
    dict_literal_keys,
)
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_GITHUB_PROCESSOR_SOURCE = REPO_ROOT / "src/dev_health_ops/processors/github.py"
_THIS_FILE = pathlib.Path(__file__)

_ITEM_A_NUMBER = 1
_ITEM_B_NUMBER = 2


class _ReachedFetch(Exception):
    """Raised by _FakeGitHubCodeClient.get_pull_detail the instant
    _collect_github_pr_objects's REAL loop reaches it for a given listed
    item's number -- see module docstring for how the harness reads three
    distinct outcomes off of this."""

    def __init__(self, number: int) -> None:
        super().__init__(f"reached get_pull_detail for #{number}")
        self.number = number


class _FakeGitHubCodeClient:
    """The one dependency seam _collect_github_pr_objects calls through:
    `_github_code_client_from_connector(connector)`. Everything else in
    the function body is real, unmodified processors/github.py code."""

    def __init__(self, items: list[SimpleNamespace]) -> None:
        self._items = items

    async def iter_pulls(self, *_args: Any, **_kwargs: Any) -> list[SimpleNamespace]:
        return self._items

    async def get_pull_detail(self, _owner: str, _repo: str, number: int) -> Any:
        raise _ReachedFetch(number)

    def drain_usage_observations(self) -> list[Any]:
        return []

    async def close(self) -> None:
        return None


def _parse(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _decide(case: dict[str, Any]) -> dict[str, Any]:
    module = load_live_module(_GITHUB_PROCESSOR_SOURCE)

    updated_at = _parse(case.get("updated_at"))
    since = _parse(case.get("since"))
    until = _parse(case.get("until"))

    item_a = SimpleNamespace(number=_ITEM_A_NUMBER, updated_at=updated_at)
    item_b = SimpleNamespace(number=_ITEM_B_NUMBER, updated_at=None)
    fake_client = _FakeGitHubCodeClient([item_a, item_b])
    # _collect_github_pr_objects looks this name up as a module global at
    # CALL time, so reassigning it on the freshly-loaded module object
    # (not the source file) redirects exactly one dependency seam without
    # touching anything else the real function does.
    module._github_code_client_from_connector = lambda _connector: fake_client

    async def run() -> int | None:
        try:
            await module._collect_github_pr_objects(
                connector=None,
                owner="o",
                repo_name="r",
                repo_id="00000000-0000-0000-0000-000000000000",
                state="all",
                since=since,
                until=until,
                usage_sink=None,
            )
        except _ReachedFetch as reached:
            return reached.number
        return None

    reached_number = asyncio.run(run())

    if reached_number == _ITEM_A_NUMBER:
        return {"excluded": False, "stop": False}
    if reached_number == _ITEM_B_NUMBER:
        return {"excluded": True, "stop": False}
    if reached_number is None:
        return {"excluded": True, "stop": True}
    raise AssertionError(f"unexpected sentinel number {reached_number!r}")


def _reflected_fields() -> frozenset[str]:
    """The complete field set _decide's own `return {...}` literals are
    capable of emitting (codex finding #1). _decide has four return
    statements (one per outcome); dict_literal_keys unions the keys across
    all of them via ast.walk, so this stays correct even though no single
    return statement is the "whole" story on its own."""
    return dict_literal_keys(_THIS_FILE.read_text(), "_decide", (RETURN_LITERAL,))


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/prs/window",
        build_row=_decide,
        reflected_fields=_reflected_fields,
        excluded_fields={},
    )
)
