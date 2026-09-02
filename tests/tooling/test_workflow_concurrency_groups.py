"""Push and pull_request runs for one commit must not cancel each other.

WHY THIS EXISTS (CHAOS-4814)

`go.yml` fires on both `push` and `pull_request`. That is intended -- the push
run covers direct pushes, the pull_request run covers the PR. What is not
intended is the two landing in the SAME concurrency group, because
`cancel-in-progress` then lets them race to cancel one another.

The collapse happens in the expression, not the triggers:

    ${{ github.head_ref || github.ref_name }}

`github.head_ref` is the branch on a `pull_request` event and EMPTY on a
`push`, where `github.ref_name` is the same branch. So both events evaluate to
the identical string and share a group.

The symptom is disproportionate to the cause and looks nothing like its source:
a cancelled check-run bearing a REQUIRED context name is left on the head
commit, and branch protection refuses the merge -- while every workflow run
reports success and `gh pr checks` shows green. Measured on #2110: both runs
created in the same second, the pull_request twin cancelled, 15 cancelled
check-runs, `mergeStateStatus=BLOCKED` with nothing failing.

It is a RACE, not a certainty. On #2115 the twins were also created in the same
second and both survived. That intermittency is why it was diagnosed as flake
several times before being traced.

WHAT THIS ASSERTS

That no workflow keys its concurrency group on an expression which evaluates to
the same value for `push` and `pull_request`. The test does not hard-code the
correct expression -- it models what the two events would produce and fails when
they collide, so a NEW divergent spelling is caught rather than only the one
that was found.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_DIR = REPO_ROOT / ".github" / "workflows"

# Workflows that deliberately use a single global group. A mirror must never run
# twice concurrently regardless of event, so collapsing events is correct there.
_SINGLE_GROUP_BY_DESIGN = {"mirror-test-images.yml"}

# What each context evaluates to, per event, for a branch pushed to a PR.
_PUSH_CONTEXT = {
    "github.head_ref": "",
    "github.ref_name": "topic-branch",
    "github.ref": "refs/heads/topic-branch",
    "github.event.pull_request.number": "",
    "github.event.number": "",
    "github.workflow": "W",
    "github.event_name": "push",
    "github.sha": "abc123",
    "github.run_id": "1",
}
_PULL_REQUEST_CONTEXT = {
    "github.head_ref": "topic-branch",
    "github.ref_name": "7/merge",
    "github.ref": "refs/pull/7/merge",
    "github.event.pull_request.number": "7",
    "github.event.number": "7",
    "github.workflow": "W",
    "github.event_name": "pull_request",
    "github.sha": "def456",
    "github.run_id": "2",
}

_EXPRESSION = re.compile(r"\$\{\{(.+?)\}\}", re.DOTALL)


def _evaluate(expression: str, context: dict[str, str]) -> str:
    """Evaluate the `a || b` chains these groups actually use.

    Deliberately small. Anything it cannot model raises, so an unrecognised
    expression fails the test rather than silently evaluating to something
    convenient -- an unknown shape must not be assumed safe.
    """
    for alternative in expression.split("||"):
        token = alternative.strip()
        if token.startswith(("'", '"')) and token.endswith(("'", '"')):
            return token[1:-1]
        if token in context:
            if context[token]:
                return context[token]
            continue
        raise AssertionError(
            f"concurrency expression uses {token!r}, which this test cannot "
            "model; extend the context above rather than assuming it is safe"
        )
    return ""


def _render(group: str, context: dict[str, str]) -> str:
    return _EXPRESSION.sub(lambda m: _evaluate(m.group(1), context), group)


def _workflows_with_both_triggers() -> list[tuple[Path, str]]:
    found: list[tuple[Path, str]] = []
    for path in sorted(
        list(WORKFLOW_DIR.glob("*.yml")) + list(WORKFLOW_DIR.glob("*.yaml"))
    ):
        if path.name in _SINGLE_GROUP_BY_DESIGN:
            continue
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        triggers = document.get(True) or document.get("on") or {}
        if not ("push" in triggers and "pull_request" in triggers):
            continue
        group = (document.get("concurrency") or {}).get("group")
        if group:
            found.append((path, str(group)))
    return found


def test_there_is_something_to_check() -> None:
    """Guard the guard: this must not pass by finding nothing."""
    assert _workflows_with_both_triggers(), (
        "no workflow declares both push and pull_request with a concurrency "
        "group -- this test would pass vacuously, which is the failure mode it "
        "exists to prevent"
    )


@pytest.mark.parametrize(
    "path,group",
    _workflows_with_both_triggers(),
    ids=lambda value: value.name if isinstance(value, Path) else "",
)
def test_push_and_pull_request_do_not_share_a_concurrency_group(
    path: Path, group: str
) -> None:
    """The twins must land in different groups, or they can cancel each other."""
    on_push = _render(group, _PUSH_CONTEXT)
    on_pull_request = _render(group, _PULL_REQUEST_CONTEXT)

    assert on_push != on_pull_request, (
        f"{path.name}: concurrency group {group!r} evaluates to the SAME value "
        f"for both events ({on_push!r}), so a push run and a pull_request run "
        "for one commit share a group and `cancel-in-progress` lets them race. "
        "The loser leaves a cancelled check-run carrying a required context "
        "name on the head commit, and branch protection refuses the merge while "
        "every workflow run reports success. Key on "
        "`github.event.pull_request.number || github.ref` as the other "
        "workflows do."
    )
