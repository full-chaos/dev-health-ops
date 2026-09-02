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
            literal = token[1:-1]
            if literal:
                return literal
            # `''` is FALSY in Actions, so `${{ '' || github.ref }}` falls
            # through to the ref. Returning the empty literal made both events
            # model as empty and therefore equal -- a false alarm, but the model
            # must match GitHub, not be conservative in a direction of its own.
            continue
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


# Events whose runs would collide on a feature branch. `merge_group`, `schedule`
# and `workflow_dispatch` are excluded: none produces a second run for a PR tip.
_COLLIDING_EVENTS = ("push", "pull_request")


def _branch_patterns(triggers: dict, event: str) -> list[str]:
    config = triggers.get(event)
    if not isinstance(config, dict):
        return []
    branches = config.get("branches")
    if branches is None:
        return ["**"]  # no filter means every branch
    return [str(b) for b in branches]


def _fires_on_feature_branches(patterns: list[str]) -> bool:
    """True if the pattern list can match a branch other than main."""
    return any(p not in {"main", "master"} for p in patterns)


@pytest.mark.parametrize(
    "path,group",
    _workflows_with_both_triggers(),
    ids=lambda value: value.name if isinstance(value, Path) else "",
)
def test_a_shared_group_requires_that_only_one_event_fires_on_a_branch(
    path: Path, group: str
) -> None:
    """Two ways to be safe; a workflow must pick one.

    Either the events land in DIFFERENT concurrency groups, or only ONE of them
    can fire on a feature branch. `go.yml` takes the second: CHAOS-4676 wants a
    single group so GitHub arbitrates "one authoritative run per branch tip",
    and CHAOS-4814 removes the twin by restricting `push` to `main` rather than
    splitting the group.

    This fails if `push` ever widens back to `**` while the group stays shared,
    which is the exact regression that would silently restore the cancelled-twin
    tax -- and it would look like a routine trigger change in review.
    """
    document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    triggers = document.get(True) or document.get("on") or {}

    groups = {
        event: _render(
            group, _PUSH_CONTEXT if event == "push" else _PULL_REQUEST_CONTEXT
        ).casefold()
        for event in _COLLIDING_EVENTS
    }
    if len(set(groups.values())) == len(groups):
        return  # distinct groups: the twins cannot cancel each other

    firing = [
        event
        for event in _COLLIDING_EVENTS
        if _fires_on_feature_branches(_branch_patterns(triggers, event))
    ]
    assert len(firing) <= 1, (
        f"{path.name}: {firing} all fire on feature branches AND share the "
        f"concurrency group {group!r} (both render {groups['push']!r}). The two "
        "runs will race to cancel each other, and the loser leaves a cancelled "
        "check-run carrying a required context name on the head commit -- "
        "branch protection then refuses the merge while every run reports "
        "success. Either split the group, or restrict a trigger so only one "
        "event fires on a branch."
    )
