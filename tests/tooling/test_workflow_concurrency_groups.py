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

# Workflows that deliberately use a single global group for EVERY event. As of
# CHAOS-4928, mirror-test-images.yml is no longer one of these: it now shares
# a single group across only push and schedule (deliberately -- see its own
# concurrency comment), while pull_request and workflow_dispatch each get
# their own isolated group. See
# test_mirror_test_images_serialises_push_and_schedule_but_isolates_dispatch_and_pr
# below for that file's actual (narrower) contract.
_SINGLE_GROUP_BY_DESIGN: set[str] = set()

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
    # `inputs.*` only exists in the event payload for `workflow_dispatch` --
    # a `push` event has no `inputs` object at all, so any reference to it
    # is unconditionally empty (CHAOS-4921's group key falls back through
    # `github.event.inputs.ref` before `github.sha`).
    "github.event.inputs.ref": "",
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
    # Same reasoning as _PUSH_CONTEXT: `inputs.*` does not exist outside
    # `workflow_dispatch`, so it is unconditionally empty on `pull_request`.
    "github.event.inputs.ref": "",
}
# CHAOS-4928: mirror-test-images.yml's group key also needs `schedule` and
# `workflow_dispatch` modelled -- the two events its push-shared group must
# now be told apart from.
_SCHEDULE_CONTEXT = {
    "github.head_ref": "",
    "github.ref_name": "main",
    "github.ref": "refs/heads/main",
    "github.event.pull_request.number": "",
    "github.event.number": "",
    "github.workflow": "W",
    "github.event_name": "schedule",
    "github.sha": "abc123",
    "github.run_id": "3",
    "github.event.inputs.ref": "",
}
_WORKFLOW_DISPATCH_CONTEXT = {
    "github.head_ref": "",
    "github.ref_name": "topic-branch",
    "github.ref": "refs/heads/topic-branch",
    "github.event.pull_request.number": "",
    "github.event.number": "",
    "github.workflow": "W",
    "github.event_name": "workflow_dispatch",
    "github.sha": "ghi789",
    "github.run_id": "4",
    # mirror-test-images.yml declares no custom `inputs:` schema, so this
    # stays empty even for workflow_dispatch there -- but other workflows
    # could define one, so the token itself must still resolve.
    "github.event.inputs.ref": "",
}

_EXPRESSION = re.compile(r"\$\{\{(.+?)\}\}", re.DOTALL)
# CHAOS-4928: matches the one compound shape a group key now uses --
# `(github.some_context == 'literal' && github.other_context)` -- as a single
# `||`-alternative. GitHub Actions' `&&`/`||` return one of their OPERAND
# VALUES (JS-like), not a coerced boolean: `A && B` is `A` if `A` is falsy,
# else `B`; `A || B` is `A` if `A` is truthy, else `B`. Here the left side of
# `&&` is itself an `==` comparison, which DOES evaluate to a real boolean,
# so `false && B` is exactly `false` -- falsy either way, so the difference
# doesn't matter for what this test checks, but the comment says so because
# assuming coercion transparently, then also matching it in the compound
# form.
_CONDITIONAL = re.compile(
    r"^\(\s*(?P<lhs_ctx>[\w.]+)\s*==\s*'(?P<lhs_val>[^']*)'\s*&&\s*(?P<rhs_ctx>[\w.]+)\s*\)$"
)


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
        conditional = _CONDITIONAL.match(token)
        if conditional:
            lhs_ctx, lhs_val, rhs_ctx = conditional.group(
                "lhs_ctx", "lhs_val", "rhs_ctx"
            )
            if lhs_ctx not in context or rhs_ctx not in context:
                raise AssertionError(
                    f"concurrency expression uses {token!r}, which this test "
                    "cannot model; extend the context above rather than "
                    "assuming it is safe"
                )
            if context[lhs_ctx] != lhs_val:
                continue  # the `==` is false -> this alternative is falsy
            if context[rhs_ctx]:
                return context[rhs_ctx]
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
        # WORKFLOW-level concurrency only. A JOB-level `concurrency:` block
        # (mirror-test-images.yml's `mirror` job has one, CHAOS-4928) is
        # unmodelled here -- deliberately not extended to cover it. This
        # test hunts for an expression that renders the SAME value for two
        # events that must NOT share a slot; the `mirror` job's group is the
        # static literal `mirror-publish`, identical for every event by
        # construction, with no event-conditional expression to mis-render.
        # There's nothing for this evaluator to get wrong about a value that
        # never varies. If a future job-level group EVER becomes
        # event-conditional, it will need its own modelling here -- it gets
        # none today because none is needed.
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


def test_mirror_test_images_serialises_push_and_schedule_but_isolates_dispatch_and_pr() -> (
    None
):
    """mirror-test-images.yml's group key is deliberately event-shaped (CHAOS-4928).

    Before this, ALL four events shared one bare-literal group, repo-wide.
    Measured live: a `workflow_dispatch` on a feature branch (introducing a
    brand-new mirrored image, meant to finish BEFORE that branch's own Tests
    run pulls it -- see the workflow's own header) sat pending for 2m23s and
    was evicted by an unrelated `pull_request` run one second after a third,
    also-unrelated event landed. `cancel-in-progress: false` only protects a
    RUNNING job; GitHub still allows just one PENDING run per group, so a
    third event bumps a second, still-pending one -- the mechanism CHAOS-4921
    round 1 hit on docker-images.yml, here worse because NOTHING in the old
    group key varied by ref, PR, or commit at all.

    `push` (to main) and `schedule` now deliberately keep ONE shared group:
    this job is idempotent and cumulative, so losing an unrelated pending run
    there is cheap -- the next one re-derives and mirrors everything the tree
    currently needs. `pull_request` and `workflow_dispatch` are the opposite:
    each carries branch-specific intent an unrelated later event must not
    evict, so each gets its own isolated group.

    A concurrent `imagetools create` to the same destination TAG from two
    different refs is NOT re-litigated here: it is benign by construction
    (digest-preserving copies, every consumer pins `tag@digest`), which is
    the workflow's own concurrency comment, not this test's job to verify.
    """
    path = WORKFLOW_DIR / "mirror-test-images.yml"
    document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    group = str((document.get("concurrency") or {}).get("group"))

    contexts = {
        "push": _PUSH_CONTEXT,
        "schedule": _SCHEDULE_CONTEXT,
        "pull_request": _PULL_REQUEST_CONTEXT,
        "workflow_dispatch": _WORKFLOW_DISPATCH_CONTEXT,
    }
    rendered = {
        event: _render(group, ctx).casefold() for event, ctx in contexts.items()
    }

    assert rendered["push"] == rendered["schedule"], (
        f"push and schedule must share one serialised group (got {rendered['push']!r} "
        f"vs {rendered['schedule']!r}) -- losing that guarantee reopens the "
        "concurrent-registry-write race this design deliberately accepts only "
        "between THIS pair, not generally."
    )
    shared_value = rendered["push"]
    for event in ("pull_request", "workflow_dispatch"):
        assert rendered[event] != shared_value, (
            f"{event}'s group must not collide with the shared push/schedule "
            f"group (both rendered {shared_value!r}) -- an unrelated push or "
            "the weekly schedule could then evict this event's pending run, "
            "exactly CHAOS-4928's measured bug."
        )
    assert rendered["pull_request"] != rendered["workflow_dispatch"], (
        f"pull_request and workflow_dispatch must not share a group either "
        f"(both rendered {rendered['pull_request']!r}) -- an unrelated PR "
        "could then evict a dispatch's pending run, or vice versa."
    )
