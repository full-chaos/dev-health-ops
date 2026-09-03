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
# CHAOS-4906 fleet audit (09-03): the canonical shape distinguishes a push
# to `main` from a push to any other branch, so `_PUSH_CONTEXT` above (a
# feature-branch push) is no longer enough on its own -- these two add the
# main-push and merge_group cases the canonical shape treats specially
# (sha-keyed, never cancelled).
_PUSH_MAIN_CONTEXT = {
    "github.head_ref": "",
    "github.ref_name": "main",
    "github.ref": "refs/heads/main",
    "github.event.pull_request.number": "",
    "github.event.number": "",
    "github.workflow": "W",
    "github.event_name": "push",
    "github.sha": "mainsha1",
    "github.run_id": "5",
    "github.event.inputs.ref": "",
}
_MERGE_GROUP_CONTEXT = {
    "github.head_ref": "",
    "github.ref_name": "gh-readonly-queue/main/pr-7-abcdef",
    "github.ref": "refs/heads/gh-readonly-queue/main/pr-7-abcdef",
    "github.event.pull_request.number": "",
    "github.event.number": "",
    "github.workflow": "W",
    "github.event_name": "merge_group",
    "github.sha": "mergegroupsha1",
    "github.run_id": "6",
    "github.event.inputs.ref": "",
}

_EXPRESSION = re.compile(r"\$\{\{(.+?)\}\}", re.DOTALL)

# CHAOS-4906 fleet audit (09-03): the fleet's canonical concurrency shape
# grew a genuinely nested boolean expression --
#   pr.number || ((ref == 'refs/heads/main' || event_name == 'merge_group')
#                  && sha) || ref
# -- an OR-inside-an-AND-inside-an-OR, plus a plain `!=` comparison for
# cancel-in-progress. The previous evaluator handled exactly one compound
# shape via a single-purpose regex (`_CONDITIONAL`) and a naive
# `.split("||")` that does not respect parentheses at all -- it mis-tokenized
# the new expression's nested parens outright (confirmed live: it produced
# the mangled token `"((github.ref == 'refs/heads/main'"` and raised).
# Replaced with a real (if minimal) recursive-descent evaluator for the
# actual GitHub Actions expression grammar this fleet uses: `||`/`&&`
# (JS-like, returning an OPERAND's value, not a coerced boolean),
# `==`/`!=` comparisons (which DO produce a real boolean), string literals,
# parenthesised grouping, and bare context tokens. Deliberately still not
# general-purpose -- an operator or literal shape outside this grammar
# raises rather than being assumed safe, same philosophy as its
# predecessor, just with a grammar wide enough to actually cover what the
# fleet's workflows use today.
_TOKEN = re.compile(
    r"""\s*(?:(?P<op>\|\||&&|==|!=|\(|\))|(?P<str>'[^']*')|(?P<ident>[A-Za-z_][\w.]*))"""
)


def _tokenize(expression: str) -> list[str]:
    tokens: list[str] = []
    pos = 0
    length = len(expression)
    while pos < length:
        match = _TOKEN.match(expression, pos)
        if not match or match.end() == pos:
            if expression[pos:].strip() == "":
                break
            raise AssertionError(
                f"concurrency expression uses a shape this test cannot "
                f"tokenize at {expression[pos:]!r} (full expression "
                f"{expression!r}); extend _TOKEN/_tokenize rather than "
                "assuming it is safe"
            )
        pos = match.end()
        token = match.group("op") or match.group("str") or match.group("ident")
        if token is not None:
            tokens.append(token)
    return tokens


class _Parser:
    """Recursive-descent evaluator over `||`/`&&`/`==`/`!=`/parens/tokens.

    Follows GitHub Actions' actual (JS-like) semantics: `A || B` returns `A`
    if `A` is truthy else `B`; `A && B` returns `B` if `A` is truthy else
    `A`. A comparison (`==`/`!=`) always produces a real `bool`. A bare
    context token resolves through `context`, where an empty string models
    "falsy/undefined" -- an unmodelled token raises rather than being
    assumed safe.
    """

    def __init__(self, tokens: list[str], context: dict[str, str], source: str):
        self._tokens = tokens
        self._context = context
        self._source = source
        self._pos = 0

    def _peek(self) -> str | None:
        return self._tokens[self._pos] if self._pos < len(self._tokens) else None

    def _advance(self) -> str:
        token = self._tokens[self._pos]
        self._pos += 1
        return token

    def parse(self) -> bool | str:
        value = self._parse_or()
        if self._pos != len(self._tokens):
            raise AssertionError(
                f"concurrency expression {self._source!r} has trailing "
                f"tokens this test cannot model: {self._tokens[self._pos :]!r}"
            )
        return value

    def _parse_or(self) -> bool | str:
        value = self._parse_and()
        while self._peek() == "||":
            self._advance()
            rhs = self._parse_and()
            value = value if _truthy(value) else rhs
        return value

    def _parse_and(self) -> bool | str:
        value = self._parse_comparison()
        while self._peek() == "&&":
            self._advance()
            rhs = self._parse_comparison()
            value = rhs if _truthy(value) else value
        return value

    def _parse_comparison(self) -> bool | str:
        lhs = self._parse_atom()
        if self._peek() in ("==", "!="):
            op = self._advance()
            rhs = self._parse_atom()
            return (lhs == rhs) if op == "==" else (lhs != rhs)
        return lhs

    def _parse_atom(self) -> bool | str:
        token = self._peek()
        if token is None:
            raise AssertionError(
                f"concurrency expression {self._source!r} ended where a "
                "value was expected"
            )
        if token == "(":
            self._advance()
            value = self._parse_or()
            if self._advance() != ")":
                raise AssertionError(
                    f"concurrency expression {self._source!r} is missing a "
                    "closing paren"
                )
            return value
        self._advance()
        if token.startswith("'") and token.endswith("'"):
            return token[1:-1]
        if token not in self._context:
            raise AssertionError(
                f"concurrency expression {self._source!r} references "
                f"{token!r}, which this test's context does not model; "
                "extend the context above rather than assuming it is safe"
            )
        return self._context[token]


def _truthy(value: bool | str) -> bool:
    return value if isinstance(value, bool) else value != ""


def _evaluate(expression: str, context: dict[str, str]) -> str:
    """Evaluate one `${{ ... }}` expression's inner text to its rendered
    string value (used for concurrency GROUP keys, which always resolve to
    a string in every shape this fleet uses -- a top-level boolean result
    here is a modelling bug in the expression or the test, not a valid
    render, so it raises rather than silently stringifying `True`/`False`
    in a way GitHub itself would not)."""
    value = _Parser(_tokenize(expression), context, expression).parse()
    if isinstance(value, bool):
        raise AssertionError(
            f"concurrency expression {expression!r} rendered to a bare "
            f"boolean ({value!r}) at the top level -- group keys must "
            "resolve to a string; this is a real modelling mismatch, not "
            "safe to stringify away"
        )
    return value


def _evaluate_boolean(expression: str, context: dict[str, str]) -> bool:
    """Evaluate a `cancel-in-progress` expression (a real top-level
    boolean, unlike a group key) to `True`/`False`."""
    inner_match = _EXPRESSION.search(expression)
    inner = inner_match.group(1).strip() if inner_match else expression.strip()
    value = _Parser(_tokenize(inner), context, expression).parse()
    return _truthy(value)


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


# CHAOS-4906 fleet audit (09-03, hosted-concurrency queue investigation):
# codeql-analysis.yml and integration.yml both had `cancel-in-progress:
# false` while keying their group by PR number -- codeql-analysis.yml
# actually triggers on `pull_request`, so a re-push there did NOT cancel
# its own prior run (both share the same PR-numbered group; without
# cancellation, the old one just sits queued/running behind the new one),
# piling up stale runs for superseded commits and permanently occupying a
# slice of hosted concurrency for work whose result nobody will ever read.
# This guard checks every workflow with a PR-numbered group and an actual
# `pull_request` trigger: `cancel-in-progress` must not be hardcoded
# `false`, and if it is an expression, it must evaluate truthy for a
# `pull_request` event. (`_evaluate_boolean` is the general recursive-
# descent evaluator defined above, alongside `_evaluate`/`_render`.)


def test_pr_numbered_groups_cancel_on_pull_request() -> None:
    """A workflow whose concurrency group is keyed by the PR number, and
    which actually triggers on `pull_request`, must cancel a stale run on
    that event. See the module comment above this test for the measured
    consequence when it doesn't (codeql-analysis.yml, before its fix)."""
    checked = 0
    for path in sorted(WORKFLOW_DIR.glob("*.yml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        triggers = document.get(True) or document.get("on") or {}
        if "pull_request" not in triggers:
            continue
        concurrency = document.get("concurrency") or {}
        group = str(concurrency.get("group", ""))
        if "github.event.pull_request.number" not in group:
            continue  # a different group design; not this test's concern
        checked += 1
        cancel = concurrency.get("cancel-in-progress")
        if cancel is True:
            continue  # cancels unconditionally, pull_request included
        if cancel is False:
            pytest.fail(
                f"{path.name}: cancel-in-progress is hardcoded false while "
                "this workflow triggers on pull_request and keys its group "
                "by PR number -- a re-push will not cancel its own stale "
                "run, letting it pile up for a superseded commit and "
                "permanently occupy hosted concurrency"
            )
        assert _evaluate_boolean(str(cancel), _PULL_REQUEST_CONTEXT), (
            f"{path.name}: cancel-in-progress ({cancel!r}) does not "
            "evaluate true for a pull_request event -- a re-push will not "
            "cancel its own stale run"
        )
    assert checked, (
        "no workflow has both a pull_request trigger and a PR-numbered "
        "concurrency group -- this test would pass vacuously, which is "
        "the failure mode it exists to prevent"
    )


# Workflows with their own deliberate, narrower, already-tested contract
# instead of the fleet's canonical shape below. mirror-test-images.yml
# shares ONE group across push+schedule on purpose (CHAOS-4928, tested by
# test_mirror_test_images_serialises_push_and_schedule_but_isolates_dispatch_and_pr
# above) -- a repo-wide lock chris's 09-03 ruling explicitly carves out.
_CANONICAL_SHAPE_EXEMPT: set[str] = {"mirror-test-images.yml"}


def _in_scope_workflows() -> list[Path]:
    found: list[Path] = []
    for path in sorted(WORKFLOW_DIR.glob("*.yml")):
        if path.name in _CANONICAL_SHAPE_EXEMPT:
            continue
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        triggers = document.get(True) or document.get("on") or {}
        if not isinstance(triggers, dict):
            continue
        if "push" in triggers or "pull_request" in triggers:
            found.append(path)
    return found


def _with_inputs_action(context: dict[str, str]) -> dict[str, str]:
    # docs-cloudflare.yml's group has a trailing `${{ inputs.action ||
    # 'preview' }}` segment that every other in-scope workflow's group
    # lacks. `inputs.*` (unlike `github.event.inputs.*`) is a context this
    # module never otherwise models; adding it here, local to this test,
    # keeps that one file's extra segment from making every context dict
    # above carry a token every other workflow never references.
    return {**context, "inputs.action": ""}


def _cancels(cancel: object, context: dict[str, str]) -> bool:
    """`cancel-in-progress` is sometimes a bare YAML boolean (`true`/
    `false`) and sometimes an `${{ ... }}` expression string -- normalise
    both through the same evaluator's truthiness so a regression back to a
    bare `true` (which WOULD cancel main/merge_group) is caught by the
    same assertions as an equivalent but wrongly-shaped expression."""
    if isinstance(cancel, bool):
        return cancel
    return _evaluate_boolean(str(cancel), context)


@pytest.mark.parametrize(
    "path",
    _in_scope_workflows(),
    ids=lambda value: value.name if isinstance(value, Path) else "",
)
def test_canonical_shape_never_cancels_main_or_merge_group(path: Path) -> None:
    """CHAOS-4906 fleet ruling (09-03, chris binding, incident-informed).

    Every workflow that triggers on `pull_request` or `push` must key its
    concurrency group by PR number (`pull_request`), by ref (a push to a
    non-main branch), or by commit sha (a push to `main`, or a
    `merge_group` entry) -- and must cancel a stale `pull_request` run and
    a stale non-main-branch push, while NEVER cancelling a main push or a
    `merge_group` entry.

    That "never" is not theoretical: a main push or merge_group entry WAS
    cancelled by an earlier, less careful version of exactly this kind of
    group key, and each time it was a measured, ticketed incident, not a
    hypothetical --
      - CHAOS-3948 / CHAOS-4676 (go.yml): a burst of fast-follow merges to
        main cancelled each other's Go coverage; a merge_group burst did
        the same to go-storage-integration.
      - CHAOS-4921 / #2149 (docker-images.yml, arc-runner-image.yml): a
        fast-follow push to main cancelled an in-flight or pending image
        build for an earlier commit -- that commit's image was simply
        never built.
      - CHAOS-4946 (typecheck.yml): a fast-follow push to main cancelled
        the INTRODUCING commit's own post-merge check and, separately, its
        fix commit's check -- misattributing the regression to a later,
        unrelated PR.

    This is the single test asserting that canonical shape fleet-wide,
    rather than one bespoke regex per workflow -- a future workflow that
    copies an old, uncancelled-main-unsafe pattern fails here immediately.
    """
    document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    triggers = document.get(True) or document.get("on") or {}
    concurrency = document.get("concurrency") or {}
    group = str(concurrency.get("group", ""))
    cancel = concurrency.get("cancel-in-progress")

    assert group, (
        f"{path.name}: triggers on pull_request or push but declares no "
        "concurrency group at all"
    )
    assert cancel is not None, (
        f"{path.name}: triggers on pull_request or push but declares no "
        "cancel-in-progress at all"
    )

    if "pull_request" in triggers:
        pr_context = _with_inputs_action(_PULL_REQUEST_CONTEXT)
        assert _cancels(cancel, pr_context) is True, (
            f"{path.name}: cancel-in-progress does not cancel a stale "
            "pull_request run -- a re-push will pile up behind it"
        )
        pr_group = _render(group, pr_context)
        assert _PULL_REQUEST_CONTEXT["github.event.pull_request.number"] in pr_group, (
            f"{path.name}: pull_request group {pr_group!r} is not keyed by PR number"
        )
        assert _PULL_REQUEST_CONTEXT["github.sha"] not in pr_group, (
            f"{path.name}: pull_request group {pr_group!r} is keyed by "
            "commit sha instead of PR number -- a re-push would get its "
            "own group and never cancel the run it superseded"
        )

    if "push" in triggers:
        non_main_context = _with_inputs_action(_PUSH_CONTEXT)
        assert _cancels(cancel, non_main_context) is True, (
            f"{path.name}: cancel-in-progress does not cancel a stale push "
            "to a non-main branch"
        )
        non_main_group = _render(group, non_main_context)
        assert _PUSH_CONTEXT["github.sha"] not in non_main_group, (
            f"{path.name}: a push to a non-main branch is keyed by commit "
            f"sha ({non_main_group!r}) instead of ref"
        )

        main_context = _with_inputs_action(_PUSH_MAIN_CONTEXT)
        assert _cancels(cancel, main_context) is False, (
            f"{path.name}: cancel-in-progress cancels a push to main -- "
            "reopens CHAOS-3948/CHAOS-4921/#2149/CHAOS-4946 (see this "
            "test's docstring)"
        )
        main_group = _render(group, main_context)
        assert _PUSH_MAIN_CONTEXT["github.sha"] in main_group, (
            f"{path.name}: a push to main is not keyed by commit sha (got "
            f"{main_group!r}) -- two different commits pushed to main "
            "close together would share a group and could cancel or "
            "displace each other"
        )
        assert _PUSH_MAIN_CONTEXT["github.ref"] not in main_group, (
            f"{path.name}: a push to main's group {main_group!r} still "
            "contains the literal ref -- every main push would share ONE "
            "group instead of one per commit"
        )

    if "merge_group" in triggers:
        mg_context = _with_inputs_action(_MERGE_GROUP_CONTEXT)
        assert _cancels(cancel, mg_context) is False, (
            f"{path.name}: cancel-in-progress cancels a merge_group entry "
            "-- a burst of merges would lose every result but the last "
            "(exactly CHAOS-3948's merge_group half)"
        )
        mg_group = _render(group, mg_context)
        assert _MERGE_GROUP_CONTEXT["github.sha"] in mg_group, (
            f"{path.name}: a merge_group entry is not keyed by commit sha "
            f"(got {mg_group!r})"
        )
