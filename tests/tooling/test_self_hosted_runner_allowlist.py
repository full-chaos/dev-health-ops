"""CHAOS-5197 (#2242) self-hosted ARC routing: allow-list coverage.

WHY THIS TEST EXISTS
---------------------
codex round 1 found the routing condition this PR introduced
(`vars.SELF_HOSTED_RUNNERS == 'enabled' && (github.event_name != 'pull_request'
|| ... same-repo ...)`) was a DENY-list: it treated every event that is NOT a
`pull_request` as automatically safe for a self-hosted runner. `merge_group`
(a pre-existing trigger on several of these workflows, unrelated to this PR)
satisfies `!= 'pull_request'` unconditionally -- so a merge-queue run, which
can carry not-yet-fully-merged fork PR code, was newly eligible for the ARC
pool on this PR's 5 REQUIRED-CONTEXT jobs specifically (before this PR they
were always hosted, regardless of event type).

Fix (team-lead ruling, D10: allow-list, never deny-list): the condition now
enumerates the SAFE event shapes explicitly -- `pull_request` from the same
repo, `push` to `main`, `workflow_dispatch`, `release` -- and treats
EVERYTHING else (merge_group included) as self-hosted-INeligible by default.
The hosted twin (`go-container-reproducibility`) is the literal `!(...)`
negation of the same expression, so "exactly one leg runs" holds by
construction rather than by two independently-derived boolean expressions
staying in sync by hand.

codex round 2 F2 added a fifth term, `schedule`: go.yml's nightly cron
(CHAOS-3948) always targets main's HEAD by GitHub's own semantics (a
`schedule` trigger has no ref/actor input to spoof, unlike `workflow_dispatch`
-- it is exactly as trustworthy as `push` to `main`, no fork-safety condition
needed). The original 4-term list omitted it, which the OLD deny-list had
allowed (`schedule != 'pull_request'`) -- a real coverage regression (the
nightly ARC exercise silently stopped), not a security one, introduced by
the deny-list-to-allow-list switch itself and caught one round later.

codex round 2 F1 (P1): `workflow_dispatch` was allow-listed unconditionally,
but docker-images.yml's `workflow_dispatch` trigger accepts an arbitrary
`inputs.ref` (any branch/tag/SHA) that its checkout step prefers over
`github.sha` -- a dispatch with `inputs.ref=refs/pull/<n>/merge` for a FORK
PR would route that untrusted checkout onto the ARC pool, defeating the
`pull_request` term's own fork guard entirely via a different event type.
The fix narrows the `workflow_dispatch` term to only the case where
`inputs.ref` is empty/absent -- the "build main's HEAD" case, exactly as
trustworthy as `push` to `main` -- never an operator-supplied override.
`integration.yml`/`go.yml`'s own `workflow_dispatch` triggers have no `ref`
input at all, so `github.event.inputs.ref` is simply undefined there and
this restriction is a no-op on those files; it only changes behavior on
docker-images.yml, the one file where the gap was real. NOTE: this
narrower term must NOT be used for an ELIGIBILITY condition (e.g.
`needs.changes.outputs.code == 'true' || github.event_name == 'workflow_dispatch' || ...`,
which decides whether a job runs AT ALL, not which runner it lands on) --
conflating the two during the fix accidentally corrupted 3 such sites in
docker-images.yml on the first pass; caught and reverted before commit
by checking each site's surrounding context (a `runs-on: ${{ matrix.runner }}`
or an `always() && needs....` chain, never the self-hosted ternary itself).

This test is the coverage gap codex's F3 named: the PRE-EXISTING
`test_go_arm64_and_docker_build_pool_routing.py` only asserts
`go-arm64-numeric-parity`/`go-build-worker-arm64`, NEITHER of which is a
required context -- a regression on any of the 5 required-context routing
sites (or the two Go-cache negation expressions) would have passed both
existing tests silently. This file closes that gap directly against the
real, current YAML (never a copy of the expected string maintained
separately from what's asserted against).
"""

from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS_DIR = REPO_ROOT / ".github" / "workflows"

# Every workflow file this PR (#2242) routes to the self-hosted ARC pool.
ROUTED_WORKFLOW_FILES = [
    "go-quality.yml",
    "lint.yml",
    "typecheck.yml",
    "test.yml",
    "live-e2e.yml",
    "go.yml",
    "integration.yml",
    "docker-images.yml",
]

# The literal allow-list condition every eligible `runs-on`/`if`/`cache` site
# must contain VERBATIM. Deliberately a plain string, not a regex or a
# looser substring check -- an exact match is what actually proves the real
# YAML says what this test claims it says.
ALLOWLIST_CONDITION = (
    "vars.SELF_HOSTED_RUNNERS == 'enabled' && "
    "((github.event_name == 'pull_request' && github.event.pull_request.head.repo.full_name == github.repository) || "
    "(github.event_name == 'push' && github.ref == 'refs/heads/main') || "
    "(github.event_name == 'workflow_dispatch' && "
    "(github.event.inputs.ref == '' || github.event.inputs.ref == null)) || "
    "github.event_name == 'release' || "
    "github.event_name == 'schedule')"
)

# The old deny-list fragment this fix replaces. Its mere presence anywhere
# in a routed workflow is the regression this test exists to catch --
# unlike the allow-list terms above (which could theoretically appear in
# unrelated conditions), this exact fragment has no other legitimate use in
# these files as of this PR.
DENY_LIST_FRAGMENT = "github.event_name != 'pull_request'"


def _read(name: str) -> str:
    return (WORKFLOWS_DIR / name).read_text(encoding="utf-8")


def _iter_string_values(node, path=""):
    """Yield (path, value) for every string scalar in a parsed YAML doc.

    Scoped to STRING VALUES specifically (not raw lines) because this repo's
    workflows legitimately use `github.event_name != 'pull_request'` in
    OTHER, unrelated conditions (e.g. "should this leg push a real image",
    docker-images.yml; `coverage`'s own run-eligibility gate, test.yml) --
    a raw substring-on-any-line check would false-positive on those. The
    regression this test guards against is specifically the fragment
    appearing TOGETHER WITH `SELF_HOSTED_RUNNERS` in the SAME value.
    """
    if isinstance(node, dict):
        for key, value in node.items():
            yield from _iter_string_values(value, f"{path}.{key}")
    elif isinstance(node, list):
        for index, value in enumerate(node):
            yield from _iter_string_values(value, f"{path}[{index}]")
    elif isinstance(node, str):
        yield path, node


def test_no_deny_list_fragment_survives_in_routed_workflows() -> None:
    offenders = []
    for name in ROUTED_WORKFLOW_FILES:
        doc = yaml.safe_load(_read(name))
        for path, value in _iter_string_values(doc):
            if "SELF_HOSTED_RUNNERS" in value and DENY_LIST_FRAGMENT in value:
                offenders.append(f"{name}{path}: {value!r}")
    assert not offenders, (
        "deny-list fragment reintroduced alongside SELF_HOSTED_RUNNERS -- this "
        "treats every non-pull_request event (merge_group included) as "
        f"self-hosted-eligible by default: {offenders}"
    )


# The 5 ruleset-required contexts' own jobs, by (file, job name). Includes
# every job in the required-context-producing files that carries `runs-on`,
# not just the job whose check name matches the required context string --
# a regression on an aggregator job (`test`, `lint`, `typecheck`) is exactly
# as real a routing bug as one on its constituent job.
REQUIRED_CONTEXT_JOBS = {
    "go-quality.yml": ["go-quality"],
    "lint.yml": ["changes", "lint-job", "lint"],
    "typecheck.yml": ["typecheck-mypy", "typecheck"],
    "test.yml": ["changes", "test-matrix", "coverage", "docs-tests", "test"],
    "live-e2e.yml": ["changes", "metrics-executed-proof"],
}


def test_every_required_context_job_uses_the_allowlist_condition() -> None:
    for name, job_names in REQUIRED_CONTEXT_JOBS.items():
        jobs = yaml.safe_load(_read(name))["jobs"]
        for job_name in job_names:
            assert job_name in jobs, (
                f"{name}: job {job_name!r} not found -- renamed or removed; "
                "update this test's job name constants rather than letting "
                "this assertion go stale"
            )
            runs_on = str(jobs[job_name].get("runs-on", ""))
            assert ALLOWLIST_CONDITION in runs_on, (
                f"{name}:{job_name} runs-on does not contain the allow-list "
                f"condition verbatim -- got {runs_on!r}"
            )


# The two setup-go steps given a self-hosted-cache treatment: their `cache:`
# field negates the SAME allow-list condition, so a regression here would
# silently re-enable setup-go's default GitHub Actions cache against the ARC
# pool's shared, concurrently-written hostPath even while `runs-on` routing
# stayed correct -- a distinct failure mode from a routing bug, catchable
# only by checking this field directly.
CACHE_NEGATION_JOBS = {
    "go-quality.yml": "go-quality",
    "test.yml": "test-matrix",
    "live-e2e.yml": "metrics-executed-proof",
}


def _setup_go_cache(job: dict) -> str | None:
    for step in job.get("steps", []):
        if step.get("uses", "").startswith("actions/setup-go@"):
            return step.get("with", {}).get("cache")
    return None


def test_self_hosted_go_cache_negation_matches_the_allowlist() -> None:
    for name, job_name in CACHE_NEGATION_JOBS.items():
        jobs = yaml.safe_load(_read(name))["jobs"]
        assert job_name in jobs, f"{name}: job {job_name!r} not found"
        cache_value = _setup_go_cache(jobs[job_name])
        assert cache_value is not None, (
            f"{name}:{job_name}: no actions/setup-go step with a 'cache' input found -- "
            "update this test's job/step lookup rather than letting this assertion go stale"
        )
        expected = "!(" + ALLOWLIST_CONDITION + ")"
        assert expected in str(cache_value), (
            f"{name}:{job_name} cache negation does not contain the allow-list "
            f"condition's negation verbatim -- got {cache_value!r}"
        )


# The go-container-reproducibility twin pair: verify the hosted leg's
# condition is the LITERAL negation of the self-hosted leg's, not two
# independently-typed boolean expressions that merely happen to agree today.
def test_go_container_reproducibility_twin_is_a_literal_negation() -> None:
    jobs = yaml.safe_load(_read("go.yml"))["jobs"]
    hosted = str(jobs["go-container-reproducibility"]["if"])
    self_hosted = str(jobs["go-container-reproducibility-self-hosted"]["if"])
    assert ALLOWLIST_CONDITION.replace(" ", "") in self_hosted.replace(
        "\n", " "
    ).replace(" ", ""), (
        f"go-container-reproducibility-self-hosted's if: does not contain the "
        f"allow-list condition verbatim -- got {self_hosted!r}"
    )
    normalized_hosted = hosted.replace("\n", " ")
    assert normalized_hosted.strip().startswith("!("), (
        f"go-container-reproducibility's if: must be the literal negation "
        f"(!(...)) of the self-hosted leg's condition, not an independently "
        f"derived expression -- got {hosted!r}"
    )


# codex round 2 F3: the sites above cover only the 5 required-context files.
# These are the remaining routing sites this PR's own diff touches --
# dind-smoke-test's `if:` (both files), integration.yml's routed job, and
# the two Docker build matrix `runner:` (linux/arm64) legs -- NONE of which
# were covered by any existing test before this addition. A regression on
# any of these (e.g. reintroducing `|| github.event_name == 'merge_group'`)
# would have passed every other test in this file silently.
_REMAINING_IF_SITES = {
    ("go.yml", "dind-smoke-test"): "if",
    ("docker-images.yml", "dind-smoke-test"): "if",
}
_REMAINING_RUNS_ON_SITES = {
    ("integration.yml", "integration"): "runs-on",
}
# (file, job, matrix-include index for platform == linux/arm64) -> field name
_REMAINING_MATRIX_SITES = {
    ("docker-images.yml", "build"): "runner",
    ("docker-images.yml", "go-build"): "runner",
}


def test_dind_smoke_test_and_integration_use_the_allowlist_condition() -> None:
    for (fname, job_name), field in _REMAINING_IF_SITES.items():
        jobs = yaml.safe_load(_read(fname))["jobs"]
        assert job_name in jobs, f"{fname}: job {job_name!r} not found"
        value = str(jobs[job_name].get(field, ""))
        assert ALLOWLIST_CONDITION in value, (
            f"{fname}:{job_name} {field} does not contain the allow-list "
            f"condition verbatim -- got {value!r}"
        )
    for (fname, job_name), field in _REMAINING_RUNS_ON_SITES.items():
        jobs = yaml.safe_load(_read(fname))["jobs"]
        assert job_name in jobs, f"{fname}: job {job_name!r} not found"
        value = str(jobs[job_name].get(field, ""))
        assert ALLOWLIST_CONDITION in value, (
            f"{fname}:{job_name} {field} does not contain the allow-list "
            f"condition verbatim -- got {value!r}"
        )


def test_docker_build_matrix_arm64_legs_use_the_allowlist_condition() -> None:
    for fname, job_name in _REMAINING_MATRIX_SITES:
        jobs = yaml.safe_load(_read(fname))["jobs"]
        assert job_name in jobs, f"{fname}: job {job_name!r} not found"
        matrix = jobs[job_name]["strategy"]["matrix"]
        include = matrix.get("include", [])
        arm64_entries = [
            entry for entry in include if entry.get("platform") == "linux/arm64"
        ]
        assert len(arm64_entries) == 1, (
            f"{fname}:{job_name}: expected exactly one linux/arm64 matrix "
            f"include entry, found {len(arm64_entries)}"
        )
        runner_value = str(arm64_entries[0].get("runner", ""))
        assert ALLOWLIST_CONDITION in runner_value, (
            f"{fname}:{job_name}'s linux/arm64 matrix runner does not "
            f"contain the allow-list condition verbatim -- got {runner_value!r}"
        )
