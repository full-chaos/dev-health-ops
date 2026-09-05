"""Guard: every NATIVE golden-required family is named in the executed-proof gates.

Motivation (codex r2 on #2230, finding F2, confirmed by repro): the assertion
map in ``ci/assert_metrics_executed_proof.py`` supported ``compounding_risk``,
but BOTH callers passed explicit ``--families`` lists that omitted it. The gate
could therefore pass without ever checking that the family ran natively or wrote
a row -- a gate that does not name your family is indistinguishable, from its
exit code, from a gate that checked it and found it healthy.

WHY THIS IS AN ALLOWLIST AND NOT A BLANKET ASSERTION. The obvious form -- "every
golden:required family must appear in both lists" -- fails on 19 of 24 families
today, almost all belonging to other lanes. Shipping that from this lane would
block this PR on other people's backlogs and would assert a fleet-wide policy
this lane has no standing to set. So the gap is PINNED instead: the set of
families missing from the gates is written down explicitly, and any change to
it -- a family added to a list, a new native family, a family losing coverage --
fails this test and has to be made deliberately.

That makes the gap visible and non-regressing rather than invisible, which is
the actual defect r2 found.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
FAMILIES_JSON = REPO_ROOT / "internal/jobs/metrics/daily/families.json"
PROOF_SH = REPO_ROOT / "ci/run_metrics_executed_proof.sh"
LOCAL_VALIDATE_SH = REPO_ROOT / "ci/local_validate.sh"

# Native golden-required families NOT yet named in both gates. Every entry is a
# real coverage gap, not an exemption -- shrinking this set is the goal.
KNOWN_UNCOVERED = {
    # team_wellbeing was here and is GONE, removed 2026-09-05: #2247
    # (CHAOS-4794) added it to local_validate.sh's --families, and the proof
    # gate already named it, so it is covered by both and the reverse assertion
    # required its removal. That is the guard working in the direction that
    # matters -- coverage improved, and the pin was not allowed to keep
    # claiming a gap that had been closed. work_item_state came in the same
    # commit but only reached local_validate, so it stays below.
    "file_hotspots",
    "file_risk_hotspots",
    "work_item_state",
    "incident",
    "testops_risk",
    # --- added 2026-09-05, when merging main forward and porting two more
    # families made four more natives golden-required. Each entry is a GAP,
    # not an exemption; the reason says what would close it.
    #
    # Owned by CHAOS-4283 / #2246. The coverage decision belongs to that lane,
    # which can see whether its own E2E run produces the rows. Naming another
    # lane's families in this gate from here would assert rows this lane has no
    # evidence for, and would block this PR on their backlog.
    "work_item",
    "work_item_estimate",
    # MINE (CHAOS-4285, #2229), pinned with evidence, not by default. The
    # executed-proof E2E seed loop above (ci/run_metrics_executed_proof.sh)
    # only drives four sync targets -- cicd, deployments, incidents, tests --
    # plus `fixtures generate` for git_commits/teams. ai_governance's join is
    # ORG-scoped across ai_attribution_resolved (built from the `ai_attribution`
    # table, which NO seeded target here writes), ci_pipeline_runs FINAL
    # (seeded, by "cicd") and security_alerts FINAL (needs a "security" sync
    # target, also not seeded). With zero ai_attribution rows the INNER JOIN
    # that drives every ai_policy_events row matches nothing, so this harness
    # cannot produce a nonzero sample for this family regardless of whether the
    # Go port is correct -- adding it here would assert against a fixture gap,
    # not test the code. Closing it needs a synthetic "ai-attribution" sync
    # target (or an equivalent seed step) added to the loop, which is out of
    # scope for a port PR.
    "ai_governance",
    # MINE, and pinning them is the part of this change I like least: these are
    # the two families this stack ports, and pinning leaves them unchecked by
    # the very gate that exists to prove a native family ran. It is a gap, not
    # coverage, and it is recorded as one.
    #
    # What would close it is evidence, not a decision: does the executed-proof
    # E2E seed actually produce `review_edges_daily` rows, and does it carry the
    # ~30 days of history benchmarking's window needs on the org's ANCHOR
    # partition (it writes on exactly one partition per org/day)? Adding either
    # without that evidence makes the gate assert rows the fixture may never
    # create, which turns a green gate red for a reason unrelated to the port.
    "review_edges",
    "benchmarking",
    # MINE (CHAOS-4286, #2263/formerly #2240), pinned with evidence, same
    # discipline as ai_governance above. The E2E seed loop
    # (ci/run_metrics_executed_proof.sh) drives four sync targets -- cicd,
    # deployments, incidents, tests -- and none of them is "prs", so neither
    # git_pull_requests nor git_pull_request_reviews is seeded. This family's
    # review_outcome_edges table needs pull-request review rows and cannot
    # produce any without them, regardless of the port's correctness; whether
    # the other two edge tables (pr_deployment_edges, deployment_incident_edges)
    # can produce rows from deployments+incidents alone is unverified, so this
    # is pinned rather than asserted on an unconfirmed partial result. Closing
    # it needs a "prs" seed step added to the loop (or separate per-table
    # evidence), out of scope for a port PR.
    "work_graph_edges",
    # MINE (CHAOS-4280, #2267/formerly #2236), pinned with evidence, same
    # discipline as ai_governance and work_graph_edges above -- doubly so, in
    # fact: this family hits BOTH of their gaps at once. Its attribution
    # loader joins ai_attribution_resolved (built from `ai_attribution`, which
    # no seeded target here writes -- ai_governance's exact reason), and its
    # PR/review loaders need git_pull_requests/git_pull_request_reviews (no
    # "prs" seed target -- work_graph_edges' exact reason). With zero rows on
    # either input this harness cannot produce a nonzero sample for this
    # family regardless of the port's correctness. Closing it needs both an
    # "ai-attribution" seed step and a "prs" seed step added to the loop, out
    # of scope for a port PR.
    "ai_impact",
}


def _families() -> list[dict]:
    data = json.loads(FAMILIES_JSON.read_text())
    return data if isinstance(data, list) else data["families"]


# Matches the logical line following --families: any run of characters that are
# neither a newline nor a backslash, plus explicit backslash-newline
# continuations. The two alternatives are DISJOINT -- `[^\n\\]` excludes the
# backslash that `\\\n` requires -- so there is exactly one way to match any
# input and the engine cannot backtrack between them.
#
# The previous pattern, `((?:[\w\- ]+\\?\s*)+?)`, nested `+` inside `+?` over
# character classes that BOTH matched a space: a run of spaces could be split
# between `[\w\- ]+` and `\s*` in exponentially many ways. CodeQL flagged it as
# exponential backtracking on this PR and was right. The input here is a
# repo-controlled shell script rather than user data, so it was not reachable
# as a denial-of-service -- but "not currently exploitable" is a property of
# today's callers, not of the regex, and this one is also simpler.
_FAMILIES_FLAG = re.compile(r"--families[ \t]+((?:[^\n\\]|\\\n)*)")


def _families_flag(path: Path) -> set[str]:
    """Names passed to --families in a shell script, across line continuations."""
    match = _FAMILIES_FLAG.search(path.read_text())
    assert match, f"no --families flag found in {path}"
    # Continuations join with whitespace; anything that looks like the NEXT flag
    # (and everything after it) is not a family name.
    tokens = match.group(1).replace("\\\n", " ").split()
    names: set[str] = set()
    for token in tokens:
        if token.startswith("-"):
            break
        names.add(token)
    return names


def test_native_golden_required_families_are_named_in_both_gates() -> None:
    native_required = {
        family["name"]
        for family in _families()
        if family.get("golden") == "required" and family.get("port") == "go"
    }
    # Positive control on the enumeration: if families.json moves or the keys are
    # renamed, an empty set would make every assertion below pass vacuously.
    assert native_required, (
        "found NO native golden-required families -- the enumeration is broken"
    )

    proof = _families_flag(PROOF_SH)
    local = _families_flag(LOCAL_VALIDATE_SH)
    # Positive control on the parse: a regex that silently matched nothing would
    # make every family look uncovered, which reads as a real finding.
    assert proof, "parsed an EMPTY --families list from run_metrics_executed_proof.sh"
    assert local, "parsed an EMPTY --families list from local_validate.sh"

    uncovered = {
        name for name in native_required if name not in proof or name not in local
    }

    newly_uncovered = uncovered - KNOWN_UNCOVERED
    assert not newly_uncovered, (
        f"native golden-required famil{'y' if len(newly_uncovered) == 1 else 'ies'} "
        f"{sorted(newly_uncovered)} are not named in BOTH executed-proof gates. "
        "The gate would pass without ever checking them. Add them to the --families "
        "lists in ci/run_metrics_executed_proof.sh and ci/local_validate.sh."
    )

    # Intersect with native_required FIRST. `uncovered` is computed only over
    # families that are native here, so a pinned family that is NOT native on
    # this branch is absent from it -- and a bare `KNOWN_UNCOVERED - uncovered`
    # then reports that family as "now covered", which is false and actively
    # misleading: it is not covered, it is not even ported yet.
    #
    # This bites on a STACKED branch specifically. KNOWN_UNCOVERED is shared and
    # merges forward, but which families are `port: go` differs per branch: on
    # the compounding_risk branch `benchmarking` is still `pending`, so pinning
    # it there is inert, not a regression. The first version of this assertion
    # failed exactly that way and claimed benchmarking "IS covered by both
    # gates" on a branch whose gate lists never mention it.
    now_covered = (KNOWN_UNCOVERED & native_required) - uncovered
    assert not now_covered, (
        f"{sorted(now_covered)} is native here AND named in both gates -- remove it "
        "from KNOWN_UNCOVERED so the coverage cannot silently regress later."
    )
