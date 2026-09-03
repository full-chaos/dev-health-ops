#!/usr/bin/env bash
# Print the changed-file list typecheck.yml's relevance step should judge.
#
# CHAOS-4843, lane-4752-go's peer read of #2169: this range-selection logic
# used to live inline in typecheck.yml's `run:` block and was executed by
# nothing -- ci/typecheck_relevance.py's OWN matching logic is tested against
# a pre-built file list, but nothing pinned which diff produces that list.
# Extracted so tests/tooling/test_typecheck_relevant_diff.py can run it
# against a real scratch repo instead of trusting it by inspection. The
# workflow_dispatch short-circuit stays in typecheck.yml (already covered by
# test_typecheck_relevance_treats_workflow_dispatch_as_always_relevant): this
# script is never invoked for that event.
#
# BASE_SHA: the caller resolves this from
# `github.event.pull_request.base.sha || github.event.merge_group.base_sha ||
# github.event.before` before invoking this script (push events land on the
# third arm, `before`). That fallback expression is GitHub Actions syntax,
# evaluated before any shell runs, so it cannot be pinned here; what CAN be
# pinned, and is, is that this script picks the right range for whatever
# BASE_SHA value it is actually given -- including the two shapes `before`
# can arrive in that are NOT a usable base (CHAOS-4843, 4752-go's peer read
# of #2169, round 1, P2a):
#   - the all-zeros sentinel (`before` on the FIRST push of a new branch --
#     there is no previous commit to diff against)
#   - a sha that is no longer an ancestor of HEAD (a force-push rewrote
#     history out from under it)
# Both used to fall through to `HEAD^...HEAD` alongside the genuine "no
# BASE_SHA at all" case -- silently narrowing a multi-commit push's range to
# its LAST commit only, since `HEAD^...HEAD` only ever sees one commit no
# matter how many the actual push contained. A real multi-commit push with a
# relevant change followed by a trailing docs-only commit would report
# `relevant=false` on the whole push. `HEAD^...HEAD` is kept ONLY for the
# genuinely-no-BASE_SHA case (a bare local invocation with nothing set) --
# an invalid-but-present BASE_SHA now fails this script outright (exit
# non-zero) so the CALLER's own existing failure path takes over and fails
# OPEN to relevant=true, per typecheck.yml's "could not resolve the diff
# range" branch -- never a silent, incorrect narrowing.
set -euo pipefail

ZERO_SHA="0000000000000000000000000000000000000000"

if [ -n "${BASE_SHA:-}" ]; then
  if [ "${BASE_SHA}" = "${ZERO_SHA}" ]; then
    echo "BASE_SHA is the all-zeros sentinel (first push of a new branch -- no previous commit to diff against); refusing to guess a range" >&2
    exit 1
  fi
  if ! git merge-base --is-ancestor "${BASE_SHA}" HEAD 2>/dev/null; then
    echo "BASE_SHA (${BASE_SHA}) is not an ancestor of HEAD (a force-push likely rewrote history); refusing to guess a range" >&2
    exit 1
  fi
  range="${BASE_SHA}...HEAD"
else
  # No base sha at all: the documented LOCAL fallback (a bare invocation with
  # nothing set), never the push-event case -- push events always populate
  # BASE_SHA from `github.event.before` now. Compare against the previous
  # commit.
  range="HEAD^...HEAD"
fi

echo "diff range: ${range}" >&2
# core.quotePath=false: without it, git C-quotes any non-ASCII path in its
# --name-only output (CHAOS-4843, 4752-go's peer read of #2169, round 1,
# P2b) -- e.g. `src/café.py` prints as `"src/caf\303\251.py"`, which then
# fails to match ci/typecheck_relevance.py's `src/**` pattern and reports a
# genuinely relevant change as irrelevant.
git -c core.quotePath=false diff --name-only "${range}"
