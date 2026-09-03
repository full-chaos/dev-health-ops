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
# `github.event.pull_request.base.sha || github.event.merge_group.base_sha`
# before invoking this script -- empty for a plain push. That fallback
# expression is GitHub Actions syntax, evaluated before any shell runs, so it
# cannot be pinned here; what CAN be pinned, and is, is that this script picks
# the right range for whatever BASE_SHA value it is actually given.
set -euo pipefail

if [ -n "${BASE_SHA:-}" ]; then
  range="${BASE_SHA}...HEAD"
else
  # No base sha: a push to main. Compare against the previous commit.
  range="HEAD^...HEAD"
fi

echo "diff range: ${range}" >&2
git diff --name-only "${range}"
