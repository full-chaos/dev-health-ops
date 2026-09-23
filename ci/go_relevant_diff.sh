#!/usr/bin/env bash
# Print the changed-file list a Go-relevance decider should judge.
#
# Ported from ci/typecheck_relevant_diff.sh, whose own history is the reason
# every piece of this script exists: a naive `git diff --name-only "${range}"`
# silently produces a WRONG-BUT-PLAUSIBLE answer in three separate ways, and
# each one was found only by executing it against a real repository, not by
# reading the command.
#
# BASE_SHA: the caller resolves this from
# `github.event.pull_request.base.sha || github.event.merge_group.base_sha ||
# github.event.before` before invoking this script (push events land on the
# third arm, `before`). That fallback expression is GitHub Actions syntax,
# evaluated before any shell runs, so it cannot be pinned here; what CAN be
# pinned, and is, is that this script picks the right range for whatever
# BASE_SHA value it is actually given -- including the two shapes `before`
# can arrive in that are NOT a usable base:
#   - the all-zeros sentinel (`before` on the FIRST push of a new branch --
#     there is no previous commit to diff against)
#   - a sha that is no longer an ancestor of HEAD (a force-push rewrote
#     history out from under it)
# Both would otherwise fall through to `HEAD^...HEAD` alongside the genuine
# "no BASE_SHA at all" case -- silently narrowing a multi-commit push's range
# to its LAST commit only, since `HEAD^...HEAD` only ever sees one commit no
# matter how many the actual push contained. A real multi-commit push with a
# relevant change followed by a trailing irrelevant commit would then report
# `relevant=false` on the whole push. `HEAD^...HEAD` is kept ONLY for the
# genuinely-no-BASE_SHA case (a bare local invocation with nothing set) -- an
# invalid-but-present BASE_SHA now fails this script outright (exit
# non-zero) so the CALLER's own existing failure path takes over and fails
# OPEN to relevant=true, never a silent, incorrect narrowing.
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
# `-z`: NUL-terminates each entry instead of newline-separating them, which
# disables ALL of git's path quoting unconditionally (git's own documented
# behavior for `-z`) -- so the output is the literal byte sequence of each
# path, whatever it contains (a non-ASCII byte, a control character), with no
# quoting logic to have a gap in. ci/go_relevance.py's stdin reading detects
# and matches this format (NUL-split when a NUL byte is present); the two
# must move together.
# `--no-renames`: git's default rename detection collapses a rename into
# ONLY the post-rename path -- a file renamed from a RELEVANT path to an
# unrelated one (e.g. `internal/a.go` -> `docs/a.md`) reports just
# `docs/a.md`, which matches nothing in ci/go_relevance.py's patterns, so a
# change that effectively REMOVED a Go-relevant file from the tree reads as
# irrelevant. `--no-renames` reports the rename as a plain delete+add pair
# instead -- both the old and new path appear as separate entries, so either
# one matching a relevant pattern is enough to mark the change relevant.
git diff --no-renames --name-only -z "${range}"
