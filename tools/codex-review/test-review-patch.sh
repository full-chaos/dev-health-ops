#!/usr/bin/env bash
# test-review-patch.sh <wrapper> -- proof harness for the review-patch block of
# codex-review.sh v4.8.22 (CHAOS-6948).
#
# Twice on 2026-09-26 the reviewer could not run `git diff` (the review worktree's git
# pointer targets a path the sandbox denies). The wrapper now writes the diff to
# `.codex-review.patch` in the review worktree before the sandbox starts and names it in
# the prompt. This file runs the block the wrapper itself carries, extracted VERBATIM
# from between its `# REVIEW-PATCH-BEGIN` / `# REVIEW-PATCH-END` markers (never a copy),
# against a real throwaway git repository. A wrapper without the markers FAILS: a
# measurement that did not happen is not a pass.
#
# usage: bash test-review-patch.sh /var/lib/oci-cache/lane-scratch/_shared/codex-review.sh.v4.8.22-<sha>
set -euo pipefail

WRAPPER=${1:?usage: test-review-patch.sh <codex-review.sh wrapper>}
WORK=$(mktemp -d "${TMPDIR:-/tmp}/review-patch.XXXXXX")
trap 'rm -rf "$WORK"' EXIT

awk '/^# REVIEW-PATCH-BEGIN$/{on=1} on{print} /^# REVIEW-PATCH-END$/{if(on){found=1; exit}} END{exit found?0:1}' "$WRAPPER" > "$WORK/block.sh" || {
  echo "FAIL: $WRAPPER carries no REVIEW-PATCH-BEGIN/END block: the block under test does not exist" >&2
  exit 1
}

fails=0
fail() { echo "FAIL $1" >&2; fails=$((fails + 1)); }
ok() { echo "ok   $1"; }

# A real repository: main has a.txt; the topic branch changes a.txt and adds b.txt.
REPO=$WORK/repo
git init -q -b main "$REPO"
git -C "$REPO" config user.email t@example.invalid
git -C "$REPO" config user.name t
git -C "$REPO" config commit.gpgsign false
printf 'one\ntwo\n' > "$REPO/a.txt"
git -C "$REPO" add a.txt
git -C "$REPO" commit -q -m base
git -C "$REPO" checkout -q -b topic
printf 'one\nTWO\n' > "$REPO/a.txt"
printf 'new file\n' > "$REPO/b.txt"
git -C "$REPO" add a.txt b.txt
git -C "$REPO" commit -q -m change
TIP=$(git -C "$REPO" rev-parse HEAD)
# main moves on after the topic branched: the three-dot diff must ignore that change.
git -C "$REPO" checkout -q main
printf 'unrelated\n' > "$REPO/c.txt"
git -C "$REPO" add c.txt
git -C "$REPO" commit -q -m "main moved"
BASE_MAIN=$(git -C "$REPO" rev-parse main)
git -C "$REPO" checkout -q topic

run_block() { # base -> sets outputs in $RW, $L; returns the block's exit status
  local base=$1
  RW=$WORK/rw.$RANDOM; mkdir -p "$RW"; : > "$RW/prompt.md"
  L=$WORK/round.log; : > "$L"
  (
    set -euo pipefail
    WT=$REPO TIP=$TIP BASE=$base RW=$RW L=$L
    warn() { printf 'codex-review: %s\n' "$*" >&2; }
    # shellcheck disable=SC1090
    . "$WORK/block.sh"
  ) 2>"$WORK/stderr.log"
}

# 1. base resolves: the patch is the stat, a blank line, then the three-dot diff.
run_block main || fail "the block exited non-zero for a resolvable base"
PATCH=$RW/.codex-review.patch
[ -s "$PATCH" ] && ok "patch file written" || fail "no patch file"
grep -q ' a.txt | ' "$PATCH" && grep -q ' b.txt | ' "$PATCH" && ok "stat lists both changed files" || fail "stat missing"
grep -q '^diff --git a/b.txt b/b.txt' "$PATCH" && grep -q '^+TWO$' "$PATCH" && ok "unified diff carries the change" || fail "diff missing"
! grep -q 'unrelated\|c.txt' "$PATCH" && ok "three-dot: the change that landed on main afterwards is not in the patch" || fail "the patch carries main's later change (two-dot)"
[ "$(sed -n '1,/^$/p' "$PATCH" | grep -c 'changed')" = 1 ] && ok "stat summary line before the diff" || fail "no stat summary before the diff"
grep -q '.codex-review.patch' "$RW/prompt.md" && grep -q "main...$TIP" "$RW/prompt.md" && ok "the prompt names the file and the range" || fail "prompt does not name the patch"
grep -q '^review-patch: file=.codex-review.patch bytes=[0-9]* base='"$BASE_MAIN"' tip='"$TIP" "$L" && ok "the log records file, bytes, base sha and tip" || fail "log line missing or wrong"
[ ! -e "$PATCH.err" ] && ok "no stray .err file" || fail "stray .err file"

# 2. base unresolvable: loud, no file, the prompt tells the reviewer to run git diff.
run_block no-such-ref || fail "the block must not abort the round for an unresolvable base"
[ ! -e "$RW/.codex-review.patch" ] && ok "unresolvable base: no patch file" || fail "a patch file exists for an unresolvable base"
grep -q 'review patch: NOT written' "$WORK/stderr.log" && ok "unresolvable base: loud on stderr" || fail "not loud"
grep -q '^review-patch: unavailable base=no-such-ref' "$L" && ok "unresolvable base: log says unavailable" || fail "log line missing"
grep -q 'No review patch file could be written' "$RW/prompt.md" && ok "unresolvable base: the prompt says to run git diff itself" || fail "prompt fallback missing"

# 3. an empty diff (base == tip) still writes a patch, and says so in the stat area.
run_block "$TIP" || fail "the block exited non-zero for an empty diff"
[ -e "$RW/.codex-review.patch" ] && ok "empty diff: patch file still written" || fail "no patch file for an empty diff"

if [ "$fails" -ne 0 ]; then echo "$fails check(s) FAILED" >&2; exit 1; fi
echo "all review-patch checks passed against $WRAPPER"
