#!/usr/bin/env bash
# test-exec-classify.sh <wrapper> -- proof harness for the exec-block classifier of
# codex-review.sh v4.8.21/v4.8.22 (CHAOS-6906, Trap #419; CHAOS-6948).
#
# The classifier decides GO_EXECS / PY_EXECS / BLOCKED_HITS from a round's log and so
# whether a workspace-write round is stamped "VOID IN FORM: reviewer executed nothing".
# Through v4.8.20 it looked only at the FIRST command line of each exec block, so a
# round whose reviewer ran multi-line `bash -lc` scripts (go build/vet/test on a later
# line) counted as 0 and was stamped void although it had executed (#3276 r1: 39 exec
# blocks, 9 with go test/run/build, 3 with python; the old counter said 0).
#
# This file runs the awk program the wrapper itself carries, extracted VERBATIM from
# between its `# EXEC-CLASSIFY-BEGIN` / `# EXEC-CLASSIFY-END` markers (never a copy), so
# it tests the shipped text. A wrapper without the markers FAILS: a measurement that
# did not happen is not a pass. Each fixture below is also run through the v4.8.20
# first-line counter, and the ones meant to be undercounted assert that it undercounts,
# so a fixture that no longer discriminates fails.
#
# v4.8.22 (CHAOS-6948) adds a fourth number: the blocks that SUCCEEDED and either ran
# `git diff` or read the review patch (`.codex-review.patch`, passed to the awk as
# PATCH_NAME). A round with none of those never saw the diff it certifies and is stamped
# VOID IN FORM. The failing `git diff` (a denied worktree, the case that motivated it)
# must NOT count. Against a v4.8.21 wrapper (three numbers) the diff checks FAIL.
#
# usage: bash test-exec-classify.sh /var/lib/oci-cache/lane-scratch/_shared/codex-review.sh.v4.8.21-<sha>
set -euo pipefail

WRAPPER=${1:?usage: test-exec-classify.sh <codex-review.sh wrapper>}
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
FIXTURES=$HERE/fixtures/exec-classify
WORK=$(mktemp -d "${TMPDIR:-/tmp}/exec-classify.XXXXXX")
trap 'rm -rf "$WORK"' EXIT

BLOCKED_PAT='operation not permitted|cannot create entries|failed to initialize build cache|Read-only file system'

# Extract the classifier verbatim.
awk '/^# EXEC-CLASSIFY-BEGIN$/{on=1} on{print} /^# EXEC-CLASSIFY-END$/{if(on){found=1; exit}} END{exit found?0:1}' "$WRAPPER" > "$WORK/classify.awk" || {
  echo "FAIL: $WRAPPER carries no EXEC-CLASSIFY-BEGIN/END block: the classifier under test does not exist" >&2
  exit 1
}
[ "$(grep -c '^# EXEC-CLASSIFY-BEGIN$' "$WORK/classify.awk")" = 1 ] || { echo "FAIL: more than one classifier block" >&2; exit 1; }

PATCH_NAME='.codex-review.patch'
classify() { awk -v BLOCKED_PAT="$BLOCKED_PAT" -v PATCH_NAME="$PATCH_NAME" -f "$WORK/classify.awk" "$1"; }
# The v4.8.20 counters, verbatim: the first command line of each block only.
legacy_go() { grep -A1 '^exec$' "$1" 2>/dev/null | grep -cE 'go (test|run|build)' || true; }

fails=0
check() { # name log want-triple [legacy-go-want]
  local name=$1 log=$2 want=$3 legacy=${4:-}
  local got; got=$(classify "$log")
  # want is the (go py hits) triple; the fourth number (diff seen) has its own checks.
  got=$(printf '%s' "$got" | awk '{print $1, $2, $3}')
  if [ "$got" != "$want" ]; then
    echo "FAIL $name: classifier said '$got' (go py hits), want '$want'" >&2; fails=$((fails + 1)); return
  fi
  if [ -n "$legacy" ]; then
    local old; old=$(legacy_go "$log")
    if [ "$old" != "$legacy" ]; then
      echo "FAIL $name: the v4.8.20 first-line counter said $old, want $legacy: the fixture no longer discriminates" >&2; fails=$((fails + 1)); return
    fi
  fi
  echo "ok   $name -> $got"
}

# 1. the real #3276 r1 block: go build/vet/test on lines 3..14 of one multi-line script.
check "3276 multi-line script (real excerpt)" "$FIXTURES/3276-multiline-script.log" "1 0 0" 0

# helper: write a block. args: log, command-body (may be multi-line), status line, output lines...
block() {
  local log=$1 body=$2 status=$3; shift 3
  { printf 'exec\n%s\n%s\n' "$body" "$status"; for line in "$@"; do printf '%s\n' "$line"; done; } >> "$log"
}
newlog() { : > "$WORK/$1.log"; echo "$WORK/$1.log"; }

# 2. one-line go test (the regression the old counter already handled).
l=$(newlog oneline); block "$l" "/bin/bash -lc 'cd /w && go test ./internal/x' in /w" " succeeded in 900ms:" "ok  x"
check "one-line go test" "$l" "1 0 0" 1

# 3. a string that merely MENTIONS go test on a later line is not a run.
l=$(newlog mention)
block "$l" "/bin/bash -lc \"set -euo pipefail
printf 'later run go test to confirm\\\\n'
rg -n 'go test' docs/x.md
cat README.md\" in /w" " succeeded in 20ms:" "..."
check "go test only mentioned" "$l" "0 0 0" 0

# 4. python on a later line, at line start and inside \$( ... ).
l=$(newlog py-heredoc)
block "$l" "/bin/bash -lc \"set -e
echo start
python3 - <<'PY'
print(1)
PY\" in /w" " succeeded in 30ms:" "1"
check "python3 heredoc on a later line" "$l" "0 1 0"
l=$(newlog py-subst)
block "$l" "/bin/bash -lc \"set -e
X=\\\"\\\$(python3 - <<'PY'
print(1)
PY
)\\\"\" in /w" " succeeded in 30ms:" "1"
check "python3 inside a command substitution" "$l" "0 1 0"

# 5. command position on a later line: chained, env-prefixed, timed, absolute path.
l=$(newlog chained); block "$l" "/bin/bash -lc \"set -e
cd /w && go test ./a\" in /w" " succeeded in 10ms:" "ok"; check "cd && go test on a later line" "$l" "1 0 0" 0
l=$(newlog envprefix); block "$l" "/bin/bash -lc \"set -e
GOFLAGS=-p=2 go test ./a\" in /w" " succeeded in 10ms:" "ok"; check "VAR=val go test on a later line" "$l" "1 0 0" 0
l=$(newlog timed); block "$l" "/bin/bash -lc \"set -e
time go build ./...\" in /w" " succeeded in 10ms:" "ok"; check "time go build on a later line" "$l" "1 0 0" 0
l=$(newlog abspath); block "$l" "/bin/bash -lc \"set -e
/usr/local/go/bin/go run ./cmd/x\" in /w" " succeeded in 10ms:" "ok"; check "absolute-path go run on a later line" "$l" "1 0 0" 0
l=$(newlog vetonly); block "$l" "/bin/bash -lc \"set -e
go vet ./...\" in /w" " succeeded in 10ms:" "ok"; check "go vet alone is not a test/run/build" "$l" "0 0 0" 0
l=$(newlog pytest); block "$l" "/bin/bash -lc \"set -e
.venv/bin/pytest -q tests/x.py\" in /w" " succeeded in 10ms:" "1 passed"; check "pytest on a later line" "$l" "0 1 0"
l=$(newlog shscript); block "$l" "/bin/bash -lc \"set -e
bash ci/check_x.sh\" in /w" " succeeded in 10ms:" "ok"; check "bash script.sh on a later line" "$l" "0 1 0"
l=$(newlog cat-script); block "$l" "/bin/bash -lc \"set -e
cat ci/check_x.sh\" in /w" " succeeded in 10ms:" "ok"; check "cat script.sh is not an execution" "$l" "0 0 0"

# 6. the harness-blocked signal over a multi-line body: only a FAILED go block whose
# output shows the refusal counts.
l=$(newlog blocked-multiline)
block "$l" "/bin/bash -lc \"set -e
go test ./a\" in /w" " failed in 12ms:" "go: creating work dir: mkdir /x: operation not permitted"
check "failed multi-line go block + refusal" "$l" "1 0 1" 0
l=$(newlog blocked-succeeded)
block "$l" "/bin/bash -lc \"set -e
go test ./a\" in /w" " succeeded in 12ms:" "note: operation not permitted (shrugged off)"
check "succeeded go block + refusal text is not blocked" "$l" "1 0 0" 0
l=$(newlog blocked-nongo)
block "$l" "/bin/bash -lc \"set -e
rg -n foo ./x\" in /w" " failed in 12ms:" "operation not permitted"
check "failed non-go block + refusal text is not blocked" "$l" "0 0 0"

l=$(newlog blocked-in-command)
block "$l" "/bin/bash -lc \"set -e
printf 'grep for operation not permitted\\\\n'
go test ./a\" in /w" " failed in 12ms:" "FAIL x"
check "refusal text in the COMMAND, not the output, is not blocked" "$l" "1 0 0" 0

# 7. several blocks, an exited status, and an unterminated last block (a killed round).
l=$(newlog mixed)
block "$l" "/bin/bash -lc 'ls' in /w" " succeeded in 5ms:" "x"
block "$l" "/bin/bash -lc \"set -e
go build ./...\" in /w" " exited 1 in 40ms:" "boom"
printf 'exec\n/bin/bash -lc "set -e\ngo test ./a" in /w\n' >> "$l"
check "mixed blocks incl. exited and an unterminated one" "$l" "2 0 0" 0

# 8. no blocks at all is a zero, not an error.
l=$(newlog empty); printf 'codex\nnothing ran\n' > "$l"
check "a log with no exec block" "$l" "0 0 0" 0

# 8b. CHAOS-6948: did the round SEE the diff? The fourth number counts blocks that
# SUCCEEDED and ran `git diff` or read the review patch.
# CHAOS-7018 (D2822): the fifth number (optional 4th arg here, default unchecked) counts
# blocks whose status NEVER ARRIVED -- a batched-exec block flushed with no resolution --
# distinct from a block that resolved and failed. A block never resolved is UNKNOWN, never
# silently folded into "not seen": diffs stays a COUNT OF CONFIRMED successes only, the same
# conservative default as every other "no repro = non-existent" gate in this repo.
diffcheck() { # name log want-diff-count [want-unknown-count]
  local name=$1 log=$2 want=$3 want_unknown=${4:-} got got_unknown
  got=$(classify "$log" | awk '{print $4}')
  got_unknown=$(classify "$log" | awk '{print $5}')
  if [ "$got" != "$want" ]; then
    echo "FAIL $name: diff-seen said '${got:-<none>}', want '$want'" >&2; fails=$((fails + 1)); return
  fi
  if [ -n "$want_unknown" ] && [ "$got_unknown" != "$want_unknown" ]; then
    echo "FAIL $name: unknown-count said '${got_unknown:-<none>}', want '$want_unknown'" >&2; fails=$((fails + 1)); return
  fi
  echo "ok   $name -> diff-seen $got unknown=${got_unknown:-0}"
}
l=$(newlog diff-src-only)
block "$l" "/bin/bash -lc 'cat internal/pgmigrate/apply.go' in /w" " succeeded in 9ms:" "package pgmigrate"
block "$l" "/bin/bash -lc \"rg -n Upgrade internal/pgmigrate\" in /w" " succeeded in 9ms:" "apply.go:1: x"
block "$l" "/bin/bash -lc 'go test ./internal/pgmigrate' in /w" " succeeded in 900ms:" "ok"
diffcheck "reviewer read only source files (and ran go test): not seen" "$l" 0
l=$(newlog diff-patch-read)
block "$l" "/bin/bash -lc \"sed -n '1,200p' .codex-review.patch\" in /w" " succeeded in 9ms:" "diff --git a/x b/x"
diffcheck "read of the patch file: seen" "$l" 1
l=$(newlog diff-patch-cat-later-line)
block "$l" "/bin/bash -lc \"set -e
cd /w
cat .codex-review.patch | head -50\" in /w" " succeeded in 9ms:" "diff --git a/x b/x"
diffcheck "patch read on a later line of a script: seen" "$l" 1
l=$(newlog diff-exec)
block "$l" "/bin/bash -lc 'git diff origin/main...HEAD' in /w" " succeeded in 12ms:" "diff --git a/x b/x"
diffcheck "git diff exec: seen" "$l" 1
l=$(newlog diff-exec-later-line)
block "$l" "/bin/bash -lc \"set -e
git -C /w diff --stat origin/main...HEAD\" in /w" " succeeded in 12ms:" " x | 2 +-"
diffcheck "git -C dir diff on a later line: seen" "$l" 1
l=$(newlog diff-exec-failed)
block "$l" "/bin/bash -lc 'git diff origin/main...HEAD' in /w" " failed in 12ms:" "fatal: not a git repository: /denied/path/.git/worktrees/x"
diffcheck "git diff that FAILED (denied worktree, the motivating case): not seen" "$l" 0
l=$(newlog diff-read-failed)
block "$l" "/bin/bash -lc 'cat .codex-review.patch' in /w" " failed in 12ms:" "cat: .codex-review.patch: No such file or directory"
diffcheck "read of the patch that FAILED: not seen" "$l" 0
l=$(newlog diff-not-a-read)
block "$l" "/bin/bash -lc 'ls -l .codex-review.patch' in /w" " succeeded in 5ms:" "-rw-r--r-- 1 x x 100 .codex-review.patch"
block "$l" "/bin/bash -lc 'git diff-tree --no-commit-id --name-only HEAD' in /w" " succeeded in 5ms:" "x"
block "$l" "/bin/bash -lc 'git difftool -h' in /w" " succeeded in 5ms:" "usage"
block "$l" "/bin/bash -lc \"set -e
git diff-tree --no-commit-id --name-only HEAD\" in /w" " succeeded in 5ms:" "x"
block "$l" "/bin/bash -lc \"set -e
git difftool -h\" in /w" " succeeded in 5ms:" "usage"
diffcheck "ls of the patch, git diff-tree, git difftool (one line or later lines) are not a diff read" "$l" 0
l=$(newlog diff-other-patch-name)
block "$l" "/bin/bash -lc 'cat other.patch' in /w" " succeeded in 5ms:" "x"
diffcheck "a different patch file is not the review patch" "$l" 0
l=$(newlog diff-count-two)
block "$l" "/bin/bash -lc 'git diff --stat' in /w" " succeeded in 5ms:" "x"
block "$l" "/bin/bash -lc 'head -20 .codex-review.patch' in /w" " succeeded in 5ms:" "x"
block "$l" "/bin/bash -lc 'ls' in /w" " succeeded in 5ms:" "x"
diffcheck "two diff-seeing blocks among three" "$l" 2
l=$(newlog diff-empty); printf 'codex\nnothing ran\n' > "$l"
diffcheck "a log with no exec block: not seen" "$l" 0
# the shipped fixture (a real round that ran multi-line go scripts and never read the diff)
diffcheck "the real #3276 r1 log (no diff read): not seen" "$FIXTURES/3276-multiline-script.log" 0

# 10. CHAOS-7018 (D2821/D2822): the classifier's exec/status/output state machine assumed
# strict per-block framing (exec, command, ONE status line, output, next exec) and a SINGLE
# open block at a time. It breaks when codex issues several tool calls back-to-back before
# any of their results print -- the OLD `/^exec$/` rule fired on the NEXT block's own marker
# before the CURRENT block ever saw its status line, so `flush()` ran with `ok` still at its
# zero default and every block caught in the burst was silently scored as "not ok," diff-read
# or not. Real, reproduced shape: chaos-6908-3360-r1's first attempt (20260928T035811.log)
# shows four exec markers 2 lines apart with NO status/output between them -- one of those
# four blocks is `sed -n '1,900p' .codex-review.patch`, which DID succeed (confirmed by
# reading the real, untrimmed log: its own output follows much later) -- and the round was
# wrongly stamped VOID IN FORM (diff not seen) as a direct result.
#
# The fix (D2822, FIFO queue): a status line resolves the OLDEST still-pending block, in
# ISSUE ORDER, never the block currently at the front of the raw text -- and a block whose
# status never arrives before EOF is its own outcome, UNKNOWN, counted separately (5th
# number) and NEVER folded into "diffs" as if it were confirmed. On THIS excerpt the one
# status line that does arrive (704ms) resolves the OLDEST pending entry (the
# .codex-review-context.md read, queued first) -- not `.codex-review.patch`, whose block
# text sits textually adjacent to that status line but was issued fourth. That is the
# conservative, honest answer this excerpt can support: the genuine successful patch read
# is UNRESOLVABLE from this excerpt alone (its true result lives further down the real log,
# outside this trimmed fixture) and must read UNKNOWN, never silently promoted to SEEN on
# textual adjacency alone -- the same "no repro = non-existent" discipline this whole file
# already applies everywhere else. diffs stays 0 (nothing here CONFIRMS a diff-read
# succeeded); unknown=3 (the context.md-adjacent, .git-adjacent and patch-read blocks all
# still lack a resolution at EOF of this excerpt).
diffcheck "CHAOS-7018: a batched exec burst leaves unresolved blocks UNKNOWN, never silently 'not seen'" "$FIXTURES/3360-batched-burst.log" 0 3

# 10b. #3368 r1 P1 (EXECUTED, reproduced by the reviewer): the FIRST FIFO fix's status-line
# check was gated on `phase == "cmd"`, which only holds for the ONE line immediately after
# an `exec` marker -- so a burst that fully DRAINS (N execs, THEN N status lines back to
# back, no exec markers between the statuses) resolved only the FIRST status and silently
# dropped the rest as ordinary output text. Reviewer's own repro: 3 queued execs (go test,
# ls, a genuinely successful patch read) followed by all 3 returned statuses gave
# `1 0 0 0 2` instead of the correct `1 0 0 1 0`. This is the exact shape the earlier
# 3360-batched-burst fixture does NOT cover (that one has only ONE status line total, so
# the phase=="cmd" gate never got exercised past its first use). Unlike 3360's excerpt,
# every entry here genuinely IS resolvable -- this fixture's whole point is that a full,
# clean drain must produce zero UNKNOWN, not that the answer is honestly unknowable.
#
# D2827: checked in as a real fixture file (not an ephemeral test-local string) --
# 3368-batch-full-drain.log.
diffcheck "#3368 r1 P1 fix: a fully-drained batch (N execs, then N statuses with no exec markers between them) resolves EVERY entry, not just the first" "$FIXTURES/3368-batch-full-drain.log" 1 0
GO_AFTER_DRAIN=$(classify "$FIXTURES/3368-batch-full-drain.log" | awk '{print $1}')
if [ "$GO_AFTER_DRAIN" = 1 ]; then
  echo "ok   #3368 r1 P1 fix: the go-test entry in a fully-drained batch is still counted (gos=1)"
else
  echo "FAIL: fully-drained batch gos said '${GO_AFTER_DRAIN:-<none>}', want 1" >&2; fails=$((fails + 1))
fi

# 10c. D2827: a SECOND fixture, batch PLUS interleaved output -- not just a clean
# batch-then-statuses run. Same 3 queued execs, but each status line is followed by that
# entry's own genuine multi-line output (test result lines, an `ls -la` listing, a real
# unified diff) BEFORE the next status line arrives. Proves the fix does not just handle
# the degenerate "all statuses adjacent" case: ordinary output text between resolutions
# must never be mistaken for a status line (the status regex is specific enough that none
# of this fixture's own output content happens to match it), and the FIFO queue must keep
# draining correctly across real output, not just across bare status lines.
# 3368-batch-interleaved-output.log.
diffcheck "#3368 r1 P1 fix, D2827: a batch with real interleaved output (not just adjacent statuses) still resolves every entry" "$FIXTURES/3368-batch-interleaved-output.log" 1 0
GO_AFTER_INTERLEAVED=$(classify "$FIXTURES/3368-batch-interleaved-output.log" | awk '{print $1}')
if [ "$GO_AFTER_INTERLEAVED" = 1 ]; then
  echo "ok   #3368 r1 P1 fix, D2827: the go-test entry survives real interleaved output between resolutions (gos=1)"
else
  echo "FAIL: interleaved-output batch gos said '${GO_AFTER_INTERLEAVED:-<none>}', want 1" >&2; fails=$((fails + 1))
fi

# 11. Regression control for the same fix: a real SERIAL excerpt (chaos-6907-3359-r1, normal
# exec/status/output framing, no burst -- the queue never holds more than one entry) where a
# compound `&&`-chained command correctly FAILS as a whole (git diff denied) and a SEPARATE,
# later, `;`-joined command correctly SUCCEEDS and reads the patch. On a serial log the FIFO
# queue degenerates to the old single-block behavior exactly, so both answers -- and
# unknown=0, nothing left pending at EOF -- must be unchanged by the fix.
diffcheck "CHAOS-7018 regression control: a clean serial diff-read stays SEEN, a failed && chain stays NOT seen, unknown=0" "$FIXTURES/3359-serial-diffread.log" 1 0

# 9. every wrapper output is exactly five integers (CHAOS-7018/D2822 adds the unknown count).
for f in "$WORK"/*.log "$FIXTURES"/*.log; do
  classify "$f" | grep -Eq '^[0-9]+ [0-9]+ [0-9]+ [0-9]+ [0-9]+$' || { echo "FAIL: $f produced a malformed classification" >&2; fails=$((fails + 1)); }
done

if [ "$fails" -ne 0 ]; then echo "$fails check(s) FAILED" >&2; exit 1; fi
echo "all classifier checks passed against $WRAPPER"
