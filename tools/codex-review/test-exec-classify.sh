#!/usr/bin/env bash
# test-exec-classify.sh <wrapper> -- proof harness for the exec-block classifier of
# codex-review.sh v4.8.21 (CHAOS-6906, Trap #419).
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

classify() { awk -v BLOCKED_PAT="$BLOCKED_PAT" -f "$WORK/classify.awk" "$1"; }
# The v4.8.20 counters, verbatim: the first command line of each block only.
legacy_go() { grep -A1 '^exec$' "$1" 2>/dev/null | grep -cE 'go (test|run|build)' || true; }

fails=0
check() { # name log want-triple [legacy-go-want]
  local name=$1 log=$2 want=$3 legacy=${4:-}
  local got; got=$(classify "$log")
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

# 9. every wrapper output is exactly three integers.
for f in "$WORK"/*.log "$FIXTURES"/*.log; do
  classify "$f" | grep -Eq '^[0-9]+ [0-9]+ [0-9]+$' || { echo "FAIL: $f produced a malformed classification" >&2; fails=$((fails + 1)); }
done

if [ "$fails" -ne 0 ]; then echo "$fails check(s) FAILED" >&2; exit 1; fi
echo "all classifier checks passed against $WRAPPER"
