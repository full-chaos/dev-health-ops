#!/usr/bin/env bash
# test-review-kit-check.sh [review-kit-check.sh] -- cells for review-kit-check.sh (CHAOS-11801).
# Builds a throwaway git repo (base, r1 tip, tip), a fake gh, and fills the prompt-of-record
# template. Each cell prints "PASS <name>" or "FAIL <name>"; rc is 1 when any cell fails.
set -u

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CHECK=${1:-$HERE/review-kit-check.sh}
TEMPLATE=$HERE/testdata/codex-review-prompt-of-record.md
PIN=2234db2c3ecc66699c700b882bc176c0c0a5225ad826e0852a0af972b9d4e54c
WORK=$(mktemp -d "${TMPDIR:-/tmp}/review-kit-check-test.XXXXXX") || exit 1
trap 'rm -rf "$WORK"' EXIT

fails=0
pass() { echo "PASS $1"; }
fail() { echo "FAIL $1${2:+ ($2)}"; fails=$((fails + 1)); }

[ "$(sha256sum "$TEMPLATE" | cut -d' ' -f1)" = "$PIN" ] || { echo "FAIL setup: testdata template is not the pinned template"; exit 1; }

REPO=$WORK/repo
git init -q -b main "$REPO"
g() { git -C "$REPO" -c user.email=t@example.invalid -c user.name=t -c commit.gpgsign=false "$@"; }
printf 'one\ntwo\n' > "$REPO/a.txt"
mkdir -p "$REPO/internal" "$REPO/ci"
printf 'x\n' > "$REPO/internal/foo.go"
printf 'x\n' > "$REPO/ci/bar.sh"
g add a.txt internal ci; g commit -q -m base
BASE=$(g rev-parse HEAD)
g checkout -q -b topic
printf 'one\nTWO\nthree\n' > "$REPO/a.txt"
printf 'new\n' > "$REPO/b.txt"
g add a.txt b.txt; g commit -q -m r1
R1TIP=$(g rev-parse HEAD)
printf 'new\nmore\n' > "$REPO/b.txt"
printf '\000\001\002' > "$REPO/c.bin"
g add b.txt c.bin; g commit -q -m tip
TIP=$(g rev-parse HEAD)
g update-ref refs/remotes/origin/main "$BASE"
UNREL=$(g commit-tree "$TIP^{tree}" -m unrelated)
MISSING=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa

numstat() { g diff --numstat "$1...$2" | awk '{f++; if ($1 != "-") a += $1; if ($2 != "-") d += $2} END {printf "%d %d %d", f, a, d}'; }
read -r F A D <<<"$(numstat "$BASE" "$TIP")"
read -r F2 A2 D2 <<<"$(numstat "$R1TIP" "$TIP")"

# shellcheck disable=SC2016
printf '#!/bin/sh\necho "${FAKE_TIP:?}"\n' > "$WORK/fakegh"
chmod +x "$WORK/fakegh"
printf 'PR body\n' > "$WORK/ctx.md"

export REVIEW_KIT_TEMPLATE=$TEMPLATE REVIEW_KIT_GH=$WORK/fakegh REVIEW_KIT_REPO=$REPO FAKE_TIP=$TIP

SUMMARY='The wrapper counts executed runs in the reviewer log. It must accept the listed command forms and refuse every other form named in this sentence. The operator who launches the round is trusted.'
VERDICT=$'## Round 1 findings\n\n### Class 1 - quoted text counted (P1)\nA [bracketed] search counted as a run.\n\n| Cell | Output |\n|---|---|\n| a | 0 |\n\nVERDICT: NOT CLEAN'

# fill <round> <out> [name=value ...]: write a filled prompt; overrides replace a field value.
fill() {
  local round=$1 out=$2
  shift 2
  FILL_ROUND=$round FILL_TEMPLATE=$TEMPLATE FILL_OVERRIDES=$(printf '%s\n' "$@") \
    FILL_TIP=$TIP FILL_BASE=$BASE FILL_R1TIP=$R1TIP FILL_F=$F FILL_A=$A FILL_D=$D \
    FILL_F2=$F2 FILL_A2=$A2 FILL_D2=$D2 FILL_SUMMARY=$SUMMARY FILL_VERDICT=$VERDICT \
    python3 -I -c '
import os, re
env = os.environ
lines = open(env["FILL_TEMPLATE"], encoding="utf-8").read().split("\n")
block = lines[lines.index("---- PROMPT (copy verbatim, fill brackets) ----") + 1:lines.index("---- END ----")]
while block and block[0] == "": block.pop(0)
while block and block[-1] == "": block.pop()
rnd = env["FILL_ROUND"]
keep = []
for l in block:
    if l.startswith("[r2 only] "):
        if rnd == "2": keep.append(l[len("[r2 only] "):])
    elif l == "[r2 round 1 verdict]":
        if rnd == "2": keep.append(l)
    else:
        keep.append(l)
text = "\n".join(keep)
vals = {"N": "7", "1|2": rnd, "full tip sha": env["FILL_TIP"], "full main sha": env["FILL_BASE"],
        "main sha": env["FILL_BASE"], "tip sha": env["FILL_TIP"], "F": env["FILL_F"], "A": env["FILL_A"],
        "D": env["FILL_D"], "r1 tip": env["FILL_R1TIP"], "F2": env["FILL_F2"], "A2": env["FILL_A2"],
        "D2": env["FILL_D2"], "summary": env["FILL_SUMMARY"], "r2 round 1 verdict": env["FILL_VERDICT"]}
for kv in env["FILL_OVERRIDES"].split("\n"):
    if "=" in kv:
        k, v = kv.split("=", 1)
        vals[k] = v
def sub(m):
    k = m.group(1)
    return vals["summary"] if k.startswith("2-6 sentences") else vals[k]
print(re.sub(r"\[([^\]]*)\]", sub, text))
' > "$out"
}

# run <name> <expect-rc> <expect-substring> <prompt> <round> [env...]: one refuse/accept cell.
cell() {
  local name=$1 want_rc=$2 want_txt=$3 prompt=$4 round=$5 out rc
  shift 5
  out=$(env "$@" bash "$CHECK" 7 "$prompt" "${CELL_CTX:-$WORK/ctx.md}" "$round" 2>&1)
  rc=$?
  if [ "$rc" != "$want_rc" ]; then
    fail "$name" "rc=$rc want $want_rc"
  elif [[ $out != *"$want_txt"* ]]; then
    fail "$name" "output lacks: $want_txt"
  else
    pass "$name"
  fi
}

fill 1 "$WORK/r1.md"
fill 2 "$WORK/r2.md"

cell green-r1-verbatim 0 "review-kit-check OK template=2234db2c3ecc prompt=$(sha256sum "$WORK/r1.md" | cut -c1-12) ctx=$(sha256sum "$WORK/ctx.md" | cut -c1-12) base=$BASE tip=$TIP files=$F +$A -$D" "$WORK/r1.md" 1
cell green-r2-verbatim 0 "base=$BASE tip=$TIP files=$F +$A -$D delta=$F2 +$A2 -$D2" "$WORK/r2.md" 2

awk '{print} /^do not read outside the repository root$/{print "Focus on the parser and the quoting code."}' "$WORK/r1.md" > "$WORK/hint.md"
cell red-added-hint-line 1 "prompt differs at line" "$WORK/hint.md" 1

awk '{print} /^do not read outside the repository root$/{print "Files: internal/foo/a.go internal/foo/b.go"}' "$WORK/r1.md" > "$WORK/paths.md"
cell red-added-path-list 1 "prompt differs at line" "$WORK/paths.md" 1

sed 's/^SCOPE: the lines/SCOPE: all lines/' "$WORK/r1.md" > "$WORK/word.md"
cell red-changed-literal-word 1 "prompt differs at line" "$WORK/word.md" 1

fill 1 "$WORK/wrongF.md" "F=$((F + 1))"
cell red-wrong-F-count 1 "files" "$WORK/wrongF.md" 1

fill 1 "$WORK/wrongA.md" "A=$((A + 1))"
cell red-wrong-A-count 1 "added" "$WORK/wrongA.md" 1

fill 1 "$WORK/wrongtip.md" "full tip sha=$R1TIP" "tip sha=$R1TIP"
cell red-wrong-tip-sha 1 "tip" "$WORK/wrongtip.md" 1

fill 1 "$WORK/wrongmain.md" "full main sha=$R1TIP" "main sha=$R1TIP"
cell red-wrong-main-sha 1 "main" "$WORK/wrongmain.md" 1

awk '{print} /^Diff: /{print "Delta: `git diff x...y` — 1 files, +1/-1."}' "$WORK/r1.md" > "$WORK/r1r2line.md"
cell red-r1-holds-r2-line 1 "prompt differs at line" "$WORK/r1r2line.md" 1

awk '/^Round 1 verdict, verbatim:$/{skip=1; next} skip && /^The PR body /{skip=0} !skip{print}' "$WORK/r2.md" > "$WORK/noverdict.md"
cell red-r2-missing-verdict 1 "prompt differs at line" "$WORK/noverdict.md" 2

fill 2 "$WORK/emptyverdict.md" "r2 round 1 verdict="
cell red-r2-empty-verdict 1 "prompt differs at line" "$WORK/emptyverdict.md" 2

fill 1 "$WORK/emptysummary.md" "summary="
cell red-empty-summary-fill 1 "prompt differs at line" "$WORK/emptysummary.md" 1

PATHSUM=$'The wrapper counts runs.\ninternal/foo/a.go\ninternal/foo/b.go\ninternal/bar/c.go'
SUMMARY=$PATHSUM fill 1 "$WORK/pathsummary.md"
cell red-path-list-in-intent-bracket 1 "bracket fill holds a path list at line" "$WORK/pathsummary.md" 1

PROSE=$'The wrapper accepts and/or refuses a form.\nIt reads input/output pairs.\nIt may read/write the log.'
SUMMARY=$PROSE fill 1 "$WORK/prose.md"
cell green-slash-prose-in-intent-bracket 0 "review-kit-check OK" "$WORK/prose.md" 1

for rd in 1 2; do
  sfx=; [ "$rd" = 2 ] && sfx=-r2
  SUMMARY=$'The wrapper counts runs.\ninternal/alpha\ninternal/beta\ninternal/gamma' fill "$rd" "$WORK/internal$rd.md"
  cell "red-path-list-internal-names$sfx" 1 "bracket fill holds a path list at line" "$WORK/internal$rd.md" "$rd"
  SUMMARY=$'The wrapper counts runs.\nci/alpha\nci/beta\nci/gamma' fill "$rd" "$WORK/ci$rd.md"
  cell "red-path-list-ci-names$sfx" 1 "bracket fill holds a path list at line" "$WORK/ci$rd.md" "$rd"
  SUMMARY=$'The wrapper counts runs.\ninternal/foo/a.go\ninternal/foo/b.go\ninternal/bar/c.go' fill "$rd" "$WORK/deep$rd.md"
  cell "red-path-list-deep-paths$sfx" 1 "bracket fill holds a path list at line" "$WORK/deep$rd.md" "$rd"
  SUMMARY=$PROSE fill "$rd" "$WORK/prose$rd.md"
  cell "green-slash-prose-real-tree$sfx" 0 "review-kit-check OK" "$WORK/prose$rd.md" "$rd"
  fill "$rd" "$WORK/blank$rd.md"
  printf '\n' >> "$WORK/blank$rd.md"
  cell "red-extra-blank-line-at-eof$sfx" 1 "prompt differs at line" "$WORK/blank$rd.md" "$rd"
done

fill 2 "$WORK/unrel.md" "r1 tip=$UNREL" "F2=0" "A2=0" "D2=0"
cell red-r2-unrelated-r1-tip 1 "not an ancestor" "$WORK/unrel.md" 2

fill 1 "$WORK/missingtip.md" "full tip sha=$MISSING" "tip sha=$MISSING"
cell red-tip-missing-locally 1 "not in the local repository: fetch the PR head first" "$WORK/missingtip.md" 1 FAKE_TIP="$MISSING"

fill 1 "$WORK/emptyF.md" "F="
cell red-empty-count-fill 1 "prompt differs at line" "$WORK/emptyF.md" 1

fill 2 "$WORK/wrongdelta.md" "F2=$((F2 + 1))"
cell red-r2-wrong-delta-count 1 "delta" "$WORK/wrongdelta.md" 2

cell red-round-arg-mismatch 1 "prompt differs at line" "$WORK/r1.md" 2
fill 2 "$WORK/round1in2.md" "1|2=1"
cell red-round-field-mismatch 1 "round" "$WORK/round1in2.md" 2

cp "$TEMPLATE" "$WORK/tpl-bad.md"
printf 'x\n' >> "$WORK/tpl-bad.md"
cell red-template-sha-wrong 2 "template sha" "$WORK/r1.md" 1 REVIEW_KIT_TEMPLATE="$WORK/tpl-bad.md"

CELL_CTX=$WORK/no-such-ctx.md cell red-ctx-missing 1 "ctx" "$WORK/r1.md" 1
: > "$WORK/empty-ctx.md"
CELL_CTX=$WORK/empty-ctx.md cell red-ctx-empty 1 "ctx" "$WORK/r1.md" 1

[ "$fails" -eq 0 ]
