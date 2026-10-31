#!/usr/bin/env bash
# review-kit-check.sh <pr> <prompt> <ctx> <round 1|2> [stack-base-sha] (CHAOS-11801)
# Refuses unless the Codex prompt is the prompt-of-record template with only its bracket fields
# filled, and its shas and diff counts equal the pushed PR head, the base and `git diff --numstat`.
# Env: REVIEW_KIT_TEMPLATE (template path), REVIEW_KIT_GH (gh command), REVIEW_KIT_REPO (repo dir).
# rc: 0 pass, 1 refused, 2 template sha or usage.
set -u

PIN=2234db2c3ecc66699c700b882bc176c0c0a5225ad826e0852a0af972b9d4e54c
TEMPLATE=${REVIEW_KIT_TEMPLATE:-/home/ubuntu/devhealth/.remember/lanes/_briefs/codex-review-prompt-of-record.md}
GH=${REVIEW_KIT_GH:-gh}
REPO=${REVIEW_KIT_REPO:-.}

die() { echo "review-kit-check REFUSED: $2" >&2; exit "$1"; }

[ $# -ge 4 ] && [ $# -le 5 ] || die 2 "usage: review-kit-check.sh <pr> <prompt> <ctx> <round 1|2> [stack-base-sha]"
pr=$1 prompt=$2 ctx=$3 round=$4
[[ $pr =~ ^[0-9]+$ ]] || die 2 "usage: pr must be a number"
[ "$round" = 1 ] || [ "$round" = 2 ] || die 2 "usage: round must be 1 or 2"

tpl_sha=$(sha256sum "$TEMPLATE" 2>/dev/null | cut -d' ' -f1)
[ "$tpl_sha" = "$PIN" ] || die 2 "template sha ${tpl_sha:-unreadable} != pin $PIN"

[ -s "$prompt" ] || die 1 "prompt missing or empty: $prompt"

if [ $# -eq 5 ]; then
  base=$(git -C "$REPO" rev-parse --verify "$5^{commit}" 2>/dev/null) || die 1 "base: stack base $5 is not a commit"
else
  base=$(git -C "$REPO" rev-parse --verify origin/main 2>/dev/null) || die 1 "base: origin/main not found"
fi

REVIEW_KIT_TOP=$(git -C "$REPO" ls-tree --name-only "$base" 2>/dev/null) || die 1 "base: cannot list the tree of $base"
export REVIEW_KIT_TOP

fields=$(python3 -I -c '
import os, re, sys
tpl_path, prompt_path, rnd = sys.argv[1:4]
tpl = open(tpl_path, encoding="utf-8").read().split("\n")
block = tpl[tpl.index("---- PROMPT (copy verbatim, fill brackets) ----") + 1:tpl.index("---- END ----")]
while block and block[0] == "":
    block.pop(0)
while block and block[-1] == "":
    block.pop()
lines = []
for l in block:
    if l.startswith("[r2 only] "):
        if rnd == "2":
            lines.append(l[len("[r2 only] "):])
    elif l == "[r2 round 1 verdict]":
        if rnd == "2":
            lines.append(l)
    else:
        lines.append(l)
text = "\n".join(lines)
prompt = open(prompt_path, encoding="utf-8").read()
if prompt.endswith("\n"):
    prompt = prompt[:-1]

INLINE = r"[^\s\n](?:[^\n]*[^\s\n])?"
STRICT = {"N": r"[0-9]+", "1|2": r"[12]", "F": r"[0-9]+", "A": r"[0-9]+", "D": r"[0-9]+",
          "F2": r"[0-9]+", "A2": r"[0-9]+", "D2": r"[0-9]+",
          "full tip sha": r"[0-9a-f]{40}", "full main sha": r"[0-9a-f]{40}",
          "main sha": r"[0-9a-f]{40}", "tip sha": r"[0-9a-f]{40}", "r1 tip": r"[0-9a-f]{40}"}
SUMMARY = r"\S(?:(?!\n\n)[\s\S])*?"
VERDICT = r"\S[\s\S]*?"

TOP = set(os.environ.get("REVIEW_KIT_TOP", "").split("\n")) - {""}
STRIP = "`\"\x27(),"
def is_path(w):
    w = w.strip(STRIP).rstrip(".").strip(STRIP)
    if "/" not in w:
        return False
    return (w.startswith(("./", "../", "/")) or w.count("/") >= 2 or "." in w.rsplit("/", 1)[1]
            or w.split("/", 1)[0] in TOP)

toks = []
pos = 0
for m in re.finditer(r"\[([^\]]*)\]", text):
    toks.append(("lit", text[pos:m.start()]))
    name = m.group(1)
    if name.startswith("2-6 sentences"):
        toks.append(("fld", "summary", SUMMARY))
    elif name == "r2 round 1 verdict":
        toks.append(("fld", "verdict", VERDICT))
    else:
        toks.append(("fld", name, STRICT.get(name, INLINE)))
    pos = m.end()
toks.append(("lit", text[pos:]))

def build(upto):
    out = []
    for t in toks[:upto]:
        out.append(re.escape(t[1]) if t[0] == "lit" else "(" + t[2] + ")")
    return "".join(out)

m = re.fullmatch(build(len(toks)), prompt)
if not m:
    best = 0
    for i in range(1, len(toks) + 1):
        if re.match(build(i), prompt):
            best = i
        else:
            break
    mm = re.match(build(best), prompt) if best else None
    end = mm.end() if mm else 0
    if best < len(toks) and toks[best][0] == "lit":
        lit = toks[best][1]
        k = 0
        while k < len(lit) and end + k < len(prompt) and lit[k] == prompt[end + k]:
            k += 1
        end += k
    line = prompt.count("\n", 0, end) + 1
    shown = prompt.split("\n")[line - 1][:80] if line <= prompt.count("\n") + 1 else ""
    sys.stderr.write("prompt differs at line %d: %s\n" % (line, shown))
    sys.exit(1)

seen = {}
gi = 0
for t in toks:
    if t[0] != "fld":
        continue
    gi += 1
    if t[1] == "verdict":
        continue
    if t[1] == "summary":
        hits = 0
        off = prompt.count("\n", 0, m.start(gi))
        for k, ln in enumerate(m.group(gi).split("\n")):
            if any(is_path(w) for w in ln.split()):
                hits += 1
                if hits == 3:
                    sys.stderr.write("bracket fill holds a path list at line %d\n" % (off + k + 1))
                    sys.exit(1)
        continue
    v = m.group(gi)
    if t[1] in seen and seen[t[1]] != v:
        sys.stderr.write("prompt field [%s] has two values: %s, %s\n" % (t[1], seen[t[1]], v))
        sys.exit(1)
    seen[t[1]] = v
if seen["full tip sha"] != seen["tip sha"] or seen["full main sha"] != seen["main sha"]:
    sys.stderr.write("prompt tip or main sha differs between its header and its Diff line\n")
    sys.exit(1)
for k in ("N", "1|2", "full tip sha", "full main sha", "F", "A", "D", "r1 tip", "F2", "A2", "D2"):
    if k in seen:
        print(k + "\t" + seen[k])
' "$TEMPLATE" "$prompt" "$round") || {
  rc=$?
  [ "$rc" = 1 ] || die 1 "matcher failed rc=$rc"
  exit 1
}

field() { printf '%s\n' "$fields" | awk -F'\t' -v k="$1" '$1 == k {print $2; exit}'; }

[ "$(field N)" = "$pr" ] || die 1 "PR number: prompt $(field N) != $pr"
[ "$(field '1|2')" = "$round" ] || die 1 "round: prompt says $(field '1|2'), argument $round"

[ -s "$ctx" ] || die 1 "ctx missing or empty: $ctx"
ctx_sha=$(sha256sum "$ctx" | cut -d' ' -f1)

tip=$("$GH" pr view "$pr" --json headRefOid --jq .headRefOid 2>/dev/null)
[[ $tip =~ ^[0-9a-f]{40}$ ]] || die 1 "tip: gh pr view $pr gave no head sha"
[ "$(field 'full tip sha')" = "$tip" ] || die 1 "tip: prompt $(field 'full tip sha') != PR head $tip"
[ "$(field 'full main sha')" = "$base" ] || die 1 "main: prompt $(field 'full main sha') != base $base"

numstat() {
  local out
  out=$(git -C "$REPO" diff --numstat "$1...$2" 2>/dev/null) || return 1
  printf '%s\n' "$out" | awk '
    NF {f++; if ($1 != "-") a += $1; if ($2 != "-") d += $2}
    END {printf "%d %d %d", f, a, d}'
}
git -C "$REPO" cat-file -e "$tip^{commit}" 2>/dev/null || die 1 "tip $tip not in the local repository: fetch the PR head first"
ns=$(numstat "$base" "$tip") || die 1 "numstat failed: $base...$tip"
read -r nf na nd <<<"$ns"
[ "$(field F)" = "$nf" ] || die 1 "files: prompt $(field F) != numstat $nf"
[ "$(field A)" = "$na" ] || die 1 "added: prompt $(field A) != numstat $na"
[ "$(field D)" = "$nd" ] || die 1 "deleted: prompt $(field D) != numstat $nd"

delta=
if [ "$round" = 2 ]; then
  r1=$(field 'r1 tip')
  git -C "$REPO" cat-file -e "$r1^{commit}" 2>/dev/null || die 1 "delta: r1 tip $r1 is not a commit"
  git -C "$REPO" merge-base --is-ancestor "$r1" "$tip" 2>/dev/null || die 1 "delta: r1 tip $r1 is not an ancestor of tip $tip"
  dns=$(numstat "$r1" "$tip") || die 1 "delta numstat failed: $r1...$tip"
  read -r df da dd <<<"$dns"
  [ "$(field F2)" = "$df" ] || die 1 "delta files: prompt $(field F2) != numstat $df"
  [ "$(field A2)" = "$da" ] || die 1 "delta added: prompt $(field A2) != numstat $da"
  [ "$(field D2)" = "$dd" ] || die 1 "delta deleted: prompt $(field D2) != numstat $dd"
  delta=" delta=$df +$da -$dd"
fi

printf 'review-kit-check OK template=%s prompt=%s ctx=%s base=%s tip=%s files=%s +%s -%s%s\n' \
  "${tpl_sha:0:12}" "$(sha256sum "$prompt" | cut -c1-12)" "${ctx_sha:0:12}" "$base" "$tip" "$nf" "$na" "$nd" "$delta"
