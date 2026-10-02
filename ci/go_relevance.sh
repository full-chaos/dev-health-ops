#!/usr/bin/env bash
# Decide whether a change set is Go-relevant, for the always-run go-quality job (CHAOS-4834).
#
# This replaces ci/go_relevance.py (CHAOS-7522 S9) so that the job needs no Python and no PyYAML before it
# can decide whether to run: bash, awk and the diff producer (ci/go_relevant_diff.sh) are all it uses.
# The decision, the output lines and the failure modes are the old decider's, and a test pins them (internal/relevancedecider:
# the old decider's answers over a fixed set of changed-file lists, recorded once from the Python).
#
# WHY IT EXISTS. `go-quality` is a REQUIRED check, declared in ONE workflow that always reports. Relevance is decided INSIDE
# the job and not in the trigger, because a path-filtered twin workflow could report the same required context twice.
#
# THE PATTERN LIST IS NOT RESTATED HERE. It is read from go.yml's own `on.pull_request.paths`, which still gates that workflow's
# other jobs: one list, so the two cannot drift, and a change that is relevant enough to run the storage shards is relevant
# enough to run go-quality. GO_WORKFLOW overrides the file (the test passes a fixture); production passes nothing.
#
# INPUT (stdin): NUL-terminated paths when a NUL byte is present (ci/go_relevant_diff.sh's `git diff -z --no-renames`, which
# disables all of git's path quoting), else one path per line (blank lines skipped, each line trimmed).
# OUTPUT (stdout): "changed files: N; Go-relevant: M", up to 20 matching paths, then relevant=true|false.
# An empty change set is not evidence of irrelevance (a wrong base ref gives one): it fails CLOSED to relevant=true.
# A path filter this translator does not understand is a HARD ERROR (exit 2), never a literal: a pattern that matches
# nothing would silently mark real changes irrelevant and skip the gate.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
workflow=${GO_WORKFLOW:-${here}/../.github/workflows/go.yml}

die() { printf '%s\n' "$*" >&2; exit 2; }

# The go.yml `on.pull_request.paths` list, one pattern per line. Only the shape go.yml uses is read (a block list of quoted or
# plain scalars under `  pull_request:` / `    paths:` inside the top-level `on:` block); anything else is an error, not a guess.
read_patterns() {
  awk '
    function indent(s) { match(s, /^ */); return RLENGTH }
    /^[[:space:]]*#/ || /^[[:space:]]*$/ { next }
    {
      n = indent($0)
      if (n == 0) { inon = ($0 ~ /^(on|"on"|'\''on'\''):[[:space:]]*(#.*)?$/); inpr = 0; inpaths = 0; next }
      if (!inon) next
      if (n == 2) { inpr = ($0 ~ /^  pull_request:[[:space:]]*(#.*)?$/); inpaths = 0; next }
      if (inpr && n == 4) { inpaths = ($0 ~ /^    paths:[[:space:]]*(#.*)?$/); if ($0 ~ /^    paths:[[:space:]]*\[/) bad = 1; next }
      if (inpr && inpaths && n == 6) {
        line = $0; sub(/^      - +/, "", line)
        if ($0 !~ /^      - /) { bad = 1; next }
        if (line ~ /^'\''/) { sub(/^'\''/, "", line); sub(/'\''[[:space:]]*(#.*)?$/, "", line) }
        else if (line ~ /^"/) { sub(/^"/, "", line); sub(/"[[:space:]]*(#.*)?$/, "", line) }
        else { sub(/[[:space:]]+#.*$/, "", line); sub(/[[:space:]]+$/, "", line) }
        print line; found = 1
      }
    }
    END { if (bad) exit 3; if (!found) exit 4 }
  ' "${workflow}"
}

# A GitHub path filter as an anchored extended regex: `**/` is zero or more leading directories, `**` spans separators, `*` does
# not, everything else is literal. `.` in the ERE matches a newline (no REG_NEWLINE), so a path with an embedded newline is
# matched like any other.
glob_to_regex() {
  local pattern=$1 out='' i=0 ch
  case "${pattern}" in *[\?\[\]\!]*) die "go.yml path filter '${pattern}' uses a character this translator does not implement (? [ ] !). Extend glob_to_regex rather than letting the pattern be treated as literal text -- a filter that matches nothing silently marks real changes irrelevant and skips the gate." ;; esac
  while [ "${i}" -lt "${#pattern}" ]; do
    if [ "${pattern:i:3}" = '**/' ]; then out+='(.*/)?'; i=$((i + 3))
    elif [ "${pattern:i:2}" = '**' ]; then out+='.*'; i=$((i + 2))
    else
      ch=${pattern:i:1}
      case "${ch}" in
        '*') out+='[^/]*' ;;
        [A-Za-z0-9_/-]) out+="${ch}" ;;
        *) out+="\\${ch}" ;;
      esac
      i=$((i + 1))
    fi
  done
  printf '^%s$' "${out}"
}

raw=$(mktemp); trap 'rm -f "${raw}"' EXIT
cat > "${raw}"
changed=()
if [ "$(tr -d '\0' < "${raw}" | wc -c)" != "$(wc -c < "${raw}")" ]; then
  while IFS= read -r -d '' path || [ -n "${path}" ]; do [ -n "${path}" ] && changed+=("${path}"); done < "${raw}"
else
  while IFS= read -r line || [ -n "${line}" ]; do
    line=${line#"${line%%[![:space:]]*}"}; line=${line%"${line##*[![:space:]]}"}
    [ -n "${line}" ] && changed+=("${line}")
  done < "${raw}"
fi

if [ "${#changed[@]}" -eq 0 ]; then
  echo "no changed files resolved; treating as RELEVANT (fail closed)"
  echo "relevant=true"
  exit 0
fi

patterns=()
status=0
while IFS= read -r line; do patterns+=("${line}"); done < <(read_patterns; echo "rc=$?")
last=${patterns[${#patterns[@]} - 1]:-}
unset 'patterns[${#patterns[@]}-1]'
case "${last}" in
  rc=0) ;;
  rc=3) die "go.yml's on.pull_request.paths is not a block list this reader understands; refusing to guess" ;;
  rc=4) die "go.yml declares no pull_request paths; refusing to guess" ;;
  *) die "could not read ${workflow}" ;;
esac
[ "${#patterns[@]}" -gt 0 ] || die "go.yml declares no pull_request paths; refusing to guess"

regexes=()
for pattern in "${patterns[@]}"; do regexes+=("$(glob_to_regex "${pattern}")"); done

matched=()
for path in "${changed[@]}"; do
  for regex in "${regexes[@]}"; do
    if [[ ${path} =~ ${regex} ]]; then matched+=("${path}"); break; fi
  done
done

echo "changed files: ${#changed[@]}; Go-relevant: ${#matched[@]}"
shown=0
for path in "${matched[@]}"; do
  [ "${shown}" -lt 20 ] || break
  printf '  %s\n' "${path}"; shown=$((shown + 1))
done
if [ "${#matched[@]}" -gt 20 ]; then echo "  ... and $((${#matched[@]} - 20)) more"; fi
if [ "${#matched[@]}" -gt 0 ]; then echo "relevant=true"; else echo "relevant=false"; fi
