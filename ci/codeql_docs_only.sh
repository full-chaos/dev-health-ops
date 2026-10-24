#!/usr/bin/env bash
# Usage: codeql_docs_only.sh <base-sha> <head-sha>
# Prints "true" only when the change between the two commits is provably
# docs-only; anything else (bad input, a git error, an empty diff, any other
# path) prints "false". The paths come from git itself for exactly these two
# commits (three-dot, no rename detection so a rename lists its source too),
# never from an API that answers for a later head. A path is a docs path when
# it is docs/**, docs-data/**, any *.md, .github/docs-legacy/**, or mkdocs.yml
# at the repository root, and it is not Go source or a Go module file.
#
# This is the ONE docs-only definition: CodeQL, go-quality, go.yml, docker-images,
# live-e2e and the auth gates all call it. A docs path that a Go test reads is not
# docs-only: the base commit's .github/workflows/go.yml on.push.paths lists every
# Go test input, and a path matching one of those globs fails the verdict. A
# go.yml this script cannot read or parse also fails it.
#
# codeql_docs_only.sh --go-inputs <base-sha> prints the parsed globs instead (one
# per line, for the guard test that pins the parse to go.yml); its output is never
# "true", so a caller comparing with "true" stays fail closed.
set -u
fail() {
  echo false
  exit 0
}
print_inputs=false
if [ "$#" -eq 2 ] && [ "$1" = "--go-inputs" ]; then
  print_inputs=true
  set -- "$2" "$2"
fi
[ "$#" -eq 2 ] || fail
base=$1
head=$2
[[ $base =~ ^[0-9a-f]{40}$ && $head =~ ^[0-9a-f]{40}$ ]] || fail
# Paths are read NUL-delimited, so a path holding a newline stays one path. The
# diff is written to a file first so a git failure (even after partial output)
# is seen and fails closed; an empty diff yields no path, which is not docs-only.
list=$(mktemp)
wf=$(mktemp)
trap 'rm -f "$list" "$wf"' EXIT
git diff --name-only --no-renames -z "$base...$head" >"$list" || fail
git show "$base:.github/workflows/go.yml" >"$wf" || fail

# go_inputs: the base go.yml's on.push.paths, one glob per line. Every entry must
# have the exact shape `      - 'glob'`; any other line inside the list fails.
go_inputs() {
  awk '
    /^  push:[[:space:]]*$/ { inpush = 1; next }
    inpush && /^  [^ #]/ { inpush = 0 }
    inpush && /^    paths:[[:space:]]*$/ { inpaths = 1; next }
    inpaths {
      if ($0 ~ /^[[:space:]]*(#.*)?$/) next
      if ($0 ~ /^      - \047[^\047]+\047$/) { sub(/^      - \047/, ""); sub(/\047$/, ""); print; n++; next }
      if ($0 ~ /^      /) { bad = 1; exit }
      exit
    }
    END { if (bad || n == 0) exit 1 }
  ' "$wf"
}
# glob_regex mirrors GitHub path-filter globs (and githubGlobToRegexp in the Go
# guards): `**/` is zero or more directories, `**` spans separators, `*` does
# not; `?`, `[`, `]` and `!` are refused.
glob_regex() {
  local g=$1 out="" c
  case "$g" in *[?![]* | *]*) return 1 ;; esac
  while [ -n "$g" ]; do
    case "$g" in
      '**/'*) out+='(.*/)?'; g=${g:3} ;;
      '**'*) out+='.*'; g=${g:2} ;;
      '*'*) out+='[^/]*'; g=${g:1} ;;
      *)
        c=${g:0:1}
        case "$c" in [.^\$+\(\)\{\}\|\\]) out+="\\$c" ;; *) out+="$c" ;; esac
        g=${g:1}
        ;;
    esac
  done
  printf '^%s$' "$out"
}
inputs=$(go_inputs) || fail
if [ "$print_inputs" = true ]; then
  printf '%s\n' "$inputs"
  exit 0
fi
regexes=()
while IFS= read -r glob; do
  re=$(glob_regex "$glob") || fail
  regexes+=("$re")
done <<<"$inputs"
[ "${#regexes[@]}" -ge 1 ] || fail

count=0
while IFS= read -r -d '' path; do
  count=$((count + 1))
  case "$path" in
    # Go source and module files are what CodeQL's Go leg analyzes, wherever they sit (docs/ holds a Go package too).
    *.go | go.mod | */go.mod | go.sum | */go.sum | go.work | */go.work | go.work.sum | */go.work.sum) fail ;;
    docs/* | docs-data/* | *.md | .github/docs-legacy/* | mkdocs.yml) ;;
    *) fail ;;
  esac
  for re in "${regexes[@]}"; do
    [[ $path =~ $re ]] && fail
  done
done <"$list"
[ "$count" -ge 1 ] || fail
echo true
