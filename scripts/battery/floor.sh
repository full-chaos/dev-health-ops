#!/usr/bin/env bash
# Derive the `func Test` FLOOR for a package list, with a negative control.
#
# Usage: floor.sh <repo-root> <package-path-list...>
# Prints "<import-path>\t<count>" per package on stdout and the SUM on the last
# line as "TOTAL\t<n>".
#
# THE FLOOR IS THE SUM OVER THE WHOLE PACKAGE LIST, never one package's count.
# It replaces a `-run` selector: the packages run WHOLE, and the number of tests
# that actually ran must reach the number of top-level Test functions the tree
# declares. A selector can go stale silently; a floor derived from the tree
# cannot, because the tree is the thing that changed.
#
# THE COUNTER IS NEGATIVE-CONTROLLED. A floor is evidence only if the counter
# can also return zero for something genuinely absent -- otherwise a counter
# that returns a plausible number for everything passes every check it is put
# to. An impossible pattern must count 0 against the same tree in the same form.
set -uo pipefail

ROOT="${1:?usage: floor.sh <repo-root> <package-path-list...>}"
shift
PKGS="$*"
[ -n "${PKGS// /}" ] || { echo "floor.sh: no packages given" >&2; exit 2; }

cd "$ROOT" || { echo "floor.sh: cannot cd $ROOT" >&2; exit 2; }

listing=$(go list -f '{{.ImportPath}}	{{.Dir}}' $PKGS) || {
  echo "floor.sh: go list failed over [$PKGS]" >&2; exit 2; }
[ -n "$listing" ] || { echo "floor.sh: the package list resolved to nothing" >&2; exit 2; }

total=0
first_dir=""
while IFS=$'\t' read -r ip dir; do
  [ -n "$ip" ] || continue
  [ -n "$first_dir" ] || first_dir="$dir"
  n=$(grep -rhoE '^func Test[A-Za-z0-9_]*\(' "$dir"/*_test.go 2>/dev/null | wc -l | tr -d ' ')
  case "$n" in ''|*[!0-9]*) echo "floor.sh: func Test count unparseable for $ip ($n)" >&2; exit 2 ;; esac
  printf '%s\t%s\n' "$ip" "$n"
  total=$((total+n))
done <<< "$listing"

[ "$total" -gt 0 ] || {
  echo "floor.sh: derived floor is 0 -- the counter is broken, and a zero floor admits every vacuous run" >&2
  exit 2; }

ctrl=$(grep -rhoE '^func TestZZZNoSuchPrefix[A-Za-z0-9_]*\(' "$first_dir"/*_test.go 2>/dev/null | wc -l | tr -d ' ')
[ "$ctrl" = "0" ] || {
  echo "floor.sh: the counter's negative control found $ctrl matches for a pattern nothing should match" >&2
  exit 2; }
echo "floor.sh: counter control ok -- an impossible pattern counts 0 while the real one counts $total" >&2

printf 'TOTAL\t%s\n' "$total"
