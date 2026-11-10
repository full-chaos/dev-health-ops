#!/usr/bin/env bash
# CHAOS-13144: acr-db-init-fetch.sh writes the pinned script only after the sha256 matches, and refuses a bad fetch or a bad pin.
# FETCH=<script> runs the cells against another copy of the script (the mutant runs do).
set -uo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
fetch=${FETCH:-$here/acr-db-init-fetch.sh}
t=$(mktemp -d -p "${TMPDIR:-$here}"); chmod 700 "$t"
trap 'rm -rf "${t:?}"' EXIT
fail=0
bad() { echo "FAIL: $1"; fail=1; }
git init -q "$t/src"; mkdir -p "$t/src/deploy/compose"; printf '#!/bin/sh\necho pinned\n' >"$t/src/deploy/compose/acr-db-init.sh"
g=(git -C "$t/src" -c user.name=t -c user.email=t@example.invalid -c commit.gpgsign=false)
"${g[@]}" add -A; "${g[@]}" commit -q -m t
c=$("${g[@]}" rev-parse HEAD); h=$(sha256sum "$t/src/deploy/compose/acr-db-init.sh" | cut -d' ' -f1)
z=$(printf 0%.0s $(seq 64)); zc=$(printf 0%.0s $(seq 40))
pin() { printf 'ACR_DB_INIT_COMMIT=%s\nACR_DB_INIT_SHA256=%s\n' "$2" "$3" >"$t/$1.pin"; }
pin ok "$c" "$h"; pin badsha "$c" "$z"; pin badcommit "$zc" "$h"; pin emptysha "$c" ""; pin emptycommit "" "$h"
printf 'ACR_DB_INIT_COMMIT=%s\n' "$c" >"$t/nosha.pin"
run() { ACR_DB_INIT_PIN="$t/$1.pin" ACR_REPO_URL="${3:-$t/src}" bash "$fetch" "$2" 2>&1; }
snap() { { ls -A "$1" 2>/dev/null; cat "$1/acr-db-init.sh" 2>/dev/null; } | sha256sum; }

# cell ok: a matching pin writes the script, executable, and prints the path
o=$(run ok "$t/o1"); rc=$?
[ "$rc" = 0 ] && [ "$(tail -1 <<<"$o")" = "$t/o1/acr-db-init.sh" ] && [ -x "$t/o1/acr-db-init.sh" ] && [ "$(sha256sum <"$t/o1/acr-db-init.sh" | cut -d' ' -f1)" = "$h" ] || bad "cell ok: matching pin: rc=$rc $o"
[ "$(ls -A "$t/o1")" = "acr-db-init.sh" ] || bad "cell ok: stray files in the out dir: $(ls -A "$t/o1")"
# cell a: sha mismatch refuses and leaves the fixed path as it was (absent, then holding an old file)
o=$(run badsha "$t/a1"); rc=$?
{ [ "$rc" != 0 ] && [ -z "$(ls -A "$t/a1")" ]; } || bad "cell a: sha mismatch, fresh dir: rc=$rc files=$(ls -A "$t/a1")"
mkdir "$t/a2"; printf 'OLD\n' >"$t/a2/acr-db-init.sh"; before=$(snap "$t/a2")
o=$(run badsha "$t/a2"); rc=$?
{ [ "$rc" != 0 ] && [ "$(snap "$t/a2")" = "$before" ]; } || bad "cell a: sha mismatch changed or littered the existing target: rc=$rc"
# cell b: a failed fetch (bad commit, bad URL) refuses and leaves the target as it was
for case in "badcommit|$t/src" "ok|$t/no-such-source"; do
  p=${case%%|*}; u=${case#*|}
  mkdir "$t/b-$p"; printf 'OLD\n' >"$t/b-$p/acr-db-init.sh"; before=$(snap "$t/b-$p")
  o=$(run "$p" "$t/b-$p" "$u"); rc=$?
  { [ "$rc" != 0 ] && [ "$(snap "$t/b-$p")" = "$before" ]; } || bad "cell b: failed fetch ($p, $u): rc=$rc"
  o=$(run "$p" "$t/b2-$p" "$u"); rc=$?
  { [ "$rc" != 0 ] && [ -z "$(ls -A "$t/b2-$p" 2>/dev/null)" ]; } || bad "cell b: failed fetch left a file ($p, $u): rc=$rc"
done
# cell c: an empty or missing pin field refuses
for p in emptysha emptycommit nosha; do
  o=$(run "$p" "$t/c-$p"); rc=$?
  { [ "$rc" != 0 ] && [ -z "$(ls -A "$t/c-$p" 2>/dev/null)" ]; } || bad "cell c: pin $p: rc=$rc files=$(ls -A "$t/c-$p" 2>/dev/null)"
done
o=$(ACR_DB_INIT_PIN="$t/missing.pin" ACR_REPO_URL="$t/src" bash "$fetch" "$t/c-missing" 2>&1); rc=$?
{ [ "$rc" != 0 ] && [ -z "$(ls -A "$t/c-missing" 2>/dev/null)" ]; } || bad "cell c: missing pin file: rc=$rc"
# cell d: the script keeps its strict-mode guard
[ "$(grep -c '^set -euo pipefail$' "$fetch")" = 1 ] || bad "cell d: set -euo pipefail missing from $fetch"
[ "$fail" = 0 ] || exit 1
echo "ok: acr-db-init-fetch.sh cells ok, a, b, c, d"
