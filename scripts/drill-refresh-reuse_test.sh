#!/usr/bin/env bash
# Test of scripts/drill-refresh-reuse.sh with a fake kubectl and a fake curl on PATH: no cluster, no docker, no network.
# Every token below is a test marker, not a credential.
#   1. `run` must not delete a namespace it did not create (create answers AlreadyExists, the namespace carries
#      another run's label), and must delete the one it did create (and labelled).
#   2. A login that fails must not print the response body: only whitelisted error text.
# DRILL_SCRIPT overrides the script under test (used to run these cells against another tree).
set -euo pipefail
cd "$(dirname "$0")"
script="${DRILL_SCRIPT:-$PWD/drill-refresh-reuse.sh}"

root="$(mktemp -d)"
trap 'rm -rf "$root"' EXIT
chmod 700 "$root"
mkdir "$root/bin"

cat >"$root/bin/kubectl" <<'FAKE'
#!/usr/bin/env bash
# Records every call. FAKE_SCEN=race: the namespace appears after the absence checks and `create` answers AlreadyExists.
a=("$@")
if [ "${a[0]:-}" = -n ]; then a=("${a[@]:2}"); fi
echo "${a[*]}" >>"$FAKE_LOG"
st="$FAKE_STATE"
case "${a[0]} ${a[1]:-}" in
  "get ns")
    [ -e "$st/ns" ] || exit 1
    case "${a[*]}" in *jsonpath*) sed -n 's/^ *drill-run: "\(.*\)"$/\1/p' "$st/ns" ;; esac
    exit 0 ;;
  "create -f")
    if [ "${FAKE_SCEN:-}" = race ] || [ "${FAKE_SCEN:-}" = race-unlabelled ]; then
      if [ "$FAKE_SCEN" = race ]; then printf 'drill-run: "someone-elses-run"\n' >"$st/ns"; else : >"$st/ns"; fi
      echo 'Error from server (AlreadyExists): namespaces "x" already exists' >&2
      exit 1
    fi
    cat >"$st/ns"; exit 0 ;;
  "delete ns") rm -f "$st/ns"; exit 0 ;;
  "rollout status")
    # swap: the create was ours, then the namespace was replaced by one that carries another run's label.
    [ "${FAKE_SCEN:-}" = swap ] && printf 'drill-run: "someone-elses-run"\n' >"$st/ns"
    exit 1 ;;
esac
exit 0
FAKE
cat >"$root/bin/curl" <<'FAKE'
#!/usr/bin/env bash
# The readiness probe (-o /dev/null) answers at once. The two logins answer per FAKE_CURL: a = S1 fails, b = S2 fails.
out=""
while [ $# -gt 0 ]; do
  [ "$1" = -o ] && out="$2"
  shift
done
[ "$out" = /dev/null ] && exit 0
n=$(($(cat "$FAKE_STATE/curl-n" 2>/dev/null || echo 0) + 1))
echo "$n" >"$FAKE_STATE/curl-n"
tokens='"refresh_token":"REFRESH_SECRET_MARKER","access_token":"ACCESS_SECRET_MARKER"'
case "$FAKE_CURL:$n" in
  a:1) printf '{%s}' "$tokens" >"$out"; printf 200 ;;
  a:2) printf '{%s,"error":"failed"}' "$tokens" >"$out"; printf 500 ;;
  b:1) printf '{%s,"error":"s2 failed"}' "$tokens" >"$out"; printf 500 ;;
  b:2) printf '{%s}' "$tokens" >"$out"; printf 200 ;;
esac
FAKE
chmod 755 "$root/bin/kubectl" "$root/bin/curl"

fails=0
check() { # name, condition result
  if [[ "$2" == ok ]]; then echo "ok   $1"; else echo "FAIL $1"; fails=$((fails + 1)); fi
}

d=ghcr.io/acme/img@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
run_drill() { # VERB CASE-NAME [ENV=VAL...] -> $root/CASE.out, $root/CASE.rc, $root/CASE.log
  local verb="$1" name="$2"
  shift 2
  mkdir -p "$root/$name.state" "$root/$name.dir"
  chmod 700 "$root/$name.dir"
  : >"$root/$name.log"
  set +e
  env "$@" PATH="$root/bin:$PATH" KUBECONFIG="$root/kubeconfig" DRILL_NS=drill-test DRILL_DIR="$root/$name.dir" \
    FAKE_STATE="$root/$name.state" FAKE_LOG="$root/$name.log" AUTH_SERVICE_IMAGE="$d" AUTH_MIGRATE_IMAGE="$d" DHO_IMAGE="$d" \
    bash "$script" "$verb" >"$root/$name.out" 2>&1
  echo $? >"$root/$name.rc"
  set -e
}

# 1a. the namespace appears after the absence checks, create says AlreadyExists: it is not ours, it stays.
run_drill run race FAKE_SCEN=race
if grep -q '^delete ns' "$root/race.log"; then r=bad; else r=ok; fi
check "run does not delete a namespace it did not create" "$r"
grep -q 'REFUSED' "$root/race.out" && [[ "$(cat "$root/race.rc")" != 0 ]] && r=ok || r=bad
check "the refusal is printed and the run exits non-zero" "$r"

run_drill run racebare FAKE_SCEN=race-unlabelled
if grep -q '^delete ns' "$root/racebare.log"; then r=bad; else r=ok; fi
check "run does not delete an unlabelled namespace it did not create" "$r"

# 1c. the namespace was ours at create time, but now carries another run's label: it stays.
run_drill run swap FAKE_SCEN=swap
if grep -q '^delete ns' "$root/swap.log"; then r=bad; else r=ok; fi
check "run does not delete a namespace whose run label is not its own" "$r"
grep -q 'REFUSED' "$root/swap.out" && r=ok || r=bad
check "the label mismatch prints a refusal" "$r"

# 1b. a namespace this run created is deleted when setup fails later.
run_drill run own
grep -q '^delete ns drill-test' "$root/own.log" && grep -q 'ns=drill-test deleted' "$root/own.out" && r=ok || r=bad
check "run deletes the namespace it created" "$r"
[[ "$(cat "$root/own.rc")" != 0 ]] && r=ok || r=bad
check "a failed setup still exits non-zero after the teardown" "$r"

# 2. a failed login prints no token and no body.
for scen in a b; do
  run_drill cells "login-$scen" FAKE_CURL="$scen"
  if grep -q 'SECRET_MARKER' "$root/login-$scen.out"; then r=bad; else r=ok; fi
  check "login failure case $scen prints no token" "$r"
  grep -q 'DRILL FAILED' "$root/login-$scen.out" && grep -q 'login refused' "$root/login-$scen.out" && r=ok || r=bad
  check "login failure case $scen still refuses the run" "$r"
done
grep -q 'login refused: s1 failed' "$root/login-a.out" && r=ok || r=bad
check "login failure case a prints the whitelisted error text" "$r"

[[ "$fails" == 0 ]] || { echo "drill-refresh-reuse_test: $fails check(s) failed" >&2; exit 1; }
echo "drill-refresh-reuse_test: ok"
