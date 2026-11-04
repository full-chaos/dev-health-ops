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
  "delete ns")
    rm -f "$st/ns"
    # delfail: the delete answers an error (the namespace is gone anyway): the teardown must pass the rc on.
    [ "${FAKE_SCEN:-}" = delfail ] && exit 1
    exit 0 ;;
  "port-forward "*)
    # FAKE_PF=fail: the forward cannot listen and exits 1 (a foreign process may hold the port). Otherwise it prints its
    # Forwarding line for the port it was given and stays alive until $FAKE_STATE/pf-die exists.
    if [ "${FAKE_PF:-}" = fail ]; then echo 'error: unable to listen on port' >&2; exit 1; fi
    echo "Forwarding from 127.0.0.1:${a[2]%%:*} -> 8095"
    while [ ! -e "$st/pf-die" ]; do sleep 0.2; done
    exit 0 ;;
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
echo "$out" >>"$FAKE_STATE/cred-requests"
n=$(($(cat "$FAKE_STATE/curl-n" 2>/dev/null || echo 0) + 1))
echo "$n" >"$FAKE_STATE/curl-n"
tokens='"refresh_token":"REFRESH_SECRET_MARKER","access_token":"ACCESS_SECRET_MARKER"'
case "$FAKE_CURL:$n" in
  a:1) printf '{%s}' "$tokens" >"$out"; printf 200 ;;
  a:2) printf '{%s,"error":"failed"}' "$tokens" >"$out"; printf 500 ;;
  b:1) printf '{%s,"error":"s2 failed"}' "$tokens" >"$out"; printf 500 ;;
  b:2) printf '{%s}' "$tokens" >"$out"; printf 200 ;;
  # die: the forward process ends right after the second login answered 200 (it ends before this curl returns)
  die:*) printf '{%s}' "$tokens" >"$out"; printf 200
    if [ "$n" = 2 ]; then : >"$FAKE_STATE/pf-die"; sleep 1.5; fi ;;
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

# 1d. the teardown verb alone, on a namespace it has no create record for, must refuse: an unlabelled namespace
# (both the record and the label are empty), and a record of another run id than DRILL_RUN_ID.
mkdir -p "$root/tdbare.state" "$root/tdbare.dir"
: >"$root/tdbare.state/ns"
run_drill teardown tdbare
if grep -q '^delete ns' "$root/tdbare.log"; then r=bad; else r=ok; fi
check "teardown without a create record does not delete an unlabelled namespace" "$r"
grep -q 'REFUSED' "$root/tdbare.out" && [[ "$(cat "$root/tdbare.rc")" != 0 ]] && r=ok || r=bad
check "teardown without a create record refuses and exits non-zero" "$r"

mkdir -p "$root/tdid.state" "$root/tdid.dir"
printf 'drill-run: "mine-1"\n' >"$root/tdid.state/ns"
printf 'rc=0 run=mine-1\n' >"$root/tdid.dir/ns-created"
run_drill teardown tdid DRILL_RUN_ID=other-run
if grep -q '^delete ns' "$root/tdid.log"; then r=bad; else r=ok; fi
check "teardown with another run id does not delete the namespace" "$r"

# 1e. a valid record and label: deleted, exit 0. A delete that fails: the rc is passed on, no "deleted" line.
for scen in good delfail; do
  mkdir -p "$root/td-$scen.state" "$root/td-$scen.dir"
  printf 'drill-run: "mine-1"\n' >"$root/td-$scen.state/ns"
  printf 'rc=0 run=mine-1\n' >"$root/td-$scen.dir/ns-created"
  run_drill teardown "td-$scen" DRILL_RUN_ID=mine-1 FAKE_SCEN=$scen
done
grep -q '^delete ns drill-test' "$root/td-good.log" && grep -q 'ns=drill-test deleted' "$root/td-good.out" && [[ "$(cat "$root/td-good.rc")" == 0 ]] && r=ok || r=bad
check "teardown deletes a namespace with its own record and label" "$r"
if [[ "$(cat "$root/td-delfail.rc")" != 0 ]] && ! grep -q 'deleted' "$root/td-delfail.out"; then r=ok; else r=bad; fi
check "a failed delete is not hidden: non-zero rc, no deleted line" "$r"

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

# 2b. the port-forward must prove its peer before any credential leaves (default port and an override).
for port in default 19991; do
  if [ "$port" = default ]; then pe=(); want_port=18230; else pe=(LOCAL_PORT="$port"); want_port="$port"; fi
  run_drill cells "pf-fail-$port" FAKE_CURL=a FAKE_PF=fail "${pe[@]}"
  if [ -s "$root/pf-fail-$port.state/cred-requests" ]; then r=bad; else r=ok; fi
  check "dead port-forward ($port): no credential request is sent to a foreign responder" "$r"
  grep -q "ABORT: port-forward on 127.0.0.1:$want_port is not proven" "$root/pf-fail-$port.out" && [[ "$(cat "$root/pf-fail-$port.rc")" != 0 ]] && r=ok || r=bad
  check "dead port-forward ($port): abort with a named reason, non-zero rc" "$r"
done
run_drill cells pf-live FAKE_CURL=a LOCAL_PORT=19991
grep -q 'Forwarding' "$root/pf-live.dir/port-forward.log" && [[ "$(wc -l <"$root/pf-live.state/cred-requests")" -ge 2 ]] && ! grep -q 'ABORT' "$root/pf-live.out" && r=ok || r=bad
check "live port-forward (control): the cells proceed and send credentials" "$r"
run_drill cells pf-die FAKE_CURL=die
grep -q 'ABORT: port-forward process is not alive; no credential sent' "$root/pf-die.out" && [[ "$(wc -l <"$root/pf-die.state/cred-requests")" == 2 ]] && r=ok || r=bad
check "port-forward dies between cells: the next credential cell aborts, no third request" "$r"

# 3. the error-text helpers print no token or other key of a body, and no line of the script prints a response file
# except through them.
eval "$(sed -n '/^safe_err() {/,/^}/p;/^detail() {/p' "$script")"
body="$root/body.json"
printf '{"refresh_token":"REFRESH_SECRET_MARKER","session_access_token":"ACCESS_SECRET_MARKER","error":"bad request","detail":{"message":"nope","access_token":"ACCESS_SECRET_MARKER"},"reason_code":"reuse"}' >"$body"
out="$(safe_err "$body") $(detail "$body")"
if [[ "$out" == *SECRET_MARKER* || "$out" != *"bad request"* ]]; then r=bad; else r=ok; fi
check "safe_err and detail print the whitelisted text and no token" "$r"
# shellcheck disable=SC2016 # the pattern is a regex for grep, not a shell expansion
if grep -nE '(echo|printf).*(jq|cat)[^|]*"\$tok/' "$script" | grep -vE 'safe_err|detail'; then r=bad; else r=ok; fi
check "no echo or printf of a response file bypasses safe_err and detail" "$r"

[[ "$fails" == 0 ]] || { echo "drill-refresh-reuse_test: $fails check(s) failed" >&2; exit 1; }
echo "drill-refresh-reuse_test: ok"
