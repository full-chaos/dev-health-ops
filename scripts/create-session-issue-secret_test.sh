#!/usr/bin/env bash
# Fake-kubectl test of create-session-issue-secret.sh (CHAOS-11217). No cluster,
# no docker. The sentinel value below is a test value, not a credential.
set -euo pipefail
cd "$(dirname "$0")"
script="$PWD/create-session-issue-secret.sh"

shmbase=/dev/shm
[ -d "$shmbase" ] && [ -w "$shmbase" ] || { echo "SKIP: no writable /dev/shm" >&2; exit 0; }
root="$(mktemp -d "$shmbase/issue-secret-test.XXXXXX")"
disk="$(mktemp -d "$PWD/.issue-secret-test.XXXXXX")"
trap 'rm -rf "$root" "$disk"' EXIT
chmod 700 "$root" "$disk"

tok="svc_authsess_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
[ "${#tok}" -eq 56 ] || { echo "bad sentinel length" >&2; exit 1; }

fake="$root/kubectl"
cat >"$fake" <<'FAKE'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$FAKE_LOG"
case "$FAKE_MODE" in
  ok) for a in "$@"; do case "$a" in --from-file=*) f="${a#--from-file=}"; f="${f#*=}"; cat "$f" >"$FAKE_SEEN" ;; esac; done ;;
  exists) echo 'Error from server (AlreadyExists): secrets "x" already exists' >&2; exit 1 ;;
  other) echo 'boom' >&2; exit 1 ;;
esac
FAKE
chmod 755 "$fake"

fails=0
fail() { echo "FAIL: $*" >&2; fails=$((fails + 1)); }

# run NAME MODE VALUEFILE [extra args]; sets out, rc.
run() {
  local mode="$1" vf="$2"; shift 2
  : >"$root/log"; rm -f "$root/seen"
  set +e
  out="$(FAKE_MODE="$mode" FAKE_LOG="$root/log" FAKE_SEEN="$root/seen" KUBECTL="$fake" \
    SESSION_ISSUE_WORKDIR_BASE="$root" bash "$script" --context ctx --namespace ns \
    --name dev-health-auth-session-issue --value-file "$vf" "$@" 2>&1)"
  rc=$?
  set -e
}

newfile() { # base, content-with-newline-flag
  local f; f="$(mktemp "$1/val.XXXXXX")"; printf '%s\n' "$2" >"$f"; chmod 0600 "$f"; echo "$f"
}
leftovers() { find "$root" -mindepth 1 -maxdepth 1 -name 'session-issue*' | wc -l; }

# 1 success: created, value never printed, source shredded, workdir clean.
vf="$(newfile "$root" "$tok")"
run ok "$vf"
[ "$rc" -eq 0 ] || fail "success rc=$rc"
[ "$out" = "dev-health-auth-session-issue key=AUTH_SESSION_ISSUE_TOKEN created" ] || fail "success output: $out"
case "$out" in *"$tok"*|*svc_authsess_A*) fail "value printed" ;; esac
grep -q "$tok" "$root/log" && fail "value in kubectl argv"
[ "$(cat "$root/seen")" = "$tok" ] || fail "secret file content wrong"
grep -q '^--context ctx --namespace ns create secret generic dev-health-auth-session-issue --from-file=AUTH_SESSION_ISSUE_TOKEN=' "$root/log" || fail "kubectl argv shape"
[ "$(wc -l <"$root/log")" -eq 1 ] || fail "more than one kubectl call"
grep -Eq ' (get|describe|apply|patch|replace|delete|exec) ' "$root/log" && fail "forbidden kubectl verb"
[ ! -e "$vf" ] || fail "source file kept after success"
[ "$(leftovers)" -eq 0 ] || fail "workdir left behind after success"

# 2 AlreadyExists: STOP, source kept, path printed, no value.
vf="$(newfile "$root" "$tok")"
run exists "$vf"
[ "$rc" -eq 1 ] || fail "exists rc=$rc"
case "$out" in *"STOP dev-health-auth-session-issue exists"*) ;; *) fail "exists output: $out" ;; esac
case "$out" in *"value file kept for retry: $vf"*) ;; *) fail "kept-path line missing: $out" ;; esac
case "$out" in *"$tok"*) fail "value printed on exists" ;; esac
[ -f "$vf" ] || fail "source removed after failure"
[ "$(leftovers)" -eq 0 ] || fail "workdir left behind after failure"

# 3 other create failure.
run other "$vf"
[ "$rc" -eq 1 ] || fail "other rc=$rc"
case "$out" in *"STOP dev-health-auth-session-issue create failed"*) ;; *) fail "other output: $out" ;; esac
[ -f "$vf" ] || fail "source removed after other failure"

# 4 refusals: kubectl must not run.
refuse() { # label, vf
  run ok "$2"
  [ "$rc" -ne 0 ] || fail "$1: accepted"
  [ ! -s "$root/log" ] || fail "$1: kubectl ran"
  case "$out" in *"$tok"*) fail "$1: value printed" ;; esac
  [ -e "$2" ] || [ -L "$2" ] || [ "$1" = missing ] || fail "$1: source removed"
}
vf="$(newfile "$disk" "$tok")";            refuse notmpfs "$vf"
vf="$(newfile "$root" "$tok")"; chmod 0640 "$vf"; refuse mode "$vf"
vf="$(newfile "$root" "$tok")"; ln -s "$vf" "$root/link"; refuse symlink "$root/link"
refuse missing "$root/nope"
vf="$(newfile "$root" "$tok")"; printf '%s\n%s\n' "$tok" "$tok" >"$vf"; refuse twolines "$vf"
vf="$(newfile "$root" "$tok ")";           refuse space "$vf"
vf="$(newfile "$root" " $tok")";           refuse leadspace "$vf"
vf="$(newfile "$root" "svc_other_${tok#svc_authsess_}")"; refuse prefix "$vf"
vf="$(newfile "$root" "${tok}A")";         refuse longvalue "$vf"
vf="$(newfile "$root" "${tok%?}")";        refuse shortvalue "$vf"
vf="$(newfile "$root" "")";                refuse empty "$vf"
printf '%s' "$tok" >"$root/nonl"; chmod 0600 "$root/nonl"
run ok "$root/nonl"; [ "$rc" -eq 0 ] || fail "no trailing newline refused"

# 5 argument refusals.
vf="$(newfile "$root" "$tok")"
for bad in "Bad_Name" "a/b" ""; do
  : >"$root/log"
  set +e
  KUBECTL="$fake" FAKE_MODE=ok FAKE_LOG="$root/log" bash "$script" --context c --namespace n --name "$bad" --value-file "$vf" >/dev/null 2>&1
  r=$?
  set -e
  [ "$r" -ne 0 ] && [ ! -s "$root/log" ] || fail "bad name '$bad' accepted"
done
set +e; KUBECTL="$fake" bash "$script" --context c --namespace n --name x --value-file "$vf" --key 'a b' >/dev/null 2>&1; r=$?; set -e
[ "$r" -ne 0 ] || fail "bad key accepted"
set +e; KUBECTL="$fake" bash "$script" --bogus >/dev/null 2>&1; r=$?; set -e
[ "$r" -ne 0 ] || fail "unknown argument accepted"
set +e; KUBECTL="$fake" bash -x "$script" --context c --namespace n --name x --value-file "$vf" >/dev/null 2>&1; r=$?; set -e
[ "$r" -ne 0 ] || fail "xtrace accepted"

# 6 signal during kubectl: workdir removed.
cat >"$root/slow" <<'SLOW'
#!/usr/bin/env bash
sleep 5
SLOW
chmod 755 "$root/slow"
vf="$(newfile "$root" "$tok")"
KUBECTL="$root/slow" SESSION_ISSUE_WORKDIR_BASE="$root" bash "$script" --context c --namespace n --name x --value-file "$vf" >/dev/null 2>&1 &
pid=$!
for _ in 1 2 3 4 5 6 7 8 9 10; do [ "$(leftovers)" -gt 0 ] && break; sleep 0.2; done
kill -TERM "$pid" 2>/dev/null || true
wait "$pid" 2>/dev/null || true
sleep 0.3
[ "$(leftovers)" -eq 0 ] || fail "workdir left behind after SIGTERM"
[ -f "$vf" ] || fail "source removed after SIGTERM"

[ "$fails" -eq 0 ] || { echo "$fails check(s) failed" >&2; exit 1; }
echo "create-session-issue-secret_test: ok"
