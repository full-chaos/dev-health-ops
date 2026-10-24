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
  if [ -e "$2" ] || [ -L "$2" ]; then
    case "$out" in *"value file kept for retry: $2"*) ;; *) fail "$1: kept-path line missing: $out" ;; esac
  fi
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

# 5b name and key shape: refused by the script before kubectl runs.
long254="$(printf 'a%.0s' $(seq 1 254))"
vf="$(newfile "$root" "$tok")"
lab64="$(printf 'a%.0s' $(seq 1 64))"
lab63="$(printf 'a%.0s' $(seq 1 63))"
lab61="$(printf 'a%.0s' $(seq 1 61))"
multi253="$lab63.$lab63.$lab63.$lab61"
multi254="$lab63.$lab63.$lab63.${lab61}a"
for bad in "a..b" ".a" "a." "-a" "a-.b" "$long254" "$lab64" "$multi254" "x.$lab64"; do
  : >"$root/log"
  set +e
  KUBECTL="$fake" FAKE_MODE=ok FAKE_LOG="$root/log" bash "$script" --context c --namespace n --name "$bad" --value-file "$vf" >/dev/null 2>&1
  r=$?
  set -e
  [ "$r" -ne 0 ] && [ ! -s "$root/log" ] || fail "bad name '${bad:0:12}' accepted"
done
for badkey in "." ".." ".x"; do
  : >"$root/log"
  set +e
  KUBECTL="$fake" FAKE_MODE=ok FAKE_LOG="$root/log" bash "$script" --context c --namespace n --name x --value-file "$vf" --key "$badkey" >/dev/null 2>&1
  r=$?
  set -e
  [ "$r" -ne 0 ] && [ ! -s "$root/log" ] || fail "bad key '$badkey' accepted"
done
: >"$root/log"
set +e
KUBECTL="$fake" FAKE_MODE=ok FAKE_LOG="$root/log" FAKE_SEEN="$root/seen" SESSION_ISSUE_WORKDIR_BASE="$root" bash "$script" --context c --namespace n --name "a.b-c.d" --value-file "$vf" >/dev/null 2>&1
r=$?
set -e
[ "$r" -eq 0 ] || fail "valid dotted name refused"
vf="$(newfile "$root" "$tok")"
: >"$root/log"
set +e
KUBECTL="$fake" FAKE_MODE=ok FAKE_LOG="$root/log" FAKE_SEEN="$root/seen" SESSION_ISSUE_WORKDIR_BASE="$root" bash "$script" --context c --namespace n --name "$multi253" --value-file "$vf" >/dev/null 2>&1
r=$?
set -e
[ "${#multi253}" -eq 253 ] && [ "$r" -eq 0 ] || fail "253-char multi-label name refused"
vf="$(newfile "$root" "$tok")"
: >"$root/log"
set +e
KUBECTL="$fake" FAKE_MODE=ok FAKE_LOG="$root/log" FAKE_SEEN="$root/seen" SESSION_ISSUE_WORKDIR_BASE="$root" bash "$script" --context c --namespace n --name "$lab63" --value-file "$vf" >/dev/null 2>&1
r=$?
set -e
[ "$r" -eq 0 ] || fail "63-char label refused"

# 5c bad name / key refusals also print the kept path.
vf="$(newfile "$root" "$tok")"
run ok "$vf" --key '.x'
[ "$rc" -eq 2 ] || fail "bad key rc=$rc"
case "$out" in *"value file kept for retry: $vf"*) ;; *) fail "bad key refusal: no kept path: $out" ;; esac
rm -f "$vf"

# 5d stdout closed (/dev/full): the create still succeeded, so the source is shredded and rc is 0.
vf="$(newfile "$root" "$tok")"
: >"$root/log"
set +e
FAKE_MODE=ok FAKE_LOG="$root/log" FAKE_SEEN="$root/seen" KUBECTL="$fake" SESSION_ISSUE_WORKDIR_BASE="$root" bash "$script" --context c --namespace n --name x --value-file "$vf" >/dev/full 2>/dev/null
r=$?
set -e
[ "$(wc -l <"$root/log")" -eq 1 ] || fail "devfull: kubectl did not run once"
[ ! -e "$vf" ] || fail "devfull: source left behind after a successful create"
[ "$r" -eq 0 ] || fail "devfull: rc=$r"
[ "$(leftovers)" -eq 0 ] || fail "devfull: workdir left behind"

# 5e shred fails after a successful create: never silent, non-zero, path printed, no value.
ro="$(mktemp -d "$shmbase/issue-secret-ro.XXXXXX")"
vf="$(newfile "$ro" "$tok")"
chmod 0500 "$ro"
if rm -f "$ro/probe" 2>/dev/null && [ ! -e "$ro/probe" ] && ! ( : >"$ro/probe" ) 2>/dev/null; then
  run ok "$vf"
  [ "$rc" -ne 0 ] || fail "shred failure: rc 0"
  case "$out" in *"secret created; value file NOT removed: $vf"*) ;; *) fail "shred failure output: $out" ;; esac
  case "$out" in *"$tok"*) fail "shred failure: value printed" ;; esac
  [ -f "$vf" ] || fail "shred failure: file gone?"
  [ "$(leftovers)" -eq 0 ] || fail "shred failure: workdir left behind"
else
  echo "SKIP shred-failure case: directory is writable despite mode 0500 (root?)" >&2
  fail "shred failure case cannot run"
fi
chmod 0700 "$ro"; rm -rf "$ro"

# 6 signal during kubectl: the child is stopped, the workdir is gone at once, the source is kept.
cat >"$root/slow" <<'SLOW'
#!/usr/bin/env bash
echo $$ >"$SLOW_PID"
sleep 30
SLOW
chmod 755 "$root/slow"
for sig in TERM INT; do
  vf="$(newfile "$root" "$tok")"
  rm -f "$root/slowpid"
  # a background job of a non-interactive shell starts with SIGINT ignored; a terminal run does not
  SLOW_PID="$root/slowpid" KUBECTL="$root/slow" SESSION_ISSUE_WORKDIR_BASE="$root" perl -e '$SIG{INT}="DEFAULT"; exec @ARGV' bash "$script" --context c --namespace n --name x --value-file "$vf" >"$root/sigout" 2>&1 &
  pid=$!
  for _ in $(seq 1 50); do [ -s "$root/slowpid" ] && [ "$(leftovers)" -gt 0 ] && break; sleep 0.1; done
  [ -s "$root/slowpid" ] || fail "$sig: slow kubectl never started"
  kpid="$(cat "$root/slowpid" 2>/dev/null || echo 0)"
  kill -"$sig" "$pid" 2>/dev/null || true
  sleep 0.5
  kill -0 "$pid" 2>/dev/null && fail "$sig: script still alive after 500 ms"
  [ "$(leftovers)" -eq 0 ] || fail "$sig: workdir left behind 500 ms after the signal"
  kill -0 "$kpid" 2>/dev/null && { fail "$sig: kubectl child still alive"; kill "$kpid" 2>/dev/null || true; }
  wait "$pid" 2>/dev/null || true
  [ -f "$vf" ] || fail "$sig: source removed after a signal"
  grep -q "value file kept for retry: $vf" "$root/sigout" || fail "$sig: kept-path line missing"
  case "$(cat "$root/sigout")" in *"$tok"*) fail "$sig: value printed" ;; esac
done

# 7 a mktemp failure leaves nothing; and the script makes exactly one mktemp call, so no later one can fail after a directory exists.
mkdir "$root/bin"
cat >"$root/bin/mktemp" <<'MK'
#!/usr/bin/env bash
n=$(cat "$MK_COUNT" 2>/dev/null || echo 0); n=$((n + 1)); echo "$n" >"$MK_COUNT"
[ "$n" -ge "${MK_FAIL_FROM:-1}" ] && { echo "injected mktemp failure" >&2; exit 1; }
exec /usr/bin/mktemp "$@"
MK
chmod 755 "$root/bin/mktemp"
vf="$(newfile "$root" "$tok")"
rm -f "$root/mkcount"
set +e
MK_COUNT="$root/mkcount" PATH="$root/bin:$PATH" KUBECTL="$fake" FAKE_MODE=ok FAKE_LOG="$root/log" SESSION_ISSUE_WORKDIR_BASE="$root" bash "$script" --context c --namespace n --name x --value-file "$vf" >/dev/null 2>&1
r=$?
set -e
[ "$r" -ne 0 ] || fail "mktemp failure accepted"
[ "$(leftovers)" -eq 0 ] || fail "workdir left behind after a mktemp failure"
[ -f "$vf" ] || fail "source removed after a mktemp failure"
rm -f "$root/mkcount"
set +e
MK_FAIL_FROM=2 MK_COUNT="$root/mkcount" PATH="$root/bin:$PATH" KUBECTL="$fake" FAKE_MODE=ok FAKE_LOG="$root/log" FAKE_SEEN="$root/seen" SESSION_ISSUE_WORKDIR_BASE="$root" bash "$script" --context c --namespace n --name x --value-file "$vf" >/dev/null 2>&1
r=$?
set -e
[ "$r" -eq 0 ] && [ "$(cat "$root/mkcount")" = "1" ] || fail "script makes more than one mktemp call (rc=$r)"

[ "$fails" -eq 0 ] || { echo "$fails check(s) failed" >&2; exit 1; }
echo "create-session-issue-secret_test: ok"
