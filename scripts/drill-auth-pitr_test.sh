#!/usr/bin/env bash
# Test of the phase helper of scripts/drill-auth-pitr.sh. No docker, no database: the driver binary is a fake. The
# canary below is a test value, not a credential. A phase called with NO extra arguments used to run a bare
# `export`, which prints every exported variable of the invoking shell (API keys included) into the drill log.
set -euo pipefail
cd "$(dirname "$0")"
script="$PWD/drill-auth-pitr.sh"

root="$(mktemp -d)"
trap 'rm -rf "$root"' EXIT
chmod 700 "$root"

fn="$(awk '/^drill\(\) \{/{p=1} p{print} p&&/^\}/{exit}' "$script")"
[[ -n "$fn" ]] || { echo "drill-auth-pitr_test: the drill() helper was not found" >&2; exit 1; }

cat >"$root/capped.sh" <<'FAKE'
#!/usr/bin/env bash
# args: -t N -- <driver> ...; records what the driver would see
printf 'label=%s phase=%s\n' "${DRILL_LABEL:-}" "${DRILL_PHASE:-}" >>"$FAKE_SEEN"
FAKE
chmod 755 "$root/capped.sh"

run_phase() { # args to drill() after the port and the phase
  (
    set +e
    eval "$fn"
    # shellcheck disable=SC2034 # read by the evaluated drill() helper
    capped="$root/capped.sh"
    # shellcheck disable=SC2034 # read by the evaluated drill() helper
    DRILL_DIR="$root"
    # shellcheck disable=SC2034
    evidence="$root"
    # shellcheck disable=SC2034
    POSTGRES_PASSWORD=not-a-real-password
    export FAKE_SEEN="$root/seen" SECRET_CANARY="canary-value-do-not-print-0123456789"
    drill 55432 "$@" 2>&1
  )
}

fails=0
check() { # name, condition result
  if [[ "$2" == ok ]]; then echo "ok   $1"; else echo "FAIL $1"; fails=$((fails + 1)); fi
}

: >"$root/seen"
out="$(run_phase migrate)"
[[ "$out" != *canary-value-do-not-print* && "$out" != *"declare -x"* && "$out" != *"export "* ]] && r=ok || r=bad
check "a phase with no extra arguments prints no environment" "$r"
grep -q 'phase=migrate' "$root/seen" && r=ok || r=bad
check "the driver still ran with its phase" "$r"

: >"$root/seen"
out="$(run_phase snapshot DRILL_LABEL=A)"
[[ "$out" != *canary-value-do-not-print* && "$out" != *"declare -x"* ]] && r=ok || r=bad
check "a phase with arguments prints no environment" "$r"
grep -q 'label=A phase=snapshot' "$root/seen" && r=ok || r=bad
check "the extra argument reaches the driver" "$r"

# the evidence summary never holds a drill secret (CHAOS-12281): every *secrets*.json stays out of the summary input, and a
# guard refuses a summary that holds a line of one (file names only, never the value)
sum_fn="$(awk '/^summary_files\(\) \{/{p=1} p{print} p&&/^\}/{exit}' "$script")"
write_fn="$(awk '/^write_summary\(\) \{/{p=1} p{print} p&&/^\}/{exit}' "$script")"
guard_fn="$(awk '/^guard_summary_no_secrets\(\) \{/{p=1} p{print} p&&/^\}/{exit}' "$script")"
[[ -n "$sum_fn" && -n "$write_fn" && -n "$guard_fn" ]] || { echo "drill-auth-pitr_test: the summary helpers were not found" >&2; exit 1; }
canary="canary-secret-value-0123456789abcdef"
ev="$root/ev"
mkdir -p "$ev"
printf '{"expect":"T","pass":true}\n' >"$ev/verify-T-exact.json"
printf '{"expect":"T","verdict":"PASS"}\n' >"$ev/post-T.json"
printf '{"expect":"T","revoked_access_accepted":0}\n' >"$ev/attempt-T-pitr.json"
printf '[{"name":"wl-revoked","value":"%s"}]\n' "$canary" >"$ev/attempt-secrets.json"
printf '{\n  "auth": {\n    "old": "%s-pretty"\n  }\n}\n' "$canary" >"$ev/drill-password-secrets.json"
run_summary() { # runs the evaluated helpers on the fixture; prints what the helper prints
  (
    set +e
    eval "$sum_fn"
    eval "$write_fn"
    eval "$guard_fn"
    # shellcheck disable=SC2034 # read by the evaluated helpers
    evidence="$ev"
    "$@" 2>&1
  )
}
out="$(run_summary write_summary)"
if [[ "$out" != *"$canary"* ]] && ! grep -qF "$canary" "$ev/summary.txt" && grep -q 'revoked_access_accepted' "$ev/summary.txt"; then r=ok; else r=bad; fi
check "the summary and its stdout hold no secret and still hold the attempt results" "$r"
out="$(run_summary guard_summary_no_secrets)" && rc=0 || rc=$?
[[ "$rc" -eq 0 && "$out" != *"$canary"* ]] && r=ok || r=bad
check "the guard passes a clean summary" "$r"
cp "$ev/summary.txt" "$root/summary.clean"
jq -c . "$ev/attempt-secrets.json" >>"$ev/summary.txt"
out="$(run_summary guard_summary_no_secrets)" && rc=0 || rc=$?
[[ "$rc" -ne 0 && "$out" == *attempt-secrets.json* && "$out" != *"$canary"* ]] && r=ok || r=bad
check "the guard refuses a planted compact copy of a secrets file, naming the file and no value" "$r"
cp "$root/summary.clean" "$ev/summary.txt"
printf '    "old": "%s-pretty"\n' "$canary" >>"$ev/summary.txt"
out="$(run_summary guard_summary_no_secrets)" && rc=0 || rc=$?
[[ "$rc" -ne 0 && "$out" == *drill-password-secrets.json* && "$out" != *"$canary"* ]] && r=ok || r=bad
check "the guard refuses a planted line of a pretty-printed secrets file, naming the file and no value" "$r"

# the stderr of a refusal, read alone, holds no secret value in either case
cp "$root/summary.clean" "$ev/summary.txt"
jq -c . "$ev/attempt-secrets.json" >>"$ev/summary.txt"
printf '    "old": "%s-pretty"\n' "$canary" >>"$ev/summary.txt"
(
  set +e
  eval "$guard_fn"
  # shellcheck disable=SC2034 # read by the evaluated helpers
  evidence="$ev"
  guard_summary_no_secrets 2>"$root/guard.err" >"$root/guard.out"
  echo "$?" >"$root/guard.rc"
)
if [[ "$(cat "$root/guard.rc")" -ne 0 ]] && grep -q 'secrets.json' "$root/guard.err" && ! grep -qF "$canary" "$root/guard.err" && ! grep -qF "$canary" "$root/guard.out"; then r=ok; else r=bad; fi
check "the guard stderr in a refusal holds no secret value" "$r"

# the filter reads the file name only: a parent directory named like a secrets file keeps the result files
evs="$root/auth-secrets/evidence"
mkdir -p "$evs"
cp "$ev/verify-T-exact.json" "$ev/post-T.json" "$ev/attempt-T-pitr.json" "$ev/attempt-secrets.json" "$evs/"
picked="$(
  set +e
  eval "$sum_fn"
  # shellcheck disable=SC2034 # read by the evaluated helpers
  evidence="$evs"
  summary_files | sed 's|.*/||' | sort | tr '\n' ' '
)"
if [[ "$picked" == "attempt-T-pitr.json post-T.json verify-T-exact.json " ]]; then r=ok; else r=bad; fi
check "a secrets-named parent directory keeps the 3 result files and drops the secrets file" "$r"

# no unguarded bare `export "$@"` anywhere in the script
if grep -nE '^[[:space:]]*export "\$@"' "$script" | grep -v 'if ((\$#))' >/dev/null; then
  grep -nE 'export "\$@"' "$script" | grep -vE 'if \(\(\$#\)\)' >/dev/null && r=bad || r=ok
else
  r=ok
fi
check "no export of \"\$@\" outside a non-empty guard" "$r"

[[ "$fails" -eq 0 ]] || { echo "drill-auth-pitr_test: $fails failure(s)" >&2; exit 1; }
echo "drill-auth-pitr_test: ok"
