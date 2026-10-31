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
    # shellcheck disable=SC2030,SC2031 # each subshell sets its own copy on purpose
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

# the password driver helper: no environment printed, its phase and extra argument reach the driver
pw_fn="$(awk '/^drill_password\(\) \{/{p=1} p{print} p&&/^\}/{exit}' "$script")"
[[ -n "$pw_fn" ]] || { echo "drill-auth-pitr_test: the drill_password() helper was not found" >&2; exit 1; }
cat >"$root/capped.sh" <<'FAKE'
#!/usr/bin/env bash
printf 'phase=%s expect=%s\n' "${DRILL_PASSWORD_PHASE:-}" "${DRILL_EXPECT:-}" >>"$FAKE_SEEN"
FAKE
chmod 755 "$root/capped.sh"
: >"$root/seen"
out="$(
  set +e
  eval "$pw_fn"
  # shellcheck disable=SC2034 # read by the evaluated drill_password() helper
  capped="$root/capped.sh"
  # shellcheck disable=SC2034
  DRILL_DIR="$root"
  # shellcheck disable=SC2034
  evidence="$root"
  # shellcheck disable=SC2034
  POSTGRES_PASSWORD=not-a-real-password
  # shellcheck disable=SC2030,SC2031 # each subshell sets its own copy on purpose
  export FAKE_SEEN="$root/seen" SECRET_CANARY="canary-value-do-not-print-0123456789"
  drill_password 55432 attempt DRILL_EXPECT=T 2>&1
)"
[[ "$out" != *canary-value-do-not-print* && "$out" != *"declare -x"* ]] && r=ok || r=bad
check "the password phase prints no environment" "$r"
grep -q 'phase=attempt expect=T' "$root/seen" && r=ok || r=bad
check "the password phase and its argument reach the driver" "$r"

# no unguarded bare `export "$@"` anywhere in the script
if grep -nE '^[[:space:]]*export "\$@"' "$script" | grep -v 'if ((\$#))' >/dev/null; then
  grep -nE 'export "\$@"' "$script" | grep -vE 'if \(\(\$#\)\)' >/dev/null && r=bad || r=ok
else
  r=ok
fi
check "no export of \"\$@\" outside a non-empty guard" "$r"

[[ "$fails" -eq 0 ]] || { echo "drill-auth-pitr_test: $fails failure(s)" >&2; exit 1; }
echo "drill-auth-pitr_test: ok"
