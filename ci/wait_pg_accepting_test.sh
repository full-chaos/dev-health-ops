#!/usr/bin/env bash
# Red/green coverage for ci/lib/wait_pg_accepting.sh.
# Hermetic: a test-local `kubectl` on PATH; any call shape it does not know exits 99. Run: bash ci/wait_pg_accepting_test.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LIB="${ROOT}/ci/lib/wait_pg_accepting.sh"
T="$(mktemp -d)"
trap 'rm -rf "${T}"' EXIT
mkdir -p "${T}/bin"
fails=0
ok() { printf 'ok   %s\n' "$1"; }
bad() { printf 'FAIL %s\n' "$1"; fails=$((fails + 1)); }

# shellcheck disable=SC2016
printf '%s\n' \
  '#!/usr/bin/env bash' \
  '# Stub kubectl: only `exec [-i] POD -- pg_isready ...` and `exec [-i] POD -- psql ... -c "SELECT 1"` are known.' \
  'args=("$@"); i=0' \
  'while [ "${i}" -lt "${#args[@]}" ] && [ "${args[$i]}" != exec ]; do i=$((i + 1)); done' \
  '[ "${args[$i]:-}" = exec ] || { echo "stub: not an exec call: $*" >&2; exit 99; }' \
  'while [ "${i}" -lt "${#args[@]}" ] && [ "${args[$i]}" != -- ]; do i=$((i + 1)); done' \
  '[ "${args[$i]:-}" = -- ] || { echo "stub: no -- in: $*" >&2; exit 99; }' \
  'cmd="${args[$((i + 1))]:-}"; rest=("${args[@]:$((i + 2))}")' \
  'case "${cmd}" in' \
  '  pg_isready) ;;' \
  '  psql) case " ${rest[*]} " in *" -c SELECT 1 "*) ;; *) echo "stub: psql without -c SELECT 1: $*" >&2; exit 99 ;; esac ;;' \
  '  *) echo "stub: unknown command ${cmd}" >&2; exit 99 ;;' \
  'esac' \
  'echo "${cmd}" >> "${FAKE_LOG}"' \
  'n=0; [ -f "${FAKE_DIR}/n" ] && n="$(cat "${FAKE_DIR}/n")"; n=$((n + 1)); echo "${n}" > "${FAKE_DIR}/n"' \
  'if [ "${n}" -le "${FAKE_FAILS}" ]; then echo "psql: FATAL:  the database system is shutting down" >&2; exit 2; fi' \
  'exit 0' > "${T}/bin/kubectl"
chmod +x "${T}/bin/kubectl"

cell() { # name fails attempts -> sets RC OUT CALLS
  local d="${T}/d.$1"; mkdir -p "${d}"; : > "${d}/log"
  set +e
  # shellcheck disable=SC2016
  OUT="$(env -i PATH="${T}/bin:/usr/bin:/bin" HOME="${T}" TMPDIR="${T}" LC_ALL=C FAKE_DIR="${d}" FAKE_LOG="${d}/log" FAKE_FAILS="$2" \
    PG_WAIT_ATTEMPTS="$3" PG_WAIT_SLEEP=0 bash -c '
      die() { printf "ERROR: %s\n" "$1" >&2; exit 1; }
      K() { kubectl --context kind-x -n rehearsal "$@"; }
      PSU() { K exec -i pod-0 -- psql -U devhealth -d devhealth -At -v ON_ERROR_STOP=1 "$@"; }
      source "$1"; wait_pg_accepting pod-0 && echo accepted' _ "${LIB}" 2>&1)"
  RC=$?
  set -e
  CALLS="$(wc -l < "${d}/log" | tr -d ' ')"
}

cell a 0 5
if [ "${RC}" = 0 ] && [ "${CALLS}" = 2 ]; then ok "a accepting at once: rc 0, one pg_isready plus one SELECT 1"; else bad "a rc=${RC} calls=${CALLS}: ${OUT}"; fi

cell b 3 10
if [ "${RC}" = 0 ] && grep -q '^accepted$' <<<"${OUT}"; then ok "b shutting down for 3 calls, then accepts: rc 0"; else bad "b rc=${RC}: ${OUT}"; fi

cell c 1000 4
if [ "${RC}" != 0 ] && grep -qF 'postgres accepted no connection after 4 attempts' <<<"${OUT}" \
  && grep -qF 'the database system is shutting down' <<<"${OUT}"; then ok "c never accepts: non-zero, bound message names the last error"; else bad "c rc=${RC}: ${OUT}"; fi
if [ "${CALLS}" = 4 ]; then ok "c bound is attempts: 4 calls"; else bad "c calls=${CALLS}"; fi

[ "${fails}" = 0 ] || { printf '%d failed\n' "${fails}"; exit 1; }
printf 'all passed\n'
