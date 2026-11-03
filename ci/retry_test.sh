#!/usr/bin/env bash
# CHAOS-11649: red/green coverage for ci/lib/retry.sh (retry_named). Hermetic: a test-local `sleep` on PATH records the
# gaps, no real waiting. Every case runs under `env -i` and again under a hosted-like environment.
# Run: bash ci/retry_test.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LIB="${ROOT}/ci/lib/retry.sh"
T="$(mktemp -d)"
trap 'rm -rf "${T}"' EXIT
mkdir -p "${T}/bin"

# shellcheck disable=SC2016
printf '%s\n' '#!/usr/bin/env bash' 'echo "$1" >> "${FAKE_SLEEP_LOG}"' 'exit 0' > "${T}/bin/sleep"
# A command that fails FAKE_FAILS times (counting runs in FAKE_COUNT) with rc FAKE_RC, then succeeds.
# shellcheck disable=SC2016
printf '%s\n' '#!/usr/bin/env bash' \
  'n=0; [ -f "${FAKE_COUNT}" ] && n="$(cat "${FAKE_COUNT}")"; n=$((n + 1)); echo "${n}" > "${FAKE_COUNT}"' \
  'echo "cmd-out run=${n} args=$*"; echo "cmd-err run=${n}" >&2' \
  'if [ "${n}" -le "${FAKE_FAILS:-0}" ]; then exit "${FAKE_RC:-1}"; fi' \
  'exit 0' > "${T}/bin/flaky"
chmod +x "${T}/bin/sleep" "${T}/bin/flaky"

fails=0
ok() { printf 'ok   %s\n' "$1"; }
bad() { printf 'FAIL %s\n' "$1"; fails=$((fails + 1)); }
[ -f "${LIB}" ] || { printf 'FAIL ci/lib/retry.sh is missing\n'; exit 1; }

HOSTED=(GITHUB_RUN_ID=4242 GITHUB_ACTIONS=true CI=true RUNNER_OS=Linux)

# call <mode> <case> <env...> -- <retry_named args...>: sets RC, OUT (stdout), ERR (stderr), RUNS, SLEEPS
call() {
  local mode="$1" name="$2" extra=() 
  shift 2
  while [ "$#" -gt 0 ] && [ "$1" != "--" ]; do extra+=("$1"); shift; done
  shift
  local d="${T}/d.${mode}.${name}"
  rm -rf "${d}"; mkdir -p "${d}"
  : > "${d}/sleeps"
  local base=(PATH="${T}/bin:/usr/bin:/bin" HOME="${T}" TMPDIR="${T}" LC_ALL=C FAKE_COUNT="${d}/count" FAKE_SLEEP_LOG="${d}/sleeps")
  [ "${mode}" = hosted ] && base+=("${HOSTED[@]}")
  set +e
  # shellcheck disable=SC2016
  env -i "${base[@]}" "${extra[@]}" bash -c 'source "$1"; shift; never() { return 1; }; always() { return 0; }; rc7only() { [ "$1" = 7 ]; }; retry_named "$@"' _ "${LIB}" "$@" >"${d}/out" 2>"${d}/err"
  RC=$?
  set -e
  OUT="$(cat "${d}/out")"; ERR="$(cat "${d}/err")"
  RUNS="$(cat "${d}/count" 2>/dev/null || echo 0)"
  SLEEPS="$(tr '\n' ' ' < "${d}/sleeps" | sed 's/ $//')"
}

for mode in plain hosted; do
  call "${mode}" a1 FAKE_FAILS=0 -- job 3 "2" -- flaky one
  if [ "${RC}" = 0 ] && [ "${RUNS}" = 1 ] && [ -z "${SLEEPS}" ]; then ok "${mode} a1 first try passes: 1 run, no sleep"; else bad "${mode} a1 rc=${RC} runs=${RUNS} sleeps=${SLEEPS}"; fi
  if printf '%s\n' "${ERR}" | grep -Eq '^retry: name=job attempt=1/3 rc=0 elapsed=[0-9]+s$'; then ok "${mode} a1 log line form"; else bad "${mode} a1 log line: ${ERR}"; fi

  call "${mode}" a2 FAKE_FAILS=2 FAKE_RC=7 -- job 3 "2" -- flaky
  if [ "${RC}" = 0 ] && [ "${RUNS}" = 3 ] && [ "${SLEEPS}" = "2 2" ]; then ok "${mode} a2 two failures then pass: 3 runs, fixed gaps 2 2"; else bad "${mode} a2 rc=${RC} runs=${RUNS} sleeps=${SLEEPS}"; fi
  if [ "$(printf '%s\n' "${ERR}" | grep -c '^retry: name=job attempt=')" = 3 ] && printf '%s\n' "${ERR}" | grep -q '^retry: name=job attempt=1/3 rc=7 ' && printf '%s\n' "${ERR}" | grep -q '^retry: name=job attempt=3/3 rc=0 '; then ok "${mode} a2 one log line per attempt, rc shown"; else bad "${mode} a2 lines: ${ERR}"; fi

  call "${mode}" a3 FAKE_FAILS=99 FAKE_RC=7 -- job 3 "2" -- flaky
  if [ "${RC}" = 7 ] && [ "${RUNS}" = 3 ] && [ "${SLEEPS}" = "2 2" ]; then ok "${mode} a3 always failing: bounded at 3 runs, rc 7 passed through, no sleep after the last"; else bad "${mode} a3 rc=${RC} runs=${RUNS} sleeps=${SLEEPS}"; fi

  call "${mode}" a4 FAKE_FAILS=99 FAKE_RC=3 -- job 1 "5" -- flaky
  if [ "${RC}" = 3 ] && [ "${RUNS}" = 1 ] && [ -z "${SLEEPS}" ]; then ok "${mode} a4 one try: no sleep, rc 3"; else bad "${mode} a4 rc=${RC} runs=${RUNS} sleeps=${SLEEPS}"; fi

  call "${mode}" a5 FAKE_FAILS=99 -- job 4 "1 4" -- flaky
  if [ "${SLEEPS}" = "1 4 4" ]; then ok "${mode} a5 gap list: one per gap, the last repeats"; else bad "${mode} a5 sleeps=${SLEEPS}"; fi

  call "${mode}" a6 FAKE_FAILS=99 -- job 4 "double:1" -- flaky
  if [ "${SLEEPS}" = "1 2 4" ]; then ok "${mode} a6 doubling backoff 1 2 4"; else bad "${mode} a6 sleeps=${SLEEPS}"; fi

  call "${mode}" a7 FAKE_FAILS=1 -- job 2 "0" -- flaky "two words" x
  if printf '%s\n' "${OUT}" | grep -qx 'cmd-out run=2 args=two words x' && printf '%s\n' "${ERR}" | grep -qx 'cmd-err run=2'; then ok "${mode} a7 command stdout/stderr and arguments pass through"; else bad "${mode} a7 out=${OUT} err=${ERR}"; fi
  if printf '%s\n' "${OUT}" | grep -q '^retry:'; then bad "${mode} a7 the log line is on stdout"; else ok "${mode} a7 the log line is on stderr only"; fi

  call "${mode}" a8 FAKE_FAILS=1 RETRY_QUIET=1 -- job 2 "0" -- flaky
  if [ "${RC}" = 0 ] && ! printf '%s\n' "${ERR}" | grep -q '^retry:'; then ok "${mode} a8 RETRY_QUIET=1 prints no retry line"; else bad "${mode} a8 err=${ERR}"; fi

  call "${mode}" b1 -- "" 3 1 -- flaky
  if [ "${RC}" = 2 ] && [ "${RUNS}" = 0 ]; then ok "${mode} b1 empty name: rc 2, command not run"; else bad "${mode} b1 rc=${RC} runs=${RUNS}"; fi
  call "${mode}" b2 -- "bad name" 3 1 -- flaky
  if [ "${RC}" = 2 ] && [ "${RUNS}" = 0 ]; then ok "${mode} b2 name with a space: rc 2"; else bad "${mode} b2 rc=${RC} runs=${RUNS}"; fi
  call "${mode}" b3 -- job 0 1 -- flaky
  if [ "${RC}" = 2 ] && [ "${RUNS}" = 0 ]; then ok "${mode} b3 tries 0: rc 2"; else bad "${mode} b3 rc=${RC} runs=${RUNS}"; fi
  call "${mode}" b4 -- job 11 1 -- flaky
  if [ "${RC}" = 2 ] && [ "${RUNS}" = 0 ]; then ok "${mode} b4 tries above 10: rc 2 (bounded)"; else bad "${mode} b4 rc=${RC} runs=${RUNS}"; fi
  call "${mode}" b5 -- job abc 1 -- flaky
  if [ "${RC}" = 2 ] && [ "${RUNS}" = 0 ]; then ok "${mode} b5 non-numeric tries: rc 2"; else bad "${mode} b5 rc=${RC} runs=${RUNS}"; fi
  call "${mode}" b6 -- job 3 "-1" -- flaky
  if [ "${RC}" = 2 ] && [ "${RUNS}" = 0 ]; then ok "${mode} b6 negative gap: rc 2"; else bad "${mode} b6 rc=${RC} runs=${RUNS}"; fi
  call "${mode}" b7 -- job 3 "double:x" -- flaky
  if [ "${RC}" = 2 ] && [ "${RUNS}" = 0 ]; then ok "${mode} b7 bad doubling base: rc 2"; else bad "${mode} b7 rc=${RC} runs=${RUNS}"; fi
  call "${mode}" b8 -- job 3 1 flaky
  if [ "${RC}" = 2 ] && [ "${RUNS}" = 0 ]; then ok "${mode} b8 no -- separator: rc 2"; else bad "${mode} b8 rc=${RC} runs=${RUNS}"; fi
  call "${mode}" b9 -- job 3 1 --
  if [ "${RC}" = 2 ]; then ok "${mode} b9 no command: rc 2"; else bad "${mode} b9 rc=${RC}"; fi
  if printf '%s\n' "${ERR}" | grep -q '^retry: usage'; then ok "${mode} b9 usage line"; else bad "${mode} b9 usage: ${ERR}"; fi

  # CHAOS-12372: RETRY_WHEN=FUNCTION decides, after each failed try, whether another try follows.
  call "${mode}" d1 FAKE_FAILS=99 FAKE_RC=7 RETRY_WHEN=never -- job 3 "2" -- flaky
  if [ "${RC}" = 7 ] && [ "${RUNS}" = 1 ] && [ -z "${SLEEPS}" ]; then ok "${mode} d1 RETRY_WHEN says no: 1 run, rc 7, no sleep"; else bad "${mode} d1 rc=${RC} runs=${RUNS} sleeps=${SLEEPS}"; fi
  call "${mode}" d2 FAKE_FAILS=99 FAKE_RC=7 RETRY_WHEN=always -- job 3 "2" -- flaky
  if [ "${RC}" = 7 ] && [ "${RUNS}" = 3 ] && [ "${SLEEPS}" = "2 2" ]; then ok "${mode} d2 RETRY_WHEN says yes: bounded at 3 runs"; else bad "${mode} d2 rc=${RC} runs=${RUNS} sleeps=${SLEEPS}"; fi
  call "${mode}" d3 FAKE_FAILS=99 FAKE_RC=7 RETRY_WHEN=rc7only -- job 3 "2" -- flaky
  if [ "${RUNS}" = 3 ]; then ok "${mode} d3 RETRY_WHEN gets the try's rc (7: retried)"; else bad "${mode} d3 runs=${RUNS}"; fi
  call "${mode}" d4 FAKE_FAILS=99 FAKE_RC=8 RETRY_WHEN=rc7only -- job 3 "2" -- flaky
  if [ "${RC}" = 8 ] && [ "${RUNS}" = 1 ]; then ok "${mode} d4 RETRY_WHEN gets the try's rc (8: stop, rc kept)"; else bad "${mode} d4 rc=${RC} runs=${RUNS}"; fi
  call "${mode}" d6 FAKE_FAILS=0 RETRY_WHEN=never -- job 3 "2" -- flaky
  if [ "${RC}" = 0 ] && [ "${RUNS}" = 1 ]; then ok "${mode} d6 RETRY_WHEN is not asked after a success"; else bad "${mode} d6 rc=${RC} runs=${RUNS}"; fi
done

# A caller with `set -e` is not killed by a failing attempt, and sees the last rc.
# shellcheck disable=SC2016
OUT="$(env -i PATH="${T}/bin:/usr/bin:/bin" HOME="${T}" LC_ALL=C FAKE_COUNT="${T}/c.e" FAKE_SLEEP_LOG="${T}/s.e" FAKE_FAILS=99 FAKE_RC=9 bash -c 'set -euo pipefail; source "$1"; rc=0; retry_named job 2 0 -- flaky >/dev/null 2>&1 || rc=$?; echo "rc=${rc}"' _ "${LIB}")"
if [ "${OUT}" = "rc=9" ]; then ok "c1 a set -e caller survives the failing attempts and gets rc 9"; else bad "c1 ${OUT}"; fi

if grep -q 'IFS' "${LIB}"; then bad "helper sets or reads IFS (semgrep ifs-tampering)"; else ok "helper sets no IFS"; fi
if grep -Eq '^[[:space:]]*(set -[a-z]*e|set -o errexit|exit )' "${LIB}"; then bad "helper changes the caller's shell options or exits"; else ok "helper leaves the caller's shell options alone and never exits"; fi

[ "${fails}" = 0 ] || { printf '%s check(s) failed\n' "${fails}"; exit 1; }
printf 'all retry checks passed\n'
