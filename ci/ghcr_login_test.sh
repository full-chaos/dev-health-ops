#!/usr/bin/env bash
# Red/green coverage for ci/lib/ghcr_login.sh: the bounded-retry ghcr.io login that backs up docker/login-action in
# merge_group jobs. Hermetic: a test-local `docker` and `sleep` on PATH, no network, no real waiting. Every case runs
# under `env -i` and again under a hosted-like environment; RETRY_QUIET is tested unset and set.
# Run: bash ci/ghcr_login_test.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT="${ROOT}/ci/lib/ghcr_login.sh"
T="$(mktemp -d)"
trap 'rm -rf "${T}"' EXIT
mkdir -p "${T}/bin"
TOKEN='ghs_FAKETOKEN0123456789abcdef'

# shellcheck disable=SC2016
printf '%s\n' '#!/usr/bin/env bash' 'echo "$1" >> "${FAKE_SLEEP_LOG}"' 'exit 0' > "${T}/bin/sleep"
# A docker that records argv, stdin and environment per run, and fails `login` FAKE_FAILS times.
# shellcheck disable=SC2016
printf '%s\n' '#!/usr/bin/env bash' \
  'n=0; [ -f "${FAKE_COUNT}" ] && n="$(cat "${FAKE_COUNT}")"; n=$((n + 1)); echo "${n}" > "${FAKE_COUNT}"' \
  'printf "%s\n" "$*" >> "${FAKE_ARGV}"; env >> "${FAKE_ENV}"; cat >> "${FAKE_STDIN}"; echo >> "${FAKE_STDIN}"' \
  'if [ "${n}" -le "${FAKE_FAILS:-0}" ]; then echo "Error: context deadline exceeded" >&2; exit 1; fi' \
  'echo "Login Succeeded"; exit 0' > "${T}/bin/docker"
chmod +x "${T}/bin/sleep" "${T}/bin/docker"

fails=0
ok() { printf 'ok   %s\n' "$1"; }
bad() { printf 'FAIL %s\n' "$1"; fails=$((fails + 1)); }
[ -f "${SCRIPT}" ] || { printf 'FAIL ci/lib/ghcr_login.sh is missing\n'; exit 1; }

HOSTED=(GITHUB_RUN_ID=4242 GITHUB_ACTIONS=true CI=true RUNNER_OS=Linux)

# call <mode> <case> <env...> -- [bash flags]: sets RC, OUT (stdout+stderr), RUNS, SLEEPS, ARGV, STDIN, DENV
call() {
  local mode="$1" name="$2" extra=() flags=()
  shift 2
  while [ "$#" -gt 0 ] && [ "$1" != "--" ]; do extra+=("$1"); shift; done
  shift
  flags=("$@")
  local d="${T}/d.${mode}.${name}"
  rm -rf "${d}"; mkdir -p "${d}"
  : > "${d}/sleeps"; : > "${d}/argv"; : > "${d}/stdin"; : > "${d}/env"
  local base=(PATH="${T}/bin:/usr/bin:/bin" HOME="${T}" TMPDIR="${T}" LC_ALL=C GHCR_USER=octo GHCR_TOKEN="${TOKEN}"
    FAKE_COUNT="${d}/count" FAKE_SLEEP_LOG="${d}/sleeps" FAKE_ARGV="${d}/argv" FAKE_STDIN="${d}/stdin" FAKE_ENV="${d}/env")
  [ "${mode}" = hosted ] && base+=("${HOSTED[@]}")
  set +e
  env -i "${base[@]}" "${extra[@]}" timeout 60 bash "${flags[@]}" "${SCRIPT}" </dev/null >"${d}/out" 2>&1
  RC=$?
  set -e
  OUT="$(cat "${d}/out")"
  RUNS="$(cat "${d}/count" 2>/dev/null || echo 0)"
  SLEEPS="$(tr '\n' ' ' < "${d}/sleeps" | sed 's/ $//')"
  ARGV="$(cat "${d}/argv")"; STDIN="$(cat "${d}/stdin")"; DENV="$(cat "${d}/env")"
}

for mode in plain hosted; do
  call "${mode}" a1 FAKE_FAILS=0 --
  if [ "${RC}" = 0 ] && [ "${RUNS}" = 1 ] && [ -z "${SLEEPS}" ]; then ok "${mode} a1 first try passes: 1 run, no sleep"; else bad "${mode} a1 rc=${RC} runs=${RUNS} sleeps=${SLEEPS}"; fi

  call "${mode}" a2 FAKE_FAILS=2 --
  if [ "${RC}" = 0 ] && [ "${RUNS}" = 3 ] && [ "${SLEEPS}" = "10 30" ]; then ok "${mode} a2 two failures then pass: rc 0, 3 runs, gaps 10 30"; else bad "${mode} a2 rc=${RC} runs=${RUNS} sleeps=${SLEEPS}"; fi
  if [ "$(printf '%s\n' "${OUT}" | grep -c '^retry: name=ghcr-login attempt=[12]/3 rc=1 ')" = 2 ] && printf '%s\n' "${OUT}" | grep -q '^retry: name=ghcr-login attempt=3/3 rc=0 '; then ok "${mode} a2 two named failed attempts in the log"; else bad "${mode} a2 out: ${OUT}"; fi

  call "${mode}" a3 FAKE_FAILS=99 --
  if [ "${RC}" != 0 ] && [ "${RUNS}" = 3 ] && [ "${SLEEPS}" = "10 30" ]; then ok "${mode} a3 three failures: hard fail rc ${RC}, bounded at 3 runs, no sleep after the last"; else bad "${mode} a3 rc=${RC} runs=${RUNS} sleeps=${SLEEPS}"; fi
  if printf '%s\n' "${OUT}" | tail -n 1 | grep -Eq '^ghcr-login: ghcr.io login failed after 3 tries \(rc=[0-9]+\)$'; then ok "${mode} a3 clear final line"; else bad "${mode} a3 last line: $(printf '%s\n' "${OUT}" | tail -n 1)"; fi

  call "${mode}" a4 FAKE_FAILS=1 --
  if printf '%s\n' "${ARGV}" | grep -qF "${TOKEN}"; then bad "${mode} a4 the token is on docker argv"; else ok "${mode} a4 the token is not on docker argv"; fi
  if [ "$(printf '%s\n' "${ARGV}" | sort -u)" = "login ghcr.io -u octo --password-stdin" ]; then ok "${mode} a4 argv is exactly: login ghcr.io -u octo --password-stdin"; else bad "${mode} a4 argv: ${ARGV}"; fi
  if [ "$(printf '%s\n' "${STDIN}" | sort -u | grep -v '^$')" = "${TOKEN}" ]; then ok "${mode} a4 the token arrives on stdin"; else bad "${mode} a4 stdin: ${STDIN}"; fi
  if printf '%s\n' "${DENV}" | grep -q '^GHCR_TOKEN='; then bad "${mode} a4 the token is in docker's environment"; else ok "${mode} a4 the token is not in docker's environment"; fi
  if printf '%s\n' "${OUT}" | grep -qF "${TOKEN}"; then bad "${mode} a4 the token is in the log"; else ok "${mode} a4 the token is not in the log"; fi

  call "${mode}" a5 FAKE_FAILS=1 SHELLOPTS=xtrace --
  if printf '%s\n' "${OUT}" | grep -qF "${TOKEN}"; then bad "${mode} a5 SHELLOPTS=xtrace leaks the token"; else ok "${mode} a5 SHELLOPTS=xtrace does not leak the token"; fi
  call "${mode}" a6 FAKE_FAILS=1 -- -x
  if printf '%s\n' "${OUT}" | grep -qF "${TOKEN}"; then bad "${mode} a6 bash -x leaks the token"; else ok "${mode} a6 bash -x does not leak the token"; fi
  if [ "${RC}" = 0 ]; then ok "${mode} a6 still logs in under bash -x"; else bad "${mode} a6 rc=${RC}"; fi

  call "${mode}" a7 FAKE_FAILS=1 RETRY_QUIET=1 --
  if [ "$(printf '%s\n' "${OUT}" | grep -c '^retry: name=ghcr-login attempt=')" = 2 ]; then ok "${mode} a7 RETRY_QUIET=1 from the caller cannot hide the attempts"; else bad "${mode} a7 out: ${OUT}"; fi
  call "${mode}" a8 FAKE_FAILS=1 RETRY_QUIET=0 --
  if [ "$(printf '%s\n' "${OUT}" | grep -c '^retry: name=ghcr-login attempt=')" = 2 ]; then ok "${mode} a8 RETRY_QUIET=0: attempts named"; else bad "${mode} a8 out: ${OUT}"; fi

  call "${mode}" a9 GHCR_TOKEN= --
  if [ "${RC}" != 0 ] && [ "${RUNS}" = 0 ]; then ok "${mode} a9 empty token: hard fail, docker not run"; else bad "${mode} a9 rc=${RC} runs=${RUNS}"; fi
  call "${mode}" a10 GHCR_USER= --
  if [ "${RC}" != 0 ] && [ "${RUNS}" = 0 ]; then ok "${mode} a10 empty user: hard fail, docker not run"; else bad "${mode} a10 rc=${RC} runs=${RUNS}"; fi
done

if grep -Eq '^[[:space:]]*set[[:space:]]+-[a-zA-Z]*x|set -o xtrace' "${SCRIPT}"; then bad "b1 the script turns xtrace ON"; else ok "b1 the script never turns xtrace on"; fi
if grep -q 'set +x' "${SCRIPT}"; then ok "b2 the script turns xtrace off"; else bad "b2 the script has no xtrace guard"; fi
if grep -q 'retry_named' "${SCRIPT}" && grep -q 'retry.sh' "${SCRIPT}"; then ok "b3 the script uses the shared retry_named"; else bad "b3 the script does not use ci/lib/retry.sh"; fi

[ "${fails}" = 0 ] || { printf '%s check(s) failed\n' "${fails}"; exit 1; }
printf 'all ghcr login checks passed\n'
