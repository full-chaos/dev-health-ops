#!/usr/bin/env bash
# Red/green coverage for ci/lib/fetch_pinned_release.sh: the authenticated, retried, sha256-checked download of a pinned
# GitHub release asset. Hermetic: a test-local `curl` and `sleep` on PATH, no network, no real waiting. The fake curl
# refuses every anonymous github.com release download with a 504 and answers api.github.com only with the token. Every
# case runs under `env -i` and again under a hosted-like environment.
# Run: bash ci/fetch_pinned_release_test.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT="${ROOT}/ci/lib/fetch_pinned_release.sh"
T="$(mktemp -d)"
trap 'rm -rf "${T}"' EXIT
mkdir -p "${T}/bin"
TOKEN='ghs_FAKETOKEN0123456789abcdef'
TOKEN_SHA="$(printf '%s' "${TOKEN}" | sha256sum | cut -d' ' -f1)"
ASSET='tool_1.2.3_linux_x64.tar.gz'
printf 'pinned-asset-bytes\n' > "${T}/asset"
GOOD_SHA="$(sha256sum "${T}/asset" | cut -d' ' -f1)"
OTHER_SHA="$(printf 'other-bytes\n' | sha256sum | cut -d' ' -f1)"

# shellcheck disable=SC2016
printf '%s\n' '#!/usr/bin/env bash' 'echo "$1" >> "${FAKE_SLEEP_LOG}"' 'exit 0' > "${T}/bin/sleep"
# A curl that records argv and environment per call. It reads a --config from stdin (`--config -`), knows the token only
# by its sha256, refuses an anonymous github.com release download (504, exit 22), answers the API only when the config
# carries `Authorization: Bearer <token>`, and fails the asset call FAKE_ASSET_FAILS times.
# shellcheck disable=SC2016
printf '%s\n' \
  '#!/usr/bin/env bash' \
  'all="$*"' \
  'printf '\''%s\n'\'' "${all}" >> "${FAKE_ARGV}"' \
  'env >> "${FAKE_ENV}"' \
  'out="" url="" cfg="" accept="" argv_bearer="" lt=""' \
  'while [ "$#" -gt 0 ]; do' \
  '  case "$1" in' \
  '    --location-trusted) lt=1; shift ;;' \
  '    -o | --output) out="$2"; shift 2 ;;' \
  '    -K | --config) if [ "$2" = - ]; then cfg="$(cat)"; else cfg="$(cat "$2")"; fi; shift 2 ;;' \
  '    -H | --header)' \
  '      case "$2" in' \
  '        [Aa]ccept:*application/octet-stream*) accept=1 ;;' \
  '        [Aa]uthorization:\ [Bb]earer\ *) argv_bearer="${2#*earer }" ;;' \
  '      esac' \
  '      shift 2 ;;' \
  '    http*) url="$1"; shift ;;' \
  '    *) shift ;;' \
  '  esac' \
  'done' \
  'case "${cfg}" in *'\''ccept: application/octet-stream'\''*) accept=1 ;; esac' \
  '# t9b: a request that carries an Authorization header must not use --location-trusted' \
  '[ -z "${lt}" ] || case "${cfg}" in *[Aa]uthorization:*) printf '\''%s\n'\'' "${all}" >> "${FAKE_LT}" ;; *) [ -z "${argv_bearer}" ] || printf '\''%s\n'\'' "${all}" >> "${FAKE_LT}" ;; esac' \
  'authed=""' \
  'bearer="$(printf '\''%s\n'\'' "${cfg}" | sed -n '\''s/^header = "Authorization: Bearer \(.*\)"$/\1/p'\'' | head -1)"' \
  '# A token on argv authenticates too, so a helper that leaks it there still downloads and is caught by t2, not by t1.' \
  '[ -n "${bearer}" ] || bearer="${argv_bearer}"' \
  'if [ -n "${bearer}" ] && [ "$(printf '\''%s'\'' "${bearer}" | sha256sum | cut -d'\'' '\'' -f1)" = "${FAKE_TOKEN_SHA}" ]; then authed=1; fi' \
  'emit() { if [ -n "${out}" ]; then cat > "${out}"; else cat; fi; }' \
  'case "${url}" in' \
  '  https://github.com/*/releases/download/*)' \
  '    if [ "${FAKE_ANON:-refuse}" = ok ]; then emit < "${FAKE_BYTES}"; exit 0; fi' \
  '    echo "curl: (22) The requested URL returned error: 504" >&2; exit 22 ;;' \
  '  https://api.github.com/repos/*/releases/tags/*)' \
  '    [ -n "${authed}" ] || { echo "curl: (22) The requested URL returned error: 401" >&2; exit 22; }' \
  '    printf '\''{"assets":[{"id":111,"name":"other.tar.gz"},{"id":4242,"name":"%s"}]}\n'\'' "${FAKE_ASSET_NAME}" | emit; exit 0 ;;' \
  '  https://api.github.com/repos/*/releases/assets/4242)' \
  '    [ -n "${authed}" ] || { echo "curl: (22) The requested URL returned error: 401" >&2; exit 22; }' \
  '    [ -n "${accept}" ] || { echo "curl: (22) the asset call has no octet-stream Accept header" >&2; exit 22; }' \
  '    n=0; [ -f "${FAKE_COUNT}" ] && n="$(cat "${FAKE_COUNT}")"; n=$((n + 1)); echo "${n}" > "${FAKE_COUNT}"' \
  '    if [ "${n}" -le "${FAKE_ASSET_FAILS:-0}" ]; then echo "curl: (22) The requested URL returned error: 504" >&2; exit 22; fi' \
  '    emit < "${FAKE_BYTES}"; exit 0 ;;' \
  'esac' \
  'echo "curl: (6) fake curl: unknown url ${url}" >&2' \
  'exit 6' > "${T}/bin/curl"
chmod +x "${T}/bin/sleep" "${T}/bin/curl"

fails=0
ok() { printf 'ok   %s\n' "$1"; }
bad() { printf 'FAIL %s\n' "$1"; fails=$((fails + 1)); }
[ -f "${SCRIPT}" ] || { printf 'FAIL ci/lib/fetch_pinned_release.sh is missing\n'; exit 1; }

HOSTED=(GITHUB_RUN_ID=4242 GITHUB_ACTIONS=true CI=true RUNNER_OS=Linux)

# call <mode> <case> <sha> <env...> -- [bash flags]: sets RC, OUT (stdout+stderr), CALLS, SLEEPS, ARGV, DENV, GOT (1 = the
# output file exists), GOTSHA
call() {
  local mode="$1" name="$2" sha="$3" extra=() flags=()
  shift 3
  while [ "$#" -gt 0 ] && [ "$1" != "--" ]; do extra+=("$1"); shift; done
  shift
  flags=("$@")
  local d="${T}/d.${mode}.${name}"
  rm -rf "${d}"; mkdir -p "${d}"
  : > "${d}/sleeps"; : > "${d}/argv"; : > "${d}/env"; : > "${d}/lt"
  local base=(PATH="${T}/bin:/usr/bin:/bin" HOME="${T}" TMPDIR="${T}" LC_ALL=C GH_TOKEN="${TOKEN}"
    FAKE_TOKEN_SHA="${TOKEN_SHA}" FAKE_ASSET_NAME="${ASSET}" FAKE_BYTES="${T}/asset" FAKE_COUNT="${d}/count"
    FAKE_SLEEP_LOG="${d}/sleeps" FAKE_ARGV="${d}/argv" FAKE_ENV="${d}/env" FAKE_LT="${d}/lt")
  [ "${mode}" = hosted ] && base+=("${HOSTED[@]}")
  set +e
  env -i "${base[@]}" "${extra[@]}" timeout 60 bash "${flags[@]}" "${SCRIPT}" \
    acme/tool v1.2.3 "${ASSET}" "${sha}" "${d}/tool.tgz" </dev/null >"${d}/out" 2>&1
  RC=$?
  set -e
  OUT="$(cat "${d}/out")"
  CALLS="$(wc -l < "${d}/argv" | tr -d ' ')"
  SLEEPS="$(tr '\n' ' ' < "${d}/sleeps" | sed 's/ $//')"
  ARGV="$(cat "${d}/argv")"; DENV="$(cat "${d}/env")"; LT="$(cat "${d}/lt")"
  GOT=0; GOTSHA=""
  if [ -f "${d}/tool.tgz" ]; then GOT=1; GOTSHA="$(sha256sum "${d}/tool.tgz" | cut -d' ' -f1)"; fi
}

for mode in plain hosted; do
  call "${mode}" t1 "${GOOD_SHA}" --
  if [ "${RC}" = 0 ] && [ "${GOT}" = 1 ] && [ "${GOTSHA}" = "${GOOD_SHA}" ]; then ok "${mode} t1 token set, anonymous fetch refused: the asset installs, sha ok"; else bad "${mode} t1 rc=${RC} got=${GOT} out: ${OUT}"; fi
  if ! grep -q 'https://github.com/.*/releases/download/' <<<"${ARGV}"; then ok "${mode} t1b with a token the anonymous URL is never called"; else bad "${mode} t1b anonymous URL called: ${ARGV}"; fi

  call "${mode}" t2 "${GOOD_SHA}" -- -x
  if [ "${RC}" = 0 ] && ! grep -qF "${TOKEN}" <<<"${ARGV}"$'\n'"${OUT}"$'\n'"${DENV}"; then ok "${mode} t2 under bash -x the token is in no curl argv, no curl environment, no output"; else bad "${mode} t2 rc=${RC}: the token leaked or the fetch failed"; fi
  call "${mode}" t2b "${GOOD_SHA}" GH_TOKEN= GITHUB_TOKEN="${TOKEN}" --
  if [ "${RC}" = 0 ] && [ "${GOT}" = 1 ] && ! grep -qF "${TOKEN}" <<<"${ARGV}"$'\n'"${OUT}"$'\n'"${DENV}"; then ok "${mode} t2b GITHUB_TOKEN alone is used, and not leaked"; else bad "${mode} t2b rc=${RC} got=${GOT} out: ${OUT}"; fi

  call "${mode}" t3 "${OTHER_SHA}" --
  if [ "${RC}" != 0 ] && [ "${GOT}" = 0 ]; then ok "${mode} t3 wrong sha256: fails closed, no file left"; else bad "${mode} t3 rc=${RC} got=${GOT}"; fi

  for s in '' abc "$(printf 'g%.0s' {1..64})" "${GOOD_SHA}0"; do
    call "${mode}" t4 "${s}" --
    if [ "${RC}" != 0 ] && [ "${CALLS}" = 0 ] && [ "${GOT}" = 0 ]; then ok "${mode} t4 bad pin '${s:0:8}...': refused, curl not run"; else bad "${mode} t4 pin '${s}' rc=${RC} calls=${CALLS} got=${GOT}"; fi
  done

  call "${mode}" t5 "${GOOD_SHA}" FAKE_ASSET_FAILS=2 --
  if [ "${RC}" = 0 ] && [ "${GOT}" = 1 ] && [ "${SLEEPS}" = "5 10" ]; then ok "${mode} t5 504 on tries 1-2: ok on try 3, waits 5 10"; else bad "${mode} t5 rc=${RC} got=${GOT} sleeps='${SLEEPS}' out: ${OUT}"; fi

  call "${mode}" t6 "${GOOD_SHA}" FAKE_ASSET_FAILS=99 --
  if [ "${RC}" != 0 ] && [ "${GOT}" = 0 ] && [ "${SLEEPS}" = "5 10 20 40" ]; then ok "${mode} t6 504 on all 5 tries: fails, waits 5 10 20 40"; else bad "${mode} t6 rc=${RC} got=${GOT} sleeps='${SLEEPS}'"; fi

  call "${mode}" t7 "${GOOD_SHA}" GH_TOKEN= FAKE_ANON=ok --
  if [ "${RC}" = 0 ] && [ "${GOT}" = 1 ] && grep -q 'https://github.com/acme/tool/releases/download/v1.2.3/' <<<"${ARGV}"; then ok "${mode} t7 no token: the anonymous URL is used"; else bad "${mode} t7 rc=${RC} got=${GOT} out: ${OUT}"; fi
  call "${mode}" t7b "${OTHER_SHA}" GH_TOKEN= FAKE_ANON=ok --
  if [ "${RC}" != 0 ] && [ "${GOT}" = 0 ]; then ok "${mode} t7b no token, wrong sha256: fails closed"; else bad "${mode} t7b rc=${RC} got=${GOT}"; fi

  call "${mode}" t9b "${GOOD_SHA}" --
  if [ "${RC}" = 0 ] && [ -z "${LT}" ]; then ok "${mode} t9b no curl call with an Authorization header uses --location-trusted"; else bad "${mode} t9b rc=${RC} --location-trusted with an Authorization header: ${LT}"; fi
  call "${mode}" t9 "${GOOD_SHA}" FAKE_ASSET_NAME=renamed.tar.gz --
  if [ "${RC}" != 0 ] && [ "${GOT}" = 0 ]; then ok "${mode} t9 the release has no asset of that name: fails"; else bad "${mode} t9 rc=${RC} got=${GOT}"; fi
done

if grep -Eq '^[[:space:]]*set[[:space:]]+-[a-zA-Z]*x|set -o xtrace' "${SCRIPT}"; then bad "b1 the script turns xtrace ON"; else ok "b1 the script never turns xtrace on"; fi
if grep -q 'retry_named' "${SCRIPT}" && grep -q 'retry.sh' "${SCRIPT}"; then ok "b2 the script uses the shared retry_named"; else bad "b2 the script does not use ci/lib/retry.sh"; fi

# t8 census: every pinned GitHub release download goes through the helper.
sites=(.github/workflows/go-quality.yml .github/workflows/security-scan.yml ci/check_auth_k8s.sh ci/check_go_api_fence_k8s.sh)
for s in "${sites[@]}"; do
  if grep -q 'fetch_pinned_release.sh' "${ROOT}/${s}"; then ok "t8 ${s} calls the helper"; else bad "t8 ${s} does not call ci/lib/fetch_pinned_release.sh"; fi
done
left="$(cd "${ROOT}" && grep -rlE 'github\.com/[^ ]+/releases/download/' .github/workflows ci \
  | grep -vxE 'ci/lib/fetch_pinned_release\.sh|ci/fetch_pinned_release_test\.sh' || true)"
if [ -z "${left}" ]; then ok "t8 no other release download URL in .github/workflows or ci"; else bad "t8 anonymous release downloads left in: $(printf '%s' "${left}" | tr '\n' ' ')"; fi

[ "${fails}" = 0 ] || { printf '%s check(s) failed\n' "${fails}"; exit 1; }
printf 'all fetch pinned release checks passed\n'
