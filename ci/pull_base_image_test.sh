#!/usr/bin/env bash
# CHAOS-11096: red/green coverage for ci/lib/pull_base_image.sh. Hermetic: a test-local
# `docker` stub on PATH, no network, no registry. Run: bash ci/pull_base_image_test.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LIB="${ROOT}/ci/lib/pull_base_image.sh"
T="$(mktemp -d)"
trap 'rm -rf "${T}"' EXIT
mkdir -p "${T}/bin"

DIGEST="sha256:$(printf 'a%.0s' {1..64})"
OTHER="sha256:$(printf 'b%.0s' {1..64})"
cat > "${T}/Dockerfile" <<DF
ARG GO_BUILD_IMAGE="mirror.gcr.io/library/golang:1.27.3-alpine@${DIGEST}"
ARG GO_RUNTIME_IMAGE="gcr.io/distroless/static-debian12:nonroot@${OTHER}"
FROM \${GO_BUILD_IMAGE} AS build
DF

cat > "${T}/bin/docker" <<'STUB'
#!/usr/bin/env bash
# Stub: any shape it does not know exits 99 loudly. `docker login` is never expected.
echo "docker $*" >> "${FAKE_LOG}"
bump() { local f="${FAKE_DIR}/$1.n"; local n=0; [ -f "$f" ] && n="$(cat "$f")"; n=$((n + 1)); echo "$n" > "$f"; echo "$n"; }
case "$1" in
  pull)
    ref="$2"
    case "${ref}" in
      mirror.gcr.io/*) key=mirror; fails="${FAKE_FAIL_MIRROR:-0}" ;;
      docker.io/*) key=dockerio; fails="${FAKE_FAIL_DOCKERIO:-0}" ;;
      *) echo "stub: unknown source ${ref}" >&2; exit 99 ;;
    esac
    n="$(bump "${key}")"
    if [ "${key}" = dockerio ] && [ "${FAKE_RATELIMIT_DOCKERIO:-0}" = 1 ]; then
      echo "toomanyrequests: You have reached your pull rate limit" >&2; exit 1
    fi
    if [ "${n}" -le "${fails}" ]; then echo "error: not found" >&2; exit 1; fi
    echo "pulled ${ref}"; exit 0 ;;
  image)
    case "$2" in
      inspect) [ "${FAKE_INSPECT_RC:-0}" = 0 ] || { echo "inspect failed" >&2; exit "${FAKE_INSPECT_RC}"; }; for d in ${FAKE_SEEN_DIGEST//,/ }; do echo "x@${d}"; done; exit 0 ;;
      rm) exit 0 ;;
    esac ;;
  tag) exit "${FAKE_TAG_RC:-0}" ;;
esac
echo "stub: unexpected invocation: $*" >&2
exit 99
STUB
chmod +x "${T}/bin/docker"

fails=0
ok() { printf 'ok   %s\n' "$1"; }
bad() { printf 'FAIL %s\n' "$1"; fails=$((fails + 1)); }

[ -f "${LIB}" ] || { printf 'FAIL ci/lib/pull_base_image.sh is missing\n'; exit 1; }

# run <name> <env assignments...> -> sets RC, OUT, and per-source counts. The helper runs under env -i with a
# fixed PATH/HOME/TMPDIR/LC_ALL, so no hosted-runner variable (GITHUB_*, RUNNER_*, CI, PULL_*, DOCKER_*) can change a case.
run() {
  FAKE_DIR="${T}/d.$1"; rm -rf "${FAKE_DIR}"; mkdir -p "${FAKE_DIR}"
  FAKE_LOG="${FAKE_DIR}/log"; : > "${FAKE_LOG}"
  shift
  set +e
  # shellcheck disable=SC2016
  OUT="$(env -i PATH="${T}/bin:${PATH}" HOME="${T}" TMPDIR="${T}" LC_ALL=C "$@" FAKE_DIR="${FAKE_DIR}" FAKE_LOG="${FAKE_LOG}" \
    PULL_BACKOFF_SECS="0 0" bash -c 'source "$1"; pull_pinned_base_image "$2"; r=$?; echo "TAG=${PINNED_BASE_TAG:-}"; exit "$r"' _ "${LIB}" "${DF:-${T}/Dockerfile}" 2>&1)"
  RC=$?
  set -e
  PULLS_MIRROR="$(grep -c '^docker pull mirror.gcr.io/' "${FAKE_LOG}" || true)"
  PULLS_DOCKERIO="$(grep -c '^docker pull docker.io/' "${FAKE_LOG}" || true)"
}
SEEN="FAKE_SEEN_DIGEST=${DIGEST}"

run t1 FAKE_FAIL_MIRROR=2 "${SEEN}"
if { [ "${RC}" = 0 ] && [ "${PULLS_MIRROR}" = 3 ] && [ "${PULLS_DOCKERIO}" = 0 ]; }; then ok "t1 mirror fails 2x then succeeds: 3 tries, no fallback"; else bad "t1 (rc=${RC} m=${PULLS_MIRROR} d=${PULLS_DOCKERIO})"; fi

run t2 FAKE_FAIL_MIRROR=9 "${SEEN}"
if { [ "${RC}" = 0 ] && [ "${PULLS_MIRROR}" = 3 ] && [ "${PULLS_DOCKERIO}" = 1 ]; }; then ok "t2 mirror all fail: 3 tries, then docker.io used"; else bad "t2 (rc=${RC} m=${PULLS_MIRROR} d=${PULLS_DOCKERIO})"; fi
if grep -q "^docker pull docker.io/library/golang@${DIGEST}\$" "${T}/d.t2/log"; then ok "t2 fallback pulls the SAME digest"; else bad "t2 fallback digest"; fi

run t3 FAKE_FAIL_MIRROR=9 FAKE_FAIL_DOCKERIO=9 "${SEEN}"
if { [ "${RC}" != 0 ] && [ "${PULLS_MIRROR}" = 3 ] && [ "${PULLS_DOCKERIO}" = 3 ]; }; then ok "t3 both fail: 3+3 tries, non-zero"; else bad "t3 (rc=${RC} m=${PULLS_MIRROR} d=${PULLS_DOCKERIO})"; fi
if printf '%s\n' "${OUT}" | grep -q "^FAIL: base image golang@${DIGEST} could not be pulled from: mirror.gcr.io (3 tries), docker.io (3 tries)"; then ok "t3 named failure line"; else bad "t3 named line"; fi

run t4 "FAKE_SEEN_DIGEST=${OTHER}"
if { [ "${RC}" != 0 ] && [ "${PULLS_MIRROR}" = 3 ] && [ "${PULLS_DOCKERIO}" = 3 ]; }; then ok "t4 digest mismatch counts as a failed try"; else bad "t4 (rc=${RC} m=${PULLS_MIRROR} d=${PULLS_DOCKERIO})"; fi

run t4b "FAKE_SEEN_DIGEST=${OTHER},${DIGEST}"
if [ "${RC}" = 0 ] && [ "${PULLS_MIRROR}" = 1 ]; then ok "t4b 2 RepoDigests, 2nd matches: pass"; else bad "t4b (rc=${RC} m=${PULLS_MIRROR})"; fi
run t4c "FAKE_SEEN_DIGEST=${OTHER},sha256:$(printf 'd%.0s' {1..64})"
if [ "${RC}" != 0 ] && [ "${PULLS_MIRROR}" = 3 ] && [ "${PULLS_DOCKERIO}" = 3 ] && printf '%s\n' "${OUT}" | grep -q 'digest=mismatch'; then ok "t4c 2 RepoDigests, none match: failed try, digest=mismatch"; else bad "t4c (rc=${RC})"; fi

run t4d FAKE_TAG_RC=42 "${SEEN}"
if [ "${RC}" = 1 ] && printf '%s\n' "${OUT}" | grep -q "^FAIL: docker tag of mirror.gcr.io/library/golang@${DIGEST} to dho-go-build-base:local-[0-9]* failed rc=42" && printf '%s\n' "${OUT}" | grep -qx 'TAG='; then ok "t4d docker tag rc 42: helper rc 1, named line, PINNED_BASE_TAG unset"; else bad "t4d (rc=${RC}): ${OUT}"; fi

run t4e FAKE_INSPECT_RC=3 "${SEEN}"
if [ "${RC}" != 0 ] && [ "${PULLS_MIRROR}" = 3 ] && [ "${PULLS_DOCKERIO}" = 3 ] && printf '%s\n' "${OUT}" | grep -q 'digest=inspect-failed' && printf '%s\n' "${OUT}" | grep -q 'rc=3 '; then ok "t4e docker image inspect rc 3: failed try (digest=inspect-failed), no tag"; else bad "t4e (rc=${RC}): ${OUT}"; fi

DF="${T}/missing-Dockerfile" run t4f "${SEEN}"
if [ "${RC}" = 1 ] && printf '%s\n' "${OUT}" | grep -q '^FAIL: cannot read .*missing-Dockerfile' && [ "${PULLS_MIRROR}" = 0 ]; then ok "t4f unreadable Dockerfile: named failure, no pull"; else bad "t4f (rc=${RC}): ${OUT}"; fi

printf 'ARG GO_BUILD_IMAGE="gcr.io/library/golang:1@%s"\n' "${DIGEST}" > "${T}/Dockerfile.nomirror"
DF="${T}/Dockerfile.nomirror" run t4g "${SEEN}"
if [ "${RC}" = 1 ] && printf '%s\n' "${OUT}" | grep -q '^FAIL: GO_BUILD_IMAGE in .* is not a mirror.gcr.io ref' && [ "${PULLS_MIRROR}" = 0 ]; then ok "t4g non-mirror ref: named failure, no pull"; else bad "t4g (rc=${RC}): ${OUT}"; fi

run t4h PINNED_BASE_TAG=stale-tag FAKE_FAIL_MIRROR=9 FAKE_FAIL_DOCKERIO=9 "${SEEN}"
if [ "${RC}" = 1 ] && printf '%s\n' "${OUT}" | grep -qx 'TAG='; then ok "t4h failure clears a stale PINNED_BASE_TAG"; else bad "t4h (rc=${RC}): ${OUT}"; fi

run t5 FAKE_FAIL_MIRROR=9 FAKE_RATELIMIT_DOCKERIO=1 "${SEEN}"
if { [ "${RC}" != 0 ] && printf '%s\n' "${OUT}" | grep -q 'rate-limited'; }; then ok "t5 docker.io rate-limit refusal is a named failed try"; else bad "t5 (rc=${RC})"; fi

run t6 FAKE_FAIL_MIRROR=9 FAKE_FAIL_DOCKERIO=9 "${SEEN}"
if [ "$(grep -c '^docker pull' "${FAKE_LOG}")" -le 6 ]; then ok "t6 pull count bounded at 6"; else bad "t6 bound"; fi
if grep -q '^docker login' "${FAKE_LOG}"; then bad "t6 docker login called"; else ok "t6 no docker login"; fi

run t7 "${SEEN}"
if printf '%s\n' "${OUT}" | grep -Eq "base-image pull: source=mirror.gcr.io try=1/3 rc=0 elapsed=[0-9]+s digest=${DIGEST}"; then ok "t7 per-try log line (source, try n/3, rc, elapsed, digest)"; else bad "t7 log line: ${OUT}"; fi

TAG1="$(printf '%s\n' "${OUT}" | sed -n 's/^TAG=//p')"
run t8 "${SEEN}"
TAG2="$(printf '%s\n' "${OUT}" | sed -n 's/^TAG=//p')"
if { [ -n "${TAG1}" ] && [ "${TAG1}" != "${TAG2}" ]; }; then ok "t8 local tag differs per process (${TAG1} vs ${TAG2})"; else bad "t8 tag collision (${TAG1} / ${TAG2})"; fi
run t9 GITHUB_RUN_ID=777 "${SEEN}"
if printf '%s\n' "${OUT}" | grep -q '^TAG=.*777'; then ok "t9 tag carries the run id"; else bad "t9 run id"; fi

sed 's/golang:1.27.3-alpine@sha256:a*/golang:9.9@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc/' "${T}/Dockerfile" > "${T}/Dockerfile2"
cp "${T}/Dockerfile2" "${T}/Dockerfile.keep"
FAKE_DIR="${T}/d.t10"; mkdir -p "${FAKE_DIR}"; FAKE_LOG="${FAKE_DIR}/log"; : > "${FAKE_LOG}"
C="sha256:$(printf 'c%.0s' {1..64})"
# shellcheck disable=SC2016
OUT="$(env -i PATH="${T}/bin:${PATH}" HOME="${T}" TMPDIR="${T}" LC_ALL=C FAKE_DIR="${FAKE_DIR}" FAKE_LOG="${FAKE_LOG}" FAKE_SEEN_DIGEST="${C}" PULL_BACKOFF_SECS="0 0" bash -c 'source "$1"; pull_pinned_base_image "$2"' _ "${LIB}" "${T}/Dockerfile2" 2>&1)" && R=0 || R=$?
if { [ "${R}" = 0 ] && grep -q "^docker pull mirror.gcr.io/library/golang@${C}\$" "${FAKE_LOG}"; }; then ok "t10 digest is read from the Dockerfile"; else bad "t10"; fi

for s in ci/check_auth_k8s.sh ci/check_session_cutover_rehearsal.sh; do
  p="$(grep -n 'pull_pinned_base_image' "${ROOT}/${s}" | head -1 | cut -d: -f1 || true)"
  b="$(grep -n '^build ' "${ROOT}/${s}" | head -1 | cut -d: -f1 || true)"
  if { [ -n "${p}" ] && [ -n "${b}" ] && [ "${p}" -lt "${b}" ]; }; then ok "${s} pre-pulls before its first build"; else bad "${s} does not pre-pull before building"; fi
  # shellcheck disable=SC2016
  if grep -qF 'GO_BUILD_IMAGE="${PINNED_BASE_TAG}"' "${ROOT}/${s}" && grep -q -- '--pull=false' "${ROOT}/${s}"; then ok "${s} builds from the local tag"; else bad "${s} build args"; fi
  if grep -E 'docker image rm' "${ROOT}/${s}" | grep -qF 'PINNED_BASE_TAG'; then ok "${s} removes PINNED_BASE_TAG in its cleanup"; else bad "${s} cleanup leaves the pulled base tag"; fi
done

[ "${fails}" = 0 ] || { printf '%s check(s) failed\n' "${fails}"; exit 1; }
printf 'all pull_base_image checks passed\n'
