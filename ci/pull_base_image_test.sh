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

# shellcheck disable=SC2016
printf '%s\n' \
  '#!/usr/bin/env bash' \
  '# Stub: any shape it does not know exits 99 loudly. `docker login` is never expected.' \
  'echo "docker $*" >> "${FAKE_LOG}"' \
  'bump() { local f="${FAKE_DIR}/$1.n"; local n=0; [ -f "$f" ] && n="$(cat "$f")"; n=$((n + 1)); echo "$n" > "$f"; echo "$n"; }' \
  'case "$1" in' \
  '  pull)' \
  '    ref="$2"' \
  '    case "${ref}" in' \
  '      mirror.gcr.io/*) key=mirror; fails="${FAKE_FAIL_MIRROR:-0}" ;;' \
  '      docker.io/*) key=dockerio; fails="${FAKE_FAIL_DOCKERIO:-0}" ;;' \
  '      gcr.io/*) key=gcr; fails="${FAKE_FAIL_GCR:-0}" ;;' \
  '      *) echo "stub: unknown source ${ref}" >&2; exit 99 ;;' \
  '    esac' \
  '    n="$(bump "${key}")"' \
  '    if [ "${key}" = dockerio ] && [ "${FAKE_RATELIMIT_DOCKERIO:-0}" = 1 ]; then' \
  '      echo "toomanyrequests: You have reached your pull rate limit" >&2; exit 1' \
  '    fi' \
  '    if [ "${n}" -le "${fails}" ]; then echo "error: not found" >&2; exit 1; fi' \
  '    echo "pulled ${ref}"; exit 0 ;;' \
  '  image)' \
  '    case "$2" in' \
  '      inspect) [ "${FAKE_INSPECT_RC:-0}" = 0 ] || { echo "inspect failed" >&2; exit "${FAKE_INSPECT_RC}"; }; for d in ${FAKE_SEEN_DIGEST//,/ }; do echo "x@${d}"; done; exit 0 ;;' \
  '      rm) exit 0 ;;' \
  '    esac ;;' \
  '  tag) exit "${FAKE_TAG_RC:-0}" ;;' \
  'esac' \
  'echo "stub: unexpected invocation: $*" >&2' \
  'exit 99' > "${T}/bin/docker"
chmod +x "${T}/bin/docker"

fails=0
ok() { printf 'ok   %s\n' "$1"; }
bad() { printf 'FAIL %s\n' "$1"; fails=$((fails + 1)); }

[ -f "${LIB}" ] || { printf 'FAIL ci/lib/pull_base_image.sh is missing\n'; exit 1; }

# run <name> <env assignments...> -> sets RC, OUT, and per-source counts. The helper runs under env -i with a
# fixed PATH/HOME/TMPDIR/LC_ALL, so no hosted-runner variable (GITHUB_*, RUNNER_*, CI, PULL_*, DOCKER_*) can change a case.
# MODE=hosted adds a hosted-like environment (GITHUB_RUN_ID, GITHUB_ACTIONS, CI, RUNNER_OS) on top. FN and TV pick the
# helper function and the tag variable it exports (default: the Go build image).
MODE=plain
HOSTED_ENV=(GITHUB_RUN_ID=4242 GITHUB_ACTIONS=true CI=true RUNNER_OS=Linux)
# norm: the parts of an output that differ per run (elapsed seconds, the process id of the tag, the temp dir).
norm() { sed -E -e 's/elapsed=[0-9]+s/elapsed=Ns/' -e 's/(local|4242|777)-[0-9]+/\1-PID/g' -e "s#${T}#TMP#g"; }
GOLDEN_OUT="${T}/golden.out"; : > "${GOLDEN_OUT}"
run() {
  local name="$1"
  FAKE_DIR="${T}/d.${MODE}.$1"; rm -rf "${FAKE_DIR}"; mkdir -p "${FAKE_DIR}"
  FAKE_LOG="${FAKE_DIR}/log"; : > "${FAKE_LOG}"
  shift
  local mode_env=()
  [ "${MODE}" = hosted ] && mode_env=("${HOSTED_ENV[@]}")
  set +e
  # shellcheck disable=SC2016
  OUT="$(env -i PATH="${T}/bin:/usr/bin:/bin" HOME="${T}" TMPDIR="${T}" LC_ALL=C "${mode_env[@]}" "$@" FAKE_DIR="${FAKE_DIR}" FAKE_LOG="${FAKE_LOG}" \
    PULL_BACKOFF_SECS="0 0" bash -c 'source "$1"; "$3" "$2"; r=$?; tv="$4"; echo "TAG=${!tv:-}"; exit "$r"' _ "${LIB}" "${DF:-${T}/Dockerfile}" "${FN:-pull_pinned_base_image}" "${TV:-PINNED_BASE_TAG}" 2>&1)"
  RC=$?
  set -e
  PULLS_MIRROR="$(grep -c '^docker pull mirror.gcr.io/' "${FAKE_LOG}" || true)"
  PULLS_DOCKERIO="$(grep -c '^docker pull docker.io/' "${FAKE_LOG}" || true)"
  PULLS_GCR="$(grep -c '^docker pull gcr.io/' "${FAKE_LOG}" || true)"
  case "${name}" in t*) printf '=== %s rc=%s mirror=%s dockerio=%s\n%s\n' "${name}" "${RC}" "${PULLS_MIRROR}" "${PULLS_DOCKERIO}" "$(printf '%s\n' "${OUT}" | norm)" >> "${GOLDEN_OUT}" ;; esac
}
SEEN="FAKE_SEEN_DIGEST=${DIGEST}"

run t1 FAKE_FAIL_MIRROR=2 "${SEEN}"
if { [ "${RC}" = 0 ] && [ "${PULLS_MIRROR}" = 3 ] && [ "${PULLS_DOCKERIO}" = 0 ]; }; then ok "t1 mirror fails 2x then succeeds: 3 tries, no fallback"; else bad "t1 (rc=${RC} m=${PULLS_MIRROR} d=${PULLS_DOCKERIO})"; fi

run t2 FAKE_FAIL_MIRROR=9 "${SEEN}"
if { [ "${RC}" = 0 ] && [ "${PULLS_MIRROR}" = 3 ] && [ "${PULLS_DOCKERIO}" = 1 ]; }; then ok "t2 mirror all fail: 3 tries, then docker.io used"; else bad "t2 (rc=${RC} m=${PULLS_MIRROR} d=${PULLS_DOCKERIO})"; fi
if grep -q "^docker pull docker.io/library/golang@${DIGEST}\$" "${T}/d.plain.t2/log"; then ok "t2 fallback pulls the SAME digest"; else bad "t2 fallback digest"; fi

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
OUT="$(env -i PATH="${T}/bin:/usr/bin:/bin" HOME="${T}" TMPDIR="${T}" LC_ALL=C FAKE_DIR="${FAKE_DIR}" FAKE_LOG="${FAKE_LOG}" FAKE_SEEN_DIGEST="${C}" PULL_BACKOFF_SECS="0 0" bash -c 'source "$1"; pull_pinned_base_image "$2"' _ "${LIB}" "${T}/Dockerfile2" 2>&1)" && R=0 || R=$?
if { [ "${R}" = 0 ] && grep -q "^docker pull mirror.gcr.io/library/golang@${C}\$" "${FAKE_LOG}"; }; then ok "t10 digest is read from the Dockerfile"; else bad "t10"; fi

if grep -q 'IFS' "${LIB}"; then bad "helper sets or reads IFS (semgrep ifs-tampering)"; else ok "helper sets no IFS"; fi

for s in ci/check_auth_k8s.sh ci/check_session_cutover_rehearsal.sh; do
  p="$(grep -n 'pull_pinned_base_image' "${ROOT}/${s}" | head -1 | cut -d: -f1 || true)"
  b="$(grep -n '^build ' "${ROOT}/${s}" | head -1 | cut -d: -f1 || true)"
  if { [ -n "${p}" ] && [ -n "${b}" ] && [ "${p}" -lt "${b}" ]; }; then ok "${s} pre-pulls before its first build"; else bad "${s} does not pre-pull before building"; fi
  # shellcheck disable=SC2016
  if grep -qF 'GO_BUILD_IMAGE="${PINNED_BASE_TAG}"' "${ROOT}/${s}" && grep -q -- '--pull=false' "${ROOT}/${s}"; then ok "${s} builds from the local tag"; else bad "${s} build args"; fi
  if grep -E 'docker image rm' "${ROOT}/${s}" | grep -qF 'PINNED_BASE_TAG'; then ok "${s} removes PINNED_BASE_TAG in its cleanup"; else bad "${s} cleanup leaves the pulled base tag"; fi
done


# --- the Go build image helper keeps its behaviour: the output of every t* case is byte-equal to the golden recorded from
# the helper before CHAOS-11649 (elapsed seconds, process ids and the temp dir normalised).
GOLDEN="${ROOT}/ci/testdata/pull_base_image.golden"
if [ -n "${GOLDEN_WRITE:-}" ]; then
  mkdir -p "$(dirname "${GOLDEN}")"; cp "${GOLDEN_OUT}" "${GOLDEN}"; ok "golden written"
elif [ -f "${GOLDEN}" ] && cmp -s "${GOLDEN_OUT}" "${GOLDEN}"; then ok "golden: every build-image case prints byte-equal output to the pre-CHAOS-11649 helper"
else bad "golden: the build-image output differs from ci/testdata/pull_base_image.golden"; diff "${GOLDEN}" "${GOLDEN_OUT}" | head -10 || true; fi

# --- CHAOS-11649: the runtime image (ARG GO_RUNTIME_IMAGE, gcr.io only) is pulled with the same rules.
RSEEN="FAKE_SEEN_DIGEST=${OTHER}"
runtime_cases() {
  local m="${MODE}"
  FN=pull_pinned_runtime_image TV=PINNED_RUNTIME_TAG
  export FN TV
  run r1 FAKE_FAIL_GCR=2 "${RSEEN}"
  if { [ "${RC}" = 0 ] && [ "${PULLS_GCR}" = 3 ] && [ "${PULLS_MIRROR}" = 0 ] && [ "${PULLS_DOCKERIO}" = 0 ]; }; then ok "${m} r1 gcr fails 2x then succeeds: 3 tries, no other source"; else bad "${m} r1 (rc=${RC} g=${PULLS_GCR} m=${PULLS_MIRROR} d=${PULLS_DOCKERIO})"; fi
  if printf '%s\n' "${OUT}" | grep -q '^TAG=dho-go-runtime-base:'; then ok "${m} r1 PINNED_RUNTIME_TAG is a dho-go-runtime-base tag"; else bad "${m} r1 tag: ${OUT}"; fi
  if grep -q "^docker pull gcr.io/distroless/static-debian12@${OTHER}\$" "${FAKE_LOG}"; then ok "${m} r1 pulls the digest of the Dockerfile ARG"; else bad "${m} r1 pull ref"; fi

  run r2 FAKE_FAIL_GCR=9 "${RSEEN}"
  if { [ "${RC}" != 0 ] && [ "${PULLS_GCR}" = 3 ] && [ "${PULLS_MIRROR}" = 0 ] && [ "${PULLS_DOCKERIO}" = 0 ]; }; then ok "${m} r2 gcr always fails: exactly 3 pulls, no fallback, non-zero"; else bad "${m} r2 (rc=${RC} g=${PULLS_GCR})"; fi
  if printf '%s\n' "${OUT}" | grep -q "^FAIL: base image static-debian12@${OTHER} could not be pulled from: gcr.io (3 tries)"; then ok "${m} r2 named failure line"; else bad "${m} r2 named line: ${OUT}"; fi
  if printf '%s\n' "${OUT}" | grep -qx 'TAG='; then ok "${m} r2 no tag after a failure"; else bad "${m} r2 tag: ${OUT}"; fi

  run r2b PINNED_RUNTIME_TAG=stale FAKE_FAIL_GCR=9 "${RSEEN}"
  if printf '%s\n' "${OUT}" | grep -qx 'TAG='; then ok "${m} r2b a failure clears a stale PINNED_RUNTIME_TAG"; else bad "${m} r2b: ${OUT}"; fi

  run r3 "FAKE_SEEN_DIGEST=${DIGEST}"
  if [ "${RC}" != 0 ] && [ "${PULLS_GCR}" = 3 ] && printf '%s\n' "${OUT}" | grep -q 'digest=mismatch'; then ok "${m} r3 digest mismatch is a failed try"; else bad "${m} r3 (rc=${RC}): ${OUT}"; fi
  run r3b FAKE_INSPECT_RC=3 "${RSEEN}"
  if [ "${RC}" != 0 ] && [ "${PULLS_GCR}" = 3 ] && printf '%s\n' "${OUT}" | grep -q 'digest=inspect-failed'; then ok "${m} r3b inspect failure is a failed try"; else bad "${m} r3b (rc=${RC}): ${OUT}"; fi

  run r4 FAKE_TAG_RC=42 "${RSEEN}"
  if [ "${RC}" = 1 ] && printf '%s\n' "${OUT}" | grep -q "^FAIL: docker tag of gcr.io/distroless/static-debian12@${OTHER} to dho-go-runtime-base:.* failed rc=42"; then ok "${m} r4 docker tag rc 42: helper rc 1, named line"; else bad "${m} r4 (rc=${RC}): ${OUT}"; fi

  run r5 "${RSEEN}"
  if printf '%s\n' "${OUT}" | grep -Eq "^base-image pull: source=gcr.io try=1/3 rc=0 elapsed=[0-9]+s digest=${OTHER}\$"; then ok "${m} r5 per-try log line"; else bad "${m} r5 log line: ${OUT}"; fi
  local rt1 bt
  rt1="$(printf '%s\n' "${OUT}" | sed -n 's/^TAG=//p')"
  FN=pull_pinned_base_image TV=PINNED_BASE_TAG run r5b "${SEEN}"
  bt="$(printf '%s\n' "${OUT}" | sed -n 's/^TAG=//p')"
  if [ -n "${rt1}" ] && [ -n "${bt}" ] && [ "${rt1%%:*}" != "${bt%%:*}" ]; then ok "${m} r5 runtime and build tags have different names"; else bad "${m} r5 tags ${rt1} / ${bt}"; fi
  if [ "${m}" = hosted ]; then
    if printf '%s\n' "${rt1}" | grep -q ':4242-'; then ok "${m} r5 tag carries GITHUB_RUN_ID"; else bad "${m} r5 run id: ${rt1}"; fi
  fi
  FN=pull_pinned_runtime_image TV=PINNED_RUNTIME_TAG run r5c GITHUB_RUN_ID=777 "${RSEEN}"
  if printf '%s\n' "${OUT}" | grep -q '^TAG=dho-go-runtime-base:777-'; then ok "${m} r5 an explicit run id wins"; else bad "${m} r5 explicit run id: ${OUT}"; fi

  C2="sha256:$(printf 'e%.0s' {1..64})"
  sed "s/static-debian12:nonroot@sha256:b*/static-debian12:nonroot@${C2}/" "${T}/Dockerfile" > "${T}/Dockerfile.rt2"
  DF="${T}/Dockerfile.rt2" run r6 "FAKE_SEEN_DIGEST=${C2}"
  if [ "${RC}" = 0 ] && grep -q "^docker pull gcr.io/distroless/static-debian12@${C2}\$" "${FAKE_LOG}"; then ok "${m} r6 digest is read from the Dockerfile"; else bad "${m} r6 (rc=${RC}): ${OUT}"; fi

  printf 'ARG GO_RUNTIME_IMAGE="mirror.gcr.io/distroless/static-debian12:nonroot@%s"\n' "${OTHER}" > "${T}/Dockerfile.rt.nogcr"
  DF="${T}/Dockerfile.rt.nogcr" run r6b "${RSEEN}"
  if [ "${RC}" = 1 ] && printf '%s\n' "${OUT}" | grep -q '^FAIL: GO_RUNTIME_IMAGE in .* is not a gcr.io ref' && [ "${PULLS_GCR}" = 0 ] && [ "${PULLS_MIRROR}" = 0 ]; then ok "${m} r6b non-gcr.io ref: named failure, no pull"; else bad "${m} r6b (rc=${RC}): ${OUT}"; fi
  printf 'ARG GO_RUNTIME_IMAGE="gcr.io/distroless/static-debian12:nonroot"\n' > "${T}/Dockerfile.rt.nodigest"
  DF="${T}/Dockerfile.rt.nodigest" run r6c "${RSEEN}"
  if [ "${RC}" = 1 ] && printf '%s\n' "${OUT}" | grep -q '^FAIL: GO_RUNTIME_IMAGE in .* has no sha256 digest pin' && [ "${PULLS_GCR}" = 0 ]; then ok "${m} r6c no digest pin: named failure, no pull"; else bad "${m} r6c (rc=${RC}): ${OUT}"; fi
  printf 'ARG GO_BUILD_IMAGE="mirror.gcr.io/library/golang:1@%s"\n' "${DIGEST}" > "${T}/Dockerfile.rt.noarg"
  DF="${T}/Dockerfile.rt.noarg" run r6d "${RSEEN}"
  if [ "${RC}" = 1 ] && printf '%s\n' "${OUT}" | grep -q '^FAIL: ARG GO_RUNTIME_IMAGE not found in ' && [ "${PULLS_GCR}" = 0 ]; then ok "${m} r6d missing ARG: named failure, no pull"; else bad "${m} r6d (rc=${RC}): ${OUT}"; fi
  DF="${T}/missing-Dockerfile" run r6e "${RSEEN}"
  if [ "${RC}" = 1 ] && printf '%s\n' "${OUT}" | grep -q '^FAIL: cannot read .*missing-Dockerfile' && [ "${PULLS_GCR}" = 0 ]; then ok "${m} r6e unreadable Dockerfile: named failure, no pull"; else bad "${m} r6e (rc=${RC}): ${OUT}"; fi

  run r7 FAKE_FAIL_GCR=9 "${RSEEN}"
  if grep -q '^docker login' "${FAKE_LOG}"; then bad "${m} r7 docker login called"; else ok "${m} r7 no docker login"; fi
  unset FN TV
}
for MODE in plain hosted; do runtime_cases; done
MODE=plain

# the real Dockerfile carries the ARG the helper reads, pinned to gcr.io by digest
if grep -Eq '^ARG GO_RUNTIME_IMAGE="gcr\.io/[^"]+@sha256:[0-9a-f]{64}"$' "${ROOT}/docker/go-worker.Dockerfile"; then ok "r8 docker/go-worker.Dockerfile pins GO_RUNTIME_IMAGE to gcr.io by digest"; else bad "r8 the Dockerfile pin"; fi

# the worst case of the pulls stays below half of the smaller job timeout
tries="$(sed -n 's/.*PULL_TRIES:-\([0-9]*\)}.*/\1/p' "${LIB}" | head -1)"
tmo="$(sed -n 's/.*PULL_TIMEOUT_SECS:-\([0-9]*\)}.*/\1/p' "${LIB}" | head -1)"
gaps="$(sed -n 's/.*PULL_BACKOFF_SECS:-\([0-9 ]*\)}.*/\1/p' "${LIB}" | head -1)"
per_source="$(awk -v t="${tries}" -v o="${tmo}" -v g="${gaps}" 'BEGIN { n = split(g, a, " "); s = t * o; for (i = 1; i < t; i++) s += (i <= n ? a[i] : a[n]); print s }')"
sources_total=$((2 + 1))
worst=$((per_source * sources_total))
min_timeout=""
for wf in auth-k8s-proof.yml session-cutover-rehearsal.yml; do
  # shellcheck disable=SC2013
  for m in $(sed -n 's/^ *timeout-minutes: *\([0-9]*\).*/\1/p' "${ROOT}/.github/workflows/${wf}"); do
    if [ -z "${min_timeout}" ] || [ "${m}" -lt "${min_timeout}" ]; then min_timeout="${m}"; fi
  done
done
if [ -n "${per_source}" ] && [ -n "${min_timeout}" ] && [ "$((worst * 2))" -lt "$((min_timeout * 60))" ]; then ok "r9 worst-case pull time ${worst}s (3 sources x ${per_source}s) is below half of the smaller job timeout (${min_timeout} min)"; else bad "r9 budget: per_source=${per_source} worst=${worst} min_timeout=${min_timeout}"; fi

# both scripts pre-pull the runtime image before their first build, build from the local tag and clean it up
for s in ci/check_auth_k8s.sh ci/check_session_cutover_rehearsal.sh; do
  p="$(grep -n 'pull_pinned_runtime_image' "${ROOT}/${s}" | head -1 | cut -d: -f1 || true)"
  b="$(grep -n '^build ' "${ROOT}/${s}" | head -1 | cut -d: -f1 || true)"
  if { [ -n "${p}" ] && [ -n "${b}" ] && [ "${p}" -lt "${b}" ]; }; then ok "r8 ${s} pre-pulls the runtime image before its first build"; else bad "r8 ${s} does not pre-pull the runtime image before building"; fi
  # shellcheck disable=SC2016
  if grep -qF 'GO_RUNTIME_IMAGE="${PINNED_RUNTIME_TAG}"' "${ROOT}/${s}" && grep -q -- '--pull=false' "${ROOT}/${s}"; then ok "r8 ${s} builds from the local runtime tag"; else bad "r8 ${s} runtime build arg"; fi
  if grep -E 'docker image rm' "${ROOT}/${s}" | grep -qF 'PINNED_RUNTIME_TAG'; then ok "r8 ${s} removes PINNED_RUNTIME_TAG in its cleanup"; else bad "r8 ${s} cleanup leaves the pulled runtime tag"; fi
done

# the loop is shared: the pull helper uses retry_named from ci/lib/retry.sh
if grep -q 'retry_named' "${LIB}" && grep -q 'retry.sh' "${LIB}"; then ok "r10 the pull helper uses retry_named from ci/lib/retry.sh"; else bad "r10 the pull helper does not use the shared retry"; fi

[ "${fails}" = 0 ] || { printf '%s check(s) failed\n' "${fails}"; exit 1; }
printf 'all pull_base_image checks passed\n'
