#!/usr/bin/env bash
# CHAOS-10366: hermetic red/green coverage for ci/verify_base_sources.sh (the pull_request dry-run: each pinned base-image digest is
# checked AT ITS SOURCE, nothing is published). A test-local `docker` stub on PATH answers `buildx imagetools inspect`; no network,
# no registry, no credential. Run: bash ci/verify_base_sources_test.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT="${ROOT}/ci/verify_base_sources.sh"
T="$(mktemp -d)"
trap 'rm -rf "${T:?}"' EXIT
fail=0
ok() { printf 'ok   %s\n' "$1"; }
bad() { printf 'FAIL %s\n' "$1"; fail=1; }

[ -f "${SCRIPT}" ] || { printf 'FAIL ci/verify_base_sources.sh is missing\n'; exit 1; }

GO_D="sha256:$(printf 'a%.0s' $(seq 64))"
DEB_D="sha256:$(printf 'b%.0s' $(seq 64))"
mkdir -p "${T}/bin"
cat > "${T}/tsv" <<TSV
# fixture
go-build	library/golang:1.27.3-alpine@${GO_D}
tools-runtime	library/debian:bookworm-slim@${DEB_D}
TSV

# the stub answers only: docker buildx imagetools inspect --format '{{ .Manifest.Digest }}' <ref>. FAKE_DIG_GO / FAKE_DIG_DEB override
# the served digest; FAKE_FAIL=<repo substring> makes the inspect fail. Every call is logged; any other docker command is a test bug.
# shellcheck disable=SC2016  # the stub's text is single-quoted on purpose
printf '%s\n' \
  '#!/usr/bin/env bash' \
  'printf '\''%s\n'\'' "$*" >> "${DOCKER_LOG}"' \
  'if [ "$1 $2 $3" != "buildx imagetools inspect" ] || [ "$4" != "--format" ] || [ "$5" != "{{ .Manifest.Digest }}" ] || [ "$#" != 6 ]; then' \
  '  printf '\''stub docker: unexpected invocation: %s\n'\'' "$*" >&2; exit 99' \
  'fi' \
  'ref="$6"' \
  'if [ -n "${FAKE_FAIL:-}" ] && [[ "${ref}" == *"${FAKE_FAIL}"* ]]; then printf '\''manifest unknown\n'\'' >&2; exit 1; fi' \
  'case "${ref}" in' \
  '  *golang*) printf '\''%s\n'\'' "${FAKE_DIG_GO:-${GO_D}}" ;;' \
  '  *debian*) printf '\''%s\n'\'' "${FAKE_DIG_DEB:-${DEB_D}}" ;;' \
  '  *) printf '\''stub docker: unexpected ref %s\n'\'' "${ref}" >&2; exit 99 ;;' \
  'esac' > "${T}/bin/docker"
chmod +x "${T}/bin/docker"

run() { # tsv ; env passes through; sets RC, OUT
  : > "${T}/docker.log"
  RC=0
  OUT="$(PATH="${T}/bin:${PATH}" DOCKER_LOG="${T}/docker.log" GO_D="${GO_D}" DEB_D="${DEB_D}" bash "${SCRIPT}" full-chaos "$1" 2>&1)" || RC=$?
}

run "${T}/tsv"
if [ "${RC}" = 0 ] && [ "$(printf '%s\n' "${OUT}" | grep -c '^OK ')" = 2 ] && printf '%s\n' "${OUT}" | grep -q "^OK   go-build library/golang:1.27.3-alpine ${GO_D} (source)"; then ok "s1 both pinned digests are served at the source"; else bad "s1 (rc=${RC}): ${OUT}"; fi
if ! grep -qE 'imagetools create|push|login|buildx build' "${T}/docker.log" && [ "$(wc -l < "${T}/docker.log" | tr -d ' ')" = 2 ]; then ok "s2 only two read-only inspect calls; nothing is created, pushed or logged in"; else bad "s2 docker log: $(cat "${T}/docker.log")"; fi
FAKE_DIG_GO="sha256:$(printf 'c%.0s' $(seq 64))" run "${T}/tsv"
if [ "${RC}" = 1 ] && printf '%s\n' "${OUT}" | grep -q "^FAIL go-build: library/golang:1.27.3-alpine serves sha256:c.*, want ${GO_D}" && printf '%s\n' "${OUT}" | grep -q '^OK   tools-runtime'; then ok "s3 a moved digest at the source = named FAIL, the other image still checked"; else bad "s3 (rc=${RC}): ${OUT}"; fi
FAKE_FAIL=debian run "${T}/tsv"
if [ "${RC}" = 1 ] && printf '%s\n' "${OUT}" | grep -q '^FAIL tools-runtime: library/debian:bookworm-slim cannot be read at its source'; then ok "s4 an unreadable source = named FAIL"; else bad "s4 (rc=${RC}): ${OUT}"; fi
printf 'go-build\tlibrary/golang:1.27@sha1:abc\n' > "${T}/tsv.bad"
run "${T}/tsv.bad"
if [ "${RC}" != 0 ] && [ ! -s "${T}/docker.log" ]; then ok "s5 a malformed tsv fails with no registry call"; else bad "s5 (rc=${RC})"; fi
printf 'go-build\tlibrary/golang:1.27.3-alpine\n' > "${T}/tsv.nodigest"
run "${T}/tsv.nodigest"
if [ "${RC}" != 0 ] && [ ! -s "${T}/docker.log" ]; then ok "s6 a ref with no digest is rejected before any registry call"; else bad "s6 (rc=${RC})"; fi
printf '# nothing\n' > "${T}/tsv.empty"
run "${T}/tsv.empty"
if [ "${RC}" != 0 ]; then ok "s7 an empty tsv fails"; else bad "s7"; fi
run "${T}/absent.tsv"
if [ "${RC}" != 0 ]; then ok "s8 a missing tsv fails"; else bad "s8"; fi
run "${ROOT}/ci/base_images.tsv"
if printf '%s\n' "${OUT}" | grep -c '^\(OK  \|FAIL\) ' | grep -qx 2; then ok "s9 the real ci/base_images.tsv yields two checks"; else bad "s9: ${OUT}"; fi

[ "${fail}" = 0 ] || { printf 'verify_base_sources: FAILED\n'; exit 1; }
printf 'all verify_base_sources checks passed\n'
