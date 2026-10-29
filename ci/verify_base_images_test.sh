#!/usr/bin/env bash
# CHAOS-10366: hermetic red/green coverage for ci/verify_base_images.sh. A test-local `curl` stub on PATH answers the ghcr.io
# token and manifest calls; no network, no registry, no credential. Run: bash ci/verify_base_images_test.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT="${ROOT}/ci/verify_base_images.sh"
T="$(mktemp -d)"
trap 'rm -rf "${T:?}"' EXIT
fail=0
ok() { printf 'ok   %s\n' "$1"; }
bad() { printf 'FAIL %s\n' "$1"; fail=1; }

[ -f "${SCRIPT}" ] || { printf 'FAIL ci/verify_base_images.sh is missing\n'; exit 1; }

GO_D="sha256:$(printf 'a%.0s' $(seq 64))"
DEB_D="sha256:$(printf 'b%.0s' $(seq 64))"
mkdir -p "${T}/bin"
cat > "${T}/tsv" <<TSV
# fixture
go-build	library/golang:1.27.3-alpine@${GO_D}
tools-runtime	library/debian:bookworm-slim@${DEB_D}
TSV

# the stub: FAKE_NO_TOKEN=<repo-substring> refuses a token; FAKE_CODE_<kind>=<http code>; FAKE_DIG_<kind>=<digest> where kind is
# GOTAG, GODIG, DEBTAG, DEBDIG (tag or digest reference of golang or debian). Every call is logged.
# shellcheck disable=SC2016  # the stub's text is single-quoted on purpose
printf '%s\n' \
  '#!/usr/bin/env bash' \
  'printf '\''%s\n'\'' "$*" >> "${CURL_LOG}"' \
  'url=""; for a in "$@"; do case "${a}" in https://*) url="${a}" ;; esac; done' \
  'case "${url}" in' \
  '  https://ghcr.io/token*)' \
  '    if [ -n "${FAKE_NO_TOKEN:-}" ] && [[ "${url}" == *"${FAKE_NO_TOKEN}"* ]]; then exit 22; fi' \
  '    printf '\''{"token":"anon-token"}'\''; exit 0 ;;' \
  '  https://ghcr.io/v2/*/manifests/*)' \
  '    ref="${url##*/manifests/}"; kind=""' \
  '    case "${url}" in *golang*) kind=GO ;; *debian*) kind=DEB ;; esac' \
  '    case "${ref}" in sha256:*) kind="${kind}DIG" ;; *) kind="${kind}TAG" ;; esac' \
  '    code_var="FAKE_CODE_${kind}"; dig_var="FAKE_DIG_${kind}"' \
  '    code="${!code_var:-200}"' \
  '    if [ "${kind}" = GODIG ] || [ "${kind}" = DEBDIG ]; then def="${ref}"; else' \
  '      if [ "${kind}" = GOTAG ]; then def="${GO_D}"; else def="${DEB_D}"; fi; fi' \
  '    dig="${!dig_var:-${def}}"' \
  '    printf '\''HTTP/2 %s\r\n'\'' "${code}"' \
  '    [ "${code}" = 200 ] && printf '\''docker-content-digest: %s\r\n'\'' "${dig}"' \
  '    printf '\''\r\n'\''; exit 0 ;;' \
  '  *) printf '\''stub curl: unexpected url %s\n'\'' "${url}" >&2; exit 99 ;;' \
  'esac' > "${T}/bin/curl"
chmod +x "${T}/bin/curl"

run() { # args: tsv path ; env passes through; sets RC, OUT
  : > "${T}/curl.log"
  RC=0
  OUT="$(PATH="${T}/bin:${PATH}" CURL_LOG="${T}/curl.log" GO_D="${GO_D}" DEB_D="${DEB_D}" bash "${SCRIPT}" full-chaos "$1" 2>&1)" || RC=$?
}

# t1 both images readable at the pin
run "${T}/tsv"
if [ "${RC}" = 0 ] && [ "$(printf '%s\n' "${OUT}" | grep -c '^OK ')" = 2 ] && printf '%s\n' "${OUT}" | grep -q "^OK   go-build ghcr.io/full-chaos/library/golang:1.27.3-alpine ${GO_D}"; then ok "t1 both images readable anonymously at the pin"; else bad "t1 (rc=${RC}): ${OUT}"; fi
# t2 anonymous only: no credential in any call, only ghcr.io
if ! grep -qE -- '(^| )(-u|--user)( |$)|Basic|GITHUB_TOKEN|ghp_' "${T}/curl.log" && ! grep -v 'https://ghcr.io/' "${T}/curl.log" | grep -q 'https://'; then ok "t2 anonymous calls to ghcr.io only"; else bad "t2 a credential or another host appears in the calls"; fi
# t3 a private or absent package: no anonymous pull token
FAKE_NO_TOKEN=debian run "${T}/tsv"
if [ "${RC}" = 1 ] && printf '%s\n' "${OUT}" | grep -q '^FAIL tools-runtime: ghcr.io/full-chaos/library/debian is not readable anonymously (no pull token)' && printf '%s\n' "${OUT}" | grep -q '^OK   go-build'; then ok "t3 no anonymous token = named FAIL, the other image still checked"; else bad "t3 (rc=${RC}): ${OUT}"; fi
# t4 the tag is missing (404)
FAKE_CODE_GOTAG=404 run "${T}/tsv"
if [ "${RC}" = 1 ] && printf '%s\n' "${OUT}" | grep -q '^FAIL go-build: ghcr.io/full-chaos/library/golang:1.27.3-alpine answered HTTP 404 anonymously'; then ok "t4 missing tag = named FAIL"; else bad "t4 (rc=${RC}): ${OUT}"; fi
# t5 the tag serves another digest
FAKE_DIG_DEBTAG="sha256:$(printf 'c%.0s' $(seq 64))" run "${T}/tsv"
if [ "${RC}" = 1 ] && printf '%s\n' "${OUT}" | grep -q "^FAIL tools-runtime: ghcr.io/full-chaos/library/debian:bookworm-slim serves sha256:c.*, want ${DEB_D}"; then ok "t5 digest mismatch at the tag = named FAIL"; else bad "t5 (rc=${RC}): ${OUT}"; fi
# t6 the digest reference is missing (404)
FAKE_CODE_GODIG=404 run "${T}/tsv"
if [ "${RC}" = 1 ] && printf '%s\n' "${OUT}" | grep -q "^FAIL go-build: ghcr.io/full-chaos/library/golang@${GO_D} answered HTTP 404 anonymously"; then ok "t6 missing digest reference = named FAIL"; else bad "t6 (rc=${RC}): ${OUT}"; fi
# t7 the digest reference serves another digest
FAKE_DIG_GODIG="sha256:$(printf 'd%.0s' $(seq 64))" run "${T}/tsv"
if [ "${RC}" = 1 ] && printf '%s\n' "${OUT}" | grep -q "^FAIL go-build: ghcr.io/full-chaos/library/golang@${GO_D} serves sha256:d"; then ok "t7 digest mismatch at the digest reference = named FAIL"; else bad "t7 (rc=${RC}): ${OUT}"; fi
# t8 a malformed tsv is refused by the one reader, before any call
printf 'go-build\tlibrary/golang:1.27@sha1:abc\n' > "${T}/tsv.bad"
run "${T}/tsv.bad"
if [ "${RC}" != 0 ] && [ ! -s "${T}/curl.log" ]; then ok "t8 a malformed tsv fails with no registry call"; else bad "t8 (rc=${RC})"; fi
# t9 an empty tsv (no image) fails
printf '# nothing\n' > "${T}/tsv.empty"
run "${T}/tsv.empty"
if [ "${RC}" != 0 ]; then ok "t9 an empty tsv fails"; else bad "t9"; fi
# t10 a missing tsv fails
run "${T}/absent.tsv"
if [ "${RC}" != 0 ]; then ok "t10 a missing tsv fails"; else bad "t10"; fi
# t11 the real tsv parses and is checked (two images)
run "${ROOT}/ci/base_images.tsv"
if printf '%s\n' "${OUT}" | grep -c '^\(OK  \|FAIL\) ' | grep -qx 2; then ok "t11 the real ci/base_images.tsv yields two checks"; else bad "t11: ${OUT}"; fi
# t12 a status line other than 200 with no digest header (a redirect or an error page) is a FAIL, not a pass
FAKE_CODE_DEBTAG=301 run "${T}/tsv"
if [ "${RC}" = 1 ] && printf '%s\n' "${OUT}" | grep -q 'answered HTTP 301 anonymously'; then ok "t12 a non-200 answer is a FAIL"; else bad "t12 (rc=${RC}): ${OUT}"; fi

[ "${fail}" = 0 ] || { printf 'verify_base_images: FAILED\n'; exit 1; }
printf 'all verify_base_images checks passed\n'
