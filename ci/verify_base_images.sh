#!/usr/bin/env bash
# CHAOS-10366: after mirror-test-images.yml publishes the base images (ci/base_images.tsv), read each one back from ghcr.io WITHOUT
# credentials, by tag and by digest, and fail unless both serve the pin. This is the check a consumer makes: a package that is private,
# missing, or holds another digest at the tag is a named FAIL here, not a 404 in a Dockerfile build later.
#   bash ci/verify_base_images.sh <owner> [tsv]
# Anonymous on purpose: no login, no token, no -u. Reads the tsv through ci/builder_images.sh (the one parser).
set -euo pipefail

owner="${1:?owner required}"
tsv="${2:-ci/base_images.tsv}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
accept='application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json'

# manifest_answer REPO REFERENCE: prints "<http code> <digest header or ->"; code 000 and digest - when no anonymous pull token exists.
manifest_answer() {
  local repo="$1" reference="$2" token hdrs code dig
  token="$(curl -fsS --max-time 30 "https://ghcr.io/token?service=ghcr.io&scope=repository:${repo}:pull" 2>/dev/null \
    | sed -n 's/.*"token"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')" || token=""
  if [ -z "${token}" ]; then printf '000 -\n'; return 0; fi
  hdrs="$(curl -sS -I --max-time 30 -H "Authorization: Bearer ${token}" -H "Accept: ${accept}" \
    "https://ghcr.io/v2/${repo}/manifests/${reference}" 2>/dev/null | tr -d '\r')" || hdrs=""
  code="$(printf '%s\n' "${hdrs}" | sed -n '1s/^HTTP\/[0-9.]* \([0-9][0-9]*\).*/\1/p')"
  dig="$(printf '%s\n' "${hdrs}" | sed -n 's/^[Dd]ocker-[Cc]ontent-[Dd]igest:[[:space:]]*//p' | head -1)"
  printf '%s %s\n' "${code:-000}" "${dig:--}"
}

# check_one NAME DEST_LABEL REPO REFERENCE WANT
check_one() {
  local name="$1" label="$2" repo="$3" reference="$4" want="$5" ans code dig
  ans="$(manifest_answer "${repo}" "${reference}")"
  code="${ans%% *}"; dig="${ans#* }"
  if [ "${code}" = 000 ]; then
    printf 'FAIL %s: ghcr.io/%s is not readable anonymously (no pull token): the package does not exist or is private\n' "${name}" "${repo}" >&2
    return 1
  fi
  if [ "${code}" != 200 ]; then
    printf 'FAIL %s: %s answered HTTP %s anonymously\n' "${name}" "${label}" "${code}" >&2
    return 1
  fi
  if [ "${dig}" != "${want}" ]; then
    printf 'FAIL %s: %s serves %s, want %s\n' "${name}" "${label}" "${dig}" "${want}" >&2
    return 1
  fi
  return 0
}

lines="$(bash "${here}/builder_images.sh" "${owner}" "${tsv}")" || { echo "::error::${tsv} was refused by ci/builder_images.sh" >&2; exit 1; }
failed=0
seen=0
while IFS=$'\t' read -r kind name ref; do
  [ "${kind}" = upstream ] || continue
  seen=$((seen + 1))
  want="${ref##*@}"
  rest="${ref%@*}"
  repo="${owner}/${rest%:*}"
  tag="${rest##*:}"
  bad=0
  check_one "${name}" "ghcr.io/${repo}:${tag}" "${repo}" "${tag}" "${want}" || bad=1
  check_one "${name}" "ghcr.io/${repo}@${want}" "${repo}" "${want}" "${want}" || bad=1
  if [ "${bad}" = 0 ]; then
    printf 'OK   %s ghcr.io/%s:%s %s (anonymous, by tag and by digest)\n' "${name}" "${repo}" "${tag}" "${want}"
  else
    failed=1
  fi
done <<< "${lines}"
[ "${seen}" -gt 0 ] || { echo "::error::${tsv} lists no image" >&2; exit 1; }
exit "${failed}"
