#!/usr/bin/env bash
# Downloads one pinned GitHub release asset and checks its sha256. Run as
#   bash ci/lib/fetch_pinned_release.sh OWNER/REPO TAG ASSET SHA256 OUT
# With GH_TOKEN (else GITHUB_TOKEN) set, the asset comes through the REST API with that token: an anonymous
# github.com/.../releases/download fetch can get a 504 in a GitHub incident and red the PR. The token goes to curl in a
# config on stdin only: never argv, never curl's environment, never the log. curl drops it on the cross-host redirect to
# the asset storage. With no token (a local run) the anonymous URL is used. Each request gets 5 tries, 5/10/20/40 s
# apart (retry_named, ci/lib/retry.sh). A SHA256 that is not 64 hex characters, or a file that does not match it, fails
# and leaves no OUT file.
set +x
set -euo pipefail

# shellcheck source=ci/lib/retry.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/retry.sh"

if [ "$#" -ne 5 ]; then
  printf 'fetch-pinned-release: usage: fetch_pinned_release.sh OWNER/REPO TAG ASSET SHA256 OUT\n' >&2
  exit 2
fi
repo="$1" tag="$2" asset="$3" sha="$4" out="$5"
if ! printf '%s' "${sha}" | grep -qxE '[0-9a-f]{64}'; then
  printf 'fetch-pinned-release: %s: the pinned sha256 must be 64 lowercase hex characters\n' "${asset}" >&2
  exit 2
fi
token="${GH_TOKEN:-${GITHUB_TOKEN:-}}"
unset GH_TOKEN GITHUB_TOKEN

rm -f "${out}"
part="${out}.part"
trap 'rm -f "${part}" "${part}.json"' EXIT

api_get() {
  local url="$1" dest="$2" accept="$3"
  printf 'header = "Authorization: Bearer %s"\nheader = "Accept: %s"\nheader = "X-GitHub-Api-Version: 2022-11-28"\n' \
    "${token}" "${accept}" | curl --config - --fail --silent --show-error --location --output "${dest}" "${url}"
}

if [ -n "${token}" ]; then
  api="https://api.github.com/repos/${repo}/releases"
  retry_named fetch-pinned-release-tag 5 double:5 -- api_get "${api}/tags/${tag}" "${part}.json" application/vnd.github+json
  id="$(jq -r --arg n "${asset}" '[.assets[] | select(.name == $n) | .id] | if length == 1 then .[0] else empty end' "${part}.json")"
  if [ -z "${id}" ]; then
    printf 'fetch-pinned-release: %s %s has no single asset named %s\n' "${repo}" "${tag}" "${asset}" >&2
    exit 1
  fi
  retry_named fetch-pinned-release-asset 5 double:5 -- api_get "${api}/assets/${id}" "${part}" application/octet-stream
else
  printf 'fetch-pinned-release: no GH_TOKEN or GITHUB_TOKEN: anonymous download of %s\n' "${asset}" >&2
  retry_named fetch-pinned-release-anon 5 double:5 -- curl --fail --silent --show-error --location --output "${part}" \
    "https://github.com/${repo}/releases/download/${tag}/${asset}"
fi

if ! printf '%s  %s\n' "${sha}" "${part}" | sha256sum --check --strict --status; then
  printf 'fetch-pinned-release: %s does not match its pinned sha256\n' "${asset}" >&2
  exit 1
fi
mv "${part}" "${out}"
printf 'fetch-pinned-release: %s %s %s sha256 ok\n' "${repo}" "${tag}" "${asset}" >&2
