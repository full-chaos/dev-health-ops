#!/usr/bin/env bash
# CHAOS-10366: the pull_request dry-run of the base-image mirror. For each image in ci/base_images.tsv, read the pinned ref AT ITS SOURCE
# (Docker Hub) and fail unless the source serves exactly the pinned digest. Nothing is published, created or logged in: the only docker
# call is the read-only `buildx imagetools inspect`. A ref with no digest never gets here: the one reader (ci/builder_images.sh) refuses it.
#   bash ci/verify_base_sources.sh <owner> [tsv]
set -euo pipefail

owner="${1:?owner required}"
tsv="${2:-ci/base_images.tsv}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

lines="$(bash "${here}/builder_images.sh" "${owner}" "${tsv}")" || { echo "::error::${tsv} was refused by ci/builder_images.sh" >&2; exit 1; }
failed=0
seen=0
while IFS=$'\t' read -r kind name ref; do
  [ "${kind}" = upstream ] || continue
  seen=$((seen + 1))
  want="${ref##*@}"
  shown="${ref%@*}"
  if ! got="$(docker buildx imagetools inspect --format '{{ .Manifest.Digest }}' "${ref}" 2>/dev/null)"; then
    printf 'FAIL %s: %s cannot be read at its source\n' "${name}" "${shown}" >&2
    failed=1
    continue
  fi
  if [ "${got}" != "${want}" ]; then
    printf 'FAIL %s: %s serves %s, want %s\n' "${name}" "${shown}" "${got}" "${want}" >&2
    failed=1
    continue
  fi
  printf 'OK   %s %s %s (source)\n' "${name}" "${shown}" "${want}"
done <<< "${lines}"
[ "${seen}" -gt 0 ] || { echo "::error::${tsv} lists no image" >&2; exit 1; }
exit "${failed}"
