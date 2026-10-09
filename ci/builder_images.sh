#!/usr/bin/env bash
# THE single reader and validator of ci/builder_images.tsv (CHAOS-9064): the
# images docker/setup-buildx-action and docker/setup-qemu-action pull.
#
# Usage:  ci/builder_images.sh <owner> [tsv]
# Prints, per image, two tab-separated lines:
#   upstream <name> <repo:tag@sha256:digest, the ref to mirror FROM>
#   ghcr     <name> <ghcr.io/<owner>/<repo>@sha256:digest, the ref CI pulls>
# Fails when a row is malformed, a ref is not digest-pinned with a full 64-hex
# digest, or a ref names a registry other than Docker Hub (a ref under another
# registry needs no mirror and does not belong here).
set -euo pipefail

owner="${1:?owner required}"
tsv="${2:-ci/builder_images.tsv}"
[ -r "${tsv}" ] || { echo "::error::${tsv} is missing or unreadable" >&2; exit 1; }

seen=0
while IFS=$'\t' read -r name ref extra; do
  case "${name}" in ''|'#'*) continue ;; esac
  [ -z "${extra:-}" ] && [ -n "${ref:-}" ] || { echo "::error::${tsv}: row '${name}' needs exactly <name><TAB><ref>" >&2; exit 1; }
  printf '%s' "${ref}" | grep -qE '^[a-z0-9._-]+/[a-z0-9._/-]+:[A-Za-z0-9._-]+@sha256:[0-9a-f]{64}$' \
    || { echo "::error::${tsv}: '${ref}' must be <namespace>/<repo>:<tag>@sha256:<64 hex>" >&2; exit 1; }
  without_digest="${ref%@*}"
  repo="${without_digest%:*}"
  digest="${ref##*@}"
  printf 'upstream\t%s\t%s\n' "${name}" "${ref}"
  printf 'ghcr\t%s\tghcr.io/%s/%s@%s\n' "${name}" "${owner}" "${repo}" "${digest}"
  seen=$((seen + 1))
done < "${tsv}"
[ "${seen}" -gt 0 ] || { echo "::error::${tsv} lists no image" >&2; exit 1; }
