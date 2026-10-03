#!/usr/bin/env bash
# Sha swap for the prepared bump branch (gwc-prod-ops). Usage: bump-swap.sh <full-ops-sha> [--apply]
# Default = DRY RUN: resolves the three CI-built manifest-list digests for the new sha, prints the
# old->new substitutions. --apply rewrites values.prod.yaml, values.local.yaml, the vendor gitlink.
# Never touches secrets. Does NOT run `git submodule update` (resets the checkout).
set -euo pipefail
NEW=${1:?full ops sha}; APPLY=${2:-}
[[ ${#NEW} -eq 40 ]] || { echo "need the 40-char sha"; exit 2; }
S7=${NEW:0:7}; S12=${NEW:0:12}
WT=/home/ubuntu/devhealth/deploy-worktrees/gwc-prod-ops-rev180
cd "$WT"
OLD=$(git -C vendor/dev-health-ops rev-parse HEAD); O7=${OLD:0:7}; O12=${OLD:0:12}
dig() { docker buildx imagetools inspect "ghcr.io/full-chaos/$1:sha-$S7" --format '{{json .Manifest}}' | jq -r .digest; }
child() { docker buildx imagetools inspect "ghcr.io/full-chaos/$1:sha-$S7" --format '{{json .Manifest}}' | jq -r '.manifests[]|select(.platform.architecture=="arm64" and .platform.os=="linux")|.digest'; }
archs() { docker buildx imagetools inspect "ghcr.io/full-chaos/$1:sha-$S7" --format '{{json .Manifest}}' | jq -r '[.manifests[]|select(.platform.os=="linux")|.platform.architecture]|sort|join(",")'; }
# CHAOS-7674: the Python api image (dev-hops-api) is no longer built. Its pin in values.prod.yaml stays FROZEN
# (WP2 removes the line); it is neither resolved nor rewritten here.
declare -A NEWD OLDD
for img in dev-health-go-operator dev-health-go-dho dev-health-go-api-tools; do
  NEWD[$img]=$(dig $img); [[ ${NEWD[$img]} == sha256:* ]] || { echo "NO IMAGE $img:sha-$S7"; exit 3; }
  a=$(archs $img); [[ $a == amd64,arm64 ]] || { echo "$img archs=$a (need amd64,arm64)"; exit 3; }
  echo "$img ${NEWD[$img]} archs=$a"
done
DHO_ARM=$(child dev-health-go-dho); echo "dho arm64 child $DHO_ARM"
# old digests are read from the current values.prod.yaml (query-api/goApi share the dho digest)
OLD_OP=$(grep -oE 'dev-health-go-operator@sha256:[0-9a-f]{64}' values.prod.yaml | head -1 | cut -d@ -f2)
OLD_DHO=$(grep -oE 'dev-health-go-dho:sha-[0-9a-f]+@sha256:[0-9a-f]{64}' values.prod.yaml | head -1 | grep -oE 'sha256:[0-9a-f]{64}')
OLD_ARM=$(grep -oE 'sha256:[0-9a-f]{64};$' values.prod.yaml | head -1 | tr -d ';' || true)
echo "old operator=$OLD_OP"; echo "old dho=$OLD_DHO"
echo "old sha $OLD -> new $NEW"
[[ $APPLY == --apply ]] || { echo "DRY RUN (add --apply)"; exit 0; }
# values.prod.yaml/values.local.yaml sha rewrite, extracted to a sibling file, not an inline
# here-document (CHAOS-3362/CHAOS-6964: body is well over the 400-byte here-document pipe budget).
HERE=$(cd "$(dirname "$0")" && pwd)
python3 "$HERE/bump-swap-rewrite-values.py" "$OLD" "$NEW" "$O7" "$S7" "$OLD_OP" "${NEWD[dev-health-go-operator]}" "$OLD_DHO" "${NEWD[dev-health-go-dho]}" "$DHO_ARM"
git -C vendor/dev-health-ops fetch -q origin && git -C vendor/dev-health-ops checkout -q "$NEW"
git update-index --cacheinfo 160000,"$NEW",vendor/dev-health-ops
echo "applied. NEXT: make verify; update bigboy overlay compose.bigboy.images.yml; commit."
