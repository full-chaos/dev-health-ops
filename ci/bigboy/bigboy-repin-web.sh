#!/usr/bin/env bash
# bigboy-repin-web.sh (CHAOS-7019) -- repins compose/compose.bigboy.images.yml's `web` image to
# the CI-built digest for dev-health-web's main branch HEAD. `web` is pinned by digest in the
# bigboy overlay (image: + build: !reset null), same pattern as api/query-api/go-api/
# go-api-tools -- no host build, ever. Run every cut, unconditionally: a web-only merge between
# cuts must not silently leave a stale build serving pages the backend underneath has already
# changed shape for (the CHAOS-6262 web-path proof's first attempt hit exactly this).
set -euo pipefail
R=/home/ubuntu/devhealth; OV=$R/compose/compose.bigboy.images.yml

WEB_SHA=$(gh api repos/full-chaos/dev-health-web/commits/main --jq .sha)
[[ $WEB_SHA =~ ^[0-9a-f]{40}$ ]] || { echo "FAIL: could not resolve dev-health-web main HEAD sha (got '$WEB_SHA')"; exit 3; }
WEB_S7=${WEB_SHA:0:7}

WEB_DIGEST=""
for i in $(seq 1 40); do
  WEB_DIGEST=$(docker buildx imagetools inspect "ghcr.io/full-chaos/dev-health-web:sha-$WEB_S7" --format '{{json .Manifest}}' 2>/dev/null | jq -r .digest || true)
  [[ ${WEB_DIGEST:-} == sha256:* ]] && break
  sleep 15
done
[[ ${WEB_DIGEST:-} == sha256:* ]] || { echo "FAIL: no CI image ghcr.io/full-chaos/dev-health-web:sha-$WEB_S7 (main $WEB_SHA) after waiting"; exit 3; }

OLD_WEB_DIGEST=$(grep -ohE "dev-health-web@sha256:[0-9a-f]{64}" "$OV" | head -1 | cut -d@ -f2 || true)
if [[ "${OLD_WEB_DIGEST:-}" == "$WEB_DIGEST" ]]; then
  echo "web unchanged: $WEB_DIGEST (main $WEB_SHA)"
  exit 0
fi

cp -n "$OV" "$OV.bak-web-$WEB_S7"
sed -i -E "s#(dev-health-web@sha256:)[0-9a-f]{64}#\1${WEB_DIGEST#sha256:}#g" "$OV"
echo "web ${OLD_WEB_DIGEST:-none} -> $WEB_DIGEST (main $WEB_SHA)"
