#!/usr/bin/env bash
# bigboy-repin-web.sh (CHAOS-7019) -- repins compose/compose.bigboy.images.yml's `web` image to
# the CI-built digest for dev-health-web's main branch HEAD. `web` is pinned by digest in the
# bigboy overlay (image: + build: !reset null), same pattern as api/query-api/go-api/
# go-api-tools -- no host build, ever. Run every cut, unconditionally: a web-only merge between
# cuts must not silently leave a stale build serving pages the backend underneath has already
# changed shape for (the CHAOS-6262 web-path proof's first attempt hit exactly this).
set -euo pipefail
# CHAOS-7022 D2895/D2886(2): same BIGBOY_ROOT root parameter as bigboy-cut.sh -- default is
# byte-identical to the hardcoded path this script always used.
R="${BIGBOY_ROOT:-/home/ubuntu/devhealth}"; OV=$R/compose/compose.bigboy.images.yml
echo "repin-web start root=$R"
[ -e "$R/compose/compose.bigboy.images.yml" ] || { echo "FAIL: BIGBOY_ROOT=$R is missing compose/compose.bigboy.images.yml -- refusing to run against a root that is not a real bigboy tree" >&2; exit 4; }

# CHAOS-7901: which web build this cut takes, chosen by the caller and never inferred when it names one.
#   WEB_REPIN unset or `head` : web main HEAD (the behaviour of every cut before this change);
#   WEB_REPIN=skip            : web is NOT repinned; the overlay keeps its current digest (a cut that wants web
#                               unchanged, e.g. when web main HEAD is a CI-only commit with no image: cut 4 waited
#                               10 minutes and failed rc=3 for exactly that);
#   WEB_REPIN=<40 hex>        : that web commit (its CI image must exist; same wait as for HEAD).
# Anything else is refused (rc 2) before any call or write. WEB_REPIN_WAIT_TRIES (default 40) and
# WEB_REPIN_WAIT_SECS (default 15) bound the wait for the image; they exist so a test does not sit 10 minutes.
WEB_REPIN=${WEB_REPIN:-head}
WAIT_TRIES=${WEB_REPIN_WAIT_TRIES:-40}; WAIT_SECS=${WEB_REPIN_WAIT_SECS:-15}
[[ $WAIT_TRIES =~ ^[0-9]+$ && $WAIT_SECS =~ ^[0-9]+$ ]] || { echo "FAIL: WEB_REPIN_WAIT_TRIES and WEB_REPIN_WAIT_SECS must be numbers"; exit 2; }
case "$WEB_REPIN" in
  skip)
    CUR_WEB_DIGEST=$(grep -ohE "dev-health-web@[a-z0-9]+:[0-9a-f]{64}" "$OV" | head -1 | cut -d@ -f2 || true)
    echo "repin-web SKIPPED by WEB_REPIN=skip: web stays at ${CUR_WEB_DIGEST:-none}"
    exit 0 ;;
  head)
    WEB_HEAD=$(gh api repos/full-chaos/dev-health-web/commits/main --jq .sha)
    [[ $WEB_HEAD =~ ^[0-9a-f]{40}$ ]] || { echo "FAIL: could not resolve dev-health-web main HEAD sha (got '$WEB_HEAD')"; exit 3; }
    WEB_SHA=$WEB_HEAD
    # CHAOS-7687: web's "Build Docker Image" workflow has a path filter, so a CI-only commit builds NO image. Take the
    # newest commit of main's history that HAS an image (one look each, no wait), then compare it with HEAD. If only paths
    # that never enter the image differ (web's .dockerignore: .git, .github, .next, node_modules, test-results, coverage,
    # *-debug.log, .DS_Store), that image IS web at HEAD and no wait is needed. If any path that enters the image differs,
    # HEAD needs its own image: fall through to the wait for HEAD's image below (rc 3 when it never appears).
    IMG_SHA=""
    mapfile -t RECENT < <(gh api "repos/full-chaos/dev-health-web/commits?sha=$WEB_HEAD&per_page=${WEB_REPIN_LOOKBACK:-30}" --jq '.[].sha')
    for c in "${RECENT[@]}"; do
      [[ $c =~ ^[0-9a-f]{40}$ ]] || { echo "FAIL: web history gave a value that is not a commit sha (got '$c')"; exit 3; }
      d=$(docker buildx imagetools inspect "ghcr.io/full-chaos/dev-health-web:sha-${c:0:7}" --format '{{json .Manifest}}' 2>/dev/null | jq -r .digest || true)
      if [[ ${d:-} == sha256:* ]]; then IMG_SHA=$c; break; fi
    done
    if [[ -n $IMG_SHA && $IMG_SHA != "$WEB_HEAD" ]]; then
      mapfile -t CHANGED < <(gh api "repos/full-chaos/dev-health-web/compare/$IMG_SHA...$WEB_HEAD" --jq '.files[].filename')
      IN_IMAGE=0
      # the compare API lists at most 300 files: a full page is read as "may carry an image path"
      [[ ${#CHANGED[@]} -ge 300 ]] && IN_IMAGE=1
      for f in "${CHANGED[@]}"; do
        [[ $f =~ ^(\.git|\.github|\.next|node_modules|test-results|coverage)(/|$) || $f =~ ^(npm-debug\.log|pnpm-debug\.log|\.DS_Store)$ ]] || IN_IMAGE=1
      done
      if [[ $IN_IMAGE = 0 ]]; then
        echo "repin-web: web main HEAD ${WEB_HEAD:0:10} has no image; newest imaged commit ${IMG_SHA:0:10} differs only in paths outside the image (${#CHANGED[@]} files)"
        WEB_SHA=$IMG_SHA
      else
        echo "repin-web: web main HEAD ${WEB_HEAD:0:10} differs from the newest imaged commit ${IMG_SHA:0:10} in a path that enters the image: waiting for HEAD's own image"
      fi
    fi ;;
  *)
    [[ $WEB_REPIN =~ ^[0-9a-f]{40}$ ]] || { echo "FAIL: WEB_REPIN must be skip, head or a 40-hex dev-health-web commit sha (got '$WEB_REPIN')"; exit 2; }
    WEB_SHA=$WEB_REPIN ;;
esac
WEB_S7=${WEB_SHA:0:7}

WEB_DIGEST=""
for i in $(seq 1 "$WAIT_TRIES"); do
  WEB_DIGEST=$(docker buildx imagetools inspect "ghcr.io/full-chaos/dev-health-web:sha-$WEB_S7" --format '{{json .Manifest}}' 2>/dev/null | jq -r .digest || true)
  [[ ${WEB_DIGEST:-} == sha256:* ]] && break
  sleep "$WAIT_SECS"
done
[[ ${WEB_DIGEST:-} == sha256:* ]] || { echo "FAIL: no CI image ghcr.io/full-chaos/dev-health-web:sha-$WEB_S7 (web commit $WEB_SHA, WEB_REPIN=$WEB_REPIN) after waiting"; exit 3; }

OLD_WEB_DIGEST=$(grep -ohE "dev-health-web@sha256:[0-9a-f]{64}" "$OV" | head -1 | cut -d@ -f2 || true)
if [[ "${OLD_WEB_DIGEST:-}" == "$WEB_DIGEST" ]]; then
  echo "web unchanged: $WEB_DIGEST (main $WEB_SHA)"
  echo "repin-web record: mode=$WEB_REPIN web_head=${WEB_HEAD:-$WEB_SHA} image_commit=$WEB_SHA digest=$WEB_DIGEST"
  exit 0
fi

cp -n "$OV" "$OV.bak-web-$WEB_S7"
sed -i -E "s#(dev-health-web@sha256:)[0-9a-f]{64}#\1${WEB_DIGEST#sha256:}#g" "$OV"
echo "web ${OLD_WEB_DIGEST:-none} -> $WEB_DIGEST (main $WEB_SHA)"
echo "repin-web record: mode=$WEB_REPIN web_head=${WEB_HEAD:-$WEB_SHA} image_commit=$WEB_SHA digest=$WEB_DIGEST"
