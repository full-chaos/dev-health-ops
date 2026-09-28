#!/usr/bin/env bash
# bigboy re-pin (gwc-prod-ops): bigboy-repin.sh <old-full-sha-dir-suffix e.g. a97b97ed> <new-full-sha>
# Rewrites compose/compose.bigboy.images.yml digests (backup .bak-<old>) and creates _records/bigboy-<new8>/ from
# _records/bigboy-<old>/ with the tools digest swapped. No compose up/recreate here (run separately, with the
# declared COMPOSE_FILE set incl. the images overlay). No secrets touched.
set -euo pipefail
OLD8=${1:?old8}; NEW=${2:?new full sha}; N8=${NEW:0:8}; S7=${NEW:0:7}
R=/home/ubuntu/devhealth; OV=$R/compose/compose.bigboy.images.yml
# CHAOS-7014 (D2801): a mistyped OLD8 (9 chars, "bd25cd0a9" instead of "bd25cd0a") once made the
# repin's own cp of _records/bigboy-$OLD8/*.sh fail under set -euo pipefail BEFORE the sed rewrite
# below ever ran -- $OV was backed up but never actually repointed, and every downstream STEP in
# bigboy-cut.sh (migrate/up/up-workers/rest/...) still reported rc=0 against the stale, pre-cut
# images. A measurement that did not happen must FAIL, loudly: OLD8 is checked BEFORE any file is
# touched, both for shape and against the ACTUALLY-running build -- the digest compose.bigboy.
# images.yml currently pins (nothing else rewrites this file, so it is live ground truth) must be
# the same digest the registry resolves for OLD8, never trusted as a bare caller-supplied string.
case "$OLD8" in
  [0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]) ;;
  *) echo "FAIL: OLD8='$OLD8' is not exactly 8 lowercase hex characters (got ${#OLD8}) -- the exact typo class that silently no-op'd a repin once already (D2801)"; exit 4 ;;
esac
OLD_DIGEST_RESOLVED=$(docker buildx imagetools inspect "ghcr.io/full-chaos/dev-hops-api:sha-$OLD8" --format '{{json .Manifest}}' 2>/dev/null | jq -r .digest || true)
[[ ${OLD_DIGEST_RESOLVED:-} == sha256:* ]] || { echo "FAIL: OLD8=$OLD8 does not resolve to a real dev-hops-api image in the registry"; exit 4; }
OLD_DIGEST_PINNED=$(grep -ohE "dev-hops-api@sha256:[0-9a-f]{64}" "$OV" | head -1 | cut -d@ -f2)
[[ -n "$OLD_DIGEST_PINNED" ]] || { echo "FAIL: $OV has no dev-hops-api digest pinned to cross-check OLD8 against"; exit 4; }
if [[ "$OLD_DIGEST_RESOLVED" != "$OLD_DIGEST_PINNED" ]]; then
  echo "FAIL: OLD8=$OLD8 resolves to $OLD_DIGEST_RESOLVED, but $OV currently pins $OLD_DIGEST_PINNED for dev-hops-api -- OLD8 does not name the ACTUALLY-running build. Refusing rather than silently no-op'ing the repin (CHAOS-7014/D2801)."
  exit 4
fi
dig() { docker buildx imagetools inspect "ghcr.io/full-chaos/$1:sha-$S7" --format '{{json .Manifest}}' | jq -r .digest; }
declare -A O N
for i in dev-hops-api dev-health-go-dho dev-health-go-api-tools dev-health-go-operator; do
  N[$i]=$(dig $i); [[ ${N[$i]} == sha256:* ]] || { echo "no image $i"; exit 3; }
  O[$i]=$(grep -ohE "$i@sha256:[0-9a-f]{64}" $OV $R/_records/bigboy-$OLD8/*.sh 2>/dev/null | head -1 | cut -d@ -f2 || true)
  echo "$i ${O[$i]:-none} -> ${N[$i]}"
done
cp -n $OV $OV.bak-$OLD8
mkdir -p $R/_records/bigboy-$N8
cp -n $R/_records/bigboy-$OLD8/*.sh $R/_records/bigboy-$N8/
for i in "${!N[@]}"; do
  [[ -n ${O[$i]:-} ]] || continue
  sed -i "s/${O[$i]#sha256:}/${N[$i]#sha256:}/g" $OV $R/_records/bigboy-$N8/*.sh
done
# Trap #421: a per-cut script can carry a LITERAL image@sha256 pinned two or more cuts back (never touched by
# the exact-old-value sed above, since "old" here only ever means the IMMEDIATELY previous cut's digest) --
# found live 2026-09-26: run-rest-bigboy.sh's TOOLS= line stuck at rev190's tools digest through rev191 and
# rev192, so the REST prover ran a stale build and every request refused (prover_build_skew=true), while
# bigboy-cut.sh's own "STEP rest rc=0" still looked green. Force EVERY image@sha256 reference for these four
# images to the NEW digest, by IMAGE NAME, not by matching a specific old value, so a multi-cut-stale literal
# cannot survive a repin.
for i in "${!N[@]}"; do
  sed -i -E "s#($i@sha256:)[0-9a-f]{64}#\1${N[$i]#sha256:}#g" $R/_records/bigboy-$N8/*.sh
done
# STEP/B are DERIVED: run-rest-bigboy.sh reads sha.txt (Trap #328), nothing hand-typed in bigboy-rest-commands.sh
printf "%s\n" "$NEW" > $R/_records/bigboy-$N8/sha.txt
# a re-pin from a dir whose rest-commands still carry literals gets them templated:
sed -i -E "s/^STEP=.*/STEP=\${STEP:?STEP not exported}/; s/^B=[0-9a-f]{40}\$/B=\${B:?B not exported}/; s/^B_DHO=[0-9a-f]{40}\$/B_DHO=\${B:?B not exported}/" $R/_records/bigboy-$N8/bigboy-rest-commands.sh
echo "pin: $NEW" > $R/_records/bigboy-$N8/pin.md
grep -c "${N[dev-hops-api]#sha256:}" $OV
