#!/usr/bin/env bash
# bigboy-cut.sh <old8> <full new ops sha>  -- the whole bigboy phase of a group cut, sequential, logs to _records/bigboy-<new8>/cut.log.
# waits for the CI-built images -> re-pin (ops images + web, CHAOS-7019) -> migrate
# -> recreate query-api/go-api/web -> the worker plane ->
# route coverage -> log and worker checks ->
# river apply -> hook check -> CH grants check -> PG grants read-back -> the web-path smoke (R460).
# Every step prints one `STEP <name> rc=<n>` line; nothing secret is printed.
# CHAOS-8361: the stack is Go-only. The Python `api` and `metrics-api` services are not in this cut:
# `api` is in a profile nothing enables (compose.bigboy.images.yml) and the metrics-api side file is out
# of the chain. The two-plane legs (the proof-token bootstrap, the pass-bigboy*.sh scripts and the REST
# two-leg proof of the record directory) ran INSIDE the Python api container and compared the Go plane
# with the Python plane; with no Python plane they have no subject and the cut does not run them.
set -u
OLD8=${1:?old8}; NEW=${2:?full sha}; N8=${NEW:0:8}; S7=${NEW:0:7}
# CHAOS-7022 D2895/D2886(2): BIGBOY_ROOT is a root PARAMETER, not a test hook -- default is
# byte-identical to the hardcoded path this script always used, and every one of this family
# (bigboy-repin.sh, bigboy-repin-web.sh) reads the SAME env var name so a caller pointing all
# three at an isolated tree gets a fully self-consistent sandbox. Exported so those sibling
# scripts, invoked below via $HERE, see the SAME resolved value rather than each independently
# falling back to the real default.
export BIGBOY_ROOT="${BIGBOY_ROOT:-/home/ubuntu/devhealth}"
R=$BIGBOY_ROOT; REC=$R/_records/bigboy-$N8
# CHAOS-7135: the tools (this family of scripts) come from BIGBOY_TOOLS_DIR, default this script's own
# directory, so a cut run from a git worktree needs no ci/bigboy symlink under BIGBOY_ROOT. The root is
# only the running tree (compose files, _records, ops/.env); the tools are code and travel with the checkout.
# Sibling scripts (this family) resolve from HERE, never a hardcoded _records path -- proves the tracked
# move actually runs, not just coexists with a synced _records copy. Exported so a sibling that reads the
# same variable resolves to the same directory.
HERE=$(cd "${BIGBOY_TOOLS_DIR:-$(dirname "$0")}" 2>/dev/null && pwd) || { echo "FAIL: BIGBOY_TOOLS_DIR=${BIGBOY_TOOLS_DIR:-} is not a directory" >&2; exit 1; }
export BIGBOY_TOOLS_DIR=$HERE
[ -f "$HERE/bigboy-repin.sh" ] || { echo "FAIL: BIGBOY_TOOLS_DIR=$HERE has no bigboy-repin.sh -- refusing to run with a directory that is not the bigboy cut tools" >&2; exit 1; }
# Fail closed (D2895): refuse a root that is not shaped like a real bigboy tree, rather than
# silently proceeding to `cd` into it and produce confusing failures many steps later.
for need in compose/compose.bigboy.images.yml _records; do
  [ -e "$R/$need" ] || { echo "FAIL: BIGBOY_ROOT=$R is missing $need -- refusing to run against a root that is not a real bigboy tree" >&2; exit 1; }
done
# CHAOS-7131: the router overlay is part of the chain. It sets web's BACKEND_URL (the plane-split router)
# and AUTH_URL; without it `up` below recreated web from the base file alone and web lost both -- REST
# calls 500'd and the auth callback/logout used the wrong origin. It stays last so its web.environment wins.
# It is a tools file, so it is named from $HERE (absolute), not from the root: the other entries are relative
# to $R, where compose runs, and a root without ci/bigboy (CHAOS-7135) would not find it.
# CHAOS-7162: dho_api_ch is declared by a host file mounted into ClickHouse's users.d (compose.bigboy.clickhouse-users.yml), so a
# ClickHouse recreate no longer loses it. The overlay requires the path; the default is the credentials directory's file.
export DHO_API_CH_USERS_XML="${DHO_API_CH_USERS_XML:-$R/.go-api-dev/dho_api_ch.xml}"
export COMPOSE_FILE=compose.yml:compose/compose.go.workers.yml:.remember/lanes/team-lead/reconciler-sweep-override.yml:compose/compose.bigboy.images.yml:$HERE/compose.bigboy.workers.yml:$HERE/compose.bigboy.clickhouse-users.yml:$HERE/compose.bigboy.billing-edge.yml:$HERE/compose.bigboy.router.yml
cd "$R"
st() { echo "STEP $1 rc=$2 $(date -u +%T)"; }
echo "cut start $(date -u +%T) new=$NEW root=$R tools=$HERE"
for i in $(seq 1 240); do
  ok=1; for img in dev-health-go-operator dev-health-go-dho dev-health-go-api-tools; do docker buildx imagetools inspect ghcr.io/full-chaos/$img:sha-$S7 >/dev/null 2>&1 || ok=0; done
  [ $ok = 1 ] && break; sleep 30
done
[ "${ok:-0}" = 1 ] || { st images-wait 1; exit 1; }; st images-ready 0
# Resolve the operator digest BEFORE any STEP reads the compose chain: compose.bigboy.workers.yml
# requires BIGBOY_OPERATOR_IMAGE, so a config read before this export fails on the missing variable
# (the SSO group cut's secret-refs STEP reported five set secrets as BLANK for exactly that reason).
BIGBOY_OPERATOR_DIGEST=$(docker buildx imagetools inspect ghcr.io/full-chaos/dev-health-go-operator:sha-$S7 --format '{{json .Manifest}}' | jq -r .digest)
case "$BIGBOY_OPERATOR_DIGEST" in sha256:*) ;; *) st operator-resolve 1; echo "FAIL: no operator digest for sha-$S7" >&2; exit 1 ;; esac
export BIGBOY_OPERATOR_IMAGE=ghcr.io/full-chaos/dev-health-go-operator@$BIGBOY_OPERATOR_DIGEST; echo "operator=$BIGBOY_OPERATOR_DIGEST" > $REC.operator.txt; st operator-resolve 0
# CHAOS-6967(b)/CHAOS-6987 (D2728) staging (Trap #421 sibling): bigboy's query-api needs the
# same ~23 GO_API_*_ENABLED env vars prod's deploy chart sets (this switch is a plain in-memory
# flag flipped once at query-api boot, never Postgres, never the routeswitch ledger), PLUS the
# same secretKeyRef-backed names (GO_API_EDGE_JWT_SECRET, LLM_PROVIDER, OPENAI_API_KEY, LLM_MODEL,
# SETTINGS_ENCRYPTION_KEY) -- D2728 found the SAME class of drift silently left GO_API_EDGE_JWT_SECRET
# unset entirely, so a real user's JWT was rejected outright. Both classes are GENERATED from a
# deploy checkout's own values.prod.yaml (never hand-copied) and diffed by NAME against the
# checked-in overlay so a real values change fails this STEP loudly instead of silently
# re-breaking a cut. A second check confirms the LIVE, substituted config has no blank secret-ref
# value (an empty GO_API_EDGE_JWT_SECRET/etc must never pass silently -- D2728's own first apply
# went out blank because compose's default .env auto-load doesn't reach ops/.env; --env-file
# ops/.env is mandatory on every invocation below and this STEP asserts it actually took effect).
# R467: the deploy values are a PINNED input per cut -- DEPLOY_SHA (recorded in round.env and in
# $REC.deploy-sha.txt), read from DEPLOY_REPO (default $R/deploy) with `git show`, never a working
# tree and never "deploy main". generate-plane-split-router.py --deploy-sha REFUSES (rc=3) when that
# deploy commit's ops vendor pin is not $NEW, the build this cut runs; every values-driven STEP
# below reads the file it wrote ($REC.values.prod.yaml).
DEPLOY_REPO=${DEPLOY_REPO:-$R/deploy}
VALUES=""
if [ -n "${DEPLOY_SHA:-}" ]; then
  if python3 "$HERE/generate-plane-split-router.py" --deploy-repo "$DEPLOY_REPO" --deploy-sha "$DEPLOY_SHA" \
       --expect-ops-sha "$NEW" --values-out "$REC.values.prod.yaml" --format dynamic > "$REC.planes.generated" 2>"$REC.deploy-pin.err"; then
    VALUES="$REC.values.prod.yaml"; st deploy-pin 0; grep '^# deploy_sha=' "$REC.deploy-pin.err" | tee "$REC.deploy-sha.txt"
  else
    st deploy-pin 1; cat "$REC.deploy-pin.err" >&2
  fi
else
  st deploy-pin 2; echo "SKIPPED (not checked): set DEPLOY_SHA=<deploy commit whose vendor pin is $NEW> (R467)" >&2
fi
if [ -n "$VALUES" ]; then
  python3 "$HERE/generate-query-api-enabled-flags.py" "$VALUES" --out "$REC.query-api-enabled-flags.generated" 2>"$REC.query-api-enabled-flags.err"
  GEN_NAMES=$(awk -F: '{print $1}' "$REC.query-api-enabled-flags.generated" | tr -d ' ' | sort)
  LIVE_NAMES=$(awk -F: '/^      [A-Za-z_]+:/{print $1}' "$R"/compose/compose.bigboy.images.yml | tr -d ' ' | sort -u)
  MISSING=$(comm -23 <(echo "$GEN_NAMES") <(echo "$LIVE_NAMES"))
  if [ -z "$MISSING" ]; then
    st query-api-enabled-flags-current 0
  else
    st query-api-enabled-flags-current 1
    echo "DRIFT: compose/compose.bigboy.images.yml's query-api block is missing name(s) values.prod.yaml's ops.queryApi.extraEnv now has -- regenerate it (see $HERE/generate-query-api-enabled-flags.py) before trusting this cut: $MISSING" >&2
  fi
  # Fail loud on any blank substitution (D2728 class): every secret-ref name from the generated
  # list must resolve to a non-empty value in the LIVE, --env-file-substituted config.
  # CHAOS-6987 (team-lead, hard rule): `docker compose config` output never reaches a pipe, a
  # file, or a screen except through ONE redacting filter (compose-config-redacted.sh) -- it
  # emits NAME=<length> only, never a resolved value, even for a length-only check like this one.
  REDACTED=$($HERE/compose-config-redacted.sh --env-file ops/.env -f compose.yml -f compose/compose.go.workers.yml -f .remember/lanes/team-lead/reconciler-sweep-override.yml -f compose/compose.bigboy.images.yml -f $HERE/compose.bigboy.workers.yml); RED_RC=$?
  BLANK=""
  for entry in $(awk -F': \\$\\{' '/\$\{[A-Z_]+\}$/{print $1}' "$REC.query-api-enabled-flags.generated" | tr -d ' '); do
    LEN=$(echo "$REDACTED" | awk -F= -v n="$entry" '$1==n{print $2}')
    [ -n "$LEN" ] && [ "$LEN" -gt 2 ] || BLANK="$BLANK $entry"
  done
  if [ "$RED_RC" -ne 0 ]; then
    st query-api-secret-refs-nonblank 1
    echo "FAIL: compose config did not resolve (rc=$RED_RC, see the missing-variable lines above) -- no secret-ref length was read; this is NOT a blank-secret verdict" >&2
  elif [ -n "$BLANK" ]; then
    st query-api-secret-refs-nonblank 1
    echo "DRIFT: these query-api secret-ref var(s) resolve BLANK through --env-file ops/.env -- an empty verification/signing key must never go live:$BLANK" >&2
  else
    st query-api-secret-refs-nonblank 0
  fi
  # CHAOS-6987 (D2724/D2735): the LIVE router must be exactly what the generator emits from this
  # values.prod.yaml -- a hand edit, or a values change not yet applied, fails here, named.
  if [ -s "$REC.planes.generated" ] && cmp -s "$REC.planes.generated" "$R/.traefik-dynamic/planes.yml"; then
    st router-current 0
  else
    st router-current 1
    echo "DRIFT: .traefik-dynamic/planes.yml differs from generate-plane-split-router.py output for DEPLOY_SHA=$DEPLOY_SHA -- regenerate it from THAT sha (traefik hot-reloads the file) before trusting this cut" >&2
  fi
  # CHAOS-6987 (D2736): web env-name parity with prod, NAMES only -- the running web container's
  # names come from container-env-names.sh (the only sanctioned container env reader, R462).
  "$HERE/container-env-names.sh" dev-health-web-1 > "$REC.web-env-names" 2>/dev/null
  python3 "$HERE/check-web-env-parity.py" "$VALUES" "$HERE/compose.bigboy.router.yml" --live-names "$REC.web-env-names"; st web-env-parity $?
else
  st query-api-enabled-flags-current 2  # SKIPPED, not a pass: no pinned values (DEPLOY_SHA) -- this STEP did not run, it did not pass
  echo "SKIPPED (not checked): the values-driven STEPs need DEPLOY_SHA (see deploy-pin above)." >&2
fi
docker pull -q $BIGBOY_OPERATOR_IMAGE > /dev/null 2>&1; st operator-pull $?   # --no-build never pulls: the digest must be local before the recreate (rev 190 first run: "No such image")
$HERE/bigboy-repin.sh $OLD8 $NEW > $REC.repin.out 2>&1; st repin $?
[ -d $REC ] || { echo "no record dir"; exit 1; }
# CHAOS-7019: `web` is a LOCAL BUILD service in the root compose.yml, unlike query-api/
# go-api which the bigboy overlay pins to CI-built ghcr.io digests -- so a web-only merge (or a
# web+ops merge landing together, like CHAOS-6262) left a stale host build silently serving
# pages the backend underneath had already changed shape for. compose.bigboy.images.yml now
# pins `web` by digest too (build: !reset null); this STEP repins it to the CI image for web's
# OWN main branch HEAD, every cut, no host build, before `up` recreates it below.
# CHAOS-7901: the caller picks the web build by the environment of THIS script: WEB_REPIN=skip (web stays as it is),
# WEB_REPIN=<40-hex web commit> (that build) or unset/`head` (web main HEAD, the default). Cut 4 waited 10 minutes and
# ended `repin-web rc=3` because web main HEAD was a CI-only commit with no image; WEB_REPIN=skip avoids that wait.
$HERE/bigboy-repin-web.sh > $REC.repin-web.out 2>&1; rc_web=$?; st repin-web $rc_web
# CHAOS-7687: a re-pin that could not resolve an image FAILS the cut here, before migrate and before any recreate
# (a web smoke on the OLD web, with no failed cut, read as a measurement that did not happen). WEB_REPIN=skip is the
# explicit way to keep the current web pin.
[ $rc_web = 0 ] || { echo "cut stops: web re-pin failed rc=$rc_web (see $REC.repin-web.out); WEB_REPIN=skip keeps the current web pin"; exit 1; }

# CHAOS-6987 (D2728): --env-file is required from here on -- docker compose's default .env
# auto-load only reads the PROJECT ROOT .env, never ops/.env, so a compose-level ${VAR}
# substitution referencing a var that ONLY lives in ops/.env (e.g. query-api's
# GO_API_EDGE_JWT_SECRET: ${JWT_SECRET_KEY}) silently resolves to a blank string with no
# error -- caught live tonight when a first apply went out with an empty edge-JWT secret.
docker compose --env-file ops/.env run --rm --no-deps migrate > $REC/migrate.out 2>&1; st migrate $?
# CHAOS-7162: go-api's ClickHouse login must be declared durably and be live BEFORE go-api is recreated: without
# `dho_api_ch` in system.users every ClickHouse call fails with code 516 (rev 198, after a ClickHouse recreate rebuilt
# users.d from the image). Names only; the check refuses a file ClickHouse could not read (it would exit on it).
# The credentials file is passed to the check BY PATH (arg 2), never sourced here: the password is not in this shell, in the
# check's environment, or in any child's (CHAOS-8382). The check reads it by pipe and proves dho_api_ch authenticates. Nothing
# prints it. The check needs a private TMPDIR (not /tmp), set by the cut's launcher.
"$HERE/check-dho-api-ch-user.sh" "$DHO_API_CH_USERS_XML" "${DHO_API_CH_CREDS:-$R/.go-api-dev/go-api.creds}" > $REC/ch-api-user.out 2>&1; rc_chu=$?; st ch-api-user $rc_chu
[ "$rc_chu" = 0 ] || { echo "FAIL: dho_api_ch is not usable (see $REC/ch-api-user.out); ABORTING before go-api is recreated" >&2; cat $REC/ch-api-user.out >&2; exit 1; }
# CHAOS-8361: no `api` in this list (the Python api is retired). `--no-deps` skips nothing these three need:
# the one dependency the cut owns, `migrate`, ran on the line above, and the bigboy chain declares no
# envelope-keys-init service (query-api and the venue one-offs read the key files of .go-api-dev).
docker compose --env-file ops/.env up -d --no-deps --no-build query-api go-api web > $REC/up.out 2>&1; st up $?   # CHAOS-7019: web recreated from its repinned digest here too, never a host build
# CHAOS-7131: the recreated web must still carry the names the router overlay sets (names only, never values).
"$HERE/container-env-names.sh" dev-health-web-1 > "$REC.web-env-names-after-up" 2>/dev/null
"$HERE/check-web-env-required.sh" "$REC.web-env-names-after-up" BACKEND_URL AUTH_URL; st web-env-after-up $?
docker compose --env-file ops/.env up -d --no-deps --no-build go-worker go-worker-heavy go-worker-ops go-scheduler go-reconciler go-stream-ingest go-stream-external go-stream-pagerduty > $REC/up-workers.out 2>&1; rcw=$?; st up-workers $rcw; [ $rcw = 0 ] || { echo "ABORT: worker plane not recreated = INCOMPLETE pass (Trap #420)"; exit 1; }   # Trap #420: every plane from the cut's CI digests
sleep 45; curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8093/ready | grep -q 200; st go-api-ready $?
# R467: every path the router sends to a Go plane has a handler in the build just started (405/401 to a
# ROUTEPROBE request; 404 = values ahead of the build). Runs only with pinned values.
if [ -n "$VALUES" ]; then python3 "$HERE/check-route-coverage.py" "$VALUES"; st route-coverage $?; else st route-coverage 2; fi
# CHAOS-8543: the routing-ledger parity STEPs with prod (routing-enable, routing-parity; CHAOS-6987
# gap 5) are gone from the cut. They enabled every operation of ci/bigboy/routing-ops.txt that had no
# reachable routing ROW, then required a reachable row for each. Since the catalog rule query-api
# serves a catalog operation that has NO routing row: no row has to be enabled, and the check read a
# served operation with no row as missing. What this cut serves is measured by the web-path smoke
# below. The catalog is still fetched at THIS cut's sha: the web-path smoke reads it
# (DHO_SMOKE_CATALOG_FILE).
gh api "repos/full-chaos/dev-health-ops/contents/contracts/graphql/v1/go_api_operations.json?ref=$NEW" -H 'Accept: application/vnd.github.raw' > "$REC.catalog.json" 2>/dev/null && chmod 644 "$REC.catalog.json"
$HERE/bigboy-log-checks.sh $REC > $REC/log-checks.out 2>&1; st log-checks $?; tail -6 $REC/log-checks.out
$HERE/bigboy-6889-checks.sh $REC > $REC/6889-checks.out 2>&1; st worker-checks $?; tail -4 $REC/6889-checks.out | cut -c1-200
$HERE/bigboy-river-apply.sh > $REC/river-apply.out 2>&1; st river-apply $?
$HERE/bigboy-hook-check.sh $REC > $REC/hook-check.out 2>&1; st hook-check $?
$HERE/bigboy-grants-check.sh $REC > $REC/grants-check.out 2>&1; st ch-grants-check $?; tail -1 $REC/grants-check.out
$HERE/cut-contents.sh $OLD8 $NEW $REC/pg-expect.txt > $REC/contents.txt 2>&1; st cut-contents $?; cat $REC/contents.txt
[ "$(grep -vc "^#" $REC/pg-expect.txt)" -gt 0 ] && { $HERE/pg-grants-check.sh bigboy $REC/pg-expect.txt > $REC/pg-grants.out 2>&1; st pg-grants-readback $?; tail -4 $REC/pg-grants.out; } || echo "STEP pg-grants-readback SKIPPED (no PG grant deltas in the cut)"
# CHAOS-6987/R460: the web-path smoke -- the only proof this cut serves the real org
# through a real browser session, not a hand-minted token. Fails loud (rc=1) while
# DHO_SMOKE_ADMIN_EMAIL/DHO_SMOKE_ADMIN_PASSWORD_FILE are absent from ops/.env; that
# is a NAMED gap, not silently skipped, because the pass this STEP checks is the one
# chris actually hit as a P1 (R460: "plane-only passes missed 2 structural breaks").
export DHO_SMOKE_RECEIPT_PATH="/receipts/bigboy-$N8/web-path-smoke-receipt.json"
export DHO_SMOKE_CATALOG_FILE="$REC.catalog.json"
mkdir -p $REC
bash $HERE/web-path-smoke.sh > $REC/web-path-smoke.out 2>&1; st web-path-smoke $?; tail -6 $REC/web-path-smoke.out
echo "cut done $(date -u +%T)"
