#!/usr/bin/env bash
# bigboy-cut.sh <old8> <full new ops sha>  -- the whole bigboy phase of a group cut, sequential, logs to _records/bigboy-<new8>/cut.log.
# waits for the CI-built images -> re-pin (ops images + web, CHAOS-7019) -> pre-roll routing carry
# (CHAOS-7022, refuse-not-skip) -> migrate -> recreate api/query-api/go-api/web -> hook check ->
# CH grants check -> PG grants read-back -> post-cut routing repoint (CHAOS-7022, every cut) ->
# receipt/admin/superadmin/REST(leg1+2)/admin corpus batches. Every step prints one `STEP <name> rc=<n>` line; nothing secret is printed.
set -u
OLD8=${1:?old8}; NEW=${2:?full sha}; N8=${NEW:0:8}; S7=${NEW:0:7}; R=/home/ubuntu/devhealth; REC=$R/_records/bigboy-$N8
HERE=$(cd "$(dirname "$0")" && pwd)  # sibling scripts (this family) resolve from HERE, never a hardcoded _records path -- proves the tracked move actually runs, not just coexists with a synced _records copy
export COMPOSE_FILE=compose.yml:compose/compose.go.workers.yml:compose/compose.metrics-api.local.yml:.remember/lanes/team-lead/reconciler-sweep-override.yml:compose/compose.bigboy.images.yml:compose/compose.bigboy.workers.yml
cd $R
st() { echo "STEP $1 rc=$2 $(date -u +%T)"; }
echo "cut start $(date -u +%T) new=$NEW"
for i in $(seq 1 240); do
  ok=1; for img in dev-hops-api dev-health-go-operator dev-health-go-dho dev-health-go-api-tools; do docker buildx imagetools inspect ghcr.io/full-chaos/$img:sha-$S7 >/dev/null 2>&1 || ok=0; done
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
  LIVE_NAMES=$(awk -F: '/^      [A-Za-z_]+:/{print $1}' $R/compose/compose.bigboy.images.yml | tr -d ' ' | sort -u)
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
  REDACTED=$($HERE/compose-config-redacted.sh --env-file ops/.env -f compose.yml -f compose/compose.go.workers.yml -f compose/compose.metrics-api.local.yml -f .remember/lanes/team-lead/reconciler-sweep-override.yml -f compose/compose.bigboy.images.yml -f compose/compose.bigboy.workers.yml); RED_RC=$?
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
# CHAOS-7019: `web` is a LOCAL BUILD service in the root compose.yml, unlike api/query-api/
# go-api which the bigboy overlay pins to CI-built ghcr.io digests -- so a web-only merge (or a
# web+ops merge landing together, like CHAOS-6262) left a stale host build silently serving
# pages the backend underneath had already changed shape for. compose.bigboy.images.yml now
# pins `web` by digest too (build: !reset null); this STEP repins it to the CI image for web's
# OWN main branch HEAD, every cut, no host build, before `up` recreates it below.
$HERE/bigboy-repin-web.sh > $REC.repin-web.out 2>&1; st repin-web $?

for f in $R/_records/bigboy-$OLD8/pass-bigboy-corpus-admin7.sh; do [ -f $f ] && cp -n $f $REC/; done
# CHAOS-7022 (D2804/D2811): pre-roll routing carry, refuse-not-skip. Runs HERE -- after repin
# (compose/compose.bigboy.images.yml already names the round's NEW tools image, so venue-tools
# computes the NEW schema digest from its own embedded SDL) but BEFORE migrate/up/up-workers
# recreate api/query-api/go-api, while query-api is still the OLD, pre-roll, live process: the
# exact source `carry` is designed to read from (docs/contribute/architecture/go-api-wave-0-
# proof-infrastructure.md, "When the schema digest moves"). `carry` itself refuses (exit 2) when
# the live and target schema digests already agree -- the EXPECTED shape for an ordinary,
# non-schema-changing roll, not a failure. One documented, real exception (rev196, both
# bigboy's own re-cut and the actual prod roll, _records/bigboy-1b05473e/prod-rev196-step1.5-
# carry-lines.md's own ABORT RULE A): a "stale build" refusal means the live rows lag the
# actually-running build (no schema change involved at all) -- repoint against the same live
# process, then ONE retry of carry; only a second refusal is fatal. Any other non-zero result
# aborts the cut before migrate/up ever runs.
#
# D2828/D2829 (CHAOS-7022 r1 self-correction, CHAOS-7023/#3369 r1 P1): branching on carry's
# human-readable TEXT was wrong TWICE, in two different ways -- first a hand-typed grep that
# never matched the real Go string at all, then (once fixed to match) a second finding that
# text was never a stable contract in the first place: it differs between the CLI's own early
# preflight and the goapiproof package's differently-worded sentinel errors for the exact same
# condition, and a caller has no way to know which layer's wording it is reading. Structural
# fix: `carry -json` prints one `GOAPI_ROUTING_JSON {...}` line with a `reason` field from a
# small, closed vocabulary ("carried", "digest_unchanged", "stale_build", "refused", "error"),
# set at the Go call site that KNOWS why, not guessed from prose. Extracted with jq (already
# used elsewhere in this script, host-side, after `docker compose run` returns -- no new
# dependency), never grep on error text again.
ROUTING_ORG=${ROUTING_ORG:-67f1add8-9fcb-4272-addb-044b70c442c8}  # the disposable fixture org, never the local org
# carry's -catalog/-documents default to paths relative to CWD (the tools image's own WORKDIR,
# /app/go-api, per docker/go-api-tools.Dockerfile) -- but venue-tools' compose service overrides
# working_dir to /work (a host-mounted scratch dir, always empty), so those relative defaults can
# never resolve there. Point carry at the SAME files by their absolute, image-baked path instead
# (still the tools image's own catalog/documents -- "the image THIS binary was built from", per
# carry.go's flag help -- never a separately fetched copy). repoint has no -catalog/-documents
# flags at all, so it keeps its own, shorter arg list.
CARRY_ARGS="-registry-url http://query-api:8090/registry -buildinfo-url http://query-api:8090/buildinfo -json -catalog /app/go-api/src/dev_health_ops/api/graphql/go_api_operations.json -documents /app/go-api/documents.json"
REPOINT_ARGS="-registry-url http://query-api:8090/registry -buildinfo-url http://query-api:8090/buildinfo -json"
carry_reason() {
  # $1: the captured carry/repoint stdout+stderr blob. Prints the GOAPI_ROUTING_JSON line's
  # `reason` field, or empty if the line is missing/unparseable -- never dies (a missing/
  # malformed JSON line is itself meaningful: the caller below treats an empty reason as "not
  # a case we recognize", the same conservative default text-matching always fell back to).
  printf '%s\n' "$1" | grep '^GOAPI_ROUTING_JSON ' | sed 's/^GOAPI_ROUTING_JSON //' | jq -r '.reason // empty' 2>/dev/null
}
CARRY_OUT=$(docker compose --env-file ops/.env --profile venue run --rm --no-deps -T venue-tools \
  "GO_API_ROUTING_BEARER=\$(dho mint envelope -org $ROUTING_ORG -key-file /keys/envelope.pem) dho goapi routing carry $CARRY_ARGS -recorded-by bigboy-cut -review-evidence 'CHAOS-7022: pre-roll carry, cut $OLD8 -> $N8'" 2>&1)
CARRY_RC=$?
echo "$CARRY_OUT" > "$REC.routing-carry.out"
CARRY_REASON=$(carry_reason "$CARRY_OUT")
if [ $CARRY_RC -eq 0 ]; then
  st routing-carry 0
elif [ "$CARRY_REASON" = "digest_unchanged" ]; then
  st routing-carry 0; echo "no schema-digest change this cut -- nothing to carry"
elif [ "$CARRY_REASON" = "stale_build" ]; then
  echo "pre-roll carry: rows lag the actually-running build (no schema change) -- repointing then retrying once"
  REPOINT_OUT=$(docker compose --env-file ops/.env --profile venue run --rm --no-deps -T venue-tools \
    "GO_API_ROUTING_BEARER=\$(dho mint envelope -org $ROUTING_ORG -key-file /keys/envelope.pem) dho goapi routing repoint $REPOINT_ARGS -operations all-registered -recorded-by bigboy-cut -review-evidence 'CHAOS-7022: pre-roll repoint-before-retry, cut $OLD8 -> $N8'" 2>&1)
  REPOINT_RC=$?
  echo "$REPOINT_OUT" >> "$REC.routing-carry.out"
  if [ $REPOINT_RC -ne 0 ]; then
    st routing-carry 1
    echo "FAIL: pre-roll repoint-before-retry itself failed -- see $REC.routing-carry.out; ABORTING (CHAOS-7022)" >&2
    exit 1
  fi
  CARRY_OUT2=$(docker compose --env-file ops/.env --profile venue run --rm --no-deps -T venue-tools \
    "GO_API_ROUTING_BEARER=\$(dho mint envelope -org $ROUTING_ORG -key-file /keys/envelope.pem) dho goapi routing carry $CARRY_ARGS -recorded-by bigboy-cut -review-evidence 'CHAOS-7022: pre-roll carry retry after repoint, cut $OLD8 -> $N8'" 2>&1)
  CARRY_RC2=$?
  echo "$CARRY_OUT2" >> "$REC.routing-carry.out"
  if [ $CARRY_RC2 -eq 0 ]; then
    st routing-carry 0; echo "pre-roll carry: OK after repoint-then-retry"
  else
    st routing-carry 1
    echo "FAIL: pre-roll routing carry still refused after repoint-then-retry -- see $REC.routing-carry.out; ABORTING before migrate/up/up-workers (CHAOS-7022 refuse-not-skip)" >&2
    exit 1
  fi
else
  st routing-carry 1
  echo "FAIL: pre-roll routing carry refused for a reason other than 'no schema change' or a stale build (reason=${CARRY_REASON:-unrecognized}) -- see $REC.routing-carry.out; ABORTING before migrate/up/up-workers (CHAOS-7022 refuse-not-skip)" >&2
  exit 1
fi
# CHAOS-6987 (D2728): --env-file is required from here on -- docker compose's default .env
# auto-load only reads the PROJECT ROOT .env, never ops/.env, so a compose-level ${VAR}
# substitution referencing a var that ONLY lives in ops/.env (e.g. query-api's
# GO_API_EDGE_JWT_SECRET: ${JWT_SECRET_KEY}) silently resolves to a blank string with no
# error -- caught live tonight when a first apply went out with an empty edge-JWT secret.
docker compose --env-file ops/.env run --rm --no-deps migrate > $REC/migrate.out 2>&1; st migrate $?
docker compose --env-file ops/.env up -d --no-deps --no-build api query-api go-api web > $REC/up.out 2>&1; st up $?   # CHAOS-7019: web recreated from its repinned digest here too, never a host build
docker compose --env-file ops/.env up -d --no-deps --no-build go-worker go-worker-ops go-scheduler go-reconciler go-stream-ingest go-stream-external go-stream-pagerduty > $REC/up-workers.out 2>&1; rcw=$?; st up-workers $rcw; [ $rcw = 0 ] || { echo "ABORT: worker plane not recreated = INCOMPLETE pass (Trap #420)"; exit 1; }   # Trap #420: every plane from the cut's CI digests
sleep 45; curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8093/ready | grep -q 200; st go-api-ready $?
# R467: every path the router sends to a Go plane has a handler in the build just started (405/401 to a
# ROUTEPROBE request; 404 = values ahead of the build). Runs only with pinned values.
if [ -n "$VALUES" ]; then python3 "$HERE/check-route-coverage.py" "$VALUES"; st route-coverage $?; else st route-coverage 2; fi
# CHAOS-6987 gap 5 (D2731): GraphQL routing-ledger parity with prod. ci/bigboy/routing-ops.txt is the
# tracked list of operations enabled on prod; every listed operation bigboy's ledger lacks is enabled
# here (`dho goapi routing enable`, envelope minted INSIDE venue-tools, never leaves the container),
# then a fresh status read must match the list. KNOWN-MISSING entries (testopsRisk, CHAOS-6993: the
# enable proof gate needs a bigboy go-api-prove run) make this STEP rc=3 -- a named gap, never rc=0.
# The catalog is fetched at THIS cut's sha so status/enable classify against the deployed build.
ROUTING_ORG=${ROUTING_ORG:-67f1add8-9fcb-4272-addb-044b70c442c8}  # the disposable fixture org, never the local org
gh api "repos/full-chaos/dev-health-ops/contents/src/dev_health_ops/api/graphql/go_api_operations.json?ref=$NEW" -H 'Accept: application/vnd.github.raw' > "$REC.catalog.json" 2>/dev/null && chmod 644 "$REC.catalog.json"
vt() { docker compose --env-file ops/.env --profile venue run --rm --no-deps -T -v "$REC.catalog.json:/catalog.json:ro" venue-tools "$1"; }
ROUTING_ARGS='-catalog /catalog.json -registry-url http://query-api:8090/registry'
vt "dho goapi routing status -json $ROUTING_ARGS" > "$REC.routing-status-pre.json" 2>/dev/null
TO_ENABLE=$(python3 "$HERE/check-routing-parity.py" "$HERE/routing-ops.txt" "$REC.routing-status-pre.json" --to-enable 2>>"$REC.routing.err"); rc_te=$?
if [ $rc_te -ne 0 ]; then
  st routing-enable 1; echo "FAIL: routing status read is incomplete, see $REC.routing.err" >&2
elif [ -n "$TO_ENABLE" ]; then
  vt "GO_API_ROUTING_BEARER=\$(dho mint envelope -org $ROUTING_ORG -key-file /keys/envelope.pem) dho goapi routing enable $ROUTING_ARGS -buildinfo-url http://query-api:8090/buildinfo -expect-build $NEW -operations $TO_ENABLE -mode canary -recorded-by bigboy-cut -review-evidence 'CHAOS-6987 gap 5: prod parity from ci/bigboy/routing-ops.txt'" > "$REC.routing-enable.out" 2>&1
  st routing-enable $?; echo "enabled: $TO_ENABLE"
else
  st routing-enable 0; echo "enabled: none needed"
fi
vt "dho goapi routing status -json $ROUTING_ARGS" > "$REC.routing-status.json" 2>/dev/null
python3 "$HERE/check-routing-parity.py" "$HERE/routing-ops.txt" "$REC.routing-status.json"; st routing-parity $?
# CHAOS-7022 (D2811 addendum): repoint after EVERY cut, schema-change or not -- routing rows must
# never lag the actually-running build by more than one cut (the rev195->rev196 prod gap: rows
# still named the build from two rolls back, with no schema change involved at all). Provenance
# only -- repoint never touches mode/reachability (docs/contribute/architecture/go-api-wave-0-
# proof-infrastructure.md's "repoint" section), so it is safe unconditionally, every cut.
#
# D2823 (r1 P1 #1): `st` only PRINTS a step's rc -- it never fails the script -- so a failed
# post-cut repoint used to leave the cut reporting overall success while routing rows silently
# stayed stale. Capture the rc explicitly and abort, same shape as the up-workers guard above
# (Trap #420).
#
# D2823 (r1 P1 #2): pin with -expect-build $NEW, the SAME guard bigboy-graphql-prove.sh already
# uses on its own repoint call. Unpinned, this call accepts whatever build query-api's /buildinfo
# happens to report right now -- if the api/query-api/go-api recreate above (STEP up) left an
# OLDER query-api still serving (a partial or failed recreate), this call would silently write
# routing-repoint rows for that STALE build while the cut still reports success.
vt "GO_API_ROUTING_BEARER=\$(dho mint envelope -org $ROUTING_ORG -key-file /keys/envelope.pem) dho goapi routing repoint -registry-url http://query-api:8090/registry -buildinfo-url http://query-api:8090/buildinfo -expect-build $NEW -operations all-registered -recorded-by bigboy-cut -review-evidence 'CHAOS-7022: post-cut repoint, cut $OLD8 -> $N8'" > "$REC.routing-repoint.out" 2>&1
rc_repoint=$?
st routing-repoint "$rc_repoint"
[ "$rc_repoint" = 0 ] || { echo "ABORT: post-cut routing repoint failed or refused (-expect-build $NEW) -- see $REC.routing-repoint.out; routing rows may be stale (CHAOS-7022)" >&2; exit 1; }
# proof tokens are 12 h: RE-MINT at every cut (rev 188: an expired token silently refused 65 REST entries, runbook step 9)
for b in bootstrap-admin-proof.sh bootstrap-superadmin-proof.sh; do bash $R/_records/bigboy-1152962/$b > $REC/$b.out 2>&1; st $b $?; done
$HERE/bigboy-log-checks.sh $REC > $REC/log-checks.out 2>&1; st log-checks $?; tail -6 $REC/log-checks.out
$HERE/bigboy-6889-checks.sh $REC > $REC/6889-checks.out 2>&1; st worker-checks $?; tail -4 $REC/6889-checks.out | cut -c1-200
$HERE/bigboy-river-apply.sh > $REC/river-apply.out 2>&1; st river-apply $?
$HERE/bigboy-hook-check.sh $REC > $REC/hook-check.out 2>&1; st hook-check $?
$HERE/bigboy-grants-check.sh $REC > $REC/grants-check.out 2>&1; st ch-grants-check $?; tail -1 $REC/grants-check.out
$HERE/cut-contents.sh $OLD8 $NEW $REC/pg-expect.txt > $REC/contents.txt 2>&1; st cut-contents $?; cat $REC/contents.txt
[ "$(grep -vc "^#" $REC/pg-expect.txt)" -gt 0 ] && { $HERE/pg-grants-check.sh bigboy $REC/pg-expect.txt > $REC/pg-grants.out 2>&1; st pg-grants-readback $?; tail -4 $REC/pg-grants.out; } || echo "STEP pg-grants-readback SKIPPED (no PG grant deltas in the cut)"
cd $REC
for s in pass-bigboy.sh pass-bigboy-admin.sh pass-bigboy-superadmin.sh pass-bigboy-corpus-admin7.sh pass-bigboy-admin2.sh pass-bigboy-superadmin2.sh pass-bigboy-corpus-admin8.sh pass-bigboy-corpus-admin9.sh; do [ -f $s ] || continue; timeout 300 bash $s > out-${s%.sh}.txt 2>&1; st ${s%.sh} $?; done
timeout 900 bash run-rest-bigboy.sh > out-rest.txt 2>&1; st rest $?; grep -E "attempted=|exit_cause" out-rest.txt | head -4
for f in out-pass-bigboy.txt out-pass-bigboy-admin.txt out-pass-bigboy-superadmin.txt out-pass-bigboy-corpus-admin7.txt; do [ -f $f ] && echo "$f rows=$(awk -F' [|] ' 'NR>1' $f | wc -l) notidentical=$(awk -F' [|] ' 'NR>1 && ($4!~/True/||$5!~/True/)' $f | wc -l)"; done
cd $R
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
