#!/usr/bin/env bash
# bigboy-cut.sh <old8> <full new ops sha>  -- the whole bigboy phase of a group cut, sequential, logs to _records/bigboy-<new8>/cut.log.
# waits for the CI-built images -> re-pin -> migrate -> recreate api/query-api/go-api -> hook check -> CH grants check -> PG grants read-back ->
# receipt/admin/superadmin/REST(leg1+2)/admin corpus batches. Every step prints one `STEP <name> rc=<n>` line; nothing secret is printed.
set -u
OLD8=${1:?old8}; NEW=${2:?full sha}; N8=${NEW:0:8}; S7=${NEW:0:7}; R=/home/ubuntu/devhealth; REC=$R/_records/bigboy-$N8
export COMPOSE_FILE=compose.yml:compose/compose.go.workers.yml:compose/compose.metrics-api.local.yml:.remember/lanes/team-lead/reconciler-sweep-override.yml:compose/compose.bigboy.images.yml:compose/compose.bigboy.workers.yml
cd $R
st() { echo "STEP $1 rc=$2 $(date -u +%T)"; }
echo "cut start $(date -u +%T) new=$NEW"
for i in $(seq 1 240); do
  ok=1; for img in dev-hops-api dev-health-go-operator dev-health-go-dho dev-health-go-api-tools; do docker buildx imagetools inspect ghcr.io/full-chaos/$img:sha-$S7 >/dev/null 2>&1 || ok=0; done
  [ $ok = 1 ] && break; sleep 30
done
[ "${ok:-0}" = 1 ] || { st images-wait 1; exit 1; }; st images-ready 0
# CHAOS-6967(b) staging (Trap #421 sibling): bigboy's query-api needs the same ~23
# GO_API_*_ENABLED env vars prod's deploy chart sets, or a fresh cut's REST proof leg 404s
# on every correctly-pathed, correctly-bearer'd request (this switch is a plain in-memory
# flag flipped once at query-api boot, never Postgres, never the routeswitch ledger). The
# values are GENERATED from a deploy checkout's own values.prod.yaml (never hand-copied --
# a hand-copy is exactly what let this drift silent for at least two cuts), diffed against
# the checked-in overlay so a real values change fails this STEP loudly instead of silently
# re-breaking leg1. DEPLOY_CHECKOUT must point at a deploy repo working tree (e.g. the prod
# roll's own worktree) with a current values.prod.yaml.
if [ -n "${DEPLOY_CHECKOUT:-}" ] && [ -f "$DEPLOY_CHECKOUT/values.prod.yaml" ]; then
  python3 "$R/_records/generate-query-api-enabled-flags.py" "$DEPLOY_CHECKOUT/values.prod.yaml" > "$REC.query-api-enabled-flags.generated" 2>"$REC.query-api-enabled-flags.err"
  grep "_ENABLED:" $R/compose/compose.bigboy.images.yml | sort > "$REC.query-api-enabled-flags.live"
  if diff -q <(sort "$REC.query-api-enabled-flags.generated") "$REC.query-api-enabled-flags.live" >/dev/null 2>&1; then
    st query-api-enabled-flags-current 0
  else
    st query-api-enabled-flags-current 1
    echo "DRIFT: compose/compose.bigboy.images.yml's query-api GO_API_*_ENABLED block no longer matches deploy/values.prod.yaml -- regenerate it (see $R/_records/generate-query-api-enabled-flags.py) before trusting leg1's REST proof this cut." >&2
  fi
else
  st query-api-enabled-flags-current 2  # SKIPPED, not a pass: DEPLOY_CHECKOUT not given -- this STEP did not run, it did not pass
  echo "SKIPPED (not checked): set DEPLOY_CHECKOUT=<deploy repo worktree path> to verify the query-api GO_API_*_ENABLED block against the current values.prod.yaml." >&2
fi
export BIGBOY_OPERATOR_IMAGE=ghcr.io/full-chaos/dev-health-go-operator@$(docker buildx imagetools inspect ghcr.io/full-chaos/dev-health-go-operator:sha-$S7 --format '{{json .Manifest}}' | jq -r .digest); echo "operator=${BIGBOY_OPERATOR_IMAGE##*@}" > $REC.operator.txt
docker pull -q $BIGBOY_OPERATOR_IMAGE > /dev/null 2>&1; st operator-pull $?   # --no-build never pulls: the digest must be local before the recreate (rev 190 first run: "No such image")
$R/_records/bigboy-repin.sh $OLD8 $NEW > $REC.repin.out 2>&1; st repin $?
[ -d $REC ] || { echo "no record dir"; exit 1; }
for f in $R/_records/bigboy-$OLD8/pass-bigboy-corpus-admin7.sh; do [ -f $f ] && cp -n $f $REC/; done
docker compose run --rm --no-deps migrate > $REC/migrate.out 2>&1; st migrate $?
docker compose up -d --no-deps --no-build api query-api go-api > $REC/up.out 2>&1; st up $?
docker compose up -d --no-deps --no-build go-worker go-worker-ops go-scheduler go-reconciler go-stream-ingest go-stream-external go-stream-pagerduty > $REC/up-workers.out 2>&1; rcw=$?; st up-workers $rcw; [ $rcw = 0 ] || { echo "ABORT: worker plane not recreated = INCOMPLETE pass (Trap #420)"; exit 1; }   # Trap #420: every plane from the cut's CI digests
sleep 45; curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8093/ready | grep -q 200; st go-api-ready $?
# proof tokens are 12 h: RE-MINT at every cut (rev 188: an expired token silently refused 65 REST entries, runbook step 9)
for b in bootstrap-admin-proof.sh bootstrap-superadmin-proof.sh; do bash $R/_records/bigboy-1152962/$b > $REC/$b.out 2>&1; st $b $?; done
$R/_records/bigboy-log-checks.sh $REC > $REC/log-checks.out 2>&1; st log-checks $?; tail -6 $REC/log-checks.out
$R/_records/bigboy-6889-checks.sh $REC > $REC/6889-checks.out 2>&1; st worker-checks $?; tail -4 $REC/6889-checks.out | cut -c1-200
$R/_records/bigboy-river-apply.sh > $REC/river-apply.out 2>&1; st river-apply $?
$R/_records/bigboy-hook-check.sh $REC > $REC/hook-check.out 2>&1; st hook-check $?
$R/_records/bigboy-grants-check.sh $REC > $REC/grants-check.out 2>&1; st ch-grants-check $?; tail -1 $REC/grants-check.out
$R/_records/cut-contents.sh $OLD8 $NEW $REC/pg-expect.txt > $REC/contents.txt 2>&1; st cut-contents $?; cat $REC/contents.txt
[ "$(grep -vc "^#" $REC/pg-expect.txt)" -gt 0 ] && { $R/_records/pg-grants-check.sh bigboy $REC/pg-expect.txt > $REC/pg-grants.out 2>&1; st pg-grants-readback $?; tail -4 $REC/pg-grants.out; } || echo "STEP pg-grants-readback SKIPPED (no PG grant deltas in the cut)"
cd $REC
for s in pass-bigboy.sh pass-bigboy-admin.sh pass-bigboy-superadmin.sh pass-bigboy-corpus-admin7.sh pass-bigboy-admin2.sh pass-bigboy-superadmin2.sh pass-bigboy-corpus-admin8.sh pass-bigboy-corpus-admin9.sh; do [ -f $s ] || continue; timeout 300 bash $s > out-${s%.sh}.txt 2>&1; st ${s%.sh} $?; done
timeout 900 bash run-rest-bigboy.sh > out-rest.txt 2>&1; st rest $?; grep -E "attempted=|exit_cause" out-rest.txt | head -4
for f in out-pass-bigboy.txt out-pass-bigboy-admin.txt out-pass-bigboy-superadmin.txt out-pass-bigboy-corpus-admin7.txt; do [ -f $f ] && echo "$f rows=$(awk -F' [|] ' 'NR>1' $f | wc -l) notidentical=$(awk -F' [|] ' 'NR>1 && ($4!~/True/||$5!~/True/)' $f | wc -l)"; done
echo "cut done $(date -u +%T)"
