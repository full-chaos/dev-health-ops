#!/usr/bin/env bash
# bigboy-graphql-prove.sh <full ops sha> [--go-edge] -- CHAOS-6993: the bigboy GraphQL prove harness.
#
# The edge it proves through is the ROUTED /graphql (CHAOS-8361: the stack has no Python api,
# so there is no Python edge and no Python-reference mode): the plane-split router's internal
# hostname, which sends /graphql to query-api (the allow-list entry of the pinned deploy values
# names `service: query-api`, or ingress.queryApiPaths lists the path), the prover's Go-edge
# mode. The prover refuses, by name, if a Python plane answers there. `--go-edge` is still
# accepted and means the same.
#
# `dho goapi routing enable` refuses an operation that has no deployed_executed proof
# receipt for the RUNNING build (prod's receipts do not count on bigboy). This runs the
# same proof prod's STEP 216 runs (pod-r216.sh lineage), on the compose stack:
#
#   1. refuse unless venue-prove's tools image IS the go-api-tools image of <sha>
#      (prover_build_skew would refuse later; this names the cause first);
#   2. `dho goapi routing repoint` (provenance only, modes untouched; prove refuses on
#      rows that name another build), then `dho goapi prove` from venue-prove (on the stack's networks: edge = the router)
#      for the local org -- read-only against the org's data; it writes proof receipts
#      only. Both credentials are minted IN PROCESS by prove itself: the envelope key is
#      loaded from the mounted key file into the process env inside the container, the
#      edge-token key comes from compose substitution (compose.bigboy.prove.yml). No
#      credential is printed, written to a file, or put on argv;
#   3. `dho goapi routing enable` for each KNOWN-MISSING operation in routing-ops.txt
#      (default) or ENABLE_OPS, one at a time, no waiver (there is none).
#
# The local org id is derived at run time from Postgres (the single org of the local
# admin account, read-only) and never printed. Full prove output (it names the org) stays
# under $R/_records/bigboy-<sha8>/graphql-prove-<ts>/; only counts are echoed.
#
# Every STEP prints `STEP <name> rc=<n>`. Exit: 0 every step passed; 1 any step failed.
set -u
NEW=${1:?full ops sha}; N8=${NEW:0:8}; S7=${NEW:0:7}
case "${2:-}" in
  ""|--go-edge) EDGE_ARGS="-go-edge -edge-url http://traefik:3000/graphql"; EDGE_NOTE="the routed /graphql in Go-edge mode" ;;
  *) echo "usage: bigboy-graphql-prove.sh <full ops sha> [--go-edge]" >&2; exit 2 ;;
esac
# BIGBOY_ROOT is the running tree (compose files, _records, ops/.env): the same root parameter
# the bigboy-cut.sh family reads, with the same default.
R=${BIGBOY_ROOT:-/home/ubuntu/devhealth}; HERE=$(cd "$(dirname "$0")" && pwd)
TS=$(date -u +%Y%m%dT%H%M%SZ); OUT=$R/_records/bigboy-$N8/graphql-prove-$TS
LOCAL_ADMIN_EMAIL=${LOCAL_ADMIN_EMAIL:-admin@test.com}
KEY_ID=${PROVE_KEY_ID:-local-dev-20260906}  # D2726: the kid bigboy's query-api JWKS trusts
st() { echo "STEP $1 rc=$2 $(date -u +%T)"; }
fail=0
mkdir -p "$OUT/proof/bodies"; chmod 0755 "$OUT" "$OUT/proof" "$OUT/proof/bodies"
cd "$R"
BASE=(--env-file ops/.env -f compose.yml -f compose/compose.go.workers.yml
  -f .remember/lanes/team-lead/reconciler-sweep-override.yml -f compose/compose.bigboy.images.yml --profile venue)

# 1. prover build == candidate build
want=$(docker buildx imagetools inspect "ghcr.io/full-chaos/dev-health-go-api-tools:sha-$S7" --format '{{json .Manifest}}' 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["digest"])' 2>/dev/null)
pinned=$(awk '/^  venue-prove:/{f=1} f&&/image:/{sub(/.*@/,""); print; exit}' compose/compose.bigboy.images.yml)
if [ -n "$want" ] && [ "$want" = "$pinned" ]; then st prover-image 0; else
  st prover-image 1; echo "REFUSED: venue-prove image ${pinned:-<none>} is not go-api-tools:sha-$S7 (${want:-unresolved}) -- re-pin before proving" >&2; exit 1; fi

# 2. local org (read-only), exactly one
PROVE_ORG=$(docker compose "${BASE[@]}" exec -T postgres psql -U devhealth -d devhealth -X -q -tA -v ON_ERROR_STOP=1 \
  -v email="$LOCAL_ADMIN_EMAIL" <<'SQL' 2>/dev/null
SELECT DISTINCT m.org_id FROM memberships m JOIN users u ON u.id = m.user_id WHERE u.email = :'email';
SQL
)
if [ "$(printf '%s\n' "$PROVE_ORG" | grep -c .)" != 1 ]; then st local-org 1; echo "REFUSED: the local admin account must belong to exactly one org" >&2; exit 1; fi
st local-org 0; export PROVE_ORG PROVE_ARTIFACT_DIR="$OUT/proof"

catalog=$OUT/catalog.json
gh api "repos/full-chaos/dev-health-ops/contents/contracts/graphql/v1/go_api_operations.json?ref=$NEW" -H 'Accept: application/vnd.github.raw' > "$catalog" 2>/dev/null && chmod 644 "$catalog"
U="-registry-url http://query-api:8090/registry -buildinfo-url http://query-api:8090/buildinfo"

vt() { docker compose "${BASE[@]}" run --rm --no-deps -T -e PROVE_ORG -v "$catalog:/catalog.json:ro" venue-tools "$1"; }

# 2b. repoint every routing row at the running build (provenance only, modes untouched --
#     prod's STEP 0b). prove REFUSES while any row names another build (StaleRoutingRows).
vt "GO_API_ROUTING_BEARER=\$(dho mint envelope -org \"\$PROVE_ORG\" -key-file /keys/envelope.pem) dho goapi routing repoint $U \
    -expect-build $NEW -recorded-by bigboy-graphql-prove \
    -review-evidence 'CHAOS-6993: provenance repoint of every routing row to the running build $NEW before the bigboy prove'" > "$OUT/repoint.out" 2>&1
rc=$?; st repoint $rc; grep -E 'repointed total=' "$OUT/repoint.out"
[ $rc = 0 ] || { tail -3 "$OUT/repoint.out" | sed 's/org=[^ ]*/org=<local>/'; exit 1; }

# 3. prove. The key file is loaded into THIS process's env inside the container only.
docker compose "${BASE[@]}" -f "$HERE/compose.bigboy.prove.yml" run --rm --no-deps -T venue-prove \
  "GO_API_ENVELOPE_PRIVATE_KEY=\"\$(cat /keys/envelope.pem)\" dho goapi prove $U $EDGE_ARGS \
   -documents /app/go-api/documents.json -org \"\$PROVE_ORG\" -artifact-dir /proof/bodies -key-id $KEY_ID -candidate-build $NEW \
   -recorded-by bigboy-graphql-prove \
   -review-evidence 'CHAOS-6993: deployed_executed proof of query-api build $NEW on bigboy (compose venue) through $EDGE_NOTE, local org, read-only' \
   -report /proof/prove-report.json" > "$OUT/prove.out" 2>&1
rc=$?; st prove $rc; [ $rc = 0 ] || fail=1
grep -E 'edge_mode=|attempted=|PROVEN_GO_ONLY|terminal_state|refused ' "$OUT/prove.out" | sed 's/org=[^ ]*/org=<local>/' | head -20

# 4. enable the known-missing operations, one at a time
OPS=${ENABLE_OPS:-$(awk '!/^#/ && $2=="KNOWN-MISSING"{print $1}' "$HERE/routing-ops.txt")}
for op in $OPS; do
  vt "GO_API_ROUTING_BEARER=\$(dho mint envelope -org \"\$PROVE_ORG\" -key-file /keys/envelope.pem) dho goapi routing enable -catalog /catalog.json $U \
      -expect-build $NEW -operations $op -mode canary -recorded-by bigboy-graphql-prove \
      -review-evidence 'CHAOS-6993: $op proven by the bigboy deployed_executed receipt at build $NEW'" > "$OUT/enable-$op.out" 2>&1
  rc=$?; st "enable:$op" $rc; [ $rc = 0 ] || { fail=1; tail -3 "$OUT/enable-$op.out" | sed 's/org=[^ ]*/org=<local>/'; }
done

echo "records: $OUT"
exit $fail
