#!/usr/bin/env bash
# Writes testdata/session-pooler-golden.txt: the sha256 of the whole `helm template` output of the chart at
# BASE_SHA for two value sets. TestSessionRollingUpdateUnsetRendersByteEqual compares the
# current chart to it. Usage: gen-session-golden.sh <main sha>  (run from anywhere)
set -euo pipefail
sha="$(git rev-parse "$1^{commit}")"
here="$(cd "$(dirname "$0")" && pwd)"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
git archive "$sha" deploy/helm/dev-health | tar -x -C "$tmp"
cd "$tmp/deploy/helm/dev-health"
prod=(--set authService.phase=1 --set authService.database.secretName=auth-db --set authService.signingKey.secretName=auth-key --set authService.signingKey.keyId=auth-2026-10 --set authService.migrate.secretName=auth-migrate-db --set authService.migrate.secretKey=MIGRATION_URI
  --set-json 'authService.networkPolicy.egress.database.to=[{"podSelector":{"matchLabels":{"app":"auth-postgres"}}}]'
  --set goWorkers.pgbouncer.transaction.poolSize=16 --set goWorkers.pgbouncer.queueSession.poolSize=36 --set goWorkers.pgbouncer.coordinatorSession.poolSize=10)
sum() { sha256sum | cut -d' ' -f1; }
default_sum="$(helm template a . --set web.env.BACKEND_URL=http://backend.example --set web.authSecret.secretName=web-auth | sum)"
prod_sum="$(helm template a . --set web.env.BACKEND_URL=http://backend.example --set web.authSecret.secretName=web-auth "${prod[@]}" | sum)"
{
  echo "# golden-base-sha: $sha"
  echo "# generated-by: deploy/helm/dev-health/testdata/gen-session-golden.sh $sha"
  echo "default $default_sum"
  echo "prod-like $prod_sum"
} > "$here/session-pooler-golden.txt"
