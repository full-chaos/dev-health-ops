#!/usr/bin/env bash
# Rehearsal of the auth signing-key rotation on bigboy docker compose: planned rotation (S1), emergency compromise
# (S2) and issuer rollback (S3), against the REAL auth-service, auth-migrate and go-api images. Not a CI job: the lead
# runs it on bigboy, in a fresh private compose project that the script removes on exit. Never prod, never kind.
#
#   ci/rehearse_auth_key_rotation.sh --print-plan        no docker: print the plan and the image variables it needs
#   ci/rehearse_auth_key_rotation.sh --stage smoke       stack, one login, JWKS and a go-api call (minutes)
#   ci/rehearse_auth_key_rotation.sh --stage drill       smoke, then the key-compromise drill: a forged old-kid token for a live
#                                                         session, timed per verifier from the decision, and the issuer rollback (about 30 min)
#   ci/rehearse_auth_key_rotation.sh --stage rotation    S1 + S2 + S3 with the REAL floors (5 min publish, 70 min retire)
#
# Environment (no value is printed):
#   REHEARSAL_DIR         a new, empty directory (0700): keys, TLS, records, evidence
#   REHEARSAL_IMAGES_ENV  a file with DEV_HEALTH_GO_{OPERATOR,DHO,AUTH_MIGRATE,AUTH_SERVICE}_IMAGE=repo@sha256:<64 hex>
#   REHEARSAL_PREVIOUS_IMAGES_ENV  a file with DEV_HEALTH_GO_{DHO,AUTH_SERVICE}_IMAGE=repo@sha256:<64 hex> of the PREVIOUS release: the
#                         rollback in S3 and in the smoke stage is a roll back of auth-service, go-api and query-api to these images
#   REHEARSAL_RUN_ID      optional: the CI run id the digests come from (recorded)
#
# Every cell prints a literal "PASS: <name>" or "FAIL: <name>"; the first FAIL stops the run. One record file per
# scenario is written under $REHEARSAL_DIR/records (noclobber, never edited). Tokens, keys and DSNs are never printed,
# never put on argv and never written to a record: kids, sids' presence, status codes, seconds and counts only.
set -euo pipefail
umask 077
case $- in
  *x*) printf 'refused: xtrace is on\n' >&2; exit 2 ;;
esac

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
ROOT="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd -P)"
# shellcheck source=ci/lib/auth_rotation_probe.sh
source "${SCRIPT_DIR}/lib/auth_rotation_probe.sh"
# shellcheck source=ci/lib/registry_retry.sh
source "${SCRIPT_DIR}/lib/registry_retry.sh"
# shellcheck source=ci/lib/auth_workload_session_issue.sh
source "${SCRIPT_DIR}/lib/auth_workload_session_issue.sh"

readonly PG_IMAGE="postgres:18-alpine@sha256:9a8afca54e7861fd90fab5fdf4c42477a6b1cb7d293595148e674e0a3181de15"
readonly DIGEST_RE='^[a-z0-9]+([._-][a-z0-9]+)*(:[0-9]+)?(/[a-z0-9]+([._-][a-z0-9]+)*)+@sha256:[0-9a-f]{64}$'
readonly ISSUER_HOST="auth-issuer.rehearsal.test"
readonly ISSUER_URL="https://${ISSUER_HOST}:8443"
# internal/auth/keyrotation: PendingFloor = JWKS cache TTL; RetiringFloor = access ceiling + skew + cache TTL.
readonly PENDING_FLOOR=300
readonly KEY_STATE_REFRESH=15
readonly RETIRING_FLOOR=4200
# dev-health-go authverify JWKSClient: CacheTTL 5m, RefreshFloor 30s.
readonly VERIFIER_CACHE_BOUND=330
# auth-service checks a token against its own key set: key states are read again every 15 s, so a dropped kid is refused
# within the 20 s of the key-compromise objective (docs/reference/auth/readiness-and-slo.md).
readonly AUTH_SERVICE_BOUND=20
readonly KEYSET_IN_VOLUME=/run/auth/keys/keyset.json

die() {
  printf 'FAIL: %s\n' "$1" >&2
  exit 1
}

STAGE="rotation"
PRINT_PLAN=0
while [ "$#" -gt 0 ]; do
  case "$1" in
    --print-plan) PRINT_PLAN=1; shift ;;
    --stage) STAGE="${2:-}"; shift 2 ;;
    *) die "usage: ci/rehearse_auth_key_rotation.sh [--print-plan] [--stage smoke|drill|rotation]" ;;
  esac
done
case "${STAGE}" in smoke | drill | rotation) ;; *) die "--stage is smoke, drill or rotation" ;; esac

if [ "${PRINT_PLAN}" = 1 ]; then
  printf '%s\n' \
    'stage smoke:    up (postgres, clickhouse, valkey, migrate chain, auth-service, go-api, issuer-tls); register + verify one user;' \
    '                login; token header kid is a; JWKS (https, rehearsal CA) lists a; go-api /api/v1/auth/me accepts the token.' \
    'S1 planned:     adopt; stage b; register b (pending, published); activate b refused before 300 s with the remaining seconds,' \
    '                allowed after; overlap: a-token still accepted by go-api and served by query-api, b-token accepted by both,' \
    '                the probe asks both every 0.5 s: no go-api non-2xx, no query-api 401/000/5xx; rollback a (a active), rollback b' \
    '                (b active); retire a refused before 4200 s, allowed after; a leaves the JWKS; the b-token after it on both.' \
    'S2 compromise:  stage c and d, register both (published 300 s+); sessions under b; compromise b -successor c: b leaves the JWKS' \
    '                and is denied, c signs, the sessions of b are revoked (refresh refused), a session of a and the session of c still' \
    '                refresh; every verb on b refused; a manifest that names b active changes nothing.' \
    '                Window a (no restart): literal WINDOW_A lines, seconds go-api and query-api keep accepting a b-token (bound 330 s).' \
    '                Window b: compromise c -successor d, then restart go-api and query-api one at a time right after the key' \
    '                drop: literal WINDOW_B lines; a verifier that still accepts the old kid fails the run.' \
    'S3 rollback:    auth-service, go-api and query-api recreated on the previous image (no class flip): new tokens are EdDSA under the active kid, go-api accepts them, the' \
    '                compromised kid stays out of the JWKS and denied, the revoked sessions stay refused, a b-token is refused;' \
    '                back to eddsa: c signs, b is still absent.' \
    'stage drill:    smoke; register b c d; activate b and log in (a live session under b), roll back to a; S2 window a on key a' \
    '                (successor c), S2 window b on key c (successor d), S3 on c; no S1; about 30 minutes. Both compromises add:' \
    '                D0 the decision time t0 is taken before the dry run; decision->apply and apply->refusal are recorded apart.' \
    '                D1 a forged token: the claims of the live b session signed with the compromised key (header kid = that key).' \
    '                Before apply go-api, query-api and auth-service accept it (control); after apply each verifier is asked every' \
    '                5 s or less with the forged and the genuine token of that session: the first 401 with the genuine token still' \
    '                accepted is a kid refusal, at most 330 s from t0 (JWKS cache 5 min + 30 s refresh floor). A refusal while the' \
    '                genuine token is refused too is the session revoke, not the kid, and fails the cell.' \
    '                D2 auth-service (POST /api/v1/auth/validate) refuses the forged token at most 20 s from t0.' \
    '                D3 (window a) restart auth-service alone on a manifest that names the compromised key active: the JWKS' \
    '                lacks it, a new login is not signed by it, its state stays compromised, the forged token stays refused.' \
    '                D4 (window b) right after its restart each verifier refuses the forged token and accepts the genuine one.' \
    'floors:         real, once (about 80 minutes); no state_since is edited.' \
    'needs:          docker, curl, jq, openssl; REHEARSAL_DIR, REHEARSAL_IMAGES_ENV, REHEARSAL_PREVIOUS_IMAGES_ENV'
  exit 0
fi

: "${REHEARSAL_DIR:?set REHEARSAL_DIR to a new directory}"
: "${REHEARSAL_IMAGES_ENV:?set REHEARSAL_IMAGES_ENV to the image digest file}"
valid_run_id "${REHEARSAL_RUN_ID:-}" || die "REHEARSAL_RUN_ID must be digits only (the CI run id)"
[ -f "${REHEARSAL_IMAGES_ENV}" ] || die "REHEARSAL_IMAGES_ENV names no file"
for tool in docker curl jq openssl base64; do
  command -v "${tool}" >/dev/null 2>&1 || die "${tool} is required"
done
if [ -e "${REHEARSAL_DIR}" ] && [ -n "$(ls -A "${REHEARSAL_DIR}" 2>/dev/null)" ]; then
  die "${REHEARSAL_DIR} exists and is not empty"
fi
mkdir -p "${REHEARSAL_DIR}"
chmod 0700 "${REHEARSAL_DIR}"
DIR="$(cd -- "${REHEARSAL_DIR}" && pwd -P)"
export REHEARSAL_DIR="${DIR}"
mkdir -p "${DIR}/tls" "${DIR}/keys" "${DIR}/stage" "${DIR}/records" "${DIR}/evidence"

# The image file is read for four names only; each must be a digest reference.
for name in DEV_HEALTH_GO_OPERATOR_IMAGE DEV_HEALTH_GO_DHO_IMAGE DEV_HEALTH_GO_AUTH_MIGRATE_IMAGE DEV_HEALTH_GO_AUTH_SERVICE_IMAGE; do
  value="$(sed -n "s/^${name}=//p" "${REHEARSAL_IMAGES_ENV}" | tail -n 1)"
  [[ "${value}" =~ ${DIGEST_RE} ]] || die "${name} in ${REHEARSAL_IMAGES_ENV} is not a registry digest reference"
  export "${name}=${value}"
done

: "${REHEARSAL_PREVIOUS_IMAGES_ENV:?set REHEARSAL_PREVIOUS_IMAGES_ENV to the digest file of the previous release}"
[ -f "${REHEARSAL_PREVIOUS_IMAGES_ENV}" ] || die "REHEARSAL_PREVIOUS_IMAGES_ENV names no file"
for name in DHO AUTH_SERVICE; do
  value="$(sed -n "s/^DEV_HEALTH_GO_${name}_IMAGE=//p" "${REHEARSAL_PREVIOUS_IMAGES_ENV}" | tail -n 1)"
  [[ "${value}" =~ ${DIGEST_RE} ]] || die "DEV_HEALTH_GO_${name}_IMAGE in ${REHEARSAL_PREVIOUS_IMAGES_ENV} is not a registry digest reference"
  export "PREVIOUS_${name}_IMAGE=${value}"
done
CURRENT_DHO_IMAGE="${DEV_HEALTH_GO_DHO_IMAGE}"
CURRENT_AUTH_SERVICE_IMAGE="${DEV_HEALTH_GO_AUTH_SERVICE_IMAGE}"

PROJECT="authrot$$-$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n')"
KEYS_VOL="${PROJECT}_rehearsal_keys"
refuse_if_present() {
  local kind="$1" found
  shift
  found="$("$@")"
  [ -z "${found}" ] || die "a ${kind} of the compose project ${PROJECT} exists; refusing to recreate or remove it"
}
refuse_if_present container docker ps -aq --no-trunc --filter "label=com.docker.compose.project=${PROJECT}"
refuse_if_present volume docker volume ls -q --filter "name=^${PROJECT}_"
refuse_if_present network docker network ls -q --filter "name=^${PROJECT}_"

# Secrets of this run: made here, held in this shell's variables and in 0600 files under ${DIR}; never printed.
export BUGSINK_SECRET_KEY="${BUGSINK_SECRET_KEY:-auth-rotation-rehearsal}"
export COMMIT="${COMMIT:-0000000000000000000000000000000000000000}"
POSTGRES_PASSWORD="$(od -An -N12 -tx1 /dev/urandom | tr -d ' \n')"
AUTH_RUNTIME_DATABASE_PASSWORD="$(od -An -N12 -tx1 /dev/urandom | tr -d ' \n')"
AUTH_SESSION_SUCCESSOR_KEY="$(head -c 32 /dev/urandom | base64 | tr '+/' '-_' | tr -d '=\n')"
AUTH_LOGIN_LOCKOUT_KEY="$(head -c 32 /dev/urandom | base64 | tr '+/' '-_' | tr -d '=\n')"
export POSTGRES_PASSWORD AUTH_RUNTIME_DATABASE_PASSWORD AUTH_SESSION_SUCCESSOR_KEY AUTH_LOGIN_LOCKOUT_KEY
# go-api's workload token carries the rehearsal issuer: auth-service signs under it (the override) and go-api checks it.
export AUTH_TOKEN_ISSUER_URL="${ISSUER_URL}"
# go-api mail settings come from the webproof override; mail is not delivered in this rehearsal (the user is verified in SQL).
export AUTH_PROOF_SMTP_PORT=2525
export REHEARSAL_ISSUER_URL="${ISSUER_URL}"
export REHEARSAL_ACCESS_CLASS="eddsa"
export REHEARSAL_VERIFY_ISSUER="${ISSUER_URL}"

compose() {
  docker compose -p "${PROJECT}" -f "${ROOT}/compose.yml" \
    -f "${SCRIPT_DIR}/auth_compose_proof.override.yml" \
    -f "${SCRIPT_DIR}/auth_webproof_compose.override.yml" \
    -f "${SCRIPT_DIR}/auth_rotation_rehearsal.override.yml" "$@"
}

PROBE_PID=""
cleanup() {
  local status=$?
  [ -z "${PROBE_PID}" ] || kill "${PROBE_PID}" >/dev/null 2>&1 || true
  if [ "${status}" -ne 0 ]; then
    printf '\n--- services at failure ---\n' >&2
    compose --profile auth ps --all >&2 || true
    compose --profile auth logs --no-color --tail 60 auth-service go-api query-api issuer-tls >&2 || true
  fi
  compose --profile auth down --volumes --remove-orphans >/dev/null 2>&1 || true
  docker volume rm "${KEYS_VOL}" >/dev/null 2>&1 || true
  printf 'CLEANUP project %s: containers %s, volumes %s, networks %s\n' "${PROJECT}" \
    "$(docker ps -aq --filter "label=com.docker.compose.project=${PROJECT}" | grep -c . || true)" \
    "$(docker volume ls -q --filter "name=^${PROJECT}_" | grep -c . || true)" \
    "$(docker network ls -q --filter "name=^${PROJECT}_" | grep -c . || true)"
  shred -u "${DIR}"/keys/*.pem "${DIR}"/tls/issuer.key 2>/dev/null || true
  exit "${status}"
}
trap cleanup EXIT

say() { printf '%s\n' "$*"; }
elapsed_since() { printf '%d' $(($(date +%s) - $1)); }

check() {
  local name="$1"
  shift
  if "$@"; then
    rec_pass "${name}"
  else
    rec_fail "${name}"
    exit 1
  fi
}

# ---------- environment record ----------
rec_open "${DIR}/records/00-environment.txt" "environment $(date -u +%Y-%m-%dT%H:%M:%SZ)"
rec_line "stage=${STAGE} project=${PROJECT} ci_run=${REHEARSAL_RUN_ID:-unset}"
rec_line "images_env_sha256=$(sha256sum "${REHEARSAL_IMAGES_ENV}" | cut -c1-64)"
rec_line "previous_images_env_sha256=$(sha256sum "${REHEARSAL_PREVIOUS_IMAGES_ENV}" | cut -c1-64)"
rec_line "previous_dho_digest=${PREVIOUS_DHO_IMAGE##*@} previous_auth_service_digest=${PREVIOUS_AUTH_SERVICE_IMAGE##*@}"
for name in DEV_HEALTH_GO_OPERATOR_IMAGE DEV_HEALTH_GO_DHO_IMAGE DEV_HEALTH_GO_AUTH_MIGRATE_IMAGE DEV_HEALTH_GO_AUTH_SERVICE_IMAGE; do
  value="${!name}"
  rec_line "${name}=${value##*@}"
done
rec_line "pending_floor_s=${PENDING_FLOOR} retiring_floor_s=${RETIRING_FLOOR} verifier_cache_bound_s=${VERIFIER_CACHE_BOUND}"

say "step 0: pull the images by digest"
for name in DEV_HEALTH_GO_OPERATOR_IMAGE DEV_HEALTH_GO_DHO_IMAGE DEV_HEALTH_GO_AUTH_MIGRATE_IMAGE DEV_HEALTH_GO_AUTH_SERVICE_IMAGE PREVIOUS_DHO_IMAGE PREVIOUS_AUTH_SERVICE_IMAGE; do
  registry_retry_run "pull-${name}" -- docker pull --quiet "${!name}" >/dev/null
done

# ---------- keys and TLS ----------
say "step 1: keys a b c, the issuer certificate, the key-set manifests"
for kid in a b c d; do
  openssl genpkey -algorithm ed25519 -out "${DIR}/keys/${kid}.pem" 2>/dev/null
done
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 2 -subj "/CN=${ISSUER_HOST}" \
  -addext "subjectAltName=DNS:${ISSUER_HOST}" -keyout "${DIR}/tls/issuer.key" -out "${DIR}/tls/issuer.pem" 2>/dev/null
# The certificate and its key are read by the nginx user (101) through a bind mount: a throwaway pair of this run.
chmod 0644 "${DIR}/tls/issuer.key" "${DIR}/tls/issuer.pem"
cp "${SCRIPT_DIR}/auth_rotation_rehearsal.nginx.conf" "${DIR}/tls/nginx.conf"
chmod 0644 "${DIR}/tls/nginx.conf"

write_manifest() { # write_manifest KID:STATE...  -> ${DIR}/stage/keyset.json
  local entries="" pair
  for pair in "$@"; do
    entries="${entries:+${entries},}{\"kid\":\"${pair%%:*}\",\"state\":\"${pair##*:}\",\"file\":\"${pair%%:*}.pem\"}"
  done
  printf '{"keys":[%s]}\n' "${entries}" >"${DIR}/stage/keyset.json.new"
  mv -f "${DIR}/stage/keyset.json.new" "${DIR}/stage/keyset.json"
}

# stage_keys FILE...: copy files from ${DIR}/stage into the keys volume as uid 65532, mode 0400; the manifest by rename.
stage_keys() {
  local f
  for f in "$@"; do
    if [ "${f}" = keyset.json ]; then continue; fi
    cp "${DIR}/keys/${f}" "${DIR}/stage/${f}"
  done
  docker run --rm --network none -u 0 -v "${KEYS_VOL}:/k" -v "${DIR}/stage:/src:ro" "${PG_IMAGE}" sh -c '
    set -e
    for f in "$@"; do
      if [ "$f" = keyset.json ]; then continue; fi
      cp "/src/$f" "/k/$f"; chown 65532:65532 "/k/$f"; chmod 0400 "/k/$f"
    done
    cp /src/keyset.json /k/.keyset.staged; chown 65532:65532 /k/.keyset.staged; chmod 0400 /k/.keyset.staged
    mv -f /k/.keyset.staged /k/keyset.json
  ' sh "$@"
  rm -f "${DIR}"/stage/*.pem
}

# ---------- stack ----------
say "step 2: create the project, stage key a, bring the stack up"
compose --profile auth create --no-build --pull never go-api query-api auth-service issuer-tls >/dev/null
docker volume inspect "${KEYS_VOL}" >/dev/null 2>&1 || die "the keys volume ${KEYS_VOL} was not created by compose"
write_manifest a:active
stage_keys a.pem

# go-api presents its workload token on the session issue route: the bootstrap credential and the api principal first.
# The bootstrap pulls in auth-migrate, which grants auth.sessions to AUTH_SESSION_READER_ROLES (the override): those roles
# come from go-river-provision (go-api's chain), so it runs first.
compose --profile auth run --rm --pull never go-river-provision >/dev/null || die "go-river-provision failed"
auth_workload_session_issue compose --pull never || die "the go-api workload credential is not ready"
registry_retry_run rotation-up -- compose --profile auth up --no-build --pull never --detach --wait --wait-timeout 900 go-api query-api auth-service issuer-tls
psql_one() { compose exec -T postgres psql -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-postgres}" -At -c "$1"; }

# A recreated container gets new ephemeral host ports: read them again after every recreate.
refresh_ports() {
  API_PORT="$(compose port go-api 8000 | sed 's/.*://')"
  ISSUER_HOST_PORT="$(compose port issuer-tls 8443 | sed 's/.*://')"
  QUERY_PORT="$(compose port query-api 8090 | sed 's/.*://')"
  [[ "${API_PORT}" =~ ^[0-9]+$ && "${ISSUER_HOST_PORT}" =~ ^[0-9]+$ && "${QUERY_PORT}" =~ ^[0-9]+$ ]] || die "could not read the published ports"
  BASE="http://127.0.0.1:${API_PORT}"
}
refresh_ports
WEB_ORIGIN="${AUTH_PROOF_WEB_ORIGIN:-http://localhost:3000}"

# ---------- helpers: HTTP without a secret on argv ----------
jwks_doc() {
  curl -fsS --max-time 10 --cacert "${DIR}/tls/issuer.pem" --connect-to "${ISSUER_HOST}:8443:127.0.0.1:${ISSUER_HOST_PORT}" \
    "${ISSUER_URL}/.well-known/jwks.json"
}
discovery_doc() {
  curl -fsS --max-time 10 --cacert "${DIR}/tls/issuer.pem" --connect-to "${ISSUER_HOST}:8443:127.0.0.1:${ISSUER_HOST_PORT}" \
    "${ISSUER_URL}/.well-known/openid-configuration"
}
api_code() { # api_code TOKEN PATH -> http code (000 when nothing answered)
  local code
  code="$(printf 'Authorization: Bearer %s\n' "$1" | curl -sS -o /dev/null -w '%{http_code}' --max-time 10 -H @- "${BASE}$2" 2>/dev/null)" || true
  printf '%s' "${code:-000}"
}
query_code() { # query_code TOKEN -> http code of query-api's GET /api/v1/filters/options
  local code
  code="$(printf 'Authorization: Bearer %s\n' "$1" | curl -sS -o /dev/null -w '%{http_code}' --max-time 10 -H @- "http://127.0.0.1:${QUERY_PORT}/api/v1/filters/options" 2>/dev/null)" || true
  printf '%s' "${code:-000}"
}
post_json() { # post_json PATH JQ-FILTER: the body is built from the environment -> P_CODE P_BODY
  local out
  out="$(curl -sS --max-time 20 -H 'Content-Type: application/json' -H "Origin: ${WEB_ORIGIN}" \
    --data-binary @<(jq -nc "$2") -w '\n%{http_code}' "${BASE}$1" 2>/dev/null)" || out=$'\n000'
  P_CODE="${out##*$'\n'}"
  P_BODY="${out%$'\n'*}"
}
login_user() { # -> L_ACCESS L_REFRESH
  E="${EMAIL}" P="${PASSWORD}" post_json /api/v1/auth/login '{email:env.E,password:env.P}'
  [ "${P_CODE}" = 200 ] || return 1
  L_ACCESS="$(jq -er .access_token <<<"${P_BODY}")"
  L_REFRESH="$(jq -er .refresh_token <<<"${P_BODY}")"
}
login_or_die() { login_user || die "login answered ${P_CODE}"; }
refresh_with() { # refresh_with REFRESH_TOKEN -> R_CODE R_ACCESS
  R_TOKEN="$1" post_json /api/v1/auth/refresh '{refresh_token:env.R_TOKEN}'
  R_CODE="${P_CODE}"
  R_ACCESS=""
  if [ "${R_CODE}" = 200 ]; then R_ACCESS="$(jq -er .access_token <<<"${P_BODY}")"; fi
}

keys_cmd() { # keys_cmd ARGS... -> KOUT KERR KRC (auth-migrate keys, as the migration role)
  KRC=0
  compose --profile auth run --rm --no-deps -T -v "${KEYS_VOL}:/run/auth/keys:ro" -e "AUTH_SIGNING_KEYSET_FILE=${KEYSET_IN_VOLUME}" \
    auth-migrate keys "$@" >"${DIR}/evidence/keys.out" 2>"${DIR}/evidence/keys.err" || KRC=$?
  KOUT="$(cat "${DIR}/evidence/keys.out")"
  KERR="$(cat "${DIR}/evidence/keys.err")"
}
key_state() { keys_cmd list; [ "${KRC}" = 0 ] && keys_list_state "${KOUT}" "$1"; }
jwks_kid_set() { jwks_kids "$(jwks_doc)" | tr '\n' ' ' | sed 's/ $//'; }
wait_jwks_kids() { # wait_jwks_kids "a b" TIMEOUT_S
  local want="$1" deadline=$((SECONDS + $2))
  while [ "${SECONDS}" -lt "${deadline}" ]; do
    [ "$(jwks_kid_set 2>/dev/null || true)" = "${want}" ] && return 0
    sleep 3
  done
  return 1
}
login_signs_by() { # login_signs_by KID: wait out the key-state refresh (15 s), then one login; login is limited to 10 per 15 min
  sleep $((KEY_STATE_REFRESH + 5))
  login_user && kid_is "${L_ACCESS}" "$1"
}
probe_bg() { # probe_bg TOKEN OUTFILE SECONDS: go-api GET /api/v1/auth/me and query-api GET /api/v1/filters/options every 0.5 s
  # Both calls of a tick run at once, beside the 0.5 s wait: a tick lasts 0.5 s, or as long as the slower call.
  local tok="$1" out="$2" end=$((SECONDS + $3)) n=0 go_pid query_pid
  : >"${out}"
  while [ "${SECONDS}" -lt "${end}" ]; do
    n=$((n + 1))
    api_code "${tok}" /api/v1/auth/me >"${out}.go" &
    go_pid=$!
    query_code "${tok}" >"${out}.query" &
    query_pid=$!
    sleep 0.5
    wait "${go_pid}" "${query_pid}" || true
    printf '%d %s %s\n' "${n}" "$(cat "${out}.go")" "$(cat "${out}.query")" >>"${out}"
  done
  rm -f "${out}.go" "${out}.query"
}

# Predicates for check (they run in this shell, so they see the helpers above).
eq() { [ "$1" = "$2" ]; }
ge() { [ "$1" -ge "$2" ]; }
le() { [ "$1" -le "$2" ]; }
within() { [ "$1" -gt 0 ] && [ "$1" -le "$2" ]; }
refused_as() { [ "${KRC}" = 1 ] && [ "$(refusal_reason "${KERR}")" = "$1" ]; }
out_has() { grep -qF -- "$1" <<<"${KOUT}"; }
kid_is() { [ "$(jwt_header_kid "$1" 2>/dev/null || true)" = "$2" ]; }
alg_is() { [ "$(jwt_part "$1" 0 2>/dev/null | jq -r .alg 2>/dev/null || true)" = "$2" ]; }
verdict_is() { [ "$(status_verdict "$1")" = "$2" ]; }
me_accepts() { verdict_is "$(api_code "$1" /api/v1/auth/me)" accepted; }
me_refuses() { verdict_is "$(api_code "$1" /api/v1/auth/me)" refused; }
query_refuses() { [ "$(query_code "$1")" = 401 ]; }
query_serves() { local c; c="$(query_code "$1")"; [ "${c}" != 401 ] && [ "${c}" != 000 ] && [ "${c:0:1}" != 5 ]; }
jwks_lacks() { # a served key set without KID: an unreachable or malformed document is not "lacks"
  local doc rc=0
  doc="$(jwks_doc)" || return 1
  jwks_has_kid "${doc}" "$1" || rc=$?
  [ "${rc}" = 1 ]
}
same_session() { [ -n "$(jwt_claim "$1" sid 2>/dev/null || true)" ] && [ "$(jwt_claim "$1" sid)" = "$(jwt_claim "$2" sid 2>/dev/null || true)" ]; }
kid_is_not() { [ "$(jwt_header_kid "$1" 2>/dev/null || true)" != "$2" ]; }
validate_verdict() { # validate_verdict TOKEN -> A (valid), R (refused) or X: auth-service POST /api/v1/auth/validate, through go-api
  local out code
  out="$(T="$1" jq -nc '{token:env.T}' | curl -sS --max-time 10 -H 'Content-Type: application/json' -H "Origin: ${WEB_ORIGIN}" \
    --data-binary @- -w '\n%{http_code}' "${BASE}/api/v1/auth/validate" 2>/dev/null)" || out=$'\n000'
  code="${out##*$'\n'}"
  if [ "${code}" != 200 ]; then printf X; return 0; fi
  case "$(jq -r .valid <<<"${out%$'\n'*}" 2>/dev/null)" in true) printf A ;; false) printf R ;; *) printf X ;; esac
}
probe_verdict() { # probe_verdict go|query|validate TOKEN -> A, R or X; R is a 401 (valid false on auth-service)
  local c
  case "$1" in
    go) c="$(api_code "$2" /api/v1/auth/me)"; case "${c}" in 2??) printf A ;; 401) printf R ;; *) printf X ;; esac ;;
    query) c="$(query_code "$2")"; case "${c}" in 401) printf R ;; 000 | 5??) printf X ;; *) printf A ;; esac ;;
    validate) validate_verdict "$2" ;;
  esac
}
# forge_probe FORGED GENUINE OUT T0 UNTIL_S VERIFIERS: until UNTIL_S seconds after T0, ask auth-service and (VERIFIERS 1) go-api
# and query-api with both tokens, both at once, one line per tick in the forge probe form (ci/lib/auth_rotation_probe.sh). A tick
# is 1 s plus the call time while auth-service still accepts the forged token, then 3 s plus the call time. A column is asked two
# more ticks after its forged token's first refusal, then not again ("-"): a refusal that does not hold is seen.
forge_probe() {
  local forged="$1" genuine="$2" out="$3" t0="$4" until_s="$5" s tag line
  local -A more=([go]=3 [query]=3 [validate]=3) seen=([go]=0 [query]=0 [validate]=0)
  if [ "$6" != 1 ]; then more[go]=0; more[query]=0; fi
  : >"${out}"
  while :; do
    s="$(elapsed_since "${t0}")"
    [ "${s}" -le "${until_s}" ] || break
    for tag in go query validate; do
      if [ "${more[${tag}]}" -gt 0 ]; then
        probe_verdict "${tag}" "${forged}" >"${out}.${tag}.f" &
        probe_verdict "${tag}" "${genuine}" >"${out}.${tag}.g" &
      else
        printf -- - >"${out}.${tag}.f"
        printf -- - >"${out}.${tag}.g"
      fi
    done
    wait
    line="${s}"
    for tag in go query validate; do
      line="${line} $(cat "${out}.${tag}.f") $(cat "${out}.${tag}.g")"
      if [ "$(cat "${out}.${tag}.f")" = R ]; then seen[${tag}]=1; fi
      if [ "${seen[${tag}]}" = 1 ] && [ "${more[${tag}]}" -gt 0 ]; then more[${tag}]=$((more[${tag}] - 1)); fi
    done
    printf '%s\n' "${line}" >>"${out}"
    [ $((more[go] + more[query] + more[validate])) -gt 0 ] || break
    if [ "${seen[validate]}" = 1 ]; then sleep 3; else sleep 1; fi
  done
  rm -f "${out}".*.f "${out}".*.g
}
kid_refused_within() { # kid_refused_within FILE FORGED_FIELD GENUINE_FIELD BOUND_S
  local s
  s="$(kid_refused_at "$1" "$2" "$3")" || return 1
  [ "${s}" -le "$4" ]
}
jwks_never_lists() { # jwks_never_lists KID SECONDS: every JWKS served in the next SECONDS lacks KID, and at least one is served
  local end=$((SECONDS + $2)) served=0 doc rc
  while [ "${SECONDS}" -lt "${end}" ]; do
    if doc="$(jwks_doc 2>/dev/null)"; then
      rc=0
      jwks_has_kid "${doc}" "$1" || rc=$?
      [ "${rc}" = 1 ] || return 1
      served=$((served + 1))
    fi
    sleep 3
  done
  [ "${served}" -gt 0 ]
}
# judge_forge_probe FILE OLD VERIFIERS T_APPLY_FROM_T0: D1 per verifier (VERIFIERS 1) and D2, from the probe file.
judge_forge_probe() {
  local file="$1" old="$2" off="$4" s entry name f g bound tag
  rec_line "forge_probe ticks=$(grep -c . "${file}" || true) sha256=$(sha256sum "${file}" | cut -c1-12)"
  for entry in go-api:2:3:${VERIFIER_CACHE_BOUND}:d1 query-api:4:5:${VERIFIER_CACHE_BOUND}:d1 auth-service:6:7:${AUTH_SERVICE_BOUND}:d2; do
    IFS=: read -r name f g bound tag <<<"${entry}"
    if [ "${tag}" = d1 ] && [ "$3" != 1 ]; then continue; fi
    s="$(kid_refused_at "${file}" "${f}" "${g}" || true)"
    rec_line "$(printf '%s verifier=%s forged_kid=%s first_401_from_decision_s=%s from_apply_s=%s bound_s=%s' "${tag^^}" "${name}" "${old}" "${s:-none}" "$([ -n "${s}" ] && printf '%d' $((s - off)) || printf none)" "${bound}")"
    printf '%s %s first 401 of the forged %s-token %s s after the decision (bound %s s)\n' "${tag^^}" "${name}" "${old}" "${s:-none}" "${bound}"
    check "${tag}-${name}-refuses-the-forged-${old}-token-within-${bound}s" kid_refused_within "${file}" "${f}" "${g}" "${bound}"
  done
}
sessions_revoked_ge_1() { [[ "${KOUT}" =~ sessions\ revoked:\ ([0-9]+) ]] && [ "${BASH_REMATCH[1]}" -ge 1 ]; }

# ---------- seed one user through go-api register (forwarded to auth-service: users are owned, CHAOS-13356); the host SMTP
# sink is unreachable from the bigboy docker bridge, so the verified flag is set in the throwaway database, in auth.users
# (the authority, which the forwarded login reads) and in its public.users projection ----------
say "step 3: seed one verified user"
EMAIL="rotation-$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')@example.com"
PASSWORD="Pw-$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')"
export EMAIL PASSWORD
E="${EMAIL}" P="${PASSWORD}" post_json /api/v1/auth/register '{email:env.E,password:env.P,full_name:"Rotation Rehearsal",org_name:"Rotation Rehearsal Org"}'
case "${P_CODE}" in 200 | 201 | 202) ;; *) die "register answered ${P_CODE}" ;; esac
# auth.users is guarded (authschema 0006 audit_commit_guard): a write runs only in a transaction that names itself the
# audit transaction first. One psql -c string is one transaction.
psql_one "SELECT set_config('dev_health.audit_txid', pg_current_xact_id()::text, true) IS NOT NULL; UPDATE auth.users SET email_verified_at = clock_timestamp() WHERE email_lower = lower('${EMAIL}') RETURNING 1" | grep -qx 1 || die "could not verify the seeded user in auth.users"
psql_one "UPDATE public.users SET is_verified = true WHERE email = '${EMAIL}' RETURNING 1" | grep -qx 1 || die "could not verify the seeded user in public.users"

# ---------- smoke ----------
rec_open "${DIR}/records/10-smoke.txt" "smoke $(date -u +%Y-%m-%dT%H:%M:%SZ)"
say "step 4: smoke"
check smoke-discovery-issuer-pinned eq "$(discovery_doc | jq -r .issuer)" "${ISSUER_URL}"
keys_cmd adopt -apply
check smoke-adopt-applied eq "${KRC}" 0
keys_cmd adopt -apply
check smoke-adopt-twice-refused refused_as table_not_empty
check smoke-a-active eq "$(key_state a)" active
check smoke-jwks-lists-a wait_jwks_kids "a" 60
login_or_die
TOK_A1="${L_ACCESS}"
RT_A1="${L_REFRESH}"
check smoke-token-signed-by-a kid_is "${TOK_A1}" a
check smoke-go-api-accepts-a me_accepts "${TOK_A1}"
rec_line "jwks_cache_control=$(curl -sS -D - -o /dev/null --max-time 10 --cacert "${DIR}/tls/issuer.pem" --connect-to "${ISSUER_HOST}:8443:127.0.0.1:${ISSUER_HOST_PORT}" "${ISSUER_URL}/.well-known/jwks.json" | tr -d '\r' | grep -i '^cache-control:' | head -n 1)"

# ---------- scenarios ----------
# scenario_compromise OLD NEW OTHER_REFRESH EXPECT_SET MODE: OLD is the active key, NEW a pending key published for the floor.
# MODE cache: measure how long go-api and query-api keep accepting an OLD token with no restart (window a).
# MODE restart: right after the key drop, restart go-api and then query-api one at a time and measure the same (window b).
# FORGE_GENUINE (drill): the access token of a live session that OLD never signed for; the forge cells (D1, D2, D4) sign its
# claims with OLD's private key, so a refusal of that token is the dropped kid, not the session revoke. Empty: no forge cells.
# KEEP_ENTRIES: the other kid:state entries every manifest of the scenario keeps (custody of the keys still published).
FORGE_GENUINE=""
KEEP_ENTRIES=""
scenario_compromise() {
  local old="$1" new="$2" other="$3" expect="$4" mode="$5" sold_access sold_refresh t0 t_apply verb snew_refresh
  local genuine="${FORGE_GENUINE}" forged="" probe="${DIR}/evidence/forge-probe-${old}-${mode}"
  rec_open "${DIR}/records/30-S2-emergency-compromise-${old}-${mode}.txt" "S2 emergency compromise of ${old}, mode ${mode} $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  say "S2: compromise ${old}, successor ${new}, mode ${mode}"
  login_or_die
  sold_access="${L_ACCESS}"
  sold_refresh="${L_REFRESH}"
  check "s2-session-of-${old}-signed-by-${old}" kid_is "${sold_access}" "${old}"
  check "s2-go-api-accepts-${old}" me_accepts "${sold_access}"
  check "s2-query-api-serves-${old}" query_serves "${sold_access}"
  if [ -n "${genuine}" ]; then
    say "S2 D1 control: the claims of a live session that ${old} never signed for, signed with the ${old} key"
    forged="$(forge_token "${genuine}" "${old}" "${DIR}/keys/${old}.pem" "${DIR}/stage")" || die "could not sign the forged ${old}-token"
    rec_line "forged_token kid=${old} session_kid=$(jwt_header_kid "${genuine}") same_sid=$(same_session "${forged}" "${genuine}" && echo yes || echo no)"
    check "d1-forged-token-names-${old}" kid_is "${forged}" "${old}"
    check "d1-genuine-token-not-signed-by-${old}" kid_is_not "${genuine}" "${old}"
    check d1-forged-token-holds-the-live-session same_session "${forged}" "${genuine}"
    check d1-control-go-api-accepts-the-forged-token-before-apply eq "$(probe_verdict go "${forged}")" A
    check d1-control-query-api-serves-the-forged-token-before-apply eq "$(probe_verdict query "${forged}")" A
    check d2-control-auth-service-accepts-the-forged-token-before-apply eq "$(probe_verdict validate "${forged}")" A
  fi
  # D0: t0 is the decision to drop the key, before the dry run; every refusal is timed from it.
  t0="$(date +%s)"
  keys_cmd compromise -kid "${old}" -successor "${new}"
  check s2-dry-run-moves-old-compromised out_has "${old}: active -> compromised"
  check s2-dry-run-moves-new-active out_has "${new}: pending -> active"
  keys_cmd compromise -kid "${old}" -successor "${new}" -apply
  t_apply="$(date +%s)"
  if [ -n "${forged}" ]; then
    if [ "${mode}" = cache ]; then
      forge_probe "${forged}" "${genuine}" "${probe}" "${t0}" $((VERIFIER_CACHE_BOUND + 15)) 1 &
    else
      forge_probe "${forged}" "${genuine}" "${probe}" "${t0}" 60 0 &
    fi
    PROBE_PID=$!
  fi
  check s2-compromise-applied eq "${KRC}" 0
  rec_line "decision_to_apply_s=$((t_apply - t0))"
  rec_line "$(grep -E 'sessions revoked' <<<"${KOUT}" | head -n 1)"
  check s2-sessions-revoked-at-least-one sessions_revoked_ge_1
  check "s2-${old}-compromised" eq "$(key_state "${old}")" compromised
  check "s2-${new}-active" eq "$(key_state "${new}")" active
  check "s2-${old}-left-the-jwks" wait_jwks_kids "${expect}" 60
  rec_line "jwks_without_${old}_after_s=$(elapsed_since "${t0}")"

  if [ "${mode}" = cache ]; then
    say "S2 window a: how long do go-api and query-api accept the ${old}-token of the revoked session (no restart)"
    local go_after="" q_after=""
    while [ -z "${go_after}" ] || [ -z "${q_after}" ]; do
      [ "$(elapsed_since "${t0}")" -lt 900 ] || die "a verifier still accepts the compromised-kid token after 900 s"
      if [ -z "${go_after}" ] && me_refuses "${sold_access}"; then go_after="$(elapsed_since "${t0}")"; fi
      if [ -z "${q_after}" ] && query_refuses "${sold_access}"; then q_after="$(elapsed_since "${t0}")"; fi
      sleep 5
    done
    rec_line "WINDOW_A verifier=go-api old_kid=${old} no_restart accepted_for_s=${go_after} cache_bound_s=${VERIFIER_CACHE_BOUND}"
    rec_line "WINDOW_A verifier=query-api old_kid=${old} no_restart accepted_for_s=${q_after} cache_bound_s=${VERIFIER_CACHE_BOUND}"
    printf 'WINDOW_A go-api %s s, query-api %s s\n' "${go_after}" "${q_after}"
    check s2-go-api-refuses-old-within-the-cache-bound le "${go_after}" "${VERIFIER_CACHE_BOUND}"
    check s2-query-api-refuses-old-within-the-cache-bound le "${q_after}" "${VERIFIER_CACHE_BOUND}"
    if [ -n "${forged}" ]; then
      say "S2 D1/D2: wait for the forge probe (go-api and query-api up to $((VERIFIER_CACHE_BOUND + 15)) s after the decision)"
      wait "${PROBE_PID}" || true
      PROBE_PID=""
      judge_forge_probe "${probe}" "${old}" 1 $((t_apply - t0))
    fi
  else
    if [ -n "${forged}" ]; then
      say "S2 D2: wait for the auth-service forge probe before the restarts"
      wait "${PROBE_PID}" || true
      PROBE_PID=""
      judge_forge_probe "${probe}" "${old}" 0 $((t_apply - t0))
    fi
    say "S2 window b: restart go-api, then query-api, one at a time, right after the key drop"
    local svc started after refused last_code fv="-" gv="-" tag
    for svc in go-api query-api; do
      started="$(date +%s)"
      compose --profile auth restart "${svc}" >/dev/null
      compose --profile auth up --no-build --pull never --detach --wait --wait-timeout 300 --no-deps "${svc}" >/dev/null
      refresh_ports
      refused=0
      if [ "${svc}" = go-api ]; then last_code="$(api_code "${sold_access}" /api/v1/auth/me)"; tag=go; else last_code="$(query_code "${sold_access}")"; tag=query; fi
      [ "${last_code}" = 401 ] && refused=1
      if [ -n "${forged}" ]; then
        fv="$(probe_verdict "${tag}" "${forged}")"
        gv="$(probe_verdict "${tag}" "${genuine}")"
      fi
      after="$(elapsed_since "${t0}")"
      rec_line "WINDOW_B verifier=${svc} old_kid=${old} restart_after_drop restart_took_s=$(elapsed_since "${started}") refused_at_s=${after} refused=${refused} last_code=${last_code} forged=${fv} genuine=${gv}"
      printf 'WINDOW_B %s refused=%s at %s s after the drop (forged %s, genuine %s)\n' "${svc}" "${refused}" "${after}" "${fv}" "${gv}"
      check "s2-${svc}-refuses-old-right-after-its-restart" eq "${refused}" 1
      if [ -n "${forged}" ]; then
        check "d4-${svc}-refuses-the-forged-${old}-token-right-after-its-restart" eq "${fv}" R
        check "d4-${svc}-accepts-the-live-session-right-after-its-restart" eq "${gv}" A
      fi
    done
  fi
  COMPROMISED_ACCESS="${sold_access}"
  COMPROMISED_REFRESH="${sold_refresh}"

  check "s2-new-session-signed-by-${new}" login_signs_by "${new}"
  snew_refresh="${L_REFRESH}"
  refresh_with "${sold_refresh}"
  check "s2-session-of-${old}-refresh-refused" verdict_is "${R_CODE}" refused
  if [ -n "${other}" ]; then
    refresh_with "${other}"
    check s2-session-of-the-other-kid-not-over-revoked eq "${R_CODE}" 200
    check "s2-session-of-the-other-kid-now-signed-by-${new}" kid_is "${R_ACCESS}" "${new}"
  fi
  refresh_with "${snew_refresh}"
  check "s2-session-of-${new}-still-refreshes" eq "${R_CODE}" 200

  say "S2: compromised is terminal"
  for verb in activate retire rollback compromise; do
    keys_cmd "${verb}" -kid "${old}" -apply
    check "s2-${verb}-on-${old}-refused-terminal" refused_as compromised_terminal
  done
  keys_cmd register -kid "${old}" -apply
  check "s2-register-${old}-refused" refused_as key_registered
  if [ "${mode}" = cache ]; then
    local keep=() k
    for k in ${KEEP_ENTRIES}; do keep+=("${k}"); done
    write_manifest "${old}:active" "${new}:pending" "${keep[@]}"
    stage_keys "${old}.pem"
    say "S2 D3: restart auth-service alone (a new process latches nothing from memory) on a manifest that names ${old} active"
    compose --profile auth restart auth-service >/dev/null
    compose --profile auth up --no-build --pull never --detach --wait --wait-timeout 300 --no-deps auth-service >/dev/null
    refresh_ports
    check d3-jwks-served-after-the-auth-service-restart wait_jwks_kids "${expect}" 60
    check "d3-jwks-never-lists-${old}-after-the-restart" jwks_never_lists "${old}" 30
    login_or_die
    check "d3-new-login-signed-by-${new}-not-${old}" kid_is "${L_ACCESS}" "${new}"
    check "d3-${old}-still-compromised" eq "$(key_state "${old}")" compromised
    if [ -n "${forged}" ]; then
      check "d3-auth-service-still-refuses-the-forged-${old}-token" eq "$(probe_verdict validate "${forged}")" R
      check d3-auth-service-accepts-the-live-session eq "$(probe_verdict validate "${genuine}")" A
    fi
    write_manifest "${new}:active" "${keep[@]}"
    stage_keys "${new}.pem"
  fi
}

# roll_images previous|current: recreate auth-service, go-api and query-api on the previous or the current image. The class stays
# eddsa on the current image; the previous image is run with its own default class (the variable is set empty). The verifier
# issuer stays set on both: go-api and query-api of every release since the HS256 class retired have no other verifier and
# refuse to start without it (drill run of 2026-11-10: "AUTH_ACCESS_TOKEN_ISSUER is required").
roll_images() {
  if [ "$1" = previous ]; then
    export DEV_HEALTH_GO_DHO_IMAGE="${PREVIOUS_DHO_IMAGE}" DEV_HEALTH_GO_AUTH_SERVICE_IMAGE="${PREVIOUS_AUTH_SERVICE_IMAGE}"
    export REHEARSAL_ACCESS_CLASS=""
    export REHEARSAL_VERIFY_ISSUER="${ISSUER_URL}"
  else
    export DEV_HEALTH_GO_DHO_IMAGE="${CURRENT_DHO_IMAGE}" DEV_HEALTH_GO_AUTH_SERVICE_IMAGE="${CURRENT_AUTH_SERVICE_IMAGE}"
    export REHEARSAL_ACCESS_CLASS="eddsa"
    export REHEARSAL_VERIFY_ISSUER="${ISSUER_URL}"
  fi
  compose --profile auth up --no-build --pull never --detach --wait --wait-timeout 300 --no-deps --force-recreate auth-service go-api query-api >/dev/null
  refresh_ports
}

# scenario_issuer_rollback BADKID: BADKID is compromised; COMPROMISED_ACCESS/REFRESH are a token and refresh credential of it.
scenario_issuer_rollback() {
  local bad="$1" good="$2" pre
  rec_open "${DIR}/records/40-S3-issuer-rollback.txt" "S3 issuer rollback $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  say "S3: roll auth-service, go-api and query-api back to the previous image"
  login_or_die
  pre="${L_ACCESS}"
  check s3-good-token-accepted-before-rollback me_accepts "${pre}"
  roll_images previous
  login_or_die
  check s3-new-token-is-eddsa alg_is "${L_ACCESS}" EdDSA
  check "s3-new-token-signed-by-${good}" kid_is "${L_ACCESS}" "${good}"
  check s3-go-api-accepts-the-eddsa-token me_accepts "${L_ACCESS}"
  check s3-compromised-kid-token-refused me_refuses "${COMPROMISED_ACCESS}"
  refresh_with "${COMPROMISED_REFRESH}"
  check s3-revoked-session-still-refused verdict_is "${R_CODE}" refused
  check "s3-${bad}-still-compromised" eq "$(key_state "${bad}")" compromised
  keys_cmd rollback -kid "${bad}" -apply
  check "s3-key-rollback-of-${bad}-refused" refused_as compromised_terminal
  check "s3-jwks-has-no-${bad}" jwks_lacks "${bad}"
  say "S3: forward again: roll back to the current image, class eddsa"
  roll_images current
  login_or_die
  check "s3-forward-signed-by-${good}" kid_is "${L_ACCESS}" "${good}"
  check "s3-forward-go-api-accepts-${good}" me_accepts "${L_ACCESS}"
  check "s3-forward-${bad}-still-absent" jwks_lacks "${bad}"
  check s3-forward-compromised-kid-token-refused me_refuses "${COMPROMISED_ACCESS}"
}

wait_publish_floor() { # wait_publish_floor KID EPOCH_OF_REGISTER
  until keys_cmd activate -kid "$1" && [ "${KRC}" = 0 ]; do
    [ "$(elapsed_since "$2")" -lt $((PENDING_FLOOR + 120)) ] || die "the publish floor did not pass within 120 s of ${PENDING_FLOOR} s"
    sleep 10
  done
}

if [ "${STAGE}" = smoke ]; then
  say "smoke: roll to the previous image and forward again before any floor"
  roll_images previous
  login_or_die
  check smoke-roll-token-is-eddsa alg_is "${L_ACCESS}" EdDSA
  check smoke-roll-token-signed-by-a kid_is "${L_ACCESS}" a
  check smoke-roll-go-api-accepts-eddsa me_accepts "${L_ACCESS}"
  check smoke-roll-jwks-still-served jwks_lacks b
  roll_images current
  login_or_die
  check smoke-roll-back-signed-by-a kid_is "${L_ACCESS}" a
  say "smoke done"
  exit 0
fi

if [ "${STAGE}" = drill ]; then
  rec_open "${DIR}/records/25-drill-setup.txt" "drill setup $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  say "drill: register b c d, wait the publish floor, activate b and log in (a live session under b), roll back to a"
  write_manifest a:active b:pending c:pending d:pending
  stage_keys b.pem c.pem d.pem
  for kid in b c d; do
    keys_cmd register -kid "${kid}" -apply
    check "drill-register-${kid}-applied" eq "${KRC}" 0
  done
  B_REGISTER_EPOCH="$(date +%s)"
  wait_publish_floor b "${B_REGISTER_EPOCH}"
  keys_cmd activate -kid b -apply
  check drill-activate-b-applied eq "${KRC}" 0
  # The session the forged tokens copy: signed by b only, so neither compromise below revokes it (signing_kids {b}).
  check drill-session-signed-by-b login_signs_by b
  FORGE_GENUINE="${L_ACCESS}"
  FORGE_REFRESH="${L_REFRESH}"
  check drill-go-api-accepts-the-b-session me_accepts "${FORGE_GENUINE}"
  check drill-query-api-serves-the-b-session query_serves "${FORGE_GENUINE}"
  keys_cmd rollback -kid a -apply
  check drill-rollback-a-applied eq "${KRC}" 0
  check drill-a-active-again eq "$(key_state a)" active
  check drill-b-retiring eq "$(key_state b)" retiring
  # a signs again after one key-state refresh; the next login (S2) checks it.
  sleep $((KEY_STATE_REFRESH + 5))
  KEEP_ENTRIES="b:retiring d:pending"
  scenario_compromise a c "" "b c d" cache
  KEEP_ENTRIES="b:retiring"
  # The b session is refreshed only here, after its last forge cell: a refresh would make c one of its signers.
  scenario_compromise c d "${FORGE_REFRESH}" "b d" restart
  FORGE_GENUINE=""
  KEEP_ENTRIES=""
  scenario_issuer_rollback c d
  say "records:"
  for f in "${DIR}"/records/*.txt; do printf '%s  %s\n' "$(sha256sum "${f}" | cut -c1-64)" "${f##*/}"; done
  say "drill done"
  exit 0
fi

# ---------- S1 planned rotation ----------
rec_open "${DIR}/records/20-S1-planned-rotation.txt" "S1 planned rotation $(date -u +%Y-%m-%dT%H:%M:%SZ)"
say "S1: publish b (pending), floor, activate, overlap"
login_or_die
TOK_A2="${L_ACCESS}"
write_manifest a:active b:pending
stage_keys b.pem
keys_cmd register -kid b
check s1-register-dry-run-lists-b-pending out_has "b: (new) -> pending"
keys_cmd register -kid b -apply
check s1-register-b-applied eq "${KRC}" 0
REGISTER_EPOCH="$(date +%s)"
check s1-b-pending eq "$(key_state b)" pending
check s1-b-published-before-it-signs wait_jwks_kids "a b" 60
keys_cmd activate -kid b
check s1-activate-refused-inside-floor refused_as floor_not_reached
REMAIN="$(floor_remaining_seconds "${KERR}")"
rec_line "activate_floor_remaining_s=${REMAIN}"
check s1-floor-remaining-within-publish-floor within "${REMAIN}" "${PENDING_FLOOR}"
keys_cmd activate -kid b -apply
check s1-activate-apply-refused-inside-floor refused_as floor_not_reached
check s1-refused-activate-left-a-active eq "$(key_state a)" active
check s1-refused-activate-left-b-pending eq "$(key_state b)" pending
login_or_die
check s1-a-still-signs kid_is "${L_ACCESS}" a

say "S1: wait for the publish floor (real clock)"
wait_publish_floor b "${REGISTER_EPOCH}"
PUBLISH_WAIT="$(elapsed_since "${REGISTER_EPOCH}")"
rec_line "publish_floor_passed_after_s=${PUBLISH_WAIT}"
check s1-activate-not-before-the-floor ge "${PUBLISH_WAIT}" $((PENDING_FLOOR - 5))

say "S1: activate b with a probe on the a-token across the change"
probe_bg "${TOK_A2}" "${DIR}/evidence/probe-a-overlap" 90 &
PROBE_PID=$!
sleep 15
keys_cmd activate -kid b -apply
check s1-activate-b-applied eq "${KRC}" 0
check s1-b-active eq "$(key_state b)" active
check s1-a-retiring eq "$(key_state a)" retiring
check s1-new-token-signed-by-b login_signs_by b
check s1-go-api-accepts-b me_accepts "${L_ACCESS}"
check s1-a-token-still-accepted-in-overlap me_accepts "${TOK_A2}"
check s1-query-api-serves-b query_serves "${L_ACCESS}"
check s1-query-api-serves-the-a-token-in-overlap query_serves "${TOK_A2}"
check s1-a-and-b-both-published wait_jwks_kids "a b" 60
wait "${PROBE_PID}" || true
PROBE_PID=""
rec_line "overlap_probe: $(probe_summary "${DIR}/evidence/probe-a-overlap")"
check s1-no-downtime-for-the-a-token probe_no_downtime "${DIR}/evidence/probe-a-overlap"

say "S1: rollback inside the overlap and forward again"
keys_cmd rollback -kid a -apply
check s1-rollback-a-applied eq "${KRC}" 0
check s1-after-rollback-a-active eq "$(key_state a)" active
check s1-after-rollback-b-retiring eq "$(key_state b)" retiring
check s1-after-rollback-a-signs login_signs_by a
keys_cmd rollback -kid b -apply
check s1-rollback-b-applied eq "${KRC}" 0
SWITCH_EPOCH="$(date +%s)"
check s1-after-forward-b-active eq "$(key_state b)" active
check s1-after-forward-a-retiring eq "$(key_state a)" retiring
check s1-forward-b-signs login_signs_by b
write_manifest a:retiring b:active
stage_keys a.pem

keys_cmd retire -kid a
check s1-retire-refused-inside-floor refused_as floor_not_reached
RETIRE_REMAIN="$(floor_remaining_seconds "${KERR}")"
rec_line "retire_floor_remaining_s=${RETIRE_REMAIN}"
check s1-retire-remaining-within-retire-floor within "${RETIRE_REMAIN}" "${RETIRING_FLOOR}"

# S2 needs c and d published for the publish floor before the compromises: register them now, inside the retire wait.
say "S2 prep: stage c and d and register them (published during the retire wait)"
write_manifest a:retiring b:active c:pending d:pending
stage_keys c.pem d.pem
keys_cmd register -kid c -apply
check s2-prep-register-c-applied eq "${KRC}" 0
keys_cmd register -kid d -apply
check s2-prep-register-d-applied eq "${KRC}" 0
C_REGISTER_EPOCH="$(date +%s)"

say "S1: wait for the retire floor (real clock, about 70 minutes)"
until keys_cmd retire -kid a && [ "${KRC}" = 0 ]; do
  waited="$(elapsed_since "${SWITCH_EPOCH}")"
  [ "${waited}" -lt $((RETIRING_FLOOR + 300)) ] || die "the retire floor did not pass within 300 s of ${RETIRING_FLOOR} s"
  if [ $((waited % 600)) -lt 40 ]; then
    if ! { login_user && me_accepts "${L_ACCESS}"; }; then die "the stack stopped serving during the wait"; fi
    say "  waited ${waited} s; published: $(jwks_kid_set)"
  fi
  sleep 30
done
RETIRE_WAIT="$(elapsed_since "${SWITCH_EPOCH}")"
rec_line "retire_floor_passed_after_s=${RETIRE_WAIT}"
check s1-retire-not-before-the-floor ge "${RETIRE_WAIT}" $((RETIRING_FLOOR - 5))
keys_cmd retire -kid a -apply
check s1-retire-a-applied eq "${KRC}" 0
check s1-a-retired eq "$(key_state a)" retired
check s1-a-left-the-jwks wait_jwks_kids "b c d" 60
rec_line "note: every token a signed has expired by the retire floor, so the a-token refusal is by expiry and by absence from the JWKS together"
keys_cmd rollback -kid a
check s1-retired-a-would-return-only-as-pending out_has "a: retired -> pending"
check s1-b-signs-after-retire login_signs_by b
check s1-go-api-accepts-b-after-retire me_accepts "${L_ACCESS}"
check s1-query-api-serves-b-after-retire query_serves "${L_ACCESS}"
write_manifest b:active c:pending d:pending
stage_keys b.pem c.pem d.pem

c_age="$(elapsed_since "${C_REGISTER_EPOCH}")"
check s2-c-published-past-the-floor ge "${c_age}" "${PENDING_FLOOR}"
KEEP_ENTRIES="d:pending"
scenario_compromise b c "${RT_A1}" "c d" cache
KEEP_ENTRIES=""
scenario_compromise c d "" "d" restart
scenario_issuer_rollback c d

say "records:"
for f in "${DIR}"/records/*.txt; do
  printf '%s  %s\n' "$(sha256sum "${f}" | cut -c1-64)" "${f##*/}"
done
say "rotation rehearsal done"
