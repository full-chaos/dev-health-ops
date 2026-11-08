#!/usr/bin/env bash
# scripts/drill-refresh-reuse.sh -- refresh-family reuse attack drill on real auth-service images (CHAOS-11230).
#
# Venue: the bigboy k3s, one throwaway namespace. Never kind, never prod. No secret, token or DSN is printed: every
# generated value lives in 0600 files under DRILL_DIR and reaches the cluster as a Secret, the HTTP bodies go to curl
# on stdin, and only status codes, reason texts, counts and sha256 prefixes are printed.
#
#   KUBECONFIG=/etc/rancher/k3s/k3s.yaml DRILL_NS=w11-<id>-<n> DRILL_DIR=<new 0700 dir> \
#   AUTH_SERVICE_IMAGE=<repo@sha256:...> AUTH_MIGRATE_IMAGE=<repo@sha256:...> DHO_IMAGE=<repo@sha256:...> \
#   [PULL_SECRET_SOURCE=<namespace>/<secret>] bash scripts/drill-refresh-reuse.sh run
#
# Verbs: setup (namespace, datastores, ops + auth migrations, chart install, one seeded user), cells (the drill),
# teardown (delete the namespace), run (setup, cells, then teardown whatever the result).
# The stack is the chart's auth-service in the prod posture (scripts/drill-refresh-reuse.values.yaml). At phase 3
# go-api forwards /api/v1/auth/login and /api/v1/auth/refresh to auth-service unread, so the drill calls those
# routes on auth-service itself.
#
# Cells (session S1 is attacked, session S2 of the same user is the control):
#   C5 grace: R0 again inside the 30 s grace while its successor R1 is unused answers the same R1; nothing is revoked
#   C1 reuse: after R1 -> R2, R0 again answers 401 "Refresh token reuse detected"
#   C2 the legit holder is signed out: R2 answers 401 and S1's session access token (200 before) answers 401 on
#      GET /v1/sessions/current
#   C3 the family is dead: R1 answers 401; S1 is revoked and has 0 live refresh credentials
#   C4 audit: exactly 1 auth.session.refresh_reuse_revoked row for S1, with a request id
#   C6 S2 is not touched: its refresh answers 200
set -euo pipefail
umask 077

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
repo_root="$(cd "$here/.." && pwd -P)"
digest_re='^[a-z0-9]+([._-][a-z0-9]+)*(:[0-9]+)?(/[a-z0-9]+([._-][a-z0-9]+)*)+@sha256:[0-9a-f]{64}$'
: "${KUBECONFIG:?set KUBECONFIG explicitly}"
: "${DRILL_NS:?set DRILL_NS}"
: "${DRILL_DIR:?set DRILL_DIR (a 0700 directory)}"
LOCAL_PORT="${LOCAL_PORT:-18230}"
export KUBECONFIG
RELEASE=drill
SVC="${RELEASE}-dev-health-auth-service"
sec="$DRILL_DIR/sec"
tok="$DRILL_DIR/tokens"
EMAIL="drill-reuse@example.com"
# One id per run. The namespace is created with it as a label and the teardown deletes only a namespace that carries it.
RUN_ID="${DRILL_RUN_ID:-}"

k() { kubectl -n "$DRILL_NS" "$@"; }
fail() { echo "FAIL: $*"; exit 1; }
need_images() {
  local v
  for v in AUTH_SERVICE_IMAGE AUTH_MIGRATE_IMAGE DHO_IMAGE; do
    [[ "${!v:-}" =~ $digest_re ]] || fail "$v must be a repo@sha256:<64 hex> reference"
  done
}
rand() { head -c "$1" /dev/urandom | base64 -w0 | tr '+/' '-_' | tr -d '='; }
psql_super() { k exec -i deploy/drill-postgres -c postgres -- psql -U postgres -d postgres -v ON_ERROR_STOP=1 -qAt "$@"; }

# job NAME IMAGE ENV_SECRET ARGS... : one Job whose env is every key of ENV_SECRET (envFrom), waited to completion.
job() {
  local name="$1" image="$2" envsecret="$3"
  shift 3
  local args
  args="$(printf '%s\n' "$@" | jq -R . | jq -cs .)"
  k delete job "$name" --ignore-not-found >/dev/null
  k apply -f - >/dev/null <<JOB
apiVersion: batch/v1
kind: Job
metadata: {name: $name}
spec:
  backoffLimit: 0
  template:
    metadata: {labels: {app: drill-job}}
    spec:
      restartPolicy: Never
      imagePullSecrets: [{name: ghcr-pull}]
      securityContext: {runAsNonRoot: true, runAsUser: 65532, runAsGroup: 65532}
      containers:
        - name: job
          image: $image
          imagePullPolicy: IfNotPresent
          args: $args
          envFrom: [{secretRef: {name: $envsecret}}]
          securityContext: {allowPrivilegeEscalation: false, capabilities: {drop: [ALL]}, seccompProfile: {type: RuntimeDefault}}
JOB
  if ! k wait --for=condition=complete "job/$name" --timeout=300s >/dev/null; then
    k logs "job/$name" --tail=20 2>&1 | sed 's/^/  job log: /' || true
    fail "job $name did not complete"
  fi
  echo "job $name complete"
}

gen_secrets() {
  mkdir -p "$sec" "$tok"
  local f
  for f in pg-super pg-runtime pg-migrator; do [ -e "$sec/$f" ] || rand 24 >"$sec/$f"; done
  # The admin verb's password rule: 12+ characters with a letter and a digit.
  [ -e "$sec/user-pw" ] || printf '%sA1' "$(rand 24)" >"$sec/user-pw"
  [ -e "$sec/hs256" ] || rand 48 >"$sec/hs256"
  [ -e "$sec/successor" ] || rand 32 >"$sec/successor"
  [ -e "$sec/lockout" ] || rand 32 >"$sec/lockout"
  [ -e "$sec/signing-key.pem" ] || openssl genpkey -algorithm ed25519 -out "$sec/signing-key.pem" 2>/dev/null
  local dsn="host=drill-postgres port=5432 dbname=postgres sslmode=disable"
  printf "%s user=postgres password='%s'" "$dsn" "$(cat "$sec/pg-super")" >"$sec/dsn-super"
  printf "%s user=devhealth_auth password='%s'" "$dsn" "$(cat "$sec/pg-runtime")" >"$sec/dsn-runtime"
  printf "%s user=devhealth_auth_migrator password='%s'" "$dsn" "$(cat "$sec/pg-migrator")" >"$sec/dsn-migrator"
  printf 'postgres://postgres:%s@drill-postgres:5432/postgres?sslmode=disable' "$(cat "$sec/pg-super")" >"$sec/uri-super"
}

mk_secret() { # NAME KEY=FILE...
  local name="$1"
  shift
  local a=() kv
  for kv in "$@"; do a+=("--from-file=$kv"); done
  k create secret generic "$name" "${a[@]}" --dry-run=client -o yaml | k apply -f - >/dev/null
}

new_run_id() { RUN_ID="$(date -u +%Y%m%dt%H%M%Sz)-$$-$(head -c 6 /dev/urandom | od -An -tx1 | tr -d ' \n')"; }

# create_ns : create the namespace with this run's label in ONE call (an existing namespace is AlreadyExists, never
# changed), and record the create rc and the run id in $DRILL_DIR/ns-created only after the create succeeded.
create_ns() {
  [ -n "$RUN_ID" ] || new_run_id
  printf 'apiVersion: v1\nkind: Namespace\nmetadata:\n  name: %s\n  labels:\n    drill-run: "%s"\n' "$DRILL_NS" "$RUN_ID" |
    kubectl create -f - >/dev/null || return $?
  printf 'rc=0 run=%s\n' "$RUN_ID" >"$DRILL_DIR/ns-created"
}

setup() {
  need_images
  gen_secrets
  kubectl get ns "$DRILL_NS" >/dev/null 2>&1 && fail "namespace $DRILL_NS exists: use a fresh name"
  create_ns
  echo "ns=$DRILL_NS created (run $RUN_ID)"
  if [ -n "${PULL_SECRET_SOURCE:-}" ]; then
    # By pipe only; never printed.
    kubectl -n "${PULL_SECRET_SOURCE%%/*}" get secret "${PULL_SECRET_SOURCE#*/}" -o json |
      jq '{apiVersion,kind,type,data,metadata:{name:"ghcr-pull"}}' | k apply -f - >/dev/null
    echo "pull secret ghcr-pull copied"
  fi
  mk_secret drill-pg "password=$sec/pg-super"
  # The ops migrate runs with production's settings (pgmigrate preflight: River cutover allowed, River schema river).
  printf '1' >"$sec/one"
  printf 'river' >"$sec/river"
  mk_secret drill-super-dsn "AUTH_MIGRATION_DATABASE_URI=$sec/dsn-super" "MIGRATION_DATABASE_URI=$sec/uri-super" "POSTGRES_URI=$sec/uri-super" \
    "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=$sec/one" "RIVER_DATABASE_SCHEMA=$sec/river"
  mk_secret auth-runtime-dsn "AUTH_DATABASE_URI=$sec/dsn-runtime"
  mk_secret auth-migrate-dsn "AUTH_MIGRATION_DATABASE_URI=$sec/dsn-migrator"
  mk_secret drill-hs256 "JWT_SECRET_KEY=$sec/hs256"
  mk_secret auth-session-successor "AUTH_SESSION_SUCCESSOR_KEY=$sec/successor"
  mk_secret auth-login-lockout "AUTH_LOGIN_LOCKOUT_KEY=$sec/lockout"
  mk_secret auth-key "signing-key.pem=$sec/signing-key.pem"
  printf 'redis://drill-valkey:6379/1' >"$sec/valkey-uri"
  mk_secret auth-valkey "AUTH_VALKEY_URI=$sec/valkey-uri"
  k create secret generic auth-social --dry-run=client -o yaml | k apply -f - >/dev/null
  k create secret generic drill-ops --dry-run=client -o yaml | k apply -f - >/dev/null
  # The grants and fence one-shots connect as the migration login, with the chart hook's role env.
  printf 'auth' >"$sec/schema"
  printf 'devhealth_auth' >"$sec/runtime-role"
  printf 'devhealth_auth_migrator' >"$sec/owner-role"
  printf 'devhealth_api,devhealth_query_api' >"$sec/readers"
  # The one-shots take the posture of the chart's own auth-migrate hook Job at this release's phase 3: the chart renders
  # no AUTH_USERS_OWNED there, so the Secret carries none (CHAOS-13356: no venue sets the switch false; the chart's
  # phases go with CHAOS-13351 PR-C).
  mk_secret drill-auth-oneshot "AUTH_MIGRATION_DATABASE_URI=$sec/dsn-migrator" "AUTH_DATABASE_SCHEMA=$sec/schema" \
    "AUTH_RUNTIME_DATABASE_ROLE=$sec/runtime-role" "AUTH_SCHEMA_OWNER_ROLE=$sec/owner-role" \
    "AUTH_SESSION_READER_ROLES=$sec/readers"
  mk_secret drill-owner-grants "AUTH_MIGRATION_DATABASE_URI=$sec/dsn-super" "AUTH_DATABASE_SCHEMA=$sec/schema" \
    "AUTH_RUNTIME_DATABASE_ROLE=$sec/runtime-role"
  echo "secrets created (values in $sec, 0600)"

  k apply -f "$here/drill-refresh-reuse.stack.yaml" >/dev/null
  k rollout status deploy/drill-postgres --timeout=300s >/dev/null || fail "postgres not ready"
  k rollout status deploy/drill-valkey --timeout=300s >/dev/null || fail "valkey not ready"
  echo "postgres + valkey ready"

  # 1. ops PostgreSQL schema (public.users, organizations, memberships, audit_logs, ...), as the superuser.
  job ops-migrate "$DHO_IMAGE" drill-super-dsn migrate postgres upgrade
  # 2. The auth logins, the auth schema owned by the migration login, and the two session-reader roles
  #    (compose service auth-provision). Values reach psql on stdin, never as arguments.
  {
    printf "\\set rpw '%s'\n\\set mpw '%s'\n" "$(cat "$sec/pg-runtime")" "$(cat "$sec/pg-migrator")"
    cat <<'SQL'
SELECT format('CREATE ROLE devhealth_auth LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS', :'rpw')
  WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'devhealth_auth') \gexec
SELECT format('CREATE ROLE devhealth_auth_migrator LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS', :'mpw')
  WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'devhealth_auth_migrator') \gexec
SELECT 'CREATE SCHEMA auth AUTHORIZATION devhealth_auth_migrator' WHERE NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'auth') \gexec
SELECT format('CREATE ROLE %I NOLOGIN', r) FROM unnest(ARRAY['devhealth_api','devhealth_query_api']) r
  WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) \gexec
SQL
  } | psql_super >/dev/null
  echo "auth logins + schema provisioned"
  # 3. The migration login's grant options on the public tables (compose service auth-owner-grants).
  job owner-grants "$AUTH_MIGRATE_IMAGE" drill-owner-grants owner-move -from postgres -to devhealth_auth_migrator -grants-only -apply
  # 4. The chart: its pre-install hook Job runs auth-migrate up; the Deployment waits for the grants and the fence.
  helm install "$RELEASE" "$repo_root/deploy/helm/dev-health" --namespace "$DRILL_NS" \
    -f "$here/drill-refresh-reuse.values.yaml" \
    --set "authService.image.repository=$AUTH_SERVICE_IMAGE" \
    --set "authService.migrate.image.repository=$AUTH_MIGRATE_IMAGE" --timeout 8m >/dev/null
  echo "helm release $RELEASE installed (auth-migrate hook complete)"
  job auth-grants "$AUTH_MIGRATE_IMAGE" drill-auth-oneshot grants
  job auth-fence "$AUTH_MIGRATE_IMAGE" drill-auth-oneshot restore-fence init -if-absent
  # 5. One verified user through the operator verb (`dho admin users create`, password on stdin), then one
  #    organization and an owner membership.
  local overrides
  overrides="$(jq -cn --arg img "$DHO_IMAGE" --arg e "$EMAIL" '{spec:{imagePullSecrets:[{name:"ghcr-pull"}],
    securityContext:{runAsNonRoot:true,runAsUser:65532,runAsGroup:65532},
    containers:[{name:"seed-user",image:$img,imagePullPolicy:"IfNotPresent",stdin:true,stdinOnce:true,
      args:["admin","users","create","--email",$e,"--full-name","Drill User","--password-stdin"],
      envFrom:[{secretRef:{name:"drill-super-dsn"}}],
      securityContext:{allowPrivilegeEscalation:false,capabilities:{drop:["ALL"]},seccompProfile:{type:"RuntimeDefault"}}}]}}')"
  k run seed-user -i --rm --quiet --restart=Never --image="$DHO_IMAGE" --overrides="$overrides" <"$sec/user-pw" \
    >"$DRILL_DIR/seed-user.out" 2>&1 || { sed 's/^/  seed-user: /' "$DRILL_DIR/seed-user.out"; fail "admin users create failed"; }
  psql_super >/dev/null <<SQL
WITH o AS (INSERT INTO organizations (id, name, slug) VALUES (gen_random_uuid(), 'Drill Org', 'drill-org') RETURNING id)
INSERT INTO memberships (id, user_id, org_id, role) SELECT gen_random_uuid(), u.id, o.id, 'owner' FROM users u, o WHERE u.email = '$EMAIL';
SQL
  echo "user seeded: $(psql_super -c "SELECT count(*) FROM users u JOIN memberships m ON m.user_id = u.id WHERE u.email = '$EMAIL' AND u.is_verified AND u.is_active") verified member row"
  k rollout status "deploy/$SVC" --timeout=300s >/dev/null || {
    k logs "deploy/$SVC" --tail=30 2>&1 | sed 's/^/  auth-service log: /' || true
    fail "auth-service not ready"
  }
  k get pods -l app.kubernetes.io/component=auth-service -o json |
    jq -r '.items[].status.containerStatuses[] | "container \(.name) ready=\(.ready) image=\(.imageID)"'
}

# post PATH BODYFILE OUT : POST a JSON body from a file; prints the status. The body never reaches argv.
# peer_ok : the port-forward process is alive (not gone, not a zombie). A credential goes out only while it is.
peer_ok() {
  local st
  [ -n "${PF_PID:-}" ] && st="$(awk '{print $3}' "/proc/$PF_PID/stat" 2>/dev/null)" && [ -n "$st" ] && [ "$st" != Z ] && return 0
  echo "ABORT: port-forward process is not alive; no credential sent" >&2
  return 1
}
post() { peer_ok || return 1; curl -s -o "$3" -w '%{http_code}' --max-time 20 -H 'Content-Type: application/json' --data-binary @"$2" "http://127.0.0.1:$LOCAL_PORT$1"; }
login() { # NAME -> tokens/NAME.login.json
  jq -n --arg e "$EMAIL" --rawfile p "$sec/user-pw" '{email:$e,password:$p}' >"$tok/login.body"
  post /api/v1/auth/login "$tok/login.body" "$tok/$1.login.json"
}
refresh() { # REFRESH_FILE OUT_JSON -> status
  jq -n --rawfile r "$1" '{refresh_token:$r}' >"$tok/refresh.body"
  post /api/v1/auth/refresh "$tok/refresh.body" "$2"
}
take() { jq -je ".$2 // empty" "$1" || { echo "FAIL: no $2 in $(basename "$1")" >&2; return 1; }; }
sha12() { sha256sum "$1" | cut -c1-12; }
# Error text of a response, from a fixed set of keys only, string values only, short, plain characters. The body, and
# every other key (refresh_token, access_token, session_access_token, ...), is never printed.
safe_err() { # FILE -> one line
  jq -r '[.error?, .code?, .reason_code?, .detail?, (.detail? | objects | .message?)] | map(strings | select(length <= 80)
    | gsub("[^A-Za-z0-9 _.:,-]"; "?")) | if length == 0 then "-" else join(" | ") end' "$1" 2>/dev/null || echo "-"
}
detail() { jq -r '(.detail | if type == "object" then .message else . end) // .reason_code // "-" | if type == "string" and length <= 80 then gsub("[^A-Za-z0-9 _.:,-]"; "?") else "-" end' "$1" 2>/dev/null || echo "-"; }
current() { # ACCESS_FILE OUT -> status
  peer_ok || return 1
  curl -s -o "$2" -w '%{http_code}' --max-time 20 -H @<(printf 'Authorization: Bearer %s\n' "$(cat "$1")") "http://127.0.0.1:$LOCAL_PORT/v1/sessions/current"
}
sid_of() { # the sid claim of a JWT file (claims only; the token is not printed)
  local p
  p="$(cut -d. -f2 "$1" | tr '_-' '/+')"
  while [ $((${#p} % 4)) -ne 0 ]; do p="$p="; done
  printf '%s' "$p" | base64 -d 2>/dev/null | jq -r '.sid // "none"'
}
sid_uuid() { # ses_<base64url of the 16 uuid bytes> -> the uuid text
  local b
  b="$(printf '%s' "${1#ses_}" | tr '_-' '/+')"
  while [ $((${#b} % 4)) -ne 0 ]; do b="$b="; done
  printf '%s' "$b" | base64 -d 2>/dev/null | od -An -tx1 | tr -d ' \n' |
    sed -E 's/^(.{8})(.{4})(.{4})(.{4})(.{12})$/\1-\2-\3-\4-\5/'
}
want() { # NAME GOT WANT
  if [ "$2" = "$3" ]; then echo "PASS: $1 (got $2)"; else echo "FAIL: $1 (got $2, want $3)"; return 1; fi
}

cells() {
  mkdir -p "$tok"
  k port-forward "deploy/$SVC" "$LOCAL_PORT:8095" >"$DRILL_DIR/port-forward.log" 2>&1 &
  PF_PID=$!
  trap 'kill "$PF_PID" 2>/dev/null || true' RETURN
  # Ready = the port-forward process is alive AND it printed its Forwarding line for this port. A reply on the port
  # proves nothing: another process may hold it. No credential is sent before this.
  local _ ready=0
  for _ in $(seq 1 30); do
    peer_ok 2>/dev/null || break
    if grep -qF "Forwarding from 127.0.0.1:$LOCAL_PORT -> " "$DRILL_DIR/port-forward.log" 2>/dev/null; then ready=1; break; fi
    sleep 1
  done
  if [ "$ready" != 1 ] || ! peer_ok 2>/dev/null; then
    echo "ABORT: port-forward on 127.0.0.1:$LOCAL_PORT is not proven (process gone or no Forwarding line); no credential sent"
    echo "DRILL FAILED"
    return 1
  fi
  local s
  local bad=0
  echo "== sign in twice: S1 (attacked) and S2 (control)"
  s="$(login s2)"; want "login S2" "$s" 200 || bad=1
  take "$tok/s2.login.json" refresh_token >"$tok/s2.R0"
  s="$(login s1)"; want "login S1" "$s" 200 || bad=1
  [ "$bad" = 0 ] || { echo "login refused: s1 $(safe_err "$tok/s1.login.json"); s2 $(safe_err "$tok/s2.login.json")"; echo "DRILL FAILED"; return 1; }
  take "$tok/s1.login.json" refresh_token >"$tok/R0"
  take "$tok/s1.login.json" access_token >"$tok/A0"
  local sid
  # S1 is the newest session of the user (signed in after S2); the access token's sid claim must name the same one.
  sid="$(psql_super -c "SELECT s.id FROM auth.sessions s JOIN users u ON u.id = s.principal_id
    WHERE u.email = '$EMAIL' ORDER BY s.created_at DESC LIMIT 1")"
  want "S1 session id equals the access token sid" "$(sid_uuid "$(sid_of "$tok/A0")")" "$sid" || bad=1
  want "sessions of the user" "$(psql_super -c "SELECT count(*) FROM auth.sessions s JOIN users u ON u.id = s.principal_id WHERE u.email = '$EMAIL'")" 2 || bad=1
  echo "S1 session id $sid; R0 sha12 $(sha12 "$tok/R0")"

  echo "== legit rotation R0 -> R1"
  s="$(refresh "$tok/R0" "$tok/r1.json")"; want "refresh R0" "$s" 200 || bad=1
  take "$tok/r1.json" refresh_token >"$tok/R1"
  echo "== C5 grace: R0 again at once, R1 unused"
  s="$(refresh "$tok/R0" "$tok/grace.json")"; want "C5 grace replay status" "$s" 200 || bad=1
  take "$tok/grace.json" refresh_token >"$tok/R1g"
  want "C5 grace replay answers the same successor" "$(cmp -s "$tok/R1" "$tok/R1g" && echo same || echo different)" same || bad=1
  want "C5 session still live" "$(psql_super -c "SELECT revoked_at IS NULL FROM auth.sessions WHERE id = '$sid'")" t || bad=1

  echo "== legit rotation R1 -> R2"
  s="$(refresh "$tok/R1" "$tok/r2.json")"; want "refresh R1" "$s" 200 || bad=1
  take "$tok/r2.json" refresh_token >"$tok/R2"
  # The session routes take the session-audience token (session_access_token, the eddsa class answer).
  take "$tok/r2.json" session_access_token >"$tok/A2"
  s="$(current "$tok/A2" "$tok/cur-before.json")"; want "S1 access token live before the attack" "$s" 200 || bad=1

  echo "== C1 attacker replays R0 (consumed, its successor used)"
  s="$(refresh "$tok/R0" "$tok/attack.json")"; want "C1 reuse status" "$s" 401 || bad=1
  want "C1 reuse text" "$(detail "$tok/attack.json")" "Refresh token reuse detected" || bad=1

  echo "== C2 the legit holder is signed out"
  s="$(refresh "$tok/R2" "$tok/legit-after.json")"; want "C2 legit refresh R2" "$s" 401 || bad=1
  echo "  legit refresh text: $(detail "$tok/legit-after.json")"
  s="$(current "$tok/A2" "$tok/cur-after.json")"; want "C2 legit access token on /v1/sessions/current" "$s" 401 || bad=1
  echo "  current refusal: $(safe_err "$tok/cur-after.json")"

  echo "== C3 the whole family is dead"
  s="$(refresh "$tok/R1" "$tok/attack-r1.json")"; want "C3 attacker replays R1" "$s" 401 || bad=1
  want "C3 S1 revoked" "$(psql_super -c "SELECT revoked_at IS NOT NULL FROM auth.sessions WHERE id = '$sid'")" t || bad=1
  want "C3 live credentials of S1" "$(psql_super -c "SELECT count(*) FROM auth.refresh_credentials WHERE session_id = '$sid' AND consumed_at IS NULL AND revoked_at IS NULL")" 0 || bad=1
  echo "  credentials of S1 (total): $(psql_super -c "SELECT count(*) FROM auth.refresh_credentials WHERE session_id = '$sid'")"

  echo "== C4 audit"
  want "C4 reuse audit rows for S1" "$(psql_super -c "SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.session.refresh_reuse_revoked' AND attributes->>'target_id' = '$sid'")" 1 || bad=1
  want "C4 reuse audit row has a request id" "$(psql_super -c "SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.session.refresh_reuse_revoked' AND attributes->>'target_id' = '$sid' AND request_id IS NOT NULL")" 1 || bad=1

  echo "== C6 control: S2 of the same user is not touched"
  s="$(refresh "$tok/s2.R0" "$tok/s2.r1.json")"; want "C6 S2 refresh" "$s" 200 || bad=1

  echo "== auth-service reuse log line"
  k logs "deploy/$SVC" 2>/dev/null | grep -F 'refresh credential reuse' | jq -c '{msg, session_id, family_id, reason_code}' 2>/dev/null | head -3 || true
  if [ "$bad" != 0 ]; then
    echo "DRILL FAILED"
    return 1
  fi
  echo "DRILL OK"
}

# teardown : delete the namespace ONLY when this run created it: $DRILL_DIR/ns-created holds a successful create (rc=0)
# and a run id, that id is this run's, and the live namespace carries the same id as its drill-run label. Otherwise it
# refuses and returns non-zero. Every step is rc guarded with `|| return $?` (errexit is off when a caller runs
# `teardown || ...`).
teardown() {
  local made have
  made="$(sed -n 's/^rc=0 run=\([a-z0-9-]*\)$/\1/p' "$DRILL_DIR/ns-created" 2>/dev/null)" || made=""
  have="$(kubectl get ns "$DRILL_NS" -o 'jsonpath={.metadata.labels.drill-run}' 2>/dev/null)" || have=""
  if [ -z "$made" ] || { [ -n "$RUN_ID" ] && [ "$RUN_ID" != "$made" ]; } || [ "$have" != "$made" ]; then
    echo "REFUSED: ns=$DRILL_NS was not created by this run: not deleting it"
    return 1
  fi
  kubectl delete ns "$DRILL_NS" --wait=true --timeout=300s >/dev/null || return $?
  if kubectl get ns "$DRILL_NS" >/dev/null 2>&1; then echo "ns=$DRILL_NS still present"; return 1; fi
  echo "ns=$DRILL_NS deleted"
}

case "${1:-}" in
  setup) setup ;;
  cells) cells ;;
  teardown) teardown ;;
  run)
    # A subshell with errexit on (not under || or &&, which would turn errexit off inside it): a failure or a FAIL
    # exit in setup or cells ends the subshell, and the teardown still runs.
    # Never tear down a namespace this run did not create: the teardown checks the create record and the run label.
    [ -n "$RUN_ID" ] || new_run_id
    kubectl get ns "$DRILL_NS" >/dev/null 2>&1 && fail "namespace $DRILL_NS exists: use a fresh name"
    set +e
    (
      set -e
      setup
      cells
    )
    rc=$?
    set -e
    teardown || rc=1
    exit "$rc"
    ;;
  *) echo "usage: drill-refresh-reuse.sh setup|cells|teardown|run" >&2; exit 2 ;;
esac
