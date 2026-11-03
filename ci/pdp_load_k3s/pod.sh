#!/bin/sh
# Runs INSIDE the runner pod of ci/pdp_load_k3s.sh (postgres:18-alpine, uid 65532, files in /work). Prints status lines only:
# a token, a reference or a bootstrap value is read from a file and written to a file, never printed and never on argv.
#   pod.sh mint        exchange the bootstrap value for a fresh workload token, then issue the user session (the reference)
#   pod.sh preflight   three decisions: allow, high-risk allow, deny; fails before any load if one is not what its name says
#   pod.sh run ARGS    loadtest ARGS, the three inputs in its environment
set -eu
umask 077
W=/work
HOST="${PDP_HOST:?}"
PORT="${PDP_PORT:-8095}"

# send METHOD PATH CONTENT-TYPE BEARER-FILE|- BODY-FILE: the answer lands in $W/answer, the status is printed
send() {
  length=$(wc -c <"$5")
  {
    printf '%s %s HTTP/1.1\r\nHost: %s:%s\r\nConnection: close\r\nContent-Type: %s\r\nContent-Length: %s\r\n' "$1" "$2" "$HOST" "$PORT" "$3" "$length"
    if [ "$4" != - ]; then
      printf 'Authorization: Bearer '
      tr -d '\r\n' <"$4"
      printf '\r\n'
    fi
    printf '\r\n'
    cat "$5"
  } >"$W/request"
  { cat "$W/request"; sleep 1; } | nc -w 10 "$HOST" "$PORT" >"$W/answer"
  rm -f "$W/request"
  sed -n '1s/^HTTP\/1\.1 \([0-9]*\).*/\1/p' "$W/answer" | tr -d '\r'
}

field() { sed -n "s/.*\"$1\":\"\([^\"]*\)\".*/\1/p" "$W/answer" | head -1 | tr -d '\r\n'; }

mint() {
  : "${PDP_PRINCIPAL:?}" "${PDP_ORG:?}"
  rm -f "$W/workload.token" "$W/principal.ref"
  {
    printf 'grant_type=urn:ietf:params:oauth:grant-type:token-exchange&subject_token='
    tr -d '\r\n' <"$W/api.bootstrap"
    printf '&subject_token_type=urn:dev-health:params:oauth:token-type:workload-bootstrap&audience=auth-service'
  } >"$W/body"
  status=$(send POST /internal/v1/token/exchange application/x-www-form-urlencoded - "$W/body")
  [ "$status" = 200 ] || { echo "FAIL: exchange status=$status"; exit 1; }
  field access_token >"$W/workload.token"
  [ -s "$W/workload.token" ] || { echo "FAIL: the exchange answered no access_token"; exit 1; }
  printf '{"principal_id":"%s","organization_id":"%s","amr":["pwd"]}' "$PDP_PRINCIPAL" "$PDP_ORG" >"$W/body"
  status=$(send POST /internal/v1/sessions application/json "$W/workload.token" "$W/body")
  [ "$status" = 200 ] || { echo "FAIL: session issue status=$status"; exit 1; }
  field access_token >"$W/principal.ref"
  [ -s "$W/principal.ref" ] || { echo "FAIL: the session issue answered no access_token"; exit 1; }
  rm -f "$W/body" "$W/answer"
  echo "minted: workload token and principal reference (values in files)"
}

question() { # ACTION TYPE KEY WITH-ORG
  printf '{"schema_version":"authorization_request.v1","principal_reference":"%s","action":"%s","resource":{"type":"%s","key":"%s"' \
    "$(tr -d '\r\n' <"$W/principal.ref")" "$1" "$2" "$3"
  if [ "$4" = 1 ]; then printf ',"organization_id":"%s"' "$PDP_ORG"; fi
  printf '},"context":{"request_id":"req_PDPPREFLIGHT00000000000001","client_service":"dev-health-ops-api"}}'
}

expect() { # NAME ACTION TYPE KEY WITH-ORG WANT(true|false)
  question "$2" "$3" "$4" "$5" >"$W/body"
  status=$(send POST /v1/authorize application/json "$W/workload.token" "$W/body")
  [ "$status" = 200 ] || { echo "FAIL: preflight $1 status=$status"; exit 1; }
  grep -q "\"allowed\":$6" "$W/answer" || { echo "FAIL: preflight $1 did not decide allowed=$6 (reason $(field reason_code))"; exit 1; }
  echo "PASS: preflight $1 allowed=$6"
}

preflight() {
  expect low analytics.read organization acme 1 true
  expect high settings.write organization acme 1 true
  expect deny admin.users platform platform 0 false
  rm -f "$W/body" "$W/answer"
}

run() {
  AUTH_LOADTEST_WORKLOAD_TOKEN="$(tr -d '\r\n' <"$W/workload.token")"
  AUTH_LOADTEST_PRINCIPAL_REFERENCE="$(tr -d '\r\n' <"$W/principal.ref")"
  AUTH_LOADTEST_ORG_ID="${PDP_ORG:?}"
  export AUTH_LOADTEST_WORKLOAD_TOKEN AUTH_LOADTEST_PRINCIPAL_REFERENCE AUTH_LOADTEST_ORG_ID
  exec timeout "${PDP_RUN_CAP:-240}" "$W/loadtest" "$@"
}

cmd="${1:?usage: pod.sh mint|preflight|run ARGS}"
shift
case "$cmd" in
  mint) mint ;;
  preflight) preflight ;;
  run) run "$@" ;;
  *) echo "usage: pod.sh mint|preflight|run ARGS" >&2; exit 2 ;;
esac
