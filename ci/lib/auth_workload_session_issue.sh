#!/usr/bin/env bash
# go-api's session issue credential in the compose proofs: its workload token, from the Compose bootstrap credential.
# Sourced; it sets no shell option and never exits the caller.
#   auth_workload_session_issue COMPOSE_FN [RUN_FLAG...]   COMPOSE_FN is the caller's compose function (project and -f
#                                                          files); RUN_FLAGs go to `compose run` (for example --build).
# It runs `COMPOSE_FN --profile auth run RUN_FLAG... --rm auth-workload-bootstrap` (compose.yml: seeds the workload registry and writes
# the api credential to the workload_bootstrap volume, 0600 uid 65532; go-api reads it from a read-only mount), reads the
# wire principal id of the api workload (ci/pdp_load_k3s/subject.sql) and exports what auth-service and go-api need:
#   AUTH_TOKEN_ISSUER_URL                 kept when the caller set it, else https://auth.workload-proof.example (a signed
#                                         claim; nothing dials it). go-api's AUTH_WORKLOAD_ISSUER is the same value.
#   AUTH_WORKLOAD_BOOTSTRAP=true          auth-service mounts the bootstrap exchange
#   AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT   the prn_ id whose workload token the session issue route accepts
#   AUTH_SESSION_ISSUE_CREDENTIAL=workload  go-api presents its workload token on the session issue route
# Call it after the ops migrations ran and before auth-service and go-api start with the sessions flag. Run it once per
# project: bootstrap-create refuses a workload that holds a live credential. No credential value is read or printed.
# Returns 0, or 1 with an ERROR line naming the step.

_AUTH_WORKLOAD_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

auth_workload_session_issue() {
  local compose_fn="$1" subject
  shift
  export AUTH_TOKEN_ISSUER_URL="${AUTH_TOKEN_ISSUER_URL:-https://auth.workload-proof.example}"
  export AUTH_WORKLOAD_BOOTSTRAP=true
  if ! "${compose_fn}" --profile auth run "$@" --rm auth-workload-bootstrap; then
    printf 'ERROR: auth-workload-bootstrap failed\n' >&2
    return 1
  fi
  if ! subject="$("${compose_fn}" exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-postgres}" -At -f - \
    <"${_AUTH_WORKLOAD_LIB_DIR}/../pdp_load_k3s/subject.sql")"; then
    printf 'ERROR: could not read the api workload principal\n' >&2
    return 1
  fi
  if ! [[ "${subject}" =~ ^prn_[A-Za-z0-9_-]{22}$ ]]; then
    printf 'ERROR: the api workload principal id is not a wire principal id (%s)\n' "${subject}" >&2
    return 1
  fi
  export AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT="${subject}"
  export AUTH_SESSION_ISSUE_CREDENTIAL=workload
  printf 'session issue credential: go-api workload token (subject %s, issuer %s)\n' "${subject}" "${AUTH_TOKEN_ISSUER_URL}"
}
