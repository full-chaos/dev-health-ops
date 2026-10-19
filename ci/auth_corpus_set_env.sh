#!/usr/bin/env bash
# Changes one environment variable of the go-api container of the private compose project that
# ci/auth_corpus_external.sh brought up, then recreates go-api (only go-api, only through compose) and waits,
# bounded, until it is healthy again (CHAOS-10160).
#
#   ci/auth_corpus_set_env.sh NAME VALUE      (an empty VALUE removes NAME)
#
# The external auth corpus (internal/testsupport/venueoracle SetTargetEnv) runs this from the repository root
# whenever a corpus case needs the target's environment changed. It reads the project and the state file from the
# environment that ci/auth_corpus_external.sh exported; the state file holds every variable the compose project is
# started with, so a recreate keeps all of them (JWT key, mail settings, and on leg acp the six AUTH_PROXY_* flags).
set -euo pipefail

if [ "$#" -ne 2 ]; then
  printf 'usage: %s NAME VALUE\n' "$0" >&2
  exit 2
fi
NAME="$1"
VALUE="$2"

die() {
  printf 'ERROR: %s\n' "$1" >&2
  exit 1
}

PROJECT="${AUTH_CORPUS_PROJECT:-}"
STATE="${AUTH_CORPUS_STATE:-}"
[[ "${PROJECT}" =~ ^authcorpus[0-9a-f]+$ ]] || die "AUTH_CORPUS_PROJECT must be the authcorpus<hex> project of ci/auth_corpus_external.sh (got '${PROJECT}')"
[ -f "${STATE}" ] || die "AUTH_CORPUS_STATE must name the state file of that project (got '${STATE}')"
[[ "${NAME}" =~ ^[A-Z][A-Z0-9_]*$ ]] || die "NAME must be an upper-case environment variable name (got '${NAME}')"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
ROOT="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd -P)"
LEG="${AUTH_CORPUS_LEG:-go-api}"

compose() {
  local files=(-f "${ROOT}/compose.yml" -f "${SCRIPT_DIR}/auth_corpus_external.override.yml")
  if [ "${LEG}" = "acp" ]; then
    files+=(-f "${SCRIPT_DIR}/auth_corpus_external.acp.override.yml")
  fi
  docker compose -p "${PROJECT}" "${files[@]}" --profile auth "$@"
}

# Rewrite the state: drop NAME, add it back unless it is being removed. AUTH_PROXY_* lines stay as they are.
grep -v "^export ${NAME}=" "${STATE}" >"${STATE}.new" || true
if [ -n "${VALUE}" ]; then
  printf 'export %s=%q\n' "${NAME}" "${VALUE}" >>"${STATE}.new"
fi
mv -- "${STATE}.new" "${STATE}"

# shellcheck disable=SC1090
. "${STATE}"
if [ -z "${VALUE}" ]; then
  unset "${NAME}"
fi

printf 'auth_corpus_set_env: %s %s; recreating go-api of %s\n' "${NAME}" "$([ -n "${VALUE}" ] && echo set || echo unset)" "${PROJECT}"
compose up --detach --no-deps --force-recreate --no-build go-api

deadline=$((SECONDS + 120))
while :; do
  container="$(compose ps -q go-api)"
  health=""
  if [ -n "${container}" ]; then
    health="$(docker inspect --format '{{.State.Health.Status}}' "${container}" 2>/dev/null || true)"
  fi
  if [ "${health}" = "healthy" ]; then
    printf 'auth_corpus_set_env: go-api healthy\n'
    exit 0
  fi
  if [ "${SECONDS}" -ge "${deadline}" ]; then
    compose logs --no-color --tail 40 go-api >&2 || true
    die "go-api is '${health}' 120 s after the recreate"
  fi
  sleep 2
done
