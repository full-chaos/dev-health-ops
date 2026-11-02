#!/usr/bin/env bash
# Cells for the argument and refusal paths of ci/rehearse_auth_key_rotation.sh. Hermetic: no docker, no network;
# a docker stub on PATH records any call (there must be none). Run: bash ci/rehearse_auth_key_rotation_test.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT="${ROOT}/ci/rehearse_auth_key_rotation.sh"
T="$(mktemp -d)"
trap 'rm -rf "${T}"' EXIT
mkdir -p "${T}/bin"
# shellcheck disable=SC2016
printf '%s\n' '#!/usr/bin/env bash' 'echo "docker $*" >> "${FAKE_LOG}"' 'exit 99' >"${T}/bin/docker"
chmod +x "${T}/bin/docker"
export FAKE_LOG="${T}/docker.log"
: >"${FAKE_LOG}"
failed=0
ok() { printf 'PASS: %s\n' "$1"; }
bad() { printf 'FAIL: %s\n' "$1"; failed=1; }
run() { PATH="${T}/bin:${PATH}" "$@"; }

plan="$(run bash "${SCRIPT}" --print-plan)"
for needle in 'stage smoke' 'S1 planned' 'S2 compromise' 'S3 rollback' 'real, once'; do
  if grep -qF -- "${needle}" <<<"${plan}"; then ok "plan-names-${needle// /-}"; else bad "plan-names-${needle// /-}"; fi
done

rc=0
run bash "${SCRIPT}" --stage nonsense >/dev/null 2>&1 || rc=$?
if [ "${rc}" = 1 ]; then ok unknown-stage-refused; else bad "unknown-stage-refused (rc ${rc})"; fi

rc=0
env -u REHEARSAL_DIR run bash "${SCRIPT}" --stage smoke >/dev/null 2>&1 || rc=$?
if [ "${rc}" != 0 ]; then ok no-dir-refused; else bad no-dir-refused; fi

printf 'DEV_HEALTH_GO_OPERATOR_IMAGE=ghcr.io/full-chaos/dev-health-go-operator:latest\n' >"${T}/tag.env"
mkdir -p "${T}/full"
echo x >"${T}/full/f"
rc=0
REHEARSAL_DIR="${T}/full" REHEARSAL_IMAGES_ENV="${T}/tag.env" run bash "${SCRIPT}" --stage smoke >/dev/null 2>&1 || rc=$?
if [ "${rc}" = 1 ]; then ok non-empty-dir-refused; else bad "non-empty-dir-refused (rc ${rc})"; fi

printf 'DEV_HEALTH_GO_OPERATOR_IMAGE=ghcr.io/full-chaos/dev-health-go-operator:latest\n' >"${T}/tag.env"
rc=0
out="$(REHEARSAL_DIR="${T}/new1" REHEARSAL_IMAGES_ENV="${T}/tag.env" run bash "${SCRIPT}" --stage smoke 2>&1)" || rc=$?
if [ "${rc}" = 1 ] && grep -q 'not a registry digest reference' <<<"${out}"; then ok tag-image-refused; else bad "tag-image-refused (rc ${rc})"; fi

rc=0
out="$(REHEARSAL_DIR="${T}/new2" REHEARSAL_IMAGES_ENV="${T}/nofile.env" run bash "${SCRIPT}" --stage smoke 2>&1)" || rc=$?
if [ "${rc}" = 1 ]; then ok missing-image-file-refused; else bad "missing-image-file-refused (rc ${rc})"; fi

rc=0
run bash -x "${SCRIPT}" --print-plan >/dev/null 2>&1 || rc=$?
if [ "${rc}" = 2 ]; then ok xtrace-refused; else bad "xtrace-refused (rc ${rc})"; fi

OVR="${ROOT}/ci/auth_rotation_rehearsal.override.yml"
# auth-service refuses the key-set file beside the single-key variables, even unset by !reset (smoke v2): they must be set empty.
for var in AUTH_SIGNING_KEY_FILE AUTH_SIGNING_KEY_ID; do
  if grep -qE "^      ${var}: \"\"\$" "${OVR}"; then ok "override-blanks-${var}"; else bad "override-blanks-${var}"; fi
done

# go-api and query-api read auth.sessions to accept an EdDSA access token (smoke v5: missing_grant SELECT on sessions): the
# migration jobs grant the session readers, as the chart does with authService.migrations.sessionReaders.
for job in auth-migrate auth-grants; do
  if awk -v job="${job}:" '$1 == job {on=1; next} /^  [a-z]/ {on=0} on && /AUTH_SESSION_READER_ROLES: devhealth_api,devhealth_query_api/ {found=1} END {exit !found}' "${OVR}"; then ok "override-grants-session-readers-${job}"; else bad "override-grants-session-readers-${job}"; fi
done

# query-api's role posture must expect the session read it is granted (smoke v6: "auth.sessions.id outside the manifest").
if awk '$1 == "query-api:" {on=1; next} /^  [a-z]/ {on=0} on && /AUTH_PROXY_SESSIONS: "true"/ {found=1} END {exit !found}' "${OVR}"; then ok override-query-api-expects-session-read; else bad override-query-api-expects-session-read; fi

# A recreated container gets new ephemeral host ports (smoke v7: login answered 000): every --force-recreate is followed by a port read.
recreates="$(grep -c -- '--force-recreate' "${SCRIPT}" || true)"
refreshes="$(grep -c '^ *refresh_ports$' "${SCRIPT}" || true)"
if [ "${recreates}" -gt 0 ] && [ "${refreshes}" -ge "${recreates}" ]; then ok every-recreate-rereads-the-ports; else bad "every-recreate-rereads-the-ports (${recreates} recreates, ${refreshes} reads)"; fi

# A function must be defined before its first call (smoke v8: "refresh_ports: command not found").
if [ "$(grep -n 'refresh_ports' "${SCRIPT}" | grep -v '^[0-9]*:#' | head -n 1 | cut -d: -f2- | tr -d ' ')" = 'refresh_ports(){' ]; then ok refresh-ports-defined-before-first-call; else bad refresh-ports-defined-before-first-call; fi

# The host SMTP sink is unreachable from the bigboy docker bridge (smoke v3: no route to host): the user is verified in the
# throwaway database instead.
if grep -q 'auth_webproof_smtp_sink' "${SCRIPT}"; then bad no-host-smtp-sink; else ok no-host-smtp-sink; fi
if grep -q 'SET is_verified = true' "${SCRIPT}"; then ok verifies-the-user-in-the-database; else bad verifies-the-user-in-the-database; fi

if [ ! -s "${FAKE_LOG}" ]; then ok no-docker-call-before-the-checks; else bad no-docker-call-before-the-checks; fi
exit "${failed}"
