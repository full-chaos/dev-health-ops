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

if [ ! -s "${FAKE_LOG}" ]; then ok no-docker-call-before-the-checks; else bad no-docker-call-before-the-checks; fi
exit "${failed}"
