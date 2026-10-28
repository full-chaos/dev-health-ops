#!/usr/bin/env bash
# Bounded-retry ghcr.io login, the fallback for a docker/login-action step that failed (a registry timeout). Run as
#   bash ci/lib/ghcr_login.sh
# with GHCR_USER and GHCR_TOKEN in the environment. The token goes to `docker login` on stdin only: never argv, never the log.
# Three tries, 10 s then 30 s apart (retry_named, ci/lib/retry.sh). Each failed try is named in the log. A login that
# still fails after the third try exits non-zero.
set +x
set -uo pipefail

# shellcheck source=ci/lib/retry.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/retry.sh"

user="${GHCR_USER-}"
token="${GHCR_TOKEN-}"
unset GHCR_TOKEN
if [ -z "${user}" ] || [ -z "${token}" ]; then
  printf 'ghcr-login: GHCR_USER and GHCR_TOKEN must both be set\n' >&2
  exit 2
fi

ghcr_login_once() {
  printf '%s' "${token}" | docker login ghcr.io -u "${user}" --password-stdin
}

RETRY_QUIET=0
rc=0
retry_named ghcr-login 3 "10 30" -- ghcr_login_once || rc=$?
if [ "${rc}" -ne 0 ]; then
  printf 'ghcr-login: ghcr.io login failed after 3 tries (rc=%d)\n' "${rc}" >&2
  exit "${rc}"
fi
