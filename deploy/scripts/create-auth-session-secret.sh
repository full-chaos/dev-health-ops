#!/usr/bin/env bash
# Create the Secret that holds the auth-service session successor key. CREATE-ONLY.
# It runs exactly one kubectl command (create secret). It reads no Secret, execs nothing in any pod,
# prints no value, and puts no value in argv or in a log line. Do not run it before the review of the change that added it.
#
#   key AUTH_SESSION_SUCCESSOR_KEY   32 random bytes, base64url without padding (43 characters).
#                                    Parsed by internal/auth/authconfig/config.go (sessionSuccessorKeyBytes).
#
# Where every name comes from (change a name in its source, not here; a unit test pins the chart default):
#   Secret key    AUTH_SESSION_SUCCESSOR_KEY  deploy/helm/dev-health/values.yaml authService.sessionKeys.successor.secretKey.
#   Secret name   auth-session               chosen here; the values file must set
#                                            authService.sessionKeys.successor.secretName to it (templates/auth-service.yaml).
#   Namespace     dev-health                  docs/operate/install/production.md.
# Override with NAMESPACE and SECRET_NAME if the deploy source says otherwise (--plan prints what would be used).
#
# Order: run this BEFORE the helm upgrade that sets authService.sessionKeys.successor.secretName: a pod whose Secret is missing does not start.
# The key is paired with the HS256 key go-api reads (hs256Secret); this script does not touch that one.
set -euo pipefail
umask 077

NAMESPACE="${NAMESPACE:-dev-health}"
SECRET_NAME="${SECRET_NAME:-auth-session}"
KEY_NAME="AUTH_SESSION_SUCCESSOR_KEY"

die() { printf '%s\n' "$1"; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "FAILED: $1 is not installed"; }

if [ "${1:-}" = "--plan" ]; then
  printf 'plan: namespace=%s secret=%s key=%s value=32 bytes base64url, no padding\n' "$NAMESPACE" "$SECRET_NAME" "$KEY_NAME"
  printf 'plan: one key, one create secret, nothing else\n'
  exit 0
fi

need kubectl
need openssl
need base64
need tr
need wc

WORK="$(mktemp -d)"
cleanup() { rm -rf "$WORK"; }
trap cleanup EXIT
chmod 700 "$WORK"

# 1. The key, in a 0600 file only: 32 random bytes, base64url, no padding, no newline.
openssl rand -base64 32 | tr '+/' '-_' | tr -d '=\n' > "$WORK/key"

# 2. Self-check: the same rule auth-service applies at startup. Prints only pass or fail.
check_len="$(tr '_-' '/+' < "$WORK/key" | base64 -d 2>/dev/null | wc -c | tr -d ' ')" || true
[ "${check_len:-0}" = "32" ] || die "FAILED: the generated key is not 32 bytes"
# MUTATION-ANCHOR-STDOUT

# 3. Create the Secret: create, never apply. The value is passed by file, never as an argument.
# The error text goes to a 0600 file and is only searched for one word.
if ! kubectl -n "$NAMESPACE" create secret generic "$SECRET_NAME" \
  --from-file="${KEY_NAME}=${WORK}/key" >/dev/null 2>"$WORK/err"; then
  if grep -q AlreadyExists "$WORK/err"; then
    die "STOPPED: the Secret already exists; nothing was changed. To replace it, a person deletes the Secret first, then runs this again"
  fi
  die "FAILED: the Secret was not created"
fi

printf 'created: secret %s, key %s, in namespace %s\n' "$SECRET_NAME" "$KEY_NAME" "$NAMESPACE"
printf 'next: the helm upgrade sets authService.sessionKeys.successor.secretName to %s\n' "$SECRET_NAME"
