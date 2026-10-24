#!/usr/bin/env bash
# Create-only host script for the dedicated session-issue Secret.
# Copies the session issue credential from a 0600 file on tmpfs into a private
# tmpfs work directory and creates the Secret with `kubectl create` from that
# copy. It never mints, reads no Secret and prints no value: only Secret names,
# key names, "created" and, on failure, the source file PATH.
#
#   create-session-issue-secret.sh --context C --namespace N --name SECRET \
#       --value-file PATH [--key AUTH_SESSION_ISSUE_TOKEN]
#
# The source file is shredded after a successful create and kept on failure.
set -euo pipefail
umask 077
export LC_ALL=C

case $- in
  *x*) printf 'refused: xtrace is on\n' >&2; exit 2 ;;
esac

context="" namespace="" name="" valuefile="" key="AUTH_SESSION_ISSUE_TOKEN"
while [ "$#" -gt 0 ]; do
  case "$1" in
    --context) context="${2:-}"; shift 2 ;;
    --namespace) namespace="${2:-}"; shift 2 ;;
    --name) name="${2:-}"; shift 2 ;;
    --value-file) valuefile="${2:-}"; shift 2 ;;
    --key) key="${2:-}"; shift 2 ;;
    *) printf 'refused: unknown argument\n' >&2; exit 2 ;;
  esac
done

[ -n "$context" ] && [ -n "$namespace" ] && [ -n "$name" ] && [ -n "$valuefile" ] || {
  printf 'refused: --context, --namespace, --name and --value-file are required\n' >&2
  exit 2
}
dns1123='^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$'
keyname='^[A-Za-z0-9_-][A-Za-z0-9._-]{0,252}$'
[[ "$name" =~ $dns1123 ]] && [ "${#name}" -le 253 ] && [[ "$key" =~ $keyname ]] || {
  printf 'refused: --name or --key is not a valid name\n' >&2
  exit 2
}

refuse_file() { printf 'refused: value file %s\n' "$1" >&2; exit 2; }
[ -L "$valuefile" ] && refuse_file "is a symlink"
[ -f "$valuefile" ] || refuse_file "is not a regular file"
[ "$(stat -c %a "$valuefile")" = "600" ] || refuse_file "is not mode 0600"
[ "$(stat -c %u "$valuefile")" = "$(id -u)" ] || refuse_file "is not owned by the caller"
[ "$(stat -f -c %T "$valuefile")" = "tmpfs" ] || refuse_file "is not on tmpfs"

size="$(stat -c %s "$valuefile")"
line=""
IFS= read -r line <"$valuefile" || true
# one line: the file is the line, plus at most one trailing newline
[ "$size" -eq "${#line}" ] || [ "$size" -eq $((${#line} + 1)) ] || refuse_file "is not one line"
[[ "$line" =~ ^svc_authsess_[A-Za-z0-9_-]{43}$ ]] || refuse_file "does not have the credential shape"

base="${SESSION_ISSUE_WORKDIR_BASE:-${XDG_RUNTIME_DIR:-/dev/shm}}"
[ -d "$base" ] || { printf 'refused: no private work directory\n' >&2; exit 2; }
[ "$(stat -f -c %T "$base")" = "tmpfs" ] || { printf 'refused: work directory is not tmpfs\n' >&2; exit 2; }
workdir=""
kpid=""

cleanup() {
  local f
  [ -n "$workdir" ] || return 0
  shopt -s dotglob nullglob
  for f in "$workdir"/*; do
    [ -e "$f" ] || continue
    shred -u "$f" 2>/dev/null || rm -f "$f"
  done
  rmdir "$workdir" 2>/dev/null || true
}
kept() { printf 'value file kept for retry: %s\n' "$valuefile"; }
on_signal() {
  if [ -n "$kpid" ]; then
    kill "$kpid" 2>/dev/null || true
    wait "$kpid" 2>/dev/null || true
  fi
  cleanup
  kept
  exit "$1"
}
trap cleanup EXIT
trap 'on_signal 130' INT
trap 'on_signal 143' TERM

workdir="$(mktemp -d "${base%/}/session-issue.XXXXXX")"
errfile="$workdir/kubectl.err"
copy="$workdir/$key"
printf '%s' "$line" >"$copy"
line=""

kubectl_bin="${KUBECTL:-kubectl}"
"$kubectl_bin" --context "$context" --namespace "$namespace" create secret generic "$name" \
  --from-file="${key}=${copy}" >/dev/null 2>"$errfile" &
kpid=$!
rc=0
wait "$kpid" || rc=$?
kpid=""
if [ "$rc" -eq 0 ]; then
  printf '%s key=%s created\n' "$name" "$key"
  if ! shred -u "$valuefile" 2>/dev/null; then
    rm -f "$valuefile" 2>/dev/null || { printf 'WARN source value file not removed: %s\n' "$valuefile"; }
  fi
else
  if grep -q 'AlreadyExists' "$errfile"; then
    printf 'STOP %s exists\n' "$name"
  else
    printf 'STOP %s create failed\n' "$name"
  fi
  kept
  exit 1
fi
