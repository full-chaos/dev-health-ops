#!/usr/bin/env bash
# Create-only host script for the dedicated session-issue Secret.
# Copies the session issue credential from a 0600 file on tmpfs into a private
# tmpfs work directory and creates the Secret with `kubectl create` from that
# copy. It never mints, reads no Secret and prints no value: only Secret names,
# key names, the final state and, when the source is kept, its PATH.
#
#   create-session-issue-secret.sh --context C --namespace N --name SECRET \
#       --value-file PATH [--key AUTH_SESSION_ISSUE_TOKEN]
#
# One EXIT trap reports the true state. Exit codes:
#   0 created, source removed        3 created, source NOT removed
#   1 create failed or interrupted   2 refused (nothing was created)
#   130/143 signal before the create finished
# On 1, 2, 130 and 143 the source file is kept and its path is printed.
# shellcheck disable=SC2329 # the functions run from traps
set -euo pipefail
set +a
umask 077
export LC_ALL=C

case $- in
  *x*) printf 'refused: xtrace is on\n' >&2; exit 2 ;;
esac

valuefile="" workdir="" kpid="" line=""
created=0 removed=0 finished=0 sigrc=0
export -n line

kept() { printf 'value file kept for retry: %s\n' "$valuefile"; }

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

final() {
  local rc=$?
  trap '' INT TERM
  [ "$finished" -eq 0 ] || exit "$rc"
  finished=1
  if [ -n "$kpid" ]; then
    kill "$kpid" 2>/dev/null || true
    wait "$kpid" 2>/dev/null || true
  fi
  cleanup
  if [ "$created" -eq 1 ]; then
    { printf '%s key=%s created\n' "$name" "$key"; } 2>/dev/null || true
    if [ "$removed" -eq 1 ]; then
      { printf 'secret created; value file removed\n'; } 2>/dev/null || true
      rc=0
    else
      { printf 'secret created; value file NOT removed: %s\n' "$valuefile"; } 2>/dev/null || true
      rc=3
    fi
  elif [ -n "$valuefile" ] && { [ -e "$valuefile" ] || [ -L "$valuefile" ]; }; then
    { kept; } 2>/dev/null || true
  fi
  exit "$rc"
}
name="" key="AUTH_SESSION_ISSUE_TOKEN"
trap final EXIT

on_signal() {
  sigrc=$1
  if [ -n "$kpid" ]; then
    kill "$kpid" 2>/dev/null || true
  else
    exit "$1"
  fi
}
trap 'on_signal 130' INT
trap 'on_signal 143' TERM

refuse() {
  printf 'refused: %s\n' "$1" >&2
  exit 2
}

context="" namespace=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --context|--namespace|--name|--value-file|--key)
      [ "$#" -ge 2 ] || refuse "$1 needs a value"
      case "$1" in
        --context) context="$2" ;;
        --namespace) namespace="$2" ;;
        --name) name="$2" ;;
        --value-file) valuefile="$2" ;;
        --key) key="$2" ;;
      esac
      shift 2 ;;
    *) refuse "unknown argument" ;;
  esac
done

[ -n "$context" ] && [ -n "$namespace" ] && [ -n "$name" ] && [ -n "$valuefile" ] ||
  refuse "--context, --namespace, --name and --value-file are required"
dns1123='^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?(\.[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*$'
keyname='^[A-Za-z0-9_-][A-Za-z0-9._-]{0,252}$'
[[ "$name" =~ $dns1123 ]] && [ "${#name}" -le 253 ] && [[ "$key" =~ $keyname ]] ||
  refuse "--name or --key is not a valid name"

refuse_file() { refuse "value file $1"; }
[ -L "$valuefile" ] && refuse_file "is a symlink"
[ -f "$valuefile" ] || refuse_file "is not a regular file"
# Pin the file: every later check and the read use the open descriptor.
exec 3<"$valuefile" || refuse_file "cannot be opened"
same_file() { [ "$(stat -L -c %d:%i /proc/self/fd/3)" = "$(stat -c %d:%i "$valuefile" 2>/dev/null)" ]; }
same_file || refuse_file "changed while being opened"
[ "$(stat -L -c %a /proc/self/fd/3)" = "600" ] || refuse_file "is not mode 0600"
[ "$(stat -L -c %u /proc/self/fd/3)" = "$(id -u)" ] || refuse_file "is not owned by the caller"
[ "$(stat -L -f -c %T /proc/self/fd/3)" = "tmpfs" ] || refuse_file "is not on tmpfs"

size="$(stat -L -c %s /proc/self/fd/3)"
IFS= read -r line <&3 || true
# one line: the file is the line, plus at most one trailing newline
[ "$size" -eq "${#line}" ] || [ "$size" -eq $((${#line} + 1)) ] || refuse_file "is not one line"
[[ "$line" =~ ^svc_authsess_[A-Za-z0-9_-]{43}$ ]] || refuse_file "does not have the credential shape"

base="${SESSION_ISSUE_WORKDIR_BASE:-${XDG_RUNTIME_DIR:-/dev/shm}}"
[ -d "$base" ] || refuse "no private work directory"
[ "$(stat -f -c %T "$base")" = "tmpfs" ] || refuse "work directory is not tmpfs"

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
if [ "$rc" -ne 0 ] && [ "$sigrc" -ne 0 ]; then
  kill "$kpid" 2>/dev/null || true
  wait "$kpid" 2>/dev/null || true
  kpid=""
  printf 'STOP %s interrupted\n' "$name"
  exit "$sigrc"
fi
kpid=""
if [ "$rc" -ne 0 ]; then
  if grep -q 'AlreadyExists' "$errfile"; then
    printf 'STOP %s exists\n' "$name"
  else
    printf 'STOP %s create failed\n' "$name"
  fi
  exit 1
fi

created=1
# No signal may cut the shred short: TERM and INT are ignored (and so inherited
# by shred) until it is done. A path that no longer names the pinned file is
# left alone and reported as not removed.
trap '' INT TERM
if same_file && [ ! -L "$valuefile" ]; then
  exec 3<&-
  shred -u "$valuefile" 2>/dev/null || rm -f "$valuefile" 2>/dev/null || true
fi
if [ ! -e "$valuefile" ] && [ ! -L "$valuefile" ]; then
  removed=1
fi
exit 0
