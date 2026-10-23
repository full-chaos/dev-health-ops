#!/usr/bin/env bash
# Create-only host script for the auth-service key-set Secrets.
# Generates one Ed25519 PKCS#8 key per --key into a 0600 file, creates the
# Secret with `kubectl create` from that file, then removes the file. It reads
# no Secret and prints no value: only Secret names, key names and "created".
#
#   create-auth-keyset-secrets.sh --context C --namespace N --values FILE \
#       --key <secret-name>:<secret-key> [--key ...]
#
# FILE is the deploy values file; every --key must appear in it as a
# secretName and a secretKey line.
set -euo pipefail
umask 077
export LC_ALL=C

case $- in
  *x*) printf 'refused: xtrace is on\n' >&2; exit 2 ;;
esac

context="" namespace="" values="" keys=()
while [ "$#" -gt 0 ]; do
  case "$1" in
    --context) context="${2:-}"; shift 2 ;;
    --namespace) namespace="${2:-}"; shift 2 ;;
    --values) values="${2:-}"; shift 2 ;;
    --key) keys+=("${2:-}"); shift 2 ;;
    *) printf 'refused: unknown argument\n' >&2; exit 2 ;;
  esac
done

[ -n "$context" ] && [ -n "$namespace" ] && [ -n "$values" ] && [ "${#keys[@]}" -gt 0 ] || {
  printf 'refused: --context, --namespace, --values and at least one --key are required\n' >&2
  exit 2
}
[ -f "$values" ] || { printf 'refused: --values is not a file\n' >&2; exit 2; }

# listed NAME KEY: succeeds when ONE entry of keySet.keys in the values file
# carries both secretName: NAME and secretKey: KEY as direct fields.
listed() {
  awk -v want_name="$1" -v want_key="$2" '
    function clean(v) { sub(/[[:space:]]+#.*$/, "", v); gsub(/^[[:space:]]+|[[:space:]]+$/, "", v); gsub(/^"|"$/, "", v); return v }
    /^[[:space:]]*(#|$)/ { next }
    {
      line = $0
      ind = match(line, /[^ ]/) - 1
      if (blk >= 0 && ind > blk) { next }
      blk = -1
      if (line ~ /:[[:space:]]*[|>][-+0-9]*[[:space:]]*$/) { blk = ind }
      if (inks && ind <= ksind) { inks = 0; inkeys = 0 }
      if (inkeys && ind < keysind) { inkeys = 0; active = 0 }
      if (line ~ /^ *keySet:[[:space:]]*$/) { inks = 1; ksind = ind; inkeys = 0; active = 0; next }
      if (inks && !inkeys && line ~ /^ *keys:[[:space:]]*$/ && ind > ksind) { inkeys = 1; keysind = ind; active = 0; next }
      if (!inkeys) { next }
      if (line ~ /^ *- /) {
        dash = ind; active = 1; name = ""; key = ""
        sub(/^ *- /, "", line); ind = dash + 2
      } else if (active && ind <= dash) {
        active = 0
      }
      if (active && ind == dash + 2) {
        if (line ~ /^ *secretName:/) { v = line; sub(/^ *secretName:/, "", v); name = clean(v) }
        if (line ~ /^ *secretKey:/) { v = line; sub(/^ *secretKey:/, "", v); key = clean(v) }
        if (name == want_name && key == want_key) { found = 1 }
      }
    }
    BEGIN { blk = -1 }
    END { exit found ? 0 : 1 }
  ' "$values"
}

dns1123='^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$'
keyname='^[A-Za-z0-9._-]{1,253}$'
seen=" "
for pair in "${keys[@]}"; do
  name="${pair%%:*}"
  key="${pair#*:}"
  if [ "$name" = "$pair" ] || ! [[ "$name" =~ $dns1123 ]] || [ "${#name}" -gt 253 ] || ! [[ "$key" =~ $keyname ]] || [ "$key" = . ] || [ "$key" = .. ]; then
    printf 'refused: --key must be <secret-name>:<secret-key> with valid names\n' >&2
    exit 2
  fi
  case "$seen" in *" $pair "*) printf 'refused: repeated --key\n' >&2; exit 2 ;; esac
  seen="$seen$pair "
  if ! listed "$name" "$key"; then
    printf 'refused: %s key=%s is not one entry of the values file\n' "$name" "$key" >&2
    exit 2
  fi
done

base="${KEYSET_WORKDIR_BASE:-${XDG_RUNTIME_DIR:-/dev/shm}}"
[ -d "$base" ] || { printf 'refused: no private work directory\n' >&2; exit 2; }
workdir="$(mktemp -d "${base%/}/auth-keyset.XXXXXX")"
errdir="$(mktemp -d "${base%/}/auth-keyset-err.XXXXXX")"

cleanup() {
  local f
  shopt -s dotglob nullglob
  for f in "$workdir"/*; do
    [ -e "$f" ] || continue
    shred -u "$f" 2>/dev/null || rm -f "$f"
  done
  rm -f "$errdir/kubectl.err"
  rmdir "$workdir" "$errdir" 2>/dev/null || true
}
trap cleanup EXIT

kubectl_bin="${KUBECTL:-kubectl}"
created=0
for pair in "${keys[@]}"; do
  name="${pair%%:*}"
  key="${pair#*:}"
  value="$workdir/$key"
  openssl genpkey -algorithm ED25519 -out "$value" >/dev/null 2>&1 || { printf 'STOP %s key generation failed\n' "$name"; exit 1; }
  chmod 0600 "$value"
  if "$kubectl_bin" --context "$context" --namespace "$namespace" create secret generic "$name" \
    --from-file="${key}=${value}" >/dev/null 2>"$errdir/kubectl.err"; then
    printf '%s key=%s created\n' "$name" "$key"
    created=$((created + 1))
  else
    if grep -q 'AlreadyExists' "$errdir/kubectl.err"; then
      printf 'STOP %s exists\n' "$name"
    else
      printf 'STOP %s create failed\n' "$name"
    fi
    exit 1
  fi
  shred -u "$value" 2>/dev/null || rm -f "$value"
  rm -f "$errdir/kubectl.err"
done
printf 'value files removed (%d created)\n' "$created"
