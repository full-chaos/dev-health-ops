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

# listed NAME KEY: succeeds when ONE entry of authService.keySet.keys (or
# ops.authService.keySet.keys, the deploy umbrella chart's alias) in the FIRST
# YAML document of the values file carries both secretName: NAME and
# secretKey: KEY as direct fields of a direct item of keys.
# Block scalars need no special case: only items at the direct indent of keys
# count, and a block scalar's lines are always indented deeper than that.
# Limits, all failing closed (a false refusal, never a false accept): a quoted
# scalar left open on a line under that path, a flow-style keys value, CRLF line
# endings, a `---` after content ends the document being read.
listed() {
  awk -v want_name="$1" -v want_key="$2" '
    function clean(v) { sub(/[[:space:]]+#.*$/, "", v); gsub(/^[[:space:]]+|[[:space:]]+$/, "", v); gsub(/^"|"$/, "", v); return v }
    function path(   i, p) { p = ""; for (i = 1; i <= depth; i++) p = p "/" sk[i]; return p }
    function is_keys(   p) { p = path(); return (p == "/authService/keySet/keys" || p == "/ops/authService/keySet/keys") }
    function pop_to(limit) { while (depth > 0 && si[depth] >= limit) depth-- }
    function open_quote(v,   q, n, i, c) {
      q = substr(v, 1, 1)
      if (q != "\"" && q != "\x27") { return "" }
      n = length(v)
      for (i = 2; i <= n; i++) {
        c = substr(v, i, 1)
        if (q == "\"" && c == "\\") { i++; continue }
        if (q == "\x27" && c == "\x27") { if (substr(v, i + 1, 1) == "\x27") { i++; continue } ; return "" }
        if (q == "\"" && c == "\"") { return "" }
      }
      return q
    }
    function value_of(line,   v) {
      v = line
      if (sub(/^ *- +/, "", v) && v !~ /^[^ #][^:]*:( |$)/) { return v }
      if (match(v, /:[[:space:]]+/)) { return substr(v, RSTART + RLENGTH) }
      return ""
    }
    /^---([[:space:]]|$)/ || /^\.\.\.([[:space:]]|$)/ { if (content) { stop = 1 } ; next }
    stop { next }
    /^[[:space:]]*(#|$)/ { next }
    {
      content = 1
      line = $0
      ind = match(line, /[^ ]/) - 1
      if (qopen != "") { if (index(line, qopen) > 0) { qopen = "" } ; next }
      isdash = (line ~ /^ *- /)
      eff = isdash ? ind + 2 : ind
      v = value_of(line)
      oq = open_quote(v)
      if (oq != "") { qopen = oq; if (is_keys()) { bad = 1 } }
      if (isdash) {
        pop_to(ind + 1)
        if (is_keys()) {
          if (kd < 0) { kd = ind }
          if (ind != kd) { next }
          dash = ind; active = 1; name = ""; key = ""
        } else { active = 0 }
        sub(/^ *- /, "", line); ind = eff
      } else {
        if (active && ind <= dash) { active = 0 }
        if (active && ind > dash) {
          if (ind != dash + 2) { next }
        } else {
          if (match(line, /^ *[^ #][^:]*:( |$)/)) {
            k = line; sub(/^ */, "", k); sub(/:( .*)?$/, "", k)
            pop_to(ind)
            depth++; sk[depth] = k; si[depth] = ind
            if (is_keys()) { kd = -1 }
          }
          next
        }
      }
      if (active && ind == dash + 2) {
        if (line ~ /^ *secretName:/) { v = line; sub(/^ *secretName:/, "", v); name = clean(v) }
        if (line ~ /^ *secretKey:/) { v = line; sub(/^ *secretKey:/, "", v); key = clean(v) }
        if (name == want_name && key == want_key) { found = 1 }
      }
    }
    BEGIN { qopen = ""; kd = -1 }
    END { exit (bad || !found) ? 1 : 0 }
  ' "$values"
}

dns1123='^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$'
keyname='^[A-Za-z0-9._-]{1,253}$'
seen=" "
for pair in "${keys[@]}"; do
  name="${pair%%:*}"
  key="${pair#*:}"
  case "$key" in
    ..*) printf 'refused: a secret key must not be "." or start with ".."\n' >&2; exit 2 ;;
    .) printf 'refused: a secret key must not be "." or start with ".."\n' >&2; exit 2 ;;
  esac
  if [ "$name" = "$pair" ] || ! [[ "$name" =~ $dns1123 ]] || [ "${#name}" -gt 253 ] || ! [[ "$key" =~ $keyname ]]; then
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
errdir="$(mktemp -d "${base%/}/auth-keyset-err.XXXXXX")" || {
  rmdir "$workdir" 2>/dev/null || true
  printf 'refused: no private error directory\n' >&2
  exit 2
}

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
