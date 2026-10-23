#!/usr/bin/env bash
# No cluster: a fake kubectl records argv and the value it was handed.
set -euo pipefail
here="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
script="$here/create-auth-keyset-secrets.sh"
t="$(mktemp -d)"
trap 'rm -rf "$t"' EXIT
mkdir -p "$t/bin" "$t/work"
cat >"$t/bin/kubectl" <<'FAKE'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$FAKE_ARGV"
name=""
for a in "$@"; do case "$a" in --from-file=*) f="${a##*=}"; cat "$f" >>"$FAKE_VALUES" ;; esac; done
for a in "$@"; do case "$a" in generic) name="next" ;; *) [ "$name" = next ] && name="$a" && break ;; esac; done
if [ -n "${FAKE_EXISTS:-}" ] && [ "$name" = "$FAKE_EXISTS" ]; then
  printf 'Error from server (AlreadyExists): secrets "%s" already exists\n' "$name" >&2
  exit 1
fi
[ -z "${FAKE_FAIL:-}" ] || { printf 'boom\n' >&2; exit 1; }
if [ -n "${FAKE_FAIL_ON:-}" ] && [ "$name" = "$FAKE_FAIL_ON" ]; then printf 'boom\n' >&2; exit 1; fi
printf 'secret/%s created\n' "$name"
FAKE
chmod +x "$t/bin/kubectl"
cat >"$t/values.yaml" <<'V'
authService:
  note: |
    - secretName: key-s
      secretKey: signing-key.pem
  extraVolumes:
    - secretName: key-v
      secretKey: signing-key.pem
  keySet:
    enabled: true
    keys:
      - kid: a
        secretName: key-a
        secretKey: signing-key.pem
        note: |
          - secretName: key-s2
            secretKey: signing-key.pem
      - kid: b
        secretName: key-b
        secretKey: signing-key.pem
      - kid: c
        secretName: key-c
        secretKey: other-key.pem
      - kid: d
        secretName: key-d
        secretKey: kubectl.err
      - kid: e
        secretName: Key_E
        secretKey: signing-key.pem
      - kid: f
        secretName: key-f
        secretKey: bad key
      - kid: g
        secretName: kéy-g
        secretKey: signing-key.pem
      - kid: h
        secretName: key-h
        secretKey: kéy.pem
      - kid: i
        secretName: key-i
        secretKey: .
      - kid: j
        secretName: key-j
        secretKey: ..
  other:
    keys:
      - secretName: key-x
        secretKey: signing-key.pem
  database:
    secretName: key-z
    secretKey: db.txt
ingress:
  tls:
    - secretName: key-t
      secretKey: signing-key.pem
V
export LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8
export PATH="$t/bin:$PATH" KEYSET_WORKDIR_BASE="$t/work" FAKE_ARGV="$t/argv" FAKE_VALUES="$t/values.got"
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
run() { : >"$FAKE_ARGV"; : >"$FAKE_VALUES"; "$script" --context ctx --namespace ns --values "$t/values.yaml" "$@" >"$t/out" 2>"$t/err" || return $?; }

run --key key-a:signing-key.pem --key key-b:signing-key.pem || fail "happy path exit"
[ "$(grep -c created "$t/out")" -eq 3 ] || { cat "$t/out"; fail "expected two created lines and the removed line"; }
grep -q 'value files removed (2 created)' "$t/out" || fail "removal line"
[ -z "$(find "$t/work" -mindepth 1 | head -1)" ] || fail "work directory not empty after success"
[ "$(wc -l <"$FAKE_ARGV")" -eq 2 ] || fail "kubectl call count"
grep -q 'create secret generic key-a' "$FAKE_ARGV" || fail "no create verb"
grep -Eq ' (apply|replace|patch|get|describe) ' "$FAKE_ARGV" && fail "a verb other than create"
# No value byte reaches stdout, stderr or argv.
[ -s "$FAKE_VALUES" ] || fail "fake kubectl saw no value"
grep -q 'BEGIN PRIVATE KEY' "$FAKE_VALUES" || fail "value is not a PEM"
while IFS= read -r line; do
  case "$line" in -----*) continue ;; esac
  [ -n "$line" ] || continue
  grep -qF -- "$line" "$t/out" "$t/err" "$FAKE_ARGV" && fail "a value line reached output or argv"
done <"$FAKE_VALUES"
grep -q 'PRIVATE KEY' "$t/out" "$t/err" "$FAKE_ARGV" && fail "PEM marker in output or argv"

# AlreadyExists stops, later keys are not tried, files are removed.
FAKE_EXISTS=key-a run --key key-a:signing-key.pem --key key-b:signing-key.pem && fail "exists must exit non-zero"
grep -q 'STOP key-a exists' "$t/out" || fail "exists message"
[ "$(wc -l <"$FAKE_ARGV")" -eq 1 ] || fail "continued after AlreadyExists"
[ -z "$(find "$t/work" -mindepth 1 | head -1)" ] || fail "work directory not empty after stop"
grep -q 'created' "$t/out" && fail "claimed created after AlreadyExists"

# Any other failure stops and cleans.
FAKE_FAIL=1 run --key key-a:signing-key.pem && fail "failure must exit non-zero"
grep -q 'STOP key-a create failed' "$t/out" || fail "failure message"
[ -z "$(find "$t/work" -mindepth 1 | head -1)" ] || fail "work directory not empty after failure"

# A failure after an earlier Secret was created leaves that Secret alone.
FAKE_FAIL_ON=key-b run --key key-a:signing-key.pem --key key-b:signing-key.pem && fail "later failure must exit non-zero"
grep -q 'key-a key=signing-key.pem created' "$t/out" || fail "earlier create not reported"
grep -q 'STOP key-b create failed' "$t/out" || fail "later failure message"
grep -Eq ' (delete|replace|apply|patch) ' "$FAKE_ARGV" && fail "an earlier Secret was touched after a later failure"
[ -z "$(find "$t/work" -mindepth 1 | head -1)" ] || fail "work directory not empty after a later failure"

# A hidden key file name is removed on failure.
cat >"$t/hidden.yaml" <<'V'
authService:
  keySet:
    keys:
      - kid: h
        secretName: key-h
        secretKey: .signing-key.pem
V
FAKE_FAIL=1 "$script" --context ctx --namespace ns --values "$t/hidden.yaml" --key key-h:.signing-key.pem >"$t/out" 2>"$t/err" && fail "hidden key failure must exit non-zero"
grep -q 'STOP key-h create failed' "$t/out" || fail "the hidden key reached create"
[ -z "$(find "$t/work" -mindepth 1 | head -1)" ] || fail "a hidden value file survived a failure"

# A key named like the stderr capture file still carries its value.
run --key key-d:kubectl.err || fail "kubectl.err key"
grep -q 'key-d key=kubectl.err created' "$t/out" || fail "kubectl.err key not created"
grep -q 'BEGIN PRIVATE KEY' "$FAKE_VALUES" || fail "an empty value was handed to kubectl for a key named like the stderr file"
[ -z "$(find "$t/work" -mindepth 1 | head -1)" ] || fail "work files left after the kubectl.err key"

# Refusals: nothing is created.
refuse() { run "$@" && fail "should refuse: $*"; [ ! -s "$FAKE_ARGV" ] || fail "kubectl ran on a refusal: $*"; }
refuse --key other:signing-key.pem
refuse --key key-a:other.pem
refuse --key 'Key_A:signing-key.pem'
refuse --key key-a
refuse --key key-a:signing-key.pem --key key-a:signing-key.pem
# Listed in the values file, refused by the name regexes alone.
refuse --key Key_E:signing-key.pem
refuse --key 'key-f:bad key'
refuse --key 'kéy-g:signing-key.pem'
refuse --key 'key-h:kéy.pem'
refuse --key key-i:.
refuse --key key-j:..
# Entries outside keySet.keys do not count: a block scalar, another list, a plain map.
refuse --key key-s:signing-key.pem
refuse --key key-v:signing-key.pem
refuse --key key-t:signing-key.pem
refuse --key key-s2:signing-key.pem
refuse --key key-x:signing-key.pem
refuse --key key-a:other-key.pem
refuse --key key-c:signing-key.pem
refuse --key key-z:db.txt
: >"$FAKE_ARGV"
"$script" --namespace ns --values "$t/values.yaml" --key key-a:signing-key.pem >/dev/null 2>&1 && fail "context is required"
"$script" --context c --namespace ns --values "$t/missing" --key key-a:signing-key.pem >/dev/null 2>&1 && fail "values file is required"
bash -x "$script" --context c --namespace ns --values "$t/values.yaml" --key key-a:signing-key.pem >/dev/null 2>&1 && fail "xtrace must be refused"
[ ! -s "$FAKE_ARGV" ] || fail "kubectl ran on a refusal"
printf 'ok\n'
