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
printf 'secret/%s created\n' "$name"
FAKE
chmod +x "$t/bin/kubectl"
cat >"$t/values.yaml" <<'V'
authService:
  keySet:
    keys:
      - kid: a
        secretName: key-a
        secretKey: signing-key.pem
      - kid: b
        secretName: key-b
        secretKey: signing-key.pem
V
export PATH="$t/bin:$PATH" KEYSET_WORKDIR_BASE="$t/work" FAKE_ARGV="$t/argv" FAKE_VALUES="$t/values.got"
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
run() { : >"$FAKE_ARGV"; : >"$FAKE_VALUES"; "$script" --context ctx --namespace ns --values "$t/values.yaml" "$@" >"$t/out" 2>"$t/err" || return $?; }

run --key key-a:signing-key.pem --key key-b:signing-key.pem || fail "happy path exit"
[ "$(grep -c created "$t/out")" -eq 3 ] || { cat "$t/out"; fail "expected two created lines and the removed line"; }
grep -q 'value files removed (2 created)' "$t/out" || fail "removal line"
[ -z "$(ls -A "$t/work")" ] || fail "work directory not empty after success"
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
[ -z "$(ls -A "$t/work")" ] || fail "work directory not empty after stop"
grep -q 'created' "$t/out" && fail "claimed created after AlreadyExists"

# Any other failure stops and cleans.
FAKE_FAIL=1 run --key key-a:signing-key.pem && fail "failure must exit non-zero"
grep -q 'STOP key-a create failed' "$t/out" || fail "failure message"
[ -z "$(ls -A "$t/work")" ] || fail "work directory not empty after failure"

# Refusals: nothing is created.
refuse() { run "$@" && fail "should refuse: $*"; [ ! -s "$FAKE_ARGV" ] || fail "kubectl ran on a refusal: $*"; }
refuse --key other:signing-key.pem
refuse --key key-a:other.pem
refuse --key 'Key_A:signing-key.pem'
refuse --key key-a
refuse --key key-a:signing-key.pem --key key-a:signing-key.pem
: >"$FAKE_ARGV"
"$script" --namespace ns --values "$t/values.yaml" --key key-a:signing-key.pem >/dev/null 2>&1 && fail "context is required"
"$script" --context c --namespace ns --values "$t/missing" --key key-a:signing-key.pem >/dev/null 2>&1 && fail "values file is required"
bash -x "$script" --context c --namespace ns --values "$t/values.yaml" --key key-a:signing-key.pem >/dev/null 2>&1 && fail "xtrace must be refused"
[ ! -s "$FAKE_ARGV" ] || fail "kubectl ran on a refusal"
printf 'ok\n'
