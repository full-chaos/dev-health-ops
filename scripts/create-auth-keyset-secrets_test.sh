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
utf8_locale="$(locale -a 2>/dev/null | grep -iE '^en_US\.utf-?8$' | head -1 || true)"
[ -n "$utf8_locale" ] || utf8_locale="$(locale -a 2>/dev/null | grep -iE '^c\.utf-?8$' | head -1 || true)"
if [ -n "$utf8_locale" ]; then export LANG="$utf8_locale" LC_ALL="$utf8_locale"; else printf 'SKIP: no UTF-8 locale; the non-ASCII refusals are not exercised\n'; fi
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
refuse_msg() {
  want="$1"; shift
  run "$@" && fail "should refuse: $*"
  grep -qF -- "$want" "$t/err" || { cat "$t/err"; fail "refusal lacks message $want for $*"; }
  [ ! -s "$FAKE_ARGV" ] || fail "kubectl ran on a refusal: $*"
}
refuse_in() {
  file="$1"; shift
  : >"$FAKE_ARGV"
  "$script" --context ctx --namespace ns --values "$file" "$@" >"$t/out" 2>"$t/err" && fail "should refuse in $file: $*"
  grep -q refused "$t/err" || fail "no refusal message in $file: $*"
  [ ! -s "$FAKE_ARGV" ] || fail "kubectl ran on a refusal in $file"
}
refuse() { run "$@" && fail "should refuse: $*"; [ ! -s "$FAKE_ARGV" ] || fail "kubectl ran on a refusal: $*"; }
refuse --key other:signing-key.pem
refuse --key key-a:other.pem
refuse --key 'Key_A:signing-key.pem'
refuse --key key-a
refuse --key key-a:signing-key.pem --key key-a:signing-key.pem
# Listed in the values file, refused by the name regexes alone.
refuse_msg 'valid names' --key Key_E:signing-key.pem
refuse_msg 'valid names' --key 'key-f:bad key'
if [ -n "$utf8_locale" ]; then
  refuse_msg 'valid names' --key 'kéy-g:signing-key.pem'
  refuse_msg 'valid names' --key 'key-h:kéy.pem'
fi
refuse_msg 'must not be "."' --key key-i:.
refuse_msg 'must not be "."' --key key-j:..
refuse_msg 'must not be "."' --key key-k:..data
# Entries outside keySet.keys do not count: a block scalar, another list, a plain map.
refuse --key key-s:signing-key.pem
refuse --key key-v:signing-key.pem
refuse --key key-t:signing-key.pem
refuse --key key-s2:signing-key.pem
refuse --key key-x:signing-key.pem
refuse --key key-a:other-key.pem
refuse --key key-c:signing-key.pem
refuse --key key-z:db.txt

# Path anchoring and unreadable shapes fail closed (each file lists a decoy that a looser reader would accept).
mkv() { cat >"$t/$1"; }
mkv sib.yaml <<'V'
authService:
  keySet:
    keys:
      - secretName: key-a
        secretKey: signing-key.pem
    retired:
      - secretName: key-r
        secretKey: b.pem
V
refuse_in "$t/sib.yaml" --key key-r:b.pem
mkv nested.yaml <<'V'
authService:
  keySet:
    nested:
      keys:
        - secretName: key-n
          secretKey: n.pem
V
refuse_in "$t/nested.yaml" --key key-n:n.pem
mkv other.yaml <<'V'
queryApi:
  keySet:
    keys:
      - secretName: key-q
        secretKey: q.pem
V
refuse_in "$t/other.yaml" --key key-q:q.pem
mkv root.yaml <<'V'
keySet:
  keys:
    - secretName: key-q
      secretKey: q.pem
V
refuse_in "$t/root.yaml" --key key-q:q.pem
for ind in '|' '| # comment' '!!str |' '>-' '|2' '|+'; do
  {
    printf 'authService:\n  keySet:\n    keys:\n      - secretName: key-a\n        secretKey: signing-key.pem\n        note: %s\n' "$ind"
    printf '          - secretName: key-bs\n            secretKey: s.pem\n'
  } >"$t/bs.yaml"
  refuse_in "$t/bs.yaml" --key key-bs:s.pem
done
mkv multi.yaml <<'V'
authService:
  keySet:
    keys:
      - secretName: key-a
        secretKey: signing-key.pem
        note: "first line
          - secretName: key-mq
            secretKey: m.pem
          last"
V
refuse_in "$t/multi.yaml" --key key-mq:m.pem
refuse_in "$t/multi.yaml" --key key-a:signing-key.pem
mkv flow.yaml <<'V'
authService:
  keySet:
    keys: [{secretName: key-fl, secretKey: f.pem}]
V
refuse_in "$t/flow.yaml" --key key-fl:f.pem

# Prod shape: the deploy umbrella chart aliases ops, so authService sits under ops:.
mkv prod.yaml <<'V'
global:
  note: "a: b"
ops:
  # CHAOS-10624: comments with colons: and - dashes
  authService:
    enabled: true
    image:
      repository: ghcr.io/full-chaos/dev-health-go-auth-service@sha256:b9ac
    signingKey:
      secretName: auth-signing-key
      keyId: auth-prod-2026-10
    keySet:
      enabled: true
      keys:
        - kid: auth-prod-2026-10
          state: active
          file: auth-prod-2026-10.pem
          secretName: auth-signing-key
          secretKey: signing-key.pem
        - kid: auth-prod-2026-11
          state: pending
          file: auth-prod-2026-11.pem
          secretName: auth-signing-key-2
          secretKey: signing-key.pem
    networkPolicy:
      egress:
        database:
          to:
            - podSelector:
                matchLabels:
                  app: pg
V
: >"$FAKE_ARGV"; : >"$FAKE_VALUES"
"$script" --context ctx --namespace ns --values "$t/prod.yaml" --key auth-signing-key-2:signing-key.pem >"$t/out" 2>"$t/err" || { cat "$t/err"; fail "prod-shaped values file refused"; }
grep -q 'auth-signing-key-2 key=signing-key.pem created' "$t/out" || fail "prod-shaped create"
refuse_in "$t/prod.yaml" --key auth-signing-key-2:other.pem

# Direct items only, scalar shapes, escapes and a second document.
mkv alias.yaml <<'V'
authService:
  keySet:
    keys:
      - secretName: key-a
        secretKey: signing-key.pem
        aliases:
          - secretName: key-n
            secretKey: n.pem
V
refuse_in "$t/alias.yaml" --key key-n:n.pem
for first in 'note: &n |' 'note: !!str &n |' '- |' '-   >-'; do
  case "$first" in
    -*) body=$(printf '        extra:\n          %s\n            - secretName: key-an\n              secretKey: a.pem\n' "$first") ;;
    *) body=$(printf '        %s\n          - secretName: key-an\n            secretKey: a.pem\n' "$first") ;;
  esac
  printf 'authService:\n  keySet:\n    keys:\n      - secretName: key-a\n        secretKey: signing-key.pem\n%s\n' "$body" >"$t/anch.yaml"
  refuse_in "$t/anch.yaml" --key key-an:a.pem
done
mkv escq.yaml <<'V'
authService:
  keySet:
    keys:
      - secretName: key-a
        secretKey: signing-key.pem
        note: "a \"
          - secretName: key-eq
            secretKey: e.pem
          z"
V
refuse_in "$t/escq.yaml" --key key-eq:e.pem
refuse_in "$t/escq.yaml" --key key-a:signing-key.pem
mkv sq.yaml <<'V'
authService:
  keySet:
    keys:
      - secretName: key-a
        secretKey: signing-key.pem
        note: 'it''s
          - secretName: key-sq
            secretKey: s.pem
          z'
V
refuse_in "$t/sq.yaml" --key key-sq:s.pem
refuse_in "$t/sq.yaml" --key key-a:signing-key.pem
mkv doc2.yaml <<'V'
authService:
  keySet:
    keys:
      - secretName: key-a
        secretKey: signing-key.pem
---
authService:
  keySet:
    keys:
      - secretName: key-d2
        secretKey: d.pem
V
refuse_in "$t/doc2.yaml" --key key-d2:d.pem
printf 'authService:\r\n  keySet:\r\n    keys:\r\n      - secretName: key-a\r\n        secretKey: signing-key.pem\r\n' >"$t/crlf.yaml"
refuse_in "$t/crlf.yaml" --key key-a:signing-key.pem

# An inline value hides every deeper line: block scalars and flow values holding fake lists.
for ind in '|' '>-' '!!str |' '&a |' '|2'; do
  printf 'authService:\n  keySet:\n    keys: %s\n      %s\n        %s\n' "$ind" "- secretName: key-evil" "secretKey: e.pem" >"$t/blk.yaml"
  refuse_in "$t/blk.yaml" --key key-evil:e.pem
done
printf 'authService:\n  keySet: |\n    keys:\n      - secretName: key-evil\n        secretKey: e.pem\n' >"$t/blk.yaml"
refuse_in "$t/blk.yaml" --key key-evil:e.pem
printf 'authService: |\n  keySet:\n    keys:\n      - secretName: key-evil\n        secretKey: e.pem\n' >"$t/blk.yaml"
refuse_in "$t/blk.yaml" --key key-evil:e.pem
printf 'ops: >\n  authService:\n    keySet:\n      keys:\n        - secretName: key-evil\n          secretKey: e.pem\n' >"$t/blk.yaml"
refuse_in "$t/blk.yaml" --key key-evil:e.pem
printf 'authService:\n  keySet:\n    keys:\n      - secretName: key-a\n        secretKey: signing-key.pem\n      - |\n        secretName: key-evil\n        secretKey: e.pem\n' >"$t/blk.yaml"
refuse_in "$t/blk.yaml" --key key-evil:e.pem
# Anchors and comments on a parent are not inline values: the real file still reads.
printf 'authService:\n  keySet: &ks # c\n    keys: # c\n      - secretName: key-an\n        secretKey: a.pem\n' >"$t/anc.yaml"
: >"$FAKE_ARGV"
"$script" --context ctx --namespace ns --values "$t/anc.yaml" --key key-an:a.pem >"$t/out" 2>"$t/err" || { cat "$t/err"; fail "anchored parents refused"; }

# Quote continuation lines use the same escape rules.
printf 'authService:\n  keySet:\n    keys:\n      - secretName: key-a\n        secretKey: signing-key.pem\n        note: "x\n          \\"\n      - secretName: key-p2\n        secretKey: p.pem\n          z"\n' >"$t/q2.yaml"
refuse_in "$t/q2.yaml" --key key-p2:p.pem
refuse_in "$t/q2.yaml" --key key-a:signing-key.pem
printf "authService:\n  keySet:\n    keys:\n      - secretName: key-a\n        secretKey: signing-key.pem\n        note: 'x\n          it''s\n      - secretName: key-p3\n        secretKey: p.pem\n          z'\n" >"$t/q3.yaml"
refuse_in "$t/q3.yaml" --key key-p3:p.pem
refuse_in "$t/q3.yaml" --key key-a:signing-key.pem

printf 'authService:\n  description: "x\n    \\"\n  keySet:\n    keys:\n      - secretName: key-p4\n        secretKey: p.pem\n    z"\n' >"$t/q4.yaml"
refuse_in "$t/q4.yaml" --key key-p4:p.pem
printf "authService:\n  description: 'x\n    it''s\n  keySet:\n    keys:\n      - secretName: key-p5\n        secretKey: p.pem\n    z'\n" >"$t/q5.yaml"
refuse_in "$t/q5.yaml" --key key-p5:p.pem

# A failing second mktemp removes the first work directory.
mkdir -p "$t/mk"
cat >"$t/mk/mktemp" <<'FAKE'
#!/usr/bin/env bash
n=$(cat "$MK_COUNT" 2>/dev/null || echo 0); n=$((n + 1)); echo "$n" >"$MK_COUNT"
[ "$n" -ge 2 ] && exit 1
exec /usr/bin/mktemp "$@"
FAKE
chmod +x "$t/mk/mktemp"
: >"$FAKE_ARGV"; rm -f "$t/mkcount"
MK_COUNT="$t/mkcount" PATH="$t/mk:$PATH" "$script" --context ctx --namespace ns --values "$t/values.yaml" --key key-a:signing-key.pem >"$t/out" 2>"$t/err" && fail "mktemp failure must exit non-zero"
[ -z "$(find "$t/work" -mindepth 1 | head -1)" ] || fail "work directory left after the error directory failed"
[ ! -s "$FAKE_ARGV" ] || fail "kubectl ran after the error directory failed"

: >"$FAKE_ARGV"
"$script" --namespace ns --values "$t/values.yaml" --key key-a:signing-key.pem >/dev/null 2>&1 && fail "context is required"
"$script" --context c --namespace ns --values "$t/missing" --key key-a:signing-key.pem >/dev/null 2>&1 && fail "values file is required"
bash -x "$script" --context c --namespace ns --values "$t/values.yaml" --key key-a:signing-key.pem >/dev/null 2>&1 && fail "xtrace must be refused"
[ ! -s "$FAKE_ARGV" ] || fail "kubectl ran on a refusal"
printf 'ok\n'
