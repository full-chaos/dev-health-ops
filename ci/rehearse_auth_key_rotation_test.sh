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

printf 'DEV_HEALTH_GO_DHO_IMAGE=registry.invalid/d@sha256:%s\nDEV_HEALTH_GO_AUTH_SERVICE_IMAGE=registry.invalid/s@sha256:%s\n' "$(printf '1%.0s' $(seq 1 64))" "$(printf '1%.0s' $(seq 1 64))" >"${T}/previous.env"
if ! grep -qF 'hs256_compat' < <(grep -vE '^[[:space:]]*#' "${SCRIPT}"); then ok script-has-no-hs256-compat-class-flip; else bad script-has-no-hs256-compat-class-flip; fi
if grep -qF 'REHEARSAL_PREVIOUS_IMAGES_ENV' <<<"${plan}"; then ok plan-names-the-previous-images-file; else bad plan-names-the-previous-images-file; fi

rc=0
run bash -x "${SCRIPT}" --print-plan >/dev/null 2>&1 || rc=$?
if [ "${rc}" = 2 ]; then ok xtrace-refused; else bad "xtrace-refused (rc ${rc})"; fi

OVR="${ROOT}/ci/auth_rotation_rehearsal.override.yml"
# auth-service refuses the key-set file beside the single-key variables, even unset by !reset (smoke v2): they must be set empty.
for var in AUTH_SIGNING_KEY_FILE AUTH_SIGNING_KEY_ID; do
  if grep -qE "^      ${var}: \"\"\$" "${OVR}"; then ok "override-blanks-${var}"; else bad "override-blanks-${var}"; fi
done

# go-api and query-api read auth.sessions to accept an EdDSA access token (smoke v5: missing_grant SELECT on sessions): the
# migration jobs grant the session readers, as the chart does with authService.migrations.sessionReaders.
for job in auth-migrate auth-grants; do
  if awk -v job="${job}:" '$1 == job {on=1; next} /^  [a-z]/ {on=0} on && /AUTH_SESSION_READER_ROLES: devhealth_api,devhealth_query_api/ {found=1} END {exit !found}' "${OVR}"; then ok "override-grants-session-readers-${job}"; else bad "override-grants-session-readers-${job}"; fi
done

# query-api's role posture must expect the session read it is granted (smoke v6: "auth.sessions.id outside the manifest").
if awk '$1 == "query-api:" {on=1; next} /^  [a-z]/ {on=0} on && /AUTH_PROXY_SESSIONS: "true"/ {found=1} END {exit !found}' "${OVR}"; then ok override-query-api-expects-session-read; else bad override-query-api-expects-session-read; fi

# A recreated container gets new ephemeral host ports (smoke v7: login answered 000): every --force-recreate is followed by a port read.
recreates="$(grep -c -- '--force-recreate' "${SCRIPT}" || true)"
refreshes="$(grep -c '^ *refresh_ports$' "${SCRIPT}" || true)"
if [ "${recreates}" -gt 0 ] && [ "${refreshes}" -ge "${recreates}" ]; then ok every-recreate-rereads-the-ports; else bad "every-recreate-rereads-the-ports (${recreates} recreates, ${refreshes} reads)"; fi

# A function must be defined before its first call (smoke v8: "refresh_ports: command not found").
first_ref="$(grep -n 'refresh_ports' "${SCRIPT}" | grep -v '^[0-9]*:#' || true)"; first_ref="${first_ref%%$'\n'*}"; first_ref="${first_ref#*:}"; first_ref="${first_ref// /}"
if [ "${first_ref}" = 'refresh_ports(){' ]; then ok refresh-ports-defined-before-first-call; else bad refresh-ports-defined-before-first-call; fi

# A restarted container also gets new ephemeral host ports (drill v9: WINDOW_B read the old port, then "login answered 000").
if awk '/compose --profile auth restart/ {n=NR} n && NR<=n+3 && /refresh_ports/ {ok=1} END {exit !(n && ok)}' "${SCRIPT}"; then ok restart-rereads-the-ports; else bad restart-rereads-the-ports; fi
# A window line records the status code it judged, so a refusal that is really a dead port cannot read as a finding.
if grep -q 'WINDOW_B .*last_code=' "${SCRIPT}"; then ok window-b-records-the-code; else bad window-b-records-the-code; fi

# The host SMTP sink is unreachable from the bigboy docker bridge (smoke v3: no route to host): the user is verified in the
# throwaway database instead.
if grep -q 'auth_webproof_smtp_sink' "${SCRIPT}"; then bad no-host-smtp-sink; else ok no-host-smtp-sink; fi
if grep -q 'SET is_verified = true' "${SCRIPT}"; then ok verifies-the-user-in-the-database; else bad verifies-the-user-in-the-database; fi

# auth-service re-reads the key states every KeyStateRefresh (15 s, internal/token/states.go): a login right after a state change
# may still be signed by the old key (rotation v1: s1-new-token-signed-by-b). Login is limited to 10 per 15 min per address and
# account (internal/auth/authapi/limits.go), so a poll burns the budget (rotation v2: 429 after 10 logins): cells that expect a
# new signer wait one refresh, then log in once.
for cell in s1-new-token-signed-by-b s1-after-rollback-a-signs s1-forward-b-signs s1-b-signs-after-retire; do
  if grep -qE "^check ${cell} login_signs_by " "${SCRIPT}"; then ok "${cell}-waits-for-the-signer"; else bad "${cell}-waits-for-the-signer"; fi
done
if grep -qE 'check "s2-new-session-signed-by-\$\{new\}" login_signs_by ' "${SCRIPT}"; then ok s2-new-session-waits-for-the-signer; else bad s2-new-session-waits-for-the-signer; fi
fn="$(sed -n '/^login_signs_by() {/,/^}/p' "${SCRIPT}")"
if [ -n "${fn}" ] && ! grep -qE 'while|until|for ' <<<"${fn}"; then ok login-signs-by-does-not-poll-the-login; else bad login-signs-by-does-not-poll-the-login; fi
if [ -n "${fn}" ]; then
  probe="$(cat <<EOS
KEY_STATE_REFRESH=15
n=0
slept=0
login_user() { n=\$((n + 1)); L_ACCESS="t\${n}"; }
kid_is() { [ "\$1" = "t1" ]; }
sleep() { slept=\$1; }
${fn}
login_signs_by b && [ "\${n}" = 1 ] && [ "\${slept}" -ge 16 ] && echo waited-then-one-login
EOS
)"
  out="$(bash -c "${probe}" 2>&1 || true)"
  if grep -q waited-then-one-login <<<"${out}"; then ok login-signs-by-waits-a-refresh-then-logs-in-once; else bad login-signs-by-waits-a-refresh-then-logs-in-once; fi
else
  bad login-signs-by-waits-a-refresh-then-logs-in-once
fi

# The run id lands in a record: anything but a numeric CI run id is refused before any docker call.
for bad_id in "svc_authsess_$(printf '%043d' 0)" "postgresql://db.internal:5432/ops" "MC4CAQAwBQYDK2VwBCIEIGJhZGJhZGJhZGJhZGJhZGJhZGJhZGJhZGJhZGJhZGJh"; do
  rc=0
  out="$(REHEARSAL_RUN_ID="${bad_id}" REHEARSAL_DIR="${T}/rid" REHEARSAL_IMAGES_ENV="${T}/tag.env" run bash "${SCRIPT}" --stage smoke 2>&1)" || rc=$?
  if [ "${rc}" = 1 ] && grep -q 'REHEARSAL_RUN_ID' <<<"${out}" && [ ! -e "${T}/rid" ] && ! grep -qF -- "${bad_id}" <<<"${out}"; then ok run-id-refused-before-any-work; else bad "run-id-refused-before-any-work (rc ${rc})"; fi
done
rc=0
out="$(REHEARSAL_RUN_ID=12345 REHEARSAL_DIR="${T}/rid2" REHEARSAL_IMAGES_ENV="${T}/tag.env" run bash "${SCRIPT}" --stage smoke 2>&1)" || rc=$?
if [ "${rc}" = 1 ] && grep -q 'not a registry digest reference' <<<"${out}" && ! grep -q 'REHEARSAL_RUN_ID' <<<"${out}"; then ok numeric-run-id-passes-the-check; else bad "numeric-run-id-passes-the-check (rc ${rc})"; fi

# The S1 overlap probe asks go-api AND query-api on every tick, and the overlap check judges both: a query-api miss planted on
# one tick (go-api still 200) must fail the check the script names on the s1-no-downtime line.
probe_fn="$(sed -n '/^probe_bg() {/,/^}/p' "${SCRIPT}")"
overlap_pred="$(awk '$1 == "check" && $2 == "s1-no-downtime-for-the-a-token" {print $3; exit}' "${SCRIPT}")"
overlap_run() { # overlap_run MISS_TICK -> the overlap predicate's rc over a probe file from the script's own probe_bg
  # query_code runs in a command substitution, so its tick count lives in a file.
  local probe
  probe="$(cat <<EOS
source "${ROOT}/ci/lib/auth_rotation_probe.sh"
sleep() { SECONDS=\$((SECONDS + 1)); }
api_code() { printf 200; }
query_code() { echo x >>"${T}/qn.$1"; if [ "\$(wc -l <"${T}/qn.$1")" -eq "$1" ]; then printf 503; else printf 200; fi; }
${probe_fn}
probe_bg tok "${T}/overlap.$1" 6
${overlap_pred} "${T}/overlap.$1"
EOS
)"
  bash -c "${probe}" >/dev/null 2>&1
}
if [ -n "${probe_fn}" ] && [ -n "${overlap_pred}" ]; then
  if overlap_run 0; then ok s1-overlap-probe-passes-when-both-serve; else bad s1-overlap-probe-passes-when-both-serve; fi
  if overlap_run 3; then bad s1-overlap-probe-catches-a-query-api-miss; else ok s1-overlap-probe-catches-a-query-api-miss; fi
else
  bad s1-overlap-probe-catches-a-query-api-miss
fi

# The two calls of a tick run at once, beside the 0.5 s wait (Codex r1 P1: serial calls stretched the tick by their latency).
# go-api's stub answers only once query-api's call has started, so a serial probe records 000 for go-api.
probe_together() {
  # The stubs go to a file (printf, no here-document: CI scripts keep here-documents under 400 bytes).
  printf '%s\n' \
    "source \"${ROOT}/ci/lib/auth_rotation_probe.sh\"" \
    "sleep() { SECONDS=\$((SECONDS + 1)); }" \
    "api_code() { local i; : >\"${T}/go.started\"; for i in \$(seq 1 30); do [ -e \"${T}/query.started\" ] && { printf 200; return; }; command sleep 0.1; done; printf 000; }" \
    "query_code() { local i; : >\"${T}/query.started\"; for i in \$(seq 1 30); do [ -e \"${T}/go.started\" ] && { printf 200; return; }; command sleep 0.1; done; printf 000; }" \
    "${probe_fn}" \
    "probe_bg tok \"${T}/together\" 2" \
    "${overlap_pred} \"${T}/together\"" >"${T}/together.sh"
  bash "${T}/together.sh" >/dev/null 2>&1
}
if [ -n "${probe_fn}" ] && [ -n "${overlap_pred}" ] && probe_together && [ ! -e "${T}/together.go" ] && [ ! -e "${T}/together.query" ]; then
  ok s1-overlap-probe-asks-both-at-once; else bad s1-overlap-probe-asks-both-at-once; fi

# Every ops verifier of the new key is asked in the overlap and again after a retires: go-api and query-api, each with the
# a-token in the overlap and the b-token after the activate and after the retire.
# shellcheck disable=SC2016
for cell in 's1-query-api-serves-the-a-token-in-overlap query_serves "${TOK_A2}"' \
  's1-query-api-serves-b query_serves "${L_ACCESS}"' \
  's1-go-api-accepts-b-after-retire me_accepts "${L_ACCESS}"' \
  's1-query-api-serves-b-after-retire query_serves "${L_ACCESS}"'; do
  if grep -qxF -- "check ${cell}" "${SCRIPT}"; then ok "${cell%% *}-is-checked"; else bad "${cell%% *}-is-checked"; fi
done
# The after-retire cells judge the token of the login that b signed after the retire, not an older one.
if awk '/^check s1-b-signs-after-retire / {s=NR} /^check s1-go-api-accepts-b-after-retire / {g=NR} /^check s1-query-api-serves-b-after-retire / {q=NR} END {exit !(s && g > s && q > s)}' "${SCRIPT}"; then ok after-retire-cells-follow-the-b-login; else bad after-retire-cells-follow-the-b-login; fi

# A rollback to the previous image after F2b gives EdDSA under the active kid (good in S3, a in smoke), not HS256. rollback_cells
# prints the name of every cell the given script text fails; the planted copy below must fail all of them.
# shellcheck disable=SC2016
rollback_cells() {
  local f="$1" cell
  for cell in 's3-new-token-is-eddsa alg_is "${L_ACCESS}" EdDSA' \
    's3-new-token-signed-by-${good} kid_is "${L_ACCESS}" "${good}"' \
    's3-go-api-accepts-the-eddsa-token me_accepts "${L_ACCESS}"' \
    's3-compromised-kid-token-refused me_refuses "${COMPROMISED_ACCESS}"' \
    's3-revoked-session-still-refused verdict_is "${R_CODE}" refused' \
    'smoke-roll-token-is-eddsa alg_is "${L_ACCESS}" EdDSA' \
    'smoke-roll-token-signed-by-a kid_is "${L_ACCESS}" a' \
    'smoke-roll-go-api-accepts-eddsa me_accepts "${L_ACCESS}"'; do
    if ! grep -qxF -e "  check ${cell}" -e "  check \"${cell%% *}\" ${cell#* }" "${f}"; then echo "${cell%% *}"; fi
  done
  if ! grep -qE '^  check "s3-\$\{bad\}-jwks-absent|^  check "s3-jwks-has-no-\$\{bad\}" jwks_lacks "\$\{bad\}"$' "${f}"; then echo s3-jwks-lacks-the-compromised-kid; fi
  if grep -qi 'hs256' < <(grep -vE '^[[:space:]]*#' "${f}"); then echo no-hs256-expectation-left; fi
}
missing="$(rollback_cells "${SCRIPT}")"
# shellcheck disable=SC2016
for name in s3-new-token-is-eddsa 's3-new-token-signed-by-${good}' s3-go-api-accepts-the-eddsa-token s3-compromised-kid-token-refused \
  s3-revoked-session-still-refused smoke-roll-token-is-eddsa smoke-roll-token-signed-by-a smoke-roll-go-api-accepts-eddsa \
  s3-jwks-lacks-the-compromised-kid no-hs256-expectation-left; do
  if ! grep -qxF -- "${name}" <<<"${missing}"; then ok "rollback-pins-${name}"; else bad "rollback-pins-${name}"; fi
done
if grep -qF 'EdDSA' <<<"${plan}" && grep -qF 'S3 rollback' <<<"${plan}" && ! grep -i 'hs256' <<<"${plan}" >/dev/null; then ok plan-help-text-says-eddsa-not-hs256; else bad plan-help-text-says-eddsa-not-hs256; fi
# shellcheck disable=SC2016
sed -e 's/alg_is "${L_ACCESS}" EdDSA/alg_is "${L_ACCESS}" HS256/' -e 's/^  check s3-go-api-accepts-the-eddsa-token/  check s3-go-api-accepts-the-hs256-token/' "${SCRIPT}" >"${T}/planted.sh"
planted="$(rollback_cells "${T}/planted.sh")"
if grep -qxF s3-new-token-is-eddsa <<<"${planted}" && grep -qxF smoke-roll-token-is-eddsa <<<"${planted}" && grep -qxF s3-go-api-accepts-the-eddsa-token <<<"${planted}"; then ok planted-hs256-expectation-fails-the-cells; else bad planted-hs256-expectation-fails-the-cells; fi

# Key-compromise drill. D1 judges a forged old-kid token for a LIVE session: the compromise revokes the sessions the old key
# signed for, and go-api and query-api check the session on every request, so a refusal of such a token is the session
# revoke, not the dropped kid. The judge the script runs (its d1/d2 check lines) must hold only for a kid refusal: the
# genuine token of the same session still accepted on the tick of the first 401. Probe lines: s go_f go_g q_f q_g v_f v_g.
judge="$(awk '$1 == "check" && $2 ~ /^"\$\{tag\}-\$\{name\}-refuses-the-forged-/ {print $3; exit}' "${SCRIPT}")"
judge_fn="$(sed -n '/^kid_refused_within() {/,/^}/p' "${SCRIPT}")"
judge_run() { # judge_run FIXTURE FORGED_FIELD GENUINE_FIELD BOUND -> the rc of the script's own D1/D2 check predicate
  printf '%s\n' "source \"${ROOT}/ci/lib/auth_rotation_probe.sh\"" "${judge_fn}" "${judge} \"\$@\"" >"${T}/judge.sh"
  bash "${T}/judge.sh" "$@" >/dev/null 2>&1
}
printf '%s\n' '0 A A A A A A' '9 A A A A R A' '12 A A A A R A' '120 A A A A - -' '301 R A A A - -' '304 R A R A - -' '307 R A R A - -' >"${T}/kid.probe"
printf '%s\n' '0 A A A A A A' '7 R R R R R R' '10 R R R R R R' >"${T}/masked.probe"
printf '%s\n' '0 A A A A A A' '7 A R A R A R' '310 R R R R R R' >"${T}/genuine-gone.probe"
printf '%s\n' '0 A A A A A A' '30 R A R A R A' '33 A A A A A A' >"${T}/flap.probe"
printf '%s\n' '0 A A A A A A' '340 A A A A A A' >"${T}/never.probe"
printf '%s\n' '0 A A A A A A' '9 R A R A R' >"${T}/malformed.probe"
if [ -n "${judge}" ] && [ -n "${judge_fn}" ]; then
  if judge_run "${T}/kid.probe" 2 3 330 && judge_run "${T}/kid.probe" 4 5 330 && judge_run "${T}/kid.probe" 6 7 20; then ok d1-judge-passes-a-kid-refusal; else bad d1-judge-passes-a-kid-refusal; fi
  if ! judge_run "${T}/kid.probe" 2 3 300 && ! judge_run "${T}/kid.probe" 6 7 8; then ok d1-judge-fails-past-the-bound; else bad d1-judge-fails-past-the-bound; fi
  # The planted case: the token is refused only because its session is revoked (the genuine token of that session is refused
  # on the same tick). Today's window a measures exactly that; a judge that passes it does not measure the kid.
  for col in 2:3 4:5 6:7; do
    if ! judge_run "${T}/masked.probe" "${col%:*}" "${col#*:}" 330; then ok "d1-judge-fails-a-session-masked-refusal-${col}"; else bad "d1-judge-fails-a-session-masked-refusal-${col}"; fi
  done
  if ! judge_run "${T}/genuine-gone.probe" 2 3 330; then ok d1-judge-fails-when-the-session-dies-first; else bad d1-judge-fails-when-the-session-dies-first; fi
  if ! judge_run "${T}/flap.probe" 2 3 330; then ok d1-judge-fails-a-refusal-that-does-not-hold; else bad d1-judge-fails-a-refusal-that-does-not-hold; fi
  if ! judge_run "${T}/never.probe" 2 3 330; then ok d1-judge-fails-when-never-refused; else bad d1-judge-fails-when-never-refused; fi
  if ! judge_run "${T}/malformed.probe" 2 3 330; then ok d1-judge-fails-a-malformed-probe; else bad d1-judge-fails-a-malformed-probe; fi
else
  bad d1-judge-found-in-the-script
fi

# The rollback to the previous image keeps the verifier issuer: go-api and query-api of a release after the HS256 class
# retired refuse to start without it (drill run of 2026-11-10, S3).
roll_fn="$(sed -n '/^roll_images() {/,/^}/p' "${SCRIPT}")"
# shellcheck disable=SC2016
if [ -n "${roll_fn}" ] && ! grep -q 'REHEARSAL_VERIFY_ISSUER=""' <<<"${roll_fn}" && [ "$(grep -c 'REHEARSAL_VERIFY_ISSUER="${ISSUER_URL}"' <<<"${roll_fn}")" = 2 ]; then
  ok rollback-keeps-the-verifier-issuer; else bad rollback-keeps-the-verifier-issuer; fi

# D0: t0 is the decision, taken before the dry run of the compromise (not after the apply).
fn="$(sed -n '/^scenario_compromise() {/,/^}/p' "${SCRIPT}")"
# shellcheck disable=SC2016
t0_line="$(grep -n 't0="$(date +%s)"' <<<"${fn}" || true)"; t0_line="${t0_line%%$'\n'*}"; t0_line="${t0_line%%:*}"
# shellcheck disable=SC2016
dry_line="$(grep -n 'keys_cmd compromise -kid "${old}" -successor "${new}"$' <<<"${fn}" || true)"; dry_line="${dry_line%%$'\n'*}"; dry_line="${dry_line%%:*}"
if [ -n "${t0_line}" ] && [ -n "${dry_line}" ] && [ "${t0_line}" -lt "${dry_line}" ]; then ok d0-t0-is-the-decision-before-the-dry-run; else bad d0-t0-is-the-decision-before-the-dry-run; fi
if grep -q 'decision_to_apply_s=' <<<"${fn}" && grep -q 'from_apply_s=' "${SCRIPT}"; then ok d0-records-decision-to-apply-and-apply-to-refusal; else bad d0-records-decision-to-apply-and-apply-to-refusal; fi
# D1 control before the apply: the forged token is accepted by all three, so a later refusal is the key drop.
for cell in d1-control-go-api-accepts-the-forged-token-before-apply d1-control-query-api-serves-the-forged-token-before-apply \
  d2-control-auth-service-accepts-the-forged-token-before-apply d1-forged-token-holds-the-live-session; do
  n="$(grep -n "check ${cell} " <<<"${fn}" || true)"; n="${n%%$'\n'*}"; n="${n%%:*}"
  if [ -n "${n}" ] && [ "${n}" -lt "${dry_line:-0}" ]; then ok "${cell}-before-the-dry-run"; else bad "${cell}-before-the-dry-run"; fi
done
# D3: auth-service alone is restarted on a manifest that names the compromised key active.
# shellcheck disable=SC2016
if awk '/write_manifest "\$\{old\}:active"/ {m=NR} m && /compose --profile auth restart auth-service/ {r=NR} END {exit !(m && r > m)}' <<<"${fn}" && grep -q 'check "d3-jwks-never-lists-${old}-after-the-restart"' <<<"${fn}"; then ok d3-restarts-auth-service-on-the-old-active-manifest; else bad d3-restarts-auth-service-on-the-old-active-manifest; fi
# D4: window b is a hard check (no FINDING-only cell left).
# shellcheck disable=SC2016
if ! grep -qE 'soft_check "(s2-\$\{svc\}|d4-)' "${SCRIPT}" && grep -q 'check "d4-${svc}-refuses-the-forged-${old}-token-right-after-its-restart"' "${SCRIPT}"; then ok d4-window-b-is-a-hard-check; else bad d4-window-b-is-a-hard-check; fi

# forge_token: a throwaway key signs the header and payload of a token with the header kid replaced; openssl verifies it with
# the public key, the payload segment is unchanged, and no key or token reaches the output beyond the token itself.
(umask 077 && openssl genpkey -algorithm ed25519 -out "${T}/old.pem" 2>/dev/null && openssl pkey -in "${T}/old.pem" -pubout -out "${T}/old.pub" 2>/dev/null)
b64u() { base64 -w0 | tr '+/' '-_' | tr -d '='; }
src="$(printf '{"alg":"EdDSA","kid":"b","typ":"JWT"}' | b64u).$(printf '{"sid":"s-1","sub":"u-1"}' | b64u).$(printf 'x%.0s' $(seq 1 64) | b64u)"
mkdir -p "${T}/forge"
forged="$(bash -c 'source "$1"; forge_token "$2" a "$3" "$4"' _ "${ROOT}/ci/lib/auth_rotation_probe.sh" "${src}" "${T}/old.pem" "${T}/forge" 2>&1)" || forged=""
IFS=. read -r fh fp fs <<<"${forged}"
printf '%s' "${fh}.${fp}" >"${T}/forge.msg"
printf '%s' "${fs}" | tr -- '-_' '+/' | awk '{ n = length($0) % 4; s = $0; if (n == 2) s = s "=="; if (n == 3) s = s "="; printf "%s", s }' | base64 -d >"${T}/forge.sig" 2>/dev/null || true
if [ -n "${forged}" ] && [ "${fp}" = "$(cut -d. -f2 <<<"${src}")" ] && openssl pkeyutl -verify -rawin -pubin -inkey "${T}/old.pub" -sigfile "${T}/forge.sig" -in "${T}/forge.msg" >/dev/null 2>&1 &&
  [ "$(bash -c 'source "$1"; jwt_header_kid "$2"' _ "${ROOT}/ci/lib/auth_rotation_probe.sh" "${forged}")" = a ] && [ -z "$(ls -A "${T}/forge")" ]; then
  ok forge-token-signs-the-claims-under-the-old-kid; else bad forge-token-signs-the-claims-under-the-old-kid; fi
# shellcheck disable=SC2016
if ! grep -q . < <(grep -n 'openssl pkeyutl' "${ROOT}/ci/lib/auth_rotation_probe.sh" | grep -v -- '-inkey "${pem}" -in "${work}/forge.in"'); then ok forge-key-and-input-come-from-files; else bad forge-key-and-input-come-from-files; fi

if [ ! -s "${FAKE_LOG}" ]; then ok no-docker-call-before-the-checks; else bad no-docker-call-before-the-checks; fi
exit "${failed}"
