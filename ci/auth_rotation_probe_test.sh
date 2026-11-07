#!/usr/bin/env bash
# Cells for ci/lib/auth_rotation_probe.sh, the readers the key-rotation rehearsal judges with.
# Hermetic: fixtures only, no docker, no kubectl, no network. Run: bash ci/auth_rotation_probe_test.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LIB="${ROOT}/ci/lib/auth_rotation_probe.sh"
[ -f "${LIB}" ] || { printf 'FAIL: lib-present (%s)\n' "${LIB}" >&2; exit 1; }
# shellcheck source=ci/lib/auth_rotation_probe.sh
source "${LIB}"

T="$(mktemp -d)"
trap 'rm -rf "${T}"' EXIT
failed=0
ok() { printf 'PASS: %s\n' "$1"; }
bad() { printf 'FAIL: %s\n' "$1"; failed=1; }
expect_eq() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (got '$2', want '$3')"; fi; }
expect_rc() { local name="$1" want="$2"; shift 2; local rc=0; "$@" >/dev/null 2>&1 || rc=$?; if [ "${rc}" = "${want}" ]; then ok "${name}"; else bad "${name} (rc ${rc}, want ${want})"; fi; }

b64u() { printf '%s' "$1" | base64 | tr '+/' '-_' | tr -d '=\n'; }
jwt() { printf '%s.%s.%s' "$(b64u "$1")" "$(b64u "$2")" "c2ln"; }

TOK_A="$(jwt '{"alg":"EdDSA","kid":"a","typ":"at+jwt"}' '{"sid":"s-1","iss":"https://x","exp":1900000000}')"
TOK_B="$(jwt '{"alg":"EdDSA","kid":"kid-b_2","typ":"at+jwt"}' '{"sid":"s-2","exp":1900000001}')"
TOK_NOKID="$(jwt '{"alg":"EdDSA"}' '{"sid":"s-3"}')"

expect_eq jwt-kid-a "$(jwt_header_kid "${TOK_A}")" "a"
expect_eq jwt-kid-urlsafe "$(jwt_header_kid "${TOK_B}")" "kid-b_2"
expect_rc jwt-kid-absent-refused 1 jwt_header_kid "${TOK_NOKID}"
expect_rc jwt-kid-garbage-refused 1 jwt_header_kid "not-a-jwt"
expect_eq jwt-sid "$(jwt_claim "${TOK_A}" sid)" "s-1"
expect_rc jwt-claim-absent-refused 1 jwt_claim "${TOK_A}" nope

# A segment whose base64 holds both url-safe characters ("-" for "+", "_" for "/") decodes back to its JSON.
TOK_SAFE="$(jwt '{"alg":"EdDSA","kid":"a"}' '{"sid":"???>>>?>?>?>>>>???"}')"
case "${TOK_SAFE}" in *_*) ok fixture-has-underscore ;; *) bad fixture-has-underscore ;; esac
case "${TOK_SAFE}" in *-*) ok fixture-has-hyphen ;; *) bad fixture-has-hyphen ;; esac
expect_eq jwt-claim-urlsafe-segment "$(jwt_claim "${TOK_SAFE}" sid)" '???>>>?>?>?>>>>???'
TOK_EMPTY_KID="$(jwt '{"alg":"EdDSA","kid":""}' '{"sid":"s-4"}')"
expect_rc jwt-kid-empty-refused 1 jwt_header_kid "${TOK_EMPTY_KID}"

JWKS3='{"keys":[{"kid":"c","kty":"OKP"},{"kid":"a","kty":"OKP"},{"kid":"b","kty":"OKP"}]}'
expect_eq jwks-kids-sorted "$(jwks_kids "${JWKS3}" | tr '\n' ' ')" "a b c "
expect_rc jwks-has-b 0 jwks_has_kid "${JWKS3}" b
expect_rc jwks-lacks-d 1 jwks_has_kid "${JWKS3}" d
expect_rc jwks-prefix-not-match 1 jwks_has_kid '{"keys":[{"kid":"ab"}]}' a
expect_rc jwks-not-json-refused 2 jwks_has_kid 'x' a

expect_eq verdict-200 "$(status_verdict 200)" accepted
expect_eq verdict-204 "$(status_verdict 204)" accepted
expect_eq verdict-401 "$(status_verdict 401)" refused
expect_eq verdict-403 "$(status_verdict 403)" refused
expect_eq verdict-500 "$(status_verdict 500)" other
expect_eq verdict-000 "$(status_verdict 000)" other
expect_eq verdict-empty "$(status_verdict '')" other

# An overlap probe line is "<n> <go-api code> <query-api code>": go-api must answer 2xx; query-api must serve (not 401, 000 or 5xx).
printf '%s\n' '1 200 200' '2 200 403' '3 204 200' >"${T}/clean"
printf '%s\n' '1 200 200' '2 000 200' '3 200 200' >"${T}/drop"
printf '%s\n' '1 200 200' '2 401 200' >"${T}/refused"
printf '%s\n' '1 200 200' '2 200 401' '3 200 200' >"${T}/query.refused"
printf '%s\n' '1 200 200' '2 200 503' >"${T}/query.5xx"
printf '%s\n' '1 200 200' '2 200 000' >"${T}/query.down"
printf '%s\n' '1 200' '2 200' >"${T}/query.missing"
printf '%s\n' '1 200 200 200' >"${T}/extra.column"
printf '%s\n' '1 200 200' '2 000 503' '3 200 401' >"${T}/both"
: >"${T}/empty"
expect_rc probe-clean-passes 0 probe_no_downtime "${T}/clean"
expect_rc probe-one-drop-fails 1 probe_no_downtime "${T}/drop"
expect_rc probe-401-fails 1 probe_no_downtime "${T}/refused"
expect_rc probe-query-api-401-fails 1 probe_no_downtime "${T}/query.refused"
expect_rc probe-query-api-5xx-fails 1 probe_no_downtime "${T}/query.5xx"
expect_rc probe-query-api-000-fails 1 probe_no_downtime "${T}/query.down"
expect_rc probe-query-api-column-missing-fails 1 probe_no_downtime "${T}/query.missing"
expect_rc probe-extra-column-fails 1 probe_no_downtime "${T}/extra.column"
expect_rc probe-empty-fails 1 probe_no_downtime "${T}/empty"
expect_eq probe-summary "$(probe_summary "${T}/both")" "probes=3 go_api_non2xx=1 query_api_down=2 malformed=0"
expect_eq probe-summary-malformed "$(probe_summary "${T}/query.missing")" "probes=2 go_api_non2xx=0 query_api_down=0 malformed=2"
printf '%s\n' '1 401' '2 401' >"${T}/refused.only"
printf '%s\n' '1 401' '2 200' >"${T}/leak"
expect_rc probe-all-refused-passes 0 probe_all_refused "${T}/refused.only"
expect_rc probe-one-accepted-fails 1 probe_all_refused "${T}/leak"
expect_rc probe-all-refused-empty-fails 1 probe_all_refused "${T}/empty"

expect_eq floor-seconds "$(floor_remaining_seconds 'refused: floor_not_reached (floor reached in 4m59s)')" 299
expect_eq floor-seconds-hours "$(floor_remaining_seconds 'refused: floor_not_reached (floor reached in 1h2m3s)')" 3723
expect_eq floor-seconds-s "$(floor_remaining_seconds 'refused: floor_not_reached (floor reached in 7s)')" 7
expect_rc floor-other-refusal 1 floor_remaining_seconds 'refused: compromised_terminal'

expect_eq refusal-reason "$(refusal_reason 'refused: compromised_terminal')" compromised_terminal
expect_eq refusal-reason-floor "$(refusal_reason 'refused: floor_not_reached (floor reached in 7s)')" floor_not_reached
expect_eq refusal-reason-after-compose-lines "$(refusal_reason "$(printf ' Container x Creating \n Container x Created \nrefused: table_not_empty\n')")" table_not_empty
expect_rc refusal-reason-none 1 refusal_reason 'activate applied:'

expect_eq keys-state "$(keys_list_state "$(printf 'a\tretiring\tsince 2026-11-02T10:00:00Z\nb\tactive\tsince 2026-11-02T10:05:00Z\n')" b)" active
expect_rc keys-state-missing 1 keys_list_state "$(printf 'a\tretiring\tsince x\n')" z

( set -o noclobber; rec_open "${T}/rec1" first; rec_pass one; rec_fail two ) >/dev/null 2>&1 || true
if grep -q '^PASS: one$' "${T}/rec1" && grep -q '^FAIL: two$' "${T}/rec1"; then ok record-lines; else bad record-lines; fi
expect_rc record-no-overwrite 1 bash -c "source '${LIB}'; rec_open '${T}/rec1' again"
expect_rc record-field-namespace-with-password-text-passes 0 bash -c "source '${LIB}'; rec_open '${T}/rec3' x; rec_line 'namespace=authrot-password ci_run=1'"
expect_rc record-secret-key-field-refused 1 bash -c "source '${LIB}'; rec_open '${T}/rec4' x; rec_line 'password=abc'"
expect_rc record-token-key-field-refused 1 bash -c "source '${LIB}'; rec_open '${T}/rec5' x; rec_line 'ns=a token=abc'"
expect_rc record-no-secret-line 1 bash -c "source '${LIB}'; rec_open '${T}/rec2' x; rec_line 'dsn=postgres://u:p@h/db'"

# REHEARSAL_RUN_ID is the one operator-supplied value in a record: a numeric CI run id or nothing (r1 note: a token, a
# passwordless DSN or an encoded key passed the pattern filter in rec_line).
expect_rc run-id-numeric-passes 0 valid_run_id 12345
expect_rc run-id-unset-passes 0 valid_run_id ""
expect_rc run-id-session-token-fails 1 valid_run_id "svc_authsess_$(printf '%043d' 0)"
expect_rc run-id-dsn-fails 1 valid_run_id "postgresql://db.internal:5432/ops"
expect_rc run-id-base64-key-fails 1 valid_run_id "MC4CAQAwBQYDK2VwBCIEIGJhZGJhZGJhZGJhZGJhZGJhZGJhZGJhZGJhZGJhZGJh"
expect_rc run-id-too-long-fails 1 valid_run_id "$(printf '%030d' 1)"
expect_rc run-id-newline-fails 1 valid_run_id "$(printf '12\n34')"

exit "${failed}"
