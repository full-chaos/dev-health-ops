#!/usr/bin/env bash
# Readers and record helpers for the key-rotation rehearsals (ci/rehearse_auth_key_rotation*.sh).
# Source it. Needs jq, base64. Nothing here prints a token, a key or a DSN: kid, sid, status codes and counts only.

b64url_decode() {
  local s="$1"
  s="${s//-/+}"
  s="${s//_//}"
  case $((${#s} % 4)) in
    2) s="${s}==" ;;
    3) s="${s}=" ;;
    1) return 1 ;;
  esac
  printf '%s' "${s}" | base64 -d 2>/dev/null
}

# jwt_part TOKEN INDEX(0 header, 1 payload) -> JSON on stdout
jwt_part() {
  local token="$1" index="$2" seg
  [[ "${token}" =~ ^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*$ ]] || return 1
  IFS=. read -r -a seg <<<"${token}"
  b64url_decode "${seg[${index}]}" | jq -e . 2>/dev/null
}

jwt_header_kid() {
  jwt_part "$1" 0 | jq -er '.kid | select(type == "string" and length > 0)' 2>/dev/null || return 1
}

jwt_claim() {
  jwt_part "$1" 1 | jq -er --arg n "$2" '.[$n] | select(. != null) | tostring' 2>/dev/null || return 1
}

jwks_kids() {
  printf '%s' "$1" | jq -r '.keys[].kid' | sort
}

# jwks_has_kid JWKS KID: 0 present, 1 absent, 2 not a key set
jwks_has_kid() {
  local kids
  printf '%s' "$1" | jq -e '.keys | type == "array"' >/dev/null 2>&1 || return 2
  kids="$(jwks_kids "$1")"
  grep -Fxq -- "$2" <<<"${kids}"
}

# status_verdict CODE -> accepted | refused | other
status_verdict() {
  case "$1" in
    2[0-9][0-9]) printf 'accepted\n' ;;
    401 | 403) printf 'refused\n' ;;
    *) printf 'other\n' ;;
  esac
}

# An overlap probe file holds "<n> <go-api code> <query-api code>" lines. go-api must answer 2xx; query-api must serve: 401,
# 000 (no answer) and 5xx count as down, as query_serves judges it. A line without exactly three fields is malformed.
# shellcheck disable=SC2016
PROBE_AWK='NF != 3 { n++; mal++; next } { n++; if ($2 !~ /^2[0-9][0-9]$/) go++; if ($3 == 401 || $3 == "000" || $3 ~ /^5/) q++ }'
probe_summary() {
  awk "${PROBE_AWK}"' END { printf "probes=%d go_api_non2xx=%d query_api_down=%d malformed=%d\n", n, go, q, mal }' "$1"
}

probe_no_downtime() {
  local n go q mal
  read -r n go q mal < <(awk "${PROBE_AWK}"' END { print n + 0, go + 0, q + 0, mal + 0 }' "$1")
  [ "${n}" -gt 0 ] && [ "${go}" -eq 0 ] && [ "${q}" -eq 0 ] && [ "${mal}" -eq 0 ]
}

# A refusal probe file holds "<n> <http code>" lines.
probe_all_refused() {
  local n ok
  read -r n ok < <(awk 'NF >= 2 { n++; if ($2 == 401 || $2 == 403) ok++ } END { print n + 0, ok + 0 }' "$1")
  [ "${n}" -gt 0 ] && [ "${n}" -eq "${ok}" ]
}

# forge_token TOKEN KID PEM WORKDIR -> a token with TOKEN's payload, its header kid set to KID, signed with the Ed25519
# private key in the file PEM: what a holder of that key can mint for any session whose claims it has seen. The signing
# input goes to openssl in a 0600 file under WORKDIR (removed after); no key and no token is on argv or printed.
forge_token() {
  local token="$1" kid="$2" pem="$3" work="$4" header h64 sig rc=0
  [[ "${token}" =~ ^[A-Za-z0-9_-]+\.([A-Za-z0-9_-]+)\.[A-Za-z0-9_-]*$ ]] || return 1
  local payload="${BASH_REMATCH[1]}"
  header="$(jwt_part "${token}" 0 | jq -c --arg k "${kid}" '.kid = $k')" || return 1
  h64="$(printf '%s' "${header}" | base64 -w0 | tr '+/' '-_' | tr -d '=')"
  (umask 077 && printf '%s.%s' "${h64}" "${payload}" >"${work}/forge.in") || return 1
  sig="$(openssl pkeyutl -sign -rawin -inkey "${pem}" -in "${work}/forge.in" 2>/dev/null | base64 -w0 | tr '+/' '-_' | tr -d '=')" || rc=1
  rm -f "${work}/forge.in"
  # An Ed25519 signature is 64 bytes: 86 base64url characters.
  [ "${rc}" = 0 ] && [ "${#sig}" = 86 ] || return 1
  printf '%s.%s.%s' "${h64}" "${payload}" "${sig}"
}

# A forge probe file holds "<s> <go-api forged> <go-api genuine> <query-api forged> <query-api genuine> <auth-service forged>
# <auth-service genuine>" lines: s = seconds since the decision; each verdict A (accepted), R (refused) or X (any other
# answer), "-" when not asked. The forged token carries the claims of a live session; the genuine one is that session's own.
# kid_refused_at FILE FORGED_FIELD GENUINE_FIELD -> the seconds of the forged token's first refusal, only when that refusal
# is the kid: on every line up to and including it the genuine token is accepted (the session is live, so the refusal is
# not the session revoke), the forged token is accepted on every line before it and refused on every line after it. A
# malformed line, a refusal on which the genuine token is not accepted, or no refusal at all is no answer (rc 1).
kid_refused_at() {
  awk -v f="$2" -v g="$3" '
    NF != 7 { bad = 1; next }
    $f == "-" && $g == "-" { next }
    !seen && $f == "A" && $g == "A" { next }
    !seen && $f == "R" && $g == "A" { seen = 1; at = $1; next }
    seen && $f == "R" { next }
    { bad = 1 }
    END { if (bad || !seen) exit 1; print at }' "$1"
}

# floor_remaining_seconds "refused: floor_not_reached (floor reached in 1h2m3s)" -> seconds
floor_remaining_seconds() {
  local text="$1" d h=0 m=0 s=0
  [[ "${text}" =~ floor_not_reached\ \(floor\ reached\ in\ ([0-9hms]+)\) ]] || return 1
  d="${BASH_REMATCH[1]}"
  [[ "${d}" =~ ([0-9]+)h ]] && h="${BASH_REMATCH[1]}"
  [[ "${d}" =~ ([0-9]+)m ]] && m="${BASH_REMATCH[1]}"
  [[ "${d}" =~ ([0-9]+)s ]] && s="${BASH_REMATCH[1]}"
  printf '%d\n' $((h * 3600 + m * 60 + s))
}

refusal_reason() {
  local line
  line="$(grep -m 1 -E '^refused: [a-z_]+' <<<"$1")" || return 1
  [[ "${line}" =~ ^refused:\ ([a-z_]+) ]] || return 1
  printf '%s\n' "${BASH_REMATCH[1]}"
}

# keys_list_state LISTING KID -> the state of KID in `auth-migrate keys list` output
keys_list_state() {
  printf '%s\n' "$1" | awk -F'\t' -v k="$2" '$1 == k { print $2; found = 1 } END { exit found ? 0 : 1 }'
}

# valid_run_id VALUE: the CI run id is digits only (1-20) or empty; the one operator-supplied value a record carries.
valid_run_id() {
  [[ -z "$1" || "$1" =~ ^[0-9]{1,20}$ ]]
}

# Records: noclobber, one file per scenario, PASS/FAIL lines.
REC_FILE=""
rec_open() {
  REC_FILE="$1"
  ( set -o noclobber; printf '# %s\n' "$2" >"${REC_FILE}" ) 2>/dev/null || { REC_FILE=""; return 1; }
}
# rec_line redacts by FIELD: a whitespace-separated token is refused when its key names a secret (key=value with a
# password/secret/token/credential/private key name) or when it is shaped like one (a URL with credentials, a PEM header,
# a JWT). Plain text that merely contains a word such as "password" (a namespace name) is not a secret.
rec_line() {
  [ -n "${REC_FILE}" ] || return 1
  local tok key
  for tok in $1; do
    key="${tok%%=*}"
    if [ "${key}" != "${tok}" ] && grep -Eqi '^(.*[-_.])?(password|passwd|secret|token|credential|apikey|api[-_]key|private[-_]?key)([-_.].*)?$' <<<"${key}"; then
      printf 'refused: record line looks like a secret\n' >&2
      return 1
    fi
  done
  if grep -Eqi '://[^ /]*:[^ @]*@|BEGIN [A-Z ]*PRIVATE|eyJ[A-Za-z0-9_-]{8,}' <<<"$1"; then
    printf 'refused: record line looks like a secret\n' >&2
    return 1
  fi
  printf '%s\n' "$1" >>"${REC_FILE}"
}
rec_pass() { rec_line "PASS: $1" && printf 'PASS: %s\n' "$1"; }
rec_fail() { rec_line "FAIL: $1"; printf 'FAIL: %s\n' "$1" >&2; }
