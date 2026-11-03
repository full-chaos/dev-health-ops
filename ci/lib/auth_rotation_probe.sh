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

# A probe file holds "<n> <http code>" lines.
probe_summary() {
  awk 'NF >= 2 { n++; if ($2 !~ /^2[0-9][0-9]$/) bad++ } END { printf "probes=%d non2xx=%d\n", n, bad }' "$1"
}

probe_no_downtime() {
  local n bad
  read -r n bad < <(awk 'NF >= 2 { n++; if ($2 !~ /^2[0-9][0-9]$/) bad++ } END { print n + 0, bad + 0 }' "$1")
  [ "${n}" -gt 0 ] && [ "${bad}" -eq 0 ]
}

probe_all_refused() {
  local n ok
  read -r n ok < <(awk 'NF >= 2 { n++; if ($2 == 401 || $2 == 403) ok++ } END { print n + 0, ok + 0 }' "$1")
  [ "${n}" -gt 0 ] && [ "${n}" -eq "${ok}" ]
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

# Records: noclobber, one file per scenario, PASS/FAIL lines.
REC_FILE=""
rec_open() {
  REC_FILE="$1"
  ( set -o noclobber; printf '# %s\n' "$2" >"${REC_FILE}" ) 2>/dev/null || { REC_FILE=""; return 1; }
}
rec_line() {
  [ -n "${REC_FILE}" ] || return 1
  if grep -Eqi '://[^ /]*:[^ @]*@|password|BEGIN [A-Z ]*PRIVATE|eyJ[A-Za-z0-9_-]{8,}' <<<"$1"; then
    printf 'refused: record line looks like a secret\n' >&2
    return 1
  fi
  printf '%s\n' "$1" >>"${REC_FILE}"
}
rec_pass() { rec_line "PASS: $1" && printf 'PASS: %s\n' "$1"; }
rec_fail() { rec_line "FAIL: $1"; printf 'FAIL: %s\n' "$1" >&2; }
