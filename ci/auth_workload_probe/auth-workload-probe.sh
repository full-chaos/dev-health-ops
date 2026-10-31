#!/bin/sh
# Prints status lines only. Fails unless the exchange issues a token and both routes take it (anything but 401).
umask 077
host=auth-service; port=8095; body=/tmp/body; answer=/tmp/answer; request=/tmp/request

send() { # method path content-type bearer-file(or -)
  length=$(wc -c < "$body")
  {
    printf '%s %s HTTP/1.1\r\nHost: %s:%s\r\nConnection: close\r\nContent-Type: %s\r\nContent-Length: %s\r\n' "$1" "$2" "$host" "$port" "$3" "$length"
    if [ "$4" != - ]; then printf 'Authorization: Bearer '; cat "$4"; printf '\r\n'; fi
    printf '\r\n'
    cat "$body"
  } > "$request"
  # stdin stays open: at EOF nc closes the connection and the server cancels the request.
  { cat "$request"; sleep 2; } | nc -w 10 "$host" "$port" > "$answer"
  sed -n '1s/^HTTP\/1\.1 \([0-9]*\).*/\1/p' "$answer" | tr -d '\r'
}

{
  printf 'grant_type=urn:ietf:params:oauth:grant-type:token-exchange&subject_token='
  tr -d '\r\n' < /run/workload/api.bootstrap
  printf '&subject_token_type=urn:dev-health:params:oauth:token-type:workload-bootstrap&audience=auth-service'
} > "$body"
status=$(send POST /internal/v1/token/exchange application/x-www-form-urlencoded -)
echo "exchange status=$status"
[ "$status" = 200 ] || { sed -n 's/.*"error":"\([a-z_]*\)".*/exchange error=\1/p' "$answer"; exit 1; }
sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p' "$answer" | tr -d '\r\n' > /tmp/token
[ -s /tmp/token ] || { echo "exchange answered no access_token"; exit 1; }
sed -n 's/.*"issued_token_type":"\([^"]*\)".*/exchange issued_token_type=\1/p' "$answer"
sed -n 's/.*"expires_in":\([0-9]*\).*/exchange expires_in=\1/p' "$answer"

reason() { sed -n 's/.*"reason_code":"\([a-z_]*\)".*/\1/p' "$answer" | head -1; }
# Numeric; not 401, 403 or 5xx; a non-2xx carries a reason_code (an unmounted route answers a 404 with none).
accepted() { # label status reason
  case "$2" in
    ''|*[!0-9]*) echo "$1: no numeric status"; exit 1 ;;
    401|403|5??) echo "$1: refused"; exit 1 ;;
    2??) ;;
    *) [ -n "$3" ] || { echo "$1: a non-2xx with no reason_code"; exit 1; } ;;
  esac
}
: "${PROBE_OWNER:?}" "${PROBE_MEMBER:?}" "${PROBE_ORG:?}"
# Each issue projects its principal (and the organization) into the auth schema; the add needs both.
printf '{"principal_id":"%s","organization_id":"%s","amr":["pwd"]}' "$PROBE_OWNER" "$PROBE_ORG" > "$body"
status=$(send POST /internal/v1/sessions application/json /tmp/token); why=$(reason)
echo "session issue owner status=$status reason=$why"
accepted "session issue owner" "$status" "$why"
printf '{"principal_id":"%s","organization_id":null,"amr":["pwd"]}' "$PROBE_MEMBER" > "$body"
status=$(send POST /internal/v1/sessions application/json /tmp/token); why=$(reason)
echo "session issue member status=$status reason=$why"
accepted "session issue member" "$status" "$why"
printf '{"organization_id":"%s","principal_id":"%s","actor_principal_id":"%s","role":"member"}' "$PROBE_ORG" "$PROBE_MEMBER" "$PROBE_OWNER" > "$body"
status=$(send POST /internal/v1/memberships/add application/json /tmp/token); why=$(reason)
echo "memberships add status=$status reason=$why"
accepted "memberships add" "$status" "$why"
: > "$body"
printf 'not-a-token' > /tmp/bad
printf '{}' > "$body"
status=$(send POST /internal/v1/memberships/add application/json /tmp/bad)
echo "control: a bad bearer status=$status"
[ "$status" = 401 ] || exit 1
echo PROBE-PASS
