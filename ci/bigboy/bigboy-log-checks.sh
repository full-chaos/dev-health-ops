#!/usr/bin/env bash
# bigboy-log-checks.sh <record-dir>: post-up log gates of the cut pass (rev 188+): (1) readiness-identity refusal (ops #3251/6862): the text "authenticated as <X> (session_user) and acts as <Y>" must appear in NO plane's log,
# and every plane's ready endpoint must answer 200; (2) SQLSTATE scan (ops #3250/6867 class): distinct SQLSTATE codes seen in worker/api logs since the stack came up, 42846 (jsonb vs json COALESCE) must be 0.
# Counts and pod names only; nothing else printed. Exit 1 when (1) or (2) fails.
set -u; REC=${1:?record dir}; fail=0; cd /home/ubuntu/devhealth
NAMES=$(docker ps --format '{{.Names}}' | grep -E '^dev-health-(api|go-|query-api|worker|scheduler|reconciler)' )
tot=0; for c in $NAMES; do n=$(docker logs "$c" 2>&1 | grep -c "and acts as"); [ "$n" -gt 0 ] && { echo "IDENTITY_REFUSAL $c count=$n"; tot=$((tot+n)); }; done
echo "identity_refusal_total=$tot containers_scanned=$(echo "$NAMES" | wc -w)"; [ "$tot" = 0 ] || fail=1
ip() { docker inspect "$1" --format '{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}' 2>/dev/null | awk '{print $1}'; }
chk() { st=$(curl -s -o /dev/null -m 5 -w '%{http_code}' "http://$2"); echo "ready $1 ${2#*/}=$st"; [ "$st" = 200 ] || fail=1; }
for c in dev-health-go-api-1; do a=$(ip $c); [ -n "$a" ] && { chk $c $a:8080/readyz; }; done
for c in $(echo "$NAMES" | grep -E 'query-api'); do a=$(ip $c); [ -n "$a" ] && chk $c $a:8090/readyz; done
a=$(ip dev-health-api-1); [ -n "$a" ] && chk dev-health-api-1 $a:8000/ready
codes=$(for c in $NAMES; do docker logs "$c" 2>&1 | grep -oE "SQLSTATE [0-9A-Z]{5}|\(SQLSTATE [0-9A-Z]{5}\)" ; done | grep -oE "[0-9A-Z]{5}" | sort | uniq -c | sort -rn)
echo "sqlstate_codes: $(echo "$codes" | tr '\n' ';' | sed 's/  */ /g')"; n42846=$(echo "$codes" | awk '$2=="42846"{print $1}'); echo "sqlstate_42846=${n42846:-0}"; [ "${n42846:-0}" = 0 ] || fail=1
[ $fail = 0 ] && echo LOG_CHECKS_OK || echo LOG_CHECKS_FAIL
exit $fail
