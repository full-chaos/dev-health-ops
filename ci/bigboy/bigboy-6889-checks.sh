#!/usr/bin/env bash
# bigboy-6889-checks.sh <record-dir>: hotfix B (CHAOS-6889 dispatch on one connection + bounded lock wait) watch on the bigboy worker containers: heartbeat/renewal failures,
# lock_busy_requeued (expected only under contention), "acquire bucket advisory lock" timeouts, domain pool + execution saturation gauges, ready endpoints. Counts only. Exit 1 on heartbeat failures or lock-timeout errors.
set -u; REC=${1:?record dir}; fail=0; cd /home/ubuntu/devhealth
W=$(docker ps --format '{{.Names}}' | grep -E '^dev-health-go-worker')
echo "workers: $(echo $W | tr '\n' ' ')"
for c in $W; do
  hb=$(docker logs "$c" 2>&1 | grep -c "presence heartbeat failed"); lb=$(docker logs "$c" 2>&1 | grep -c "lock_busy_requeued"); lt=$(docker logs "$c" 2>&1 | grep -c "acquire bucket advisory lock: timeout\|acquire bucket advisory lock.*deadline"); dl=$(docker logs "$c" 2>&1 | grep -c "context deadline exceeded")
  ip=$(docker inspect "$c" --format '{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}' | awk '{print $1}')
  rd=$(curl -s -m 5 -o /dev/null -w '%{http_code}' "http://$ip:8080/readyz" 2>/dev/null); m=$(curl -s -m 5 "http://$ip:8080/metrics" 2>/dev/null | grep -E '^worker_(database_pool_saturation_ratio|execution_saturation_ratio)' | tr '\n' ' ' | cut -c1-300)
  echo "$c ready=$rd heartbeat_failed=$hb lock_busy_requeued=$lb lock_wait_timeouts=$lt deadline_exceeded=$dl $m"
  [ "$hb" = 0 ] && [ "$lt" = 0 ] || fail=1
done
[ $fail = 0 ] && echo B_CHECKS_OK || echo B_CHECKS_FAIL; exit $fail
