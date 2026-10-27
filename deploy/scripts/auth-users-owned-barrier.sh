#!/usr/bin/env bash
# CHAOS-11606: the roll-order barrier of the users-owned flip. READ-ONLY: kubectl get with jsonpath, nothing else.
# It prints no secret and no env dump: the only env values read are AUTH_USERS_OWNED and AUTH_PROXY_CREDENTIALS, by name.
#
#   up-go-api   after the stage goApi release (U1): go-api settled, no old ReplicaSet pod, every go-api pod forwards and
#               carries AUTH_USERS_OWNED=true, and no pod, Job or CronJob of the namespace (every container, init and
#               ephemeral containers included) runs an `admin users` verb.
#   up-owned    the same checks again, right before the stage owned release (U3), and no auth-service pod is owned yet.
#   down-auth   after the first rollback release (D1): no auth-service pod (a terminating one counts) carries
#               AUTH_USERS_OWNED, and the auth-service Deployment is settled. Only then may go-api go to phase 3.
#
# Exit 0 = the next step may run. Any other exit = STOP: a check failed, or kubectl failed (it fails closed).
# On success the last line is `barrier_ok mode=<mode> time=<UTC time>`: keep the time for the report.
#
# Env: RELEASE (required, the Helm release name), NAMESPACE (default dev-health).
set -uo pipefail

NAMESPACE="${NAMESPACE:-dev-health}"
MODE="${1:-}"

die() { printf 'barrier FAILED: %s\n' "$1" >&2; exit 1; }

case "$MODE" in
up-go-api | up-owned | down-auth) ;;
*) die "usage: auth-users-owned-barrier.sh up-go-api|up-owned|down-auth [--plan]" ;;
esac
if [ "${2:-}" = "--plan" ]; then
  printf 'plan: namespace=%s mode=%s; only kubectl get -o jsonpath; no exec, no secret read\n' "$NAMESPACE" "$MODE"
  exit 0
fi
[ -n "${RELEASE:-}" ] || die "RELEASE is required"
command -v kubectl >/dev/null 2>&1 || die "kubectl is not installed"

FAILS=0
fail() { printf 'barrier check failed: %s\n' "$1" >&2; FAILS=$((FAILS + 1)); }

# kget COMPONENT RESOURCE JSONPATH: the jsonpath result lands in KOUT ("" for no match); a kubectl error stops the run.
# COMPONENT selects by the release label and the component label; `ns` reads the whole namespace (no selector), for the
# admin users scan: an ad-hoc pod, Job or CronJob has no release label.
# Every call has --request-timeout=30s: a hung API call ends in a kubectl error, which dies here (fails closed).
# It is never called inside $(...): die must exit the main shell, or a kubectl failure would read as an empty answer.
KOUT=""
kget() {
  local selector=()
  if [ "$1" != ns ]; then
    selector=(-l "app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/component=$1")
  fi
  KOUT="$(kubectl -n "$NAMESPACE" --request-timeout=30s get "$2" ${selector[@]+"${selector[@]}"} -o "jsonpath=$3")" || die "kubectl get $2 ($1) failed"
}

deployment_settled() { # COMPONENT
  local rows count
  kget "$1" deploy '{range .items[*]}{.metadata.generation} {.status.observedGeneration} {.spec.replicas} {.status.updatedReplicas} {.status.availableReplicas} {.status.replicas}{"\n"}{end}'
  rows="$KOUT"
  count="$(printf '%s' "$rows" | awk 'NF' | wc -l)"
  [ "$count" -eq 1 ] || { fail "$1: want one Deployment, got $count"; return; }
  printf '%s\n' "$rows" | awk 'NF { if ($1 != $2 || $3 != $4 || $3 != $5 || $3 != $6 || $3 == 0) exit 1 }' ||
    fail "$1 Deployment is not settled (generation, spec, updated, available and total replicas must agree and be above 0): $rows"
}

old_replicasets_empty() { # COMPONENT
  local rows
  kget "$1" rs '{range .items[*]}{.metadata.name}|{.status.replicas}|{.metadata.annotations.deployment\.kubernetes\.io/revision}{"\n"}{end}'
  rows="$KOUT"
  printf '%s\n' "$rows" | awk -F'|' 'NF { rev = $3 + 0; if (rev > top) top = rev; n[NR] = $1; c[NR] = $2 + 0; r[NR] = rev }
    END { for (i in n) if (r[i] != top && c[i] != 0) { print n[i]; bad = 1 } exit bad }' >/dev/null ||
    fail "$1: an old ReplicaSet still has pods"
}

go_api_forwards() {
  local rows name deleting owned creds n=0
  kget go-api pods '{range .items[*]}{.metadata.name}|{.metadata.deletionTimestamp}|{.spec.containers[?(@.name=="go-api")].env[?(@.name=="AUTH_USERS_OWNED")].value}|{.spec.containers[?(@.name=="go-api")].env[?(@.name=="AUTH_PROXY_CREDENTIALS")].value}{"\n"}{end}'
  rows="$KOUT"
  while IFS='|' read -r name deleting owned creds; do
    [ -n "$name" ] || continue
    n=$((n + 1))
    [ -z "$deleting" ] || fail "go-api pod $name is terminating"
    [ "$owned" = true ] || fail "go-api pod $name: AUTH_USERS_OWNED is not true"
    [ "$creds" = true ] || fail "go-api pod $name: AUTH_PROXY_CREDENTIALS is not true"
  done <<<"$rows"
  [ "$n" -gt 0 ] || fail "no go-api pod listed"
}

# One segment per container (`;` after each) holds that container's command and args as kubectl prints them (JSON arrays),
# so command, args and an `sh -c` string are one text. A segment with both whole words admin and users is an admin users
# verb: any users subcommand counts. Fields: name|component|containers|initContainers|ephemeralContainers.
scan_admin_users() { # KIND ROWS
  printf '%s\n' "$2" | awk -F'|' -v kind="$1" '
    BEGIN { admin = "(^|[^A-Za-z0-9_-])admin($|[^A-Za-z0-9_-])"; users = "(^|[^A-Za-z0-9_-])users($|[^A-Za-z0-9_-])" }
    NF && $2 != "go-api" && $2 != "auth-service" {
      for (f = 3; f <= 5; f++) {
        n = split($f, seg, ";")
        for (i = 1; i <= n; i++) if (seg[i] ~ admin && seg[i] ~ users) { print kind " " $1 ": admin users verb" > "/dev/stderr"; bad = 1 }
      }
    }
    END { exit bad }' || fail "a $1 outside go-api and auth-service runs an admin users verb"
}

no_other_users_writer() {
  kget ns pods '{range .items[*]}{.metadata.name}|{.metadata.labels.app\.kubernetes\.io/component}|{range .spec.containers[*]}{.command}{.args};{end}|{range .spec.initContainers[*]}{.command}{.args};{end}|{range .spec.ephemeralContainers[*]}{.command}{.args};{end}{"\n"}{end}'
  scan_admin_users pod "$KOUT"
  kget ns jobs '{range .items[*]}{.metadata.name}|-|{range .spec.template.spec.containers[*]}{.command}{.args};{end}|{range .spec.template.spec.initContainers[*]}{.command}{.args};{end}|{"\n"}{end}'
  scan_admin_users job "$KOUT"
  kget ns cronjobs '{range .items[*]}{.metadata.name}|-|{range .spec.jobTemplate.spec.template.spec.containers[*]}{.command}{.args};{end}|{range .spec.jobTemplate.spec.template.spec.initContainers[*]}{.command}{.args};{end}|{"\n"}{end}'
  scan_admin_users cronjob "$KOUT"
}

OWNED_N=0
auth_service_owned_pods() { # sets OWNED_N
  local rows
  kget auth-service pods '{range .items[*]}{.metadata.name}|{.spec.containers[?(@.name=="auth-service")].env[?(@.name=="AUTH_USERS_OWNED")].value}{"\n"}{end}'
  rows="$KOUT"
  OWNED_N="$(printf '%s\n' "$rows" | awk -F'|' '$2 == "true" { n++ } END { print n + 0 }')"
}

case "$MODE" in
up-go-api | up-owned)
  deployment_settled go-api
  old_replicasets_empty go-api
  go_api_forwards
  no_other_users_writer
  if [ "$MODE" = up-owned ]; then
    auth_service_owned_pods
    [ "$OWNED_N" -eq 0 ] || fail "an auth-service pod is already owned: stage owned must not have rolled yet"
  fi
  ;;
down-auth)
  deployment_settled auth-service
  old_replicasets_empty auth-service
  auth_service_owned_pods
  [ "$OWNED_N" -eq 0 ] || fail "an auth-service pod still carries AUTH_USERS_OWNED=true"
  ;;
esac

[ "$FAILS" -eq 0 ] || die "$FAILS check(s) failed; do not run the next step"
printf 'barrier_ok mode=%s time=%s\n' "$MODE" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
