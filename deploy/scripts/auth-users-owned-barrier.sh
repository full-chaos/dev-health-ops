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
# The admin users scan (up-go-api, up-owned and down-auth) is FAIL-CLOSED. Every container of every pod, Job and CronJob of the
# namespace STOPs the barrier, naming kind, object and container, unless it is exactly a trusted server container; it STOPs on a
# mention of dho, a shell with a -c flag (sh, bash, ash, dash, zsh, ksh), `$(`, a backtick, `${`, eval, or the words admin and users
# together; a row that does not split cleanly STOPs too. A false STOP is cleared by hand by the operator.
# Trusted, and only for a pod container whose (name, image repository, command + args) equals one of:
#   - the containers and init containers of the go-api and auth-service ReplicaSet pod templates (the chart's servers);
#   - the containers of the live pod template of the one long-lived release workload on the fixed list below, and only when the
#     pod's ownerReference is exactly that kind and name: the bundled ClickHouse StatefulSet.
# A label never trusts anything. A Job or a CronJob is never trusted. Not seen: a `kubectl exec` into a running container (it is
# not in a pod spec), and a change to a trusted workload's own template.
#
# Env: RELEASE (required, the Helm release name), NAMESPACE (default dev-health), FULLNAME (the chart fullname; default by the
# chart's rule: RELEASE when it contains dev-health, else RELEASE-dev-health).
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

# The chart fullname (templates/_helpers.tpl:11-22) and the fixed list of trusted long-lived workloads: statefulset <fullname>-clickhouse
# (templates/clickhouse.yaml:10-12; its init container clickhouse-usersd-links runs `sh -c`, clickhouse.yaml:53-59). Checked against the
# chart: postgresql, pgbouncer, redis, query-api, web and go-workers hold no container that the fail-closed rules stop.
if [ -z "${FULLNAME:-}" ]; then
  case "$RELEASE" in
  *dev-health*) FULLNAME="$RELEASE" ;;
  *) FULLNAME="${RELEASE}-dev-health" ;;
  esac
fi
FULLNAME="${FULLNAME:0:63}"
FULLNAME="${FULLNAME%-}"
TRUSTED_STS="${FULLNAME}-clickhouse"
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

# Row formats (one jsonpath per kind). A container is `name@@image@@{.command}{.args}<<c>>` (kubectl prints each array as JSON).
# pods:  name|component|owner|containers|initContainers|ephemeralContainers (6 fields); Jobs and CronJobs: name|-|-|containers|initContainers|.
CONTAINERS='{range .spec.containers[*]}{.name}@@{.image}@@{.command}{.args}<<c>>{end}'
INIT='{range .spec.initContainers[*]}{.name}@@{.image}@@{.command}{.args}<<c>>{end}'
EPHEMERAL='{range .spec.ephemeralContainers[*]}{.name}@@{.image}@@{.command}{.args}<<c>>{end}'
# the same two, read from a workload's pod template (ReplicaSet, StatefulSet)
T_CONTAINERS='{range .spec.template.spec.containers[*]}{.name}@@{.image}@@{.command}{.args}<<c>>{end}'
T_INIT='{range .spec.template.spec.initContainers[*]}{.name}@@{.image}@@{.command}{.args}<<c>>{end}'

# scan_admin_users KIND ROWS: uses SERVER_SET (segments of the go-api and auth-service ReplicaSet templates) and STS_SET (the
# segments of the live template of TRUSTED_STS); both come in through the environment, which awk reads unmangled.
scan_admin_users() {
  printf '%s\n' "$2" | KIND="$1" SERVER_SET="$SERVER_SET" STS_SET="$STS_SET" STS_OWNER="StatefulSet/${TRUSTED_STS};" awk -F'|' '
    function repo(img) { sub(/@.*/, "", img); if (match(img, /:[^\/]*$/)) img = substr(img, 1, RSTART - 1); return img }
    function norm(seg,   p, n) { n = split(seg, p, "@@"); if (n != 3) return "!"; return p[1] "@@" repo(p[2]) "@@" p[3] }
    function load(str, set,   a, n, i, k) { n = split(str, a, "<<c>>"); for (i = 1; i <= n; i++) if (a[i] != "") { k = norm(a[i]); if (k != "!") set[k] = 1 } }
    function stop(obj, cname, why) { print kind " " obj (cname != "" ? " container " cname : "") ": " why > "/dev/stderr"; bad = 1 }
    BEGIN {
      kind = ENVIRON["KIND"]
      load(ENVIRON["SERVER_SET"], srv); load(ENVIRON["STS_SET"], sts)
      admin = "(^|[^A-Za-z0-9_-])admin($|[^A-Za-z0-9_-])"; users = "(^|[^A-Za-z0-9_-])users($|[^A-Za-z0-9_-])"
      evalw = "(^|[^A-Za-z0-9_-])eval($|[^A-Za-z0-9_-])"
      shell = "\"([^\"]*/)?(sh|bash|ash|dash|zsh|ksh)\""; cflag = "\"-[A-Za-z]*c[A-Za-z]*\""
    }
    NF {
      if (NF != 6) { stop($1, "", "malformed row (a field separator in an argument?)"); next }
      for (f = 4; f <= 6; f++) {
        n = split($f, seg, "<<c>>")
        for (i = 1; i <= n; i++) {
          if (seg[i] == "") continue
          key = norm(seg[i])
          if (key == "!") { stop($1, "", "malformed container segment"); continue }
          if (kind == "pod" && (key in srv || ($3 == ENVIRON["STS_OWNER"] && key in sts))) continue
          split(key, p, "@@"); cmd = p[3]; cname = p[1]
          if (index(cmd, "dho")) stop($1, cname, "mentions dho")
          if (cmd ~ shell && cmd ~ cflag) stop($1, cname, "runs a shell with -c")
          if (index(cmd, "$(") || index(cmd, "`") || index(cmd, "${") || cmd ~ evalw) stop($1, cname, "uses a command substitution, a variable or eval")
          if (cmd ~ admin && cmd ~ users) stop($1, cname, "holds admin and users")
        }
      }
    }
    END { exit bad }' || fail "a $1 in the namespace holds an admin users writer or a container the scan cannot clear (see above)"
}

no_other_users_writer() {
  kget go-api rs '{range .items[*]}'"$T_CONTAINERS$T_INIT"'{end}'
  local sg="$KOUT"
  kget auth-service rs '{range .items[*]}'"$T_CONTAINERS$T_INIT"'{end}'
  local sa="$KOUT"
  SERVER_SET="$sg$sa"
  kget ns statefulsets '{range .items[*]}{.metadata.name}|'"$T_CONTAINERS$T_INIT"'{"\n"}{end}'
  STS_SET="$(printf '%s\n' "$KOUT" | awk -F'|' -v want="$TRUSTED_STS" '$1 == want { print $2 }')"
  kget ns pods '{range .items[*]}{.metadata.name}|{.metadata.labels.app\.kubernetes\.io/component}|{range .metadata.ownerReferences[*]}{.kind}/{.name};{end}|'"$CONTAINERS|$INIT|$EPHEMERAL"'{"\n"}{end}'
  scan_admin_users pod "$KOUT"
  kget ns jobs '{range .items[*]}{.metadata.name}|-|-|{range .spec.template.spec.containers[*]}{.name}@@{.image}@@{.command}{.args}<<c>>{end}|{range .spec.template.spec.initContainers[*]}{.name}@@{.image}@@{.command}{.args}<<c>>{end}|{"\n"}{end}'
  scan_admin_users job "$KOUT"
  kget ns cronjobs '{range .items[*]}{.metadata.name}|-|-|{range .spec.jobTemplate.spec.template.spec.containers[*]}{.name}@@{.image}@@{.command}{.args}<<c>>{end}|{range .spec.jobTemplate.spec.template.spec.initContainers[*]}{.name}@@{.image}@@{.command}{.args}<<c>>{end}|{"\n"}{end}'
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
  no_other_users_writer
  ;;
esac

[ "$FAILS" -eq 0 ] || die "$FAILS check(s) failed; do not run the next step"
printf 'barrier_ok mode=%s time=%s\n' "$MODE" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
