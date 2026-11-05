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
# together; a row that does not split cleanly STOPs too. A false STOP is cleared by hand by the operator, or by an allowlist entry.
# Trusted, and only for a pod container whose (name, image repository, command + args) equals one of:
#   - the containers and init containers of the go-api and auth-service ReplicaSet pod templates (the chart's servers);
#   - the containers of the live pod template of the one long-lived release workload on the fixed list below, and only when the
#     pod's ownerReference is exactly that kind and name: the bundled ClickHouse StatefulSet.
# A label never trusts anything. A Job or a CronJob is never trusted. Not seen: a `kubectl exec` into a running container (it is
# not in a pod spec), and a change to a trusted workload's own template.
#
# Env: RELEASE (required, the Helm release name), NAMESPACE (default dev-health), FULLNAME (the chart fullname; default by the
# chart's rule: RELEASE when it contains dev-health, else RELEASE-dev-health), BARRIER_ALLOWLIST (optional, below).
#
# BARRIER_ALLOWLIST=<file> (CHAOS-13103): a reviewed list of known non-writers the scan STOPs on; unset or empty = nothing is
# cleared. One exact entry per line, `kind|owner|container|reason|image` (example and rules: auth-users-owned-barrier.allowlist-d6710.txt);
# exact string compare, no pattern. Only two STOPs clear: `malformed row` of a finished Job (the Job row, or a Succeeded pod it
# owns; Job active 0 and completionTime set; every image of the row equals the entry) and `shell -c` of one container of a
# Deployment's pod (owner chain pod -> ReplicaSet -> Deployment; name and full image with digest equal). Never cleared: any other
# reason, a mention of dho or of users with admin (any case), a pod labelled go-api or auth-service, a CronJob. A clear prints
# `cleared-by-allowlist <key> object=<name>` on stdout. A malformed file STOPs before any kubectl call; an entry that matches
# nothing STOPs (`allowlist-unused <key>`): the namespace changed since the review. The go-api and auth-service checks never read it.
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

# ALLOW: the checked entries of BARRIER_ALLOWLIST, one per line ("" = no allowlist). Any bad line refuses the whole file.
ALLOW=""
if [ -n "${BARRIER_ALLOWLIST:-}" ]; then
  if [ ! -f "$BARRIER_ALLOWLIST" ] || [ ! -r "$BARRIER_ALLOWLIST" ]; then die "BARRIER_ALLOWLIST is not a readable file: $BARRIER_ALLOWLIST"; fi
  ALLOW="$(awk '
    function bad(why) { printf "allowlist line %d: %s\n", NR, why > "/dev/stderr"; err = 1; ok = 0 }
    BEGIN { w = "*?[]{}()^$+\\\t\r" }
    /^#/ || $0 == "" { next }
    {
      ok = 1
      for (i = 1; i <= length(w); i++) if (index($0, substr(w, i, 1))) { bad("a pattern, wildcard or control character: entries are exact strings"); next }
      if (split($0, f, "|") != 5) { bad("want 5 fields: kind|owner|container|reason|image"); next }
      okind = f[2]; sub(/\/.*/, "", okind); oname = substr(f[2], length(okind) + 2)
      if (f[1] != "pod" && f[1] != "job") bad("kind must be pod or job")
      if (!index(f[2], "/") || (okind != "Job" && okind != "Deployment") || length(oname) > 253 || oname !~ /^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$/) bad("owner must be Job/<name> or Deployment/<name>")
      if (f[3] != "-" && (length(f[3]) > 63 || f[3] !~ /^[a-z0-9]([-a-z0-9]*[a-z0-9])?$/)) bad("container must be a container name or -")
      if (f[4] == "malformed row") { if (okind != "Job" || f[3] != "-") bad("a malformed row entry has a Job owner and container -") }
      else if (f[4] == "shell -c") { if (f[1] != "pod" || okind != "Deployment" || f[3] == "-") bad("a shell -c entry has kind pod, a Deployment owner and a container name") }
      else bad("reason must be: malformed row, or shell -c")
      d = index(f[5], "@sha256:"); hex = substr(f[5], d + 8)
      if (f[5] == "") bad("empty image digest")
      else if (!d || length(hex) != 64 || hex !~ /^[0-9a-f]+$/ || substr(f[5], 1, d - 1) !~ /^[a-z0-9][A-Za-z0-9._\/:-]*$/) bad("image must be <repository[:tag]>@sha256:<64 lowercase hex>")
      low = tolower(f[2] "|" f[3])
      if (index(low, "go-api") || index(low, "auth-service")) bad("names a go-api or auth-service workload: the allowlist never applies there")
      k = f[1] "|" f[2] "|" f[3] "|" f[4]
      if (k in seen) bad("duplicate key " k)
      seen[k] = 1
      if (ok) { print; n++ }
    }
    END { if (!err && !n) { print "allowlist: no entries" > "/dev/stderr"; err = 1 } exit err }' "$BARRIER_ALLOWLIST")" ||
    die "the allowlist $BARRIER_ALLOWLIST is malformed (see above); nothing is cleared"
fi

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
# segments of the live template of TRUSTED_STS); both come in through the environment, which awk reads unmangled. With an
# allowlist, ALLOW and the facts of allow_facts come in the same way; a clear prints to stdout, which lands in CLEARED.
CLEARED=""
scan_admin_users() {
  local out rc=0
  out="$(printf '%s\n' "$2" | KIND="$1" SERVER_SET="$SERVER_SET" STS_SET="$STS_SET" STS_OWNER="StatefulSet/${TRUSTED_STS};" \
    ALLOW="$ALLOW" PHASES="$AL_PHASES" RSOWN="$AL_RS" JOBST="$AL_JOBS" awk -F'|' '
    function repo(img) { sub(/@.*/, "", img); if (match(img, /:[^\/]*$/)) img = substr(img, 1, RSTART - 1); return img }
    function norm(seg,   p, n) { n = split(seg, p, "@@"); if (n != 3) return "!"; return p[1] "@@" repo(p[2]) "@@" p[3] }
    function load(str, set,   a, n, i, k) { n = split(str, a, "<<c>>"); for (i = 1; i <= n; i++) if (a[i] != "") { k = norm(a[i]); if (k != "!") set[k] = 1 } }
    function stop(obj, cname, why) { print kind " " obj (cname != "" ? " container " cname : "") ": " why > "/dev/stderr"; bad = 1 }
    # facts NAME -> VALUE from "name|value" lines; a line of another shape is skipped, so its object is never cleared
    function facts(str, set, nf,   a, e, n, i) { n = split(str, a, "\n"); for (i = 1; i <= n; i++) if (split(a[i], e, "|") == nf) set[e[1]] = (nf == 2 ? e[2] : e[2] "|" e[3]) }
    function fine(t) { t = tolower(t); return !index(t, "dho") && !(index(t, "users") && index(t, "admin")) }
    function cleared(k) { print "cleared-by-allowlist " k " object=" $1; return 1 }
    # A row the scan cannot split: only a finished Job on the list (its Job row, or a Succeeded pod with that Job as sole owner);
    # every image of the row (pieces split on <<c>> then @@, exactly 3 parts each) must equal the entry. Fields 1-3 hold names only.
    function clearrow(   owner, k, r, j, n, pc, i, s, q, m) {
      if (!nal || $2 "" == "go-api" || $2 "" == "auth-service") return 0
      if (kind == "pod") {
        if ($3 !~ /^Job\/[^;]+;$/ || phase[$1] "" != "Succeeded") return 0
        owner = substr($3, 1, length($3) - 1)
      } else if (kind == "job") owner = "Job/" $1
      else return 0
      k = kind "|" owner "|-|malformed row"
      if (!(k in aimg) || !(substr(owner, 5) in jobst) || jobst[substr(owner, 5)] !~ /^0?\|.+$/) return 0
      r = $0; for (j = 0; j < 3; j++) r = substr(r, index(r, "|") + 1)
      if (!fine(r)) return 0
      n = split(r, pc, "<<c>>"); m = 0
      for (i = 1; i <= n; i++) {
        s = pc[i]; sub(/^\|+/, "", s)
        if (s == "") continue
        if (split(s, q, "@@") != 3 || q[2] "" != aimg[k] "") return 0
        m++
      }
      return m > 0 && cleared(k)
    }
    # shell -c in one container of a pod: owner chain pod -> ReplicaSet -> Deployment, each the sole owner; name and image exact.
    function clearshell(sg,   rs, o, q, k) {
      if (!nal || kind != "pod" || $2 "" == "go-api" || $2 "" == "auth-service" || $3 !~ /^ReplicaSet\/[^;]+;$/) return 0
      rs = substr($3, 12, length($3) - 12); o = (rs in rsown) ? rsown[rs] "" : ""
      if (o !~ /^Deployment\/[^;]+;$/) return 0
      split(sg, q, "@@"); k = "pod|" substr(o, 1, length(o) - 1) "|" q[1] "|shell -c"
      if (!(k in aimg) || q[2] "" != aimg[k] "" || !fine(sg)) return 0
      return cleared(k)
    }
    BEGIN {
      kind = ENVIRON["KIND"]
      load(ENVIRON["SERVER_SET"], srv); load(ENVIRON["STS_SET"], sts)
      admin = "(^|[^A-Za-z0-9_-])admin($|[^A-Za-z0-9_-])"; users = "(^|[^A-Za-z0-9_-])users($|[^A-Za-z0-9_-])"
      evalw = "(^|[^A-Za-z0-9_-])eval($|[^A-Za-z0-9_-])"
      shell = "\"([^\"]*/)?(sh|bash|ash|dash|zsh|ksh)\""; cflag = "\"-[A-Za-z]*c[A-Za-z]*\""
      nal = split(ENVIRON["ALLOW"], al, "\n")
      for (i = 1; i <= nal; i++) { split(al[i], e, "|"); aimg[e[1] "|" e[2] "|" e[3] "|" e[4]] = e[5] }
      facts(ENVIRON["PHASES"], phase, 2); facts(ENVIRON["RSOWN"], rsown, 2); facts(ENVIRON["JOBST"], jobst, 3)
    }
    NF {
      if (NF != 6) { if (!clearrow()) stop($1, "", "malformed row (a field separator in an argument?)"); next }
      for (f = 4; f <= 6; f++) {
        n = split($f, seg, "<<c>>")
        for (i = 1; i <= n; i++) {
          if (seg[i] == "") continue
          key = norm(seg[i])
          if (key == "!") { stop($1, "", "malformed container segment"); continue }
          if (kind == "pod" && (key in srv || ($3 == ENVIRON["STS_OWNER"] && key in sts))) continue
          split(key, p, "@@"); cmd = p[3]; cname = p[1]
          if (index(cmd, "dho")) stop($1, cname, "mentions dho")
          if (cmd ~ shell && cmd ~ cflag && !clearshell(seg[i])) stop($1, cname, "runs a shell with -c")
          if (index(cmd, "$(") || index(cmd, "`") || index(cmd, "${") || cmd ~ evalw) stop($1, cname, "uses a command substitution, a variable or eval")
          if (cmd ~ admin && cmd ~ users) stop($1, cname, "holds admin and users")
        }
      }
    }
    END { exit bad }')" || rc=$?
  if [ -n "$out" ]; then
    printf '%s\n' "$out"
    CLEARED="$CLEARED$out"$'\n'
  fi
  [ "$rc" -eq 0 ] || fail "a $1 in the namespace holds an admin users writer or a container the scan cannot clear (see above)"
}

# allow_facts: with an allowlist only, the facts a clear needs, read after the spec rows they guard: pod phase, ReplicaSet
# owners, Job active + completionTime.
AL_PHASES="" AL_RS="" AL_JOBS=""
allow_facts() {
  [ -n "$ALLOW" ] || return 0
  kget ns pods '{range .items[*]}{.metadata.name}|{.status.phase}{"\n"}{end}'
  AL_PHASES="$KOUT"
  kget ns rs '{range .items[*]}{.metadata.name}|{range .metadata.ownerReferences[*]}{.kind}/{.name};{end}{"\n"}{end}'
  AL_RS="$KOUT"
  kget ns jobs '{range .items[*]}{.metadata.name}|{.status.active}|{.status.completionTime}{"\n"}{end}'
  AL_JOBS="$KOUT"
}

# allow_unused: an entry that cleared nothing STOPs: the file no longer states the namespace it was reviewed against.
allow_unused() {
  [ -n "$ALLOW" ] || return 0
  local unused
  unused="$(printf '%s\n' "$ALLOW" | CLEARED="$CLEARED" awk -F'|' '
    BEGIN { n = split(ENVIRON["CLEARED"], c, "\n"); for (i = 1; i <= n; i++) if (sub(/^cleared-by-allowlist /, "", c[i]) && sub(/ object=[^ ]*$/, "", c[i])) used[c[i]] = 1 }
    NF { k = $1 "|" $2 "|" $3 "|" $4; if (!(k in used)) print "allowlist-unused " k }')" || die "the allowlist use check failed"
  if [ -n "$unused" ]; then
    printf '%s\n' "$unused" >&2
    fail "an allowlist entry matched nothing: the namespace changed since the review (see above)"
  fi
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
  local rows="$KOUT"
  allow_facts
  scan_admin_users pod "$rows"
  kget ns jobs '{range .items[*]}{.metadata.name}|-|-|{range .spec.template.spec.containers[*]}{.name}@@{.image}@@{.command}{.args}<<c>>{end}|{range .spec.template.spec.initContainers[*]}{.name}@@{.image}@@{.command}{.args}<<c>>{end}|{"\n"}{end}'
  rows="$KOUT"
  allow_facts
  scan_admin_users job "$rows"
  kget ns cronjobs '{range .items[*]}{.metadata.name}|-|-|{range .spec.jobTemplate.spec.template.spec.containers[*]}{.name}@@{.image}@@{.command}{.args}<<c>>{end}|{range .spec.jobTemplate.spec.template.spec.initContainers[*]}{.name}@@{.image}@@{.command}{.args}<<c>>{end}|{"\n"}{end}'
  scan_admin_users cronjob "$KOUT"
  allow_unused
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
