#!/usr/bin/env bash
# Ready-flip reuse judge: on a pull_request ready_for_review run, name the legs that already passed on this exact head sha,
# so the ready run starts only the legs that did not. The workflows read this file from the PR's BASE commit (a change
# cannot rewrite its own judge); a base without it reuses nothing.
#
# A source is trusted only when all of these hold (anything else reuses nothing for that unit, and the leg runs):
#   - another run of this same workflow, event pull_request, head_sha == HEAD_SHA exactly, status completed;
#   - its LATEST attempt (runs/<id>/attempts/<run_attempt>/jobs, every job of that attempt);
#   - JOB-level conclusion success for every job of the unit (a skipped job inside a successful run is not success; a
#     leg that a label or the draft gate did not start is skipped, so it is never reused);
#   - an evidence unit also needs its evidence: integ = the job log has, as an output line, "all deterministic Go
#     integration shards passed" or "plan selected 0 integration shards; none ran (reason: docs|planner)", and no
#     "REUSED" output line (no reuse of a reuse); codeql-merge = a CodeQL analysis exists for refs/pull/<n>/merge at
#     MERGE_SHA (this run's merge commit), the one mergegate checks.
# Before judging, it waits (cap READY_REUSE_WAIT_S, default 1200 s) for the other runs of the same sha that are still in
# progress, so a leg in flight at the flip is reused or run, never cancelled and skipped. A newer ready run that is itself
# waiting (its ready-reuse job still running) is not waited for, so two ready runs never wait on each other.
#
# Units (READY_REUSE_UNITS, one per line): "<unit> <kind> <min> <evidence> <job name>"; repeated unit names AND together.
#   kind exact  = jobs named exactly <job name>; all success, at least <min>.
#   kind prefix = jobs whose name starts with <job name> (a matrix); all success, at least <min>.
#   kind twin   = jobs named exactly <job name> where exactly one of two routed twins runs: at least one success and
#                 every other one skipped, at least <min> jobs.
#   evidence    = - | integ | codeql-merge
# Outputs (GITHUB_OUTPUT): passed=<JSON array of unit names>, reused=<space separated unit=run:attempt:job[,job...]>.
# Each reuse prints "REUSED <unit> from run <id> attempt <a> job <ids>".
# Exit codes: 0 judged (including reuse nothing); 2 a malformed input (a wiring mistake, the leg jobs then do not start
# and the gates go red).
set -uo pipefail
export LC_ALL=C

out="${GITHUB_OUTPUT:-/dev/null}"
emit() {
  printf 'passed=%s\n' "$1" >> "${out}"
  printf 'reused=%s\n' "$2" >> "${out}"
}
none() {
  echo "READY REUSE NONE: $1; every leg runs"
  emit '[]' ''
  exit 0
}

if [ "${EVENT_NAME:-}" != pull_request ] || [ "${EVENT_ACTION:-}" != ready_for_review ]; then
  none "event ${EVENT_NAME:-<empty>}/${EVENT_ACTION:-<empty>} is not a pull_request ready_for_review"
fi

repo="${REPO:-}"
head_sha="${HEAD_SHA:-}"
merge_sha="${MERGE_SHA:-}"
self_id="${RUN_ID:-}"
pr="${PR_NUMBER:-}"
wait_cap="${READY_REUSE_WAIT_S:-1200}"
poll="${READY_REUSE_POLL_S:-20}"
units="${READY_REUSE_UNITS:-}"

bad() { echo "READY REUSE INPUT ERROR: $1" >&2; exit 2; }
[[ "${repo}" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]] || bad "REPO '${repo}'"
[[ "${head_sha}" =~ ^[0-9a-f]{40}$ ]] || bad "HEAD_SHA '${head_sha}' is not a 40-char sha"
[[ "${merge_sha}" =~ ^[0-9a-f]{40}$ ]] || bad "MERGE_SHA '${merge_sha}' is not a 40-char sha"
[[ "${self_id}" =~ ^[0-9]+$ ]] || bad "RUN_ID '${self_id}'"
[[ "${pr}" =~ ^[0-9]+$ ]] || bad "PR_NUMBER '${pr}'"
[[ "${wait_cap}" =~ ^[0-9]+$ ]] || bad "READY_REUSE_WAIT_S '${wait_cap}'"
[[ "${poll}" =~ ^[0-9]+$ ]] || bad "READY_REUSE_POLL_S '${poll}'"

# Parse the units before any API call: a malformed table is a wiring mistake, never "reuse nothing".
spec_unit=() spec_kind=() spec_min=() spec_ev=() spec_name=()
while IFS= read -r line; do
  line="${line#"${line%%[![:space:]]*}"}"
  line="${line%"${line##*[![:space:]]}"}"
  [ -n "${line}" ] || continue
  [[ "${line}" =~ ^([^[:space:]]+)[[:space:]]+([^[:space:]]+)[[:space:]]+([^[:space:]]+)[[:space:]]+([^[:space:]]+)[[:space:]]+(.+)$ ]] \
    || bad "unit line '${line}' has fewer than 5 fields"
  u="${BASH_REMATCH[1]}" k="${BASH_REMATCH[2]}" m="${BASH_REMATCH[3]}" e="${BASH_REMATCH[4]}" name="${BASH_REMATCH[5]}"
  [[ "${u}" =~ ^[a-z0-9][a-z0-9-]*$ ]] || bad "unit name '${u}'"
  case "${k}" in exact | prefix | twin) ;; *) bad "unit ${u}: kind '${k}'" ;; esac
  [[ "${m}" =~ ^[1-9][0-9]*$ ]] || bad "unit ${u}: min '${m}'"
  case "${e}" in - | integ | codeql-merge) ;; *) bad "unit ${u}: evidence '${e}'" ;; esac
  if [ "${e}" = integ ] && { [ "${k}" != exact ] || [ "${m}" != 1 ]; }; then
    bad "unit ${u}: integ evidence needs kind exact and min 1"
  fi
  spec_unit+=("${u}") spec_kind+=("${k}") spec_min+=("${m}") spec_ev+=("${e}") spec_name+=("${name}")
done <<< "${units}"
[ "${#spec_unit[@]}" -gt 0 ] || bad "READY_REUSE_UNITS is empty"

api() { gh api "$@" 2> /dev/null; }

wf_id="$(api "repos/${repo}/actions/runs/${self_id}" | jq -r '.workflow_id // empty')" || wf_id=""
[[ "${wf_id}" =~ ^[0-9]+$ ]] || none "could not read this run's workflow id"

# Runs of this workflow at this head sha, other than this run: "<id> <run_attempt> <status>", newest first.
list_runs() {
  api --paginate "repos/${repo}/actions/workflows/${wf_id}/runs?head_sha=${head_sha}&event=pull_request&per_page=100" \
    | jq -rs --arg sha "${head_sha}" --argjson self "${self_id}" '
        [.[].workflow_runs[]?
          | select(.head_sha == $sha and .event == "pull_request" and .id != $self)
          | select((.id | type) == "number" and (.run_attempt | type) == "number")]
        | sort_by(-.id) | .[] | "\(.id) \(.run_attempt) \(.status)"'
}

# Jobs of the latest attempt of a run, one JSON array; every job must belong to that attempt.
attempt_jobs() {
  api --paginate "repos/${repo}/actions/runs/$1/attempts/$2/jobs?per_page=100" \
    | jq -cs --argjson a "$2" '[.[].jobs[]? | select(.run_attempt == $a)]'
}

# A run still in progress is waited for, except a NEWER ready run whose own ready-reuse job still runs (it waits on us).
started="$(date +%s)"
while :; do
  runs="$(list_runs)" || none "could not list the runs of this workflow at ${head_sha}"
  pending=""
  while read -r id att st; do
    [ -n "${id:-}" ] || continue
    [ "${st}" = completed ] && continue
    if [ "${id}" -gt "${self_id}" ]; then
      waiter="$(attempt_jobs "${id}" "${att}" | jq -r '[.[] | select(.name == "ready-reuse" and .status != "completed")] | length')" || waiter=0
      if [ "${waiter:-0}" != 0 ]; then
        continue
      fi
    fi
    pending="${pending} ${id}"
  done <<< "${runs}"
  [ -z "${pending}" ] && break
  waited=$(($(date +%s) - started))
  if [ "${waited}" -ge "${wait_cap}" ]; then
    echo "::warning title=READY REUSE WAIT CAP::runs${pending} still in progress after ${waited}s; their legs are not reused"
    break
  fi
  echo "waiting for runs${pending} at ${head_sha} (${waited}s of ${wait_cap}s)"
  sleep "${poll}"
done

completed="$(awk '$3 == "completed" { print $1, $2 }' <<< "${runs}")"
[ -n "${completed}" ] || none "no other completed pull_request run of this workflow at ${head_sha}"

ts_re='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+Z '
integ_re="${ts_re}"'(all deterministic Go integration shards passed|plan selected 0 integration shards; none ran \(reason: (docs|planner)\))$'
reused_re="${ts_re}"'REUSED '

codeql_ok=""
codeql_merge_ok() {
  if [ -z "${codeql_ok}" ]; then
    if api --paginate "repos/${repo}/code-scanning/analyses?ref=refs/pull/${pr}/merge&per_page=100" \
      | jq -se --arg m "${merge_sha}" '[.[][]? | select(.tool.name == "CodeQL" and .commit_sha == $m)] | length > 0' > /dev/null; then
      codeql_ok=yes
    else
      codeql_ok=no
    fi
  fi
  [ "${codeql_ok}" = yes ]
}

integ_ok() {
  local log
  log="$(api --allow-escape-sequences "repos/${repo}/actions/jobs/$1/logs" | tr -d '\r' | sed $'s/\xef\xbb\xbf//g; s/\x1b\\[[0-9;]*m//g')" || return 1
  grep -qE "${reused_re}" <<< "${log}" && return 1
  grep -qE "${integ_re}" <<< "${log}"
}

# check_line <jobs json> <i>: prints the matched job ids (comma separated) when spec line i holds, else returns 1.
check_line() {
  local jobs=$1 i=$2 ids
  ids="$(jq -r --arg k "${spec_kind[$i]}" --arg n "${spec_name[$i]}" --argjson min "${spec_min[$i]}" '
    [.[] | select(if $k == "prefix" then (.name | startswith($n)) else .name == $n end)] as $m
    | ($m | map(select(.conclusion == "success")) | length) as $ok
    | if ($m | length) < $min then empty
      elif $k == "twin" then
        (if $ok >= 1 and ($m | all(.conclusion == "success" or .conclusion == "skipped")) then $m | map(select(.conclusion == "success")) else empty end)
      elif $ok == ($m | length) then $m
      else empty end
    | map(.id | tostring) | join(",")' <<< "${jobs}")" || return 1
  [ -n "${ids}" ] || return 1
  case "${spec_ev[$i]}" in
    integ) integ_ok "${ids}" || return 1 ;;
    codeql-merge) codeql_merge_ok || return 1 ;;
  esac
  printf '%s\n' "${ids}"
}

declare -A jobs_cache=()
passed=() reused=()
seen=" "
for u in "${spec_unit[@]}"; do
  case "${seen}" in *" ${u} "*) continue ;; esac
  seen="${seen}${u} "
  hit=""
  while read -r id att; do
    [ -n "${id:-}" ] || continue
    if [ -z "${jobs_cache[${id}]+x}" ]; then
      jobs_cache[${id}]="$(attempt_jobs "${id}" "${att}")" || jobs_cache[${id}]='[]'
    fi
    all=""
    ok=1
    for i in "${!spec_unit[@]}"; do
      [ "${spec_unit[$i]}" = "${u}" ] || continue
      if ids="$(check_line "${jobs_cache[${id}]}" "${i}")"; then
        all="${all:+${all},}${ids}"
      else
        ok=0
        break
      fi
    done
    if [ "${ok}" = 1 ] && [ -n "${all}" ]; then
      hit="${id}:${att}:${all}"
      echo "REUSED ${u} from run ${id} attempt ${att} job ${all}"
      break
    fi
  done <<< "${completed}"
  if [ -n "${hit}" ]; then
    passed+=("${u}")
    reused+=("${u}=${hit}")
  else
    echo "RUN ${u}: no completed run at ${head_sha} passed it on its latest attempt"
  fi
done

json="$(printf '%s\n' "${passed[@]+"${passed[@]}"}" | jq -Rsc 'split("\n") | map(select(length > 0))')"
emit "${json}" "${reused[*]+"${reused[*]}"}"
