#!/usr/bin/env bash
# CHAOS-3462 B1: the ARMED Wave 4 corpus run, invoked so it cannot report a
# false green.
#
# Two things go wrong if you run the corpus runner with a bare `pytest`:
#
#   1. CHAOS-3402's tests/_env_isolation.py scrub deletes ASK_DEV_LIVE_ACCEPTANCE
#      (and five more variables the armed path reads) in pytest_configure, before
#      any fixture runs. The arming guard then sees an unset flag, concludes
#      "nobody asked for this run", and skips every case: `144 skipped`, exit 0.
#      A green, entirely-skipped session. This script exports the module's own
#      documented DEV_HEALTH_TEST_ENV_ALLOW exemption for exactly the names the
#      armed path needs -- derived in scripts/acceptance/corpus/arming.py and
#      pinned against the real SCRUB_ENV_NAMES by a unit test, so it cannot
#      silently fall out of date.
#
#   2. Even correctly armed, a run can execute nothing and still exit 0. So after
#      pytest finishes, this asserts from the OUTSIDE -- off the run's own JUnit
#      XML -- that a non-zero number of cases actually executed. An absent or
#      unparseable report is a failure, not a pass (a measurement that did not
#      happen must fail loudly).
#
# The stack must already be up. This script never boots or tears down compose --
# scripts/acceptance/run_ask_dev_compose.sh owns that, and conflating the two
# would make a re-run of the assertions cost a full stack rebuild.
#
# usage: run_wave4_corpus.sh [extra pytest args...]

set -uo pipefail

ops_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
venv_python="${ops_root}/.venv/bin/python"
if [[ ! -x "${venv_python}" ]]; then
  echo "expected the worktree venv at ${venv_python} -- run 'uv sync --all-extras --dev' first" >&2
  exit 64
fi

if [[ "${ASK_DEV_LIVE_ACCEPTANCE:-}" != "1" ]]; then
  echo "ASK_DEV_LIVE_ACCEPTANCE=1 must be exported before running the armed corpus." >&2
  echo "Boot the stack with scripts/acceptance/run_ask_dev_compose.sh first." >&2
  exit 64
fi

# ASK_DEV_CORPUS_MIN_EXECUTED raises the executed-case floor. run_report's
# default is 1, and 1 is the right default HERE -- an interactive `-k` run is a
# legitimate reason to execute a single case. It is the wrong floor for an
# unattended lane: adversarial review (2026-08-06, MEDIUM-HIGH, reproduced with
# a synthetic JUnit report) showed a run that executes 1 of 144 cases exits 0
# and reports green, because nothing between the case-file glob and that
# assertion knows how many cases there are SUPPOSED to be. load_corpus_cases
# documents an empty result as a deliberate non-failure and
# check_script_inventory only catches cases missing a script, never scripts
# whose case file vanished -- so a bad rebase or partial merge that drops most
# case files certifies a fraction of the corpus as a pass.
#
# This is a COARSE ATTRITION FLOOR, not the closed expected-case registry the
# execution plan's section 5 item 3 mandates ("expected - received = missing ->
# red"). That registry is Lane 5a's scripts/acceptance/wave4_manifest.py, which
# does not exist yet. A floor catches catastrophic loss while surviving the
# corpus's own churn (144 -> 140 as cases are re-authored); it cannot catch a
# handful of cases going missing. Raise it to an exact set-difference when 5a
# lands, and until then do not read a green run as evidence every case ran.
#
# Validated HERE rather than beside its use below, so a bad value costs a
# second instead of failing after a twelve-minute corpus run.
min_executed_args=()
if [[ -n "${ASK_DEV_CORPUS_MIN_EXECUTED:-}" ]]; then
  if [[ ! "${ASK_DEV_CORPUS_MIN_EXECUTED}" =~ ^[1-9][0-9]*$ ]]; then
    echo "ASK_DEV_CORPUS_MIN_EXECUTED must be a positive integer, not '${ASK_DEV_CORPUS_MIN_EXECUTED}'" >&2
    echo "-- refusing to guess a floor, which would silently become the default of 1." >&2
    exit 64
  fi
  min_executed_args=(--min-executed "${ASK_DEV_CORPUS_MIN_EXECUTED}")
fi

# Merge, never clobber: an operator debugging with their own
# DEV_HEALTH_TEST_ENV_ALLOW keeps their exemption. arming.env_allow_value is the
# single source of truth for the list.
DEV_HEALTH_TEST_ENV_ALLOW="$(
  PYTHONPATH="${ops_root}/src:${ops_root}" "${venv_python}" -c \
    'import os
from scripts.acceptance.corpus.arming import env_allow_value
print(env_allow_value(os.environ.get("DEV_HEALTH_TEST_ENV_ALLOW")))' \
)" || exit 70
export DEV_HEALTH_TEST_ENV_ALLOW

# Refuse parallel execution outright, and belt-and-braces it by disabling the
# xdist plugin entirely below (`-p no:xdist`), because the scan alone is
# bypassable: pytest honors PYTEST_ADDOPTS regardless of argv, so
# `PYTEST_ADDOPTS="-n 4" run_wave4_corpus.sh` ran four workers past an
# argv-only check. The scan now covers that variable too AND the plugin is
# off, so a parallel flag from any source is an error rather than a silently
# honored one.
#
# QuotaBudget is per-process and
# per-session (its own docstring: "one instance per corpus-runner pytest
# session"), so N xdist workers would each start from the FULL monthly
# ceiling and the run could spend N times the budget while every worker
# believed it was within limits. The arming evidence also only survives the
# controller->worker boundary via the scrub record, which is a recovery
# path, not a reason to run this way on purpose.
for arg in "$@" ${PYTEST_ADDOPTS:-}; do
  case "${arg}" in
    -n|--numprocesses|-n*|--numprocesses=*|--dist|--dist=*|--forked)
      echo "refusing to run the armed corpus in parallel (${arg}): QuotaBudget is" >&2
      echo "per-process, so each worker would start from the full monthly ceiling." >&2
      exit 64
      ;;
  esac
done

report_dir="${ops_root}/tests/acceptance/artifacts/wave4"
report="${report_dir}/junit-corpus.xml"
mkdir -p "${report_dir}"
rm -f "${report}"

echo "=== DEV_HEALTH_TEST_ENV_ALLOW=${DEV_HEALTH_TEST_ENV_ALLOW}"
echo "=== armed corpus run ==="
cd "${ops_root}" || exit 70
PYTHONPATH="${ops_root}/src:${ops_root}" "${venv_python}" -m pytest \
  tests/acceptance/test_wave4_corpus_runner_live.py \
  --junitxml="${report}" \
  -p no:cacheprovider \
  -p no:xdist \
  -rs \
  -v \
  "$@"
pytest_status=$?
echo "=== pytest exit: ${pytest_status} ==="

# Runs even when pytest failed: "did anything execute" is a different question
# from "did it pass", and the answer matters most precisely when the run looks
# suspiciously clean. This can only turn a green run red, never the reverse --
# the pytest status is re-raised below if it was non-zero.
echo "=== executed-case assertion (floor: ${ASK_DEV_CORPUS_MIN_EXECUTED:-1}) ==="
PYTHONPATH="${ops_root}/src:${ops_root}" "${venv_python}" -m \
  scripts.acceptance.corpus.run_report "${report}" "${min_executed_args[@]+"${min_executed_args[@]}"}"
report_status=$?

if [[ ${report_status} -ne 0 ]]; then
  exit ${report_status}
fi

# CHAOS-3575: "did anything run" and "did what ran measure anything" are
# different questions, and the second one has been answered wrong. The armed
# run of 2026-08-07 10:03 reported `134 collected, 0 skipped`, executed 90 real
# corpus cases, and passed the executed-case assertion above -- while 59 of
# those 90 had raised HTTP 429 before their recorder ran and had therefore
# asserted NOTHING. Every count this script already checked was correct; none
# of them compared cases-executed against receipts-written, so a run that
# measured a third of the corpus was indistinguishable from a complete one.
#
# Runs even when pytest failed, for the same reason the executed-case assertion
# does: this matters MOST when the run looks like an ordinary red, because that
# is what a mass-degraded run looks like from the outside.
echo "=== receipt-coverage assertion ==="
PYTHONPATH="${ops_root}/src:${ops_root}" "${venv_python}" -m \
  scripts.acceptance.corpus.receipt_coverage "${report}" \
  --receipts-dir "${report_dir}"
coverage_status=$?

# UNMEASURED outranks a red corpus: a run that did not measure cannot be graded
# at all, so its status must never be masked by pytest's own exit code. Exit 68
# is distinct from pytest's 1..5 and from run_report's 66/67, so a caller can
# tell "the corpus went red" from "the corpus did not get measured".
if [[ ${coverage_status} -ne 0 ]]; then
  exit ${coverage_status}
fi
exit ${pytest_status}
