#!/usr/bin/env bash
# CHAOS-3575: execute the ARMED Wave 4 corpus against a stack that
# armed_corpus_boot.sh has already brought up and verified.
#
# WHY THIS IS IN-TREE: see armed_corpus_boot.sh's header. The scratch copy of
# this recipe was wiped by a reboot mid-run, and the archive it was rebuilt from
# laundered the launcher's exit code (`bash run_wave4_corpus.sh; echo "...0"`),
# which would have reported a fully-red corpus as a pass.
#
# This delegates to scripts/acceptance/run_wave4_corpus.sh rather than
# re-rolling its arming workaround. That launcher owns DEV_HEALTH_TEST_ENV_ALLOW,
# --junitxml, the executed-case assertion, and (CHAOS-3575) the receipt-coverage
# assertion, so this script deliberately adds NO assertions of its own -- one
# definition of "was this run measurable", not two that can drift.
#
# macOS bash 3.2 compatible.
set -uo pipefail

ops_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
web_root="${ASK_DEV_WEB_CONTEXT:-$(cd -- "${ops_root}/../web" 2>/dev/null && pwd || true)}"
export ASK_DEV_WEB_CONTEXT="${web_root}"
export ASK_DEV_ACCEPTANCE_API_PORT="${ASK_DEV_ACCEPTANCE_API_PORT:-18099}"
export BUGSINK_SECRET_KEY="${BUGSINK_SECRET_KEY:-ask-dev-acceptance-unused}"

export ASK_DEV_LIVE_ACCEPTANCE=1
export ASK_DEV_ACCEPTANCE_API_URL="http://127.0.0.1:${ASK_DEV_ACCEPTANCE_API_PORT}"
export TEST_SUPERUSER_EMAIL="${TEST_SUPERUSER_EMAIL:-admin@devhealth.example}"
export TEST_SUPERUSER_PASSWORD="${TEST_SUPERUSER_PASSWORD:-devhealth123}"

# QuotaBudget.from_env reads the RUNNER's env, not the container's, so the
# runner needs its own copy of the ceilings.
#
# READ THIS BEFORE CHANGING THE NUMBERS. An earlier revision set these to the
# compose file's ASK_DEV_PLATFORM_MONTHLY_* values and claimed in its own
# comment to "mirror the api service ceilings exactly". That claim was FALSE and
# it cost a run: ASK_DEV_PLATFORM_MONTHLY_COST_MAX_MICROUSD is the operator
# MAXIMUM -- the ceiling an org may be CONFIGURED UP TO -- while the EFFECTIVE
# limit is the stored per-org row, which falls back to
# PLATFORM_MONTHLY_COST_LIMIT_DEFAULT_MICROUSD ($100) when no row exists. The
# runner therefore budgeted against $200 while the server enforced $100, and 59
# of 90 cases 429'd on cost_limit_reached having recorded nothing.
#
# armed_corpus_boot.sh's PRECONDITION C now writes the per-org row so the
# effective limit really is this value. These two must be changed together.
export ASK_DEV_PLATFORM_MONTHLY_REQUEST_MAX="${ASK_DEV_PLATFORM_MONTHLY_REQUEST_MAX:-1000}"
export ASK_DEV_PLATFORM_MONTHLY_COST_MAX_MICROUSD="${ASK_DEV_PLATFORM_MONTHLY_COST_MAX_MICROUSD:-500000000}"

# CHAOS-3490: the password bridge is GONE. #1540 swapped principals.py to
# production's own password_for_alias, so the runner authenticates with the
# credentials the world actually seeded. Exporting
# ASK_DEV_ACCEPTANCE_ALLOW_PASSWORD_BRIDGE would be a no-op today and actively
# wrong at this scale: the bridge issued one admin-set-password call per
# principal, and ADMIN_PASSWORD_LIMIT is 5/hour, so ten pool principals would
# 429 after five. Receipts are stamped provisioned_via_world-seeded-credentials.

# CHAOS-3490: production limits logins to AUTH_LOGIN_IP_LIMIT ("20/15minutes",
# PER IP). Spreading the corpus over 10 principals means the boot's login proof
# (10 positives + 1 negative) plus prepare_ask_dev_acceptance.py plus this run's
# own ~11 principal/admin logins exceed 20 inside one window. This wait lets the
# IP bucket drain so the corpus does not 429 on LOGIN -- a DIFFERENT limiter
# from the per-user Ask Dev request cap the principal pool exists to dodge.
# Deliberate and stated, not a sleep-until-it-works: skip it and the run fails
# loudly on 429 rather than silently under-measuring.
if [[ "${ASK_DEV_SKIP_LOGIN_WINDOW_WAIT:-0}" != "1" ]]; then
  echo "=== waiting 16m for the per-IP login window to drain (AUTH_LOGIN_IP_LIMIT=20/15minutes) ==="
  sleep 960
fi

echo "=== ARMING PROOF ==="
echo "ASK_DEV_LIVE_ACCEPTANCE=${ASK_DEV_LIVE_ACCEPTANCE}"
PYTHONPATH="${ops_root}/src:${ops_root}" "${ops_root}/.venv/bin/python" -c "
from scripts.acceptance.corpus.arming import require_armed
require_armed(); print('require_armed(): ARMED (did not raise)')
"

echo "=== delegating to scripts/acceptance/run_wave4_corpus.sh ==="
bash "${ops_root}/scripts/acceptance/run_wave4_corpus.sh" "$@"
rc=$?
# Propagate, never launder. The archived predecessor ended with an `echo` after
# this call, making the echo's status the script's, so a fully-red corpus
# returned 0. run_wave4_corpus.sh distinguishes red (pytest 1..5) from
# unmeasured (66/67/68) and that distinction has to survive this hop.
echo "=== run_wave4_corpus.sh exit: ${rc} ==="
exit "${rc}"
