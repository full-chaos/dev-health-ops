#!/usr/bin/env bash
# python_tripwire.sh -- SOURCE this file to make Python unreachable for the rest of the shell.
#
#   source ci/python_tripwire.sh
#
# WHY (CHAOS-7384). Hosted runners have python3, so a green Go job does not prove a frozen test is
# Python-free. This file turns every attempt to start Python into a loud, named failure:
#
#   * a shim directory is PREPENDED to PATH holding python, python3, python3.13, python3.14, pip,
#     pip3 and uv; each prints "PYTHON TRIPWIRE: <argv> (parent: <cmdline>)" to stderr, appends one
#     line to $PYTHON_TRIPWIRE_LOG and exits 97;
#   * DEV_HEALTH_PYTHON and PYTHON point at the python3 shim itself, so pyoracle.Resolve (which
#     honours them first) and any absolute-path start also reach the shim, which names the caller.
#
# This file ONLY prepends PATH and exports variables, in the sourcing shell. It never changes a mode,
# never touches a system interpreter, and never writes outside its own temporary directory. Removing the
# execute bit of the runner's /usr/bin/python3* is a separate, hosted-runner-only step of
# .github/workflows/go-python-free.yml.
#
# GUARD. It refuses to run unless GITHUB_ACTIONS is "true" or PYTHON_TRIPWIRE_ALLOW_LOCAL=1, so a lane
# sourcing it on a shared host by accident gets a loud line instead of a changed environment.

_python_tripwire_refuse() {
  printf 'python_tripwire: REFUSING: %s\n' "$1" >&2
}

if [ "${GITHUB_ACTIONS:-}" != "true" ] && [ "${PYTHON_TRIPWIRE_ALLOW_LOCAL:-}" != "1" ]; then
  _python_tripwire_refuse "not on GitHub Actions and PYTHON_TRIPWIRE_ALLOW_LOCAL=1 is not set; nothing was changed"
  # shellcheck disable=SC2317 # sourced: return; executed: exit
  return 1 2>/dev/null || exit 1
fi

PYTHON_TRIPWIRE_DIR="$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/python-tripwire.XXXXXX")" || {
  _python_tripwire_refuse "could not create the shim directory"
  # shellcheck disable=SC2317
  return 1 2>/dev/null || exit 1
}
PYTHON_TRIPWIRE_LOG="${PYTHON_TRIPWIRE_LOG:-${PYTHON_TRIPWIRE_DIR}/hits.log}"
: >"${PYTHON_TRIPWIRE_LOG}"

# The shim body is ci/python_tripwire_shim.sh: @NAME@ and @LOG@ are the only substitutions.
_python_tripwire_shim="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)/python_tripwire_shim.sh"
[ -f "${_python_tripwire_shim}" ] || {
  _python_tripwire_refuse "the shim template ${_python_tripwire_shim} is missing"
  # shellcheck disable=SC2317
  return 1 2>/dev/null || exit 1
}
for _python_tripwire_name in python python3 python3.13 python3.14 pip pip3 uv; do
  sed -e "s|@NAME@|${_python_tripwire_name}|g" -e "s|@LOG@|${PYTHON_TRIPWIRE_LOG}|g" \
    "${_python_tripwire_shim}" >"${PYTHON_TRIPWIRE_DIR}/${_python_tripwire_name}"
  chmod +x "${PYTHON_TRIPWIRE_DIR}/${_python_tripwire_name}"
done
unset _python_tripwire_shim
unset _python_tripwire_name

export PYTHON_TRIPWIRE_DIR PYTHON_TRIPWIRE_LOG
export PATH="${PYTHON_TRIPWIRE_DIR}:${PATH}"
# The shim itself: pyoracle.Resolve honours DEV_HEALTH_PYTHON, then PYTHON, before the repo .venv, so a test that
# starts the interpreter by that absolute path reaches the shim too, which names the caller and logs the start.
export DEV_HEALTH_PYTHON="${PYTHON_TRIPWIRE_DIR}/python3"
export PYTHON="${PYTHON_TRIPWIRE_DIR}/python3"
printf 'python_tripwire: armed: shims in %s, DEV_HEALTH_PYTHON=%s, hits log %s\n' \
  "${PYTHON_TRIPWIRE_DIR}" "${DEV_HEALTH_PYTHON}" "${PYTHON_TRIPWIRE_LOG}" >&2
