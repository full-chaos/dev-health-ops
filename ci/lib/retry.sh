#!/usr/bin/env bash
# CHAOS-11649: one bounded retry loop for CI steps. Sourced; it sets no shell option and never exits the caller.
#   retry_named NAME TRIES BACKOFF -- COMMAND [ARGS...]
# NAME is [A-Za-z0-9._-]+. TRIES is 1..10: the COMMAND runs at most that many times, until it returns 0.
# BACKOFF is the wait in seconds between two tries: a space-separated list (one value per gap, the last repeats; one value
# is a fixed wait) or `double:N` (N, 2N, 4N, ...). No wait follows the last try.
# One line per try on stderr: `retry: name=NAME attempt=N/TRIES rc=RC elapsed=Ss` (RETRY_QUIET=1 prints none, for a
# caller that prints its own). The command's own output is not touched. Returns 0 on the first success, else the rc of the
# last try; 2 (and no run) for a bad argument.

retry_named() {
  local name="${1-}" tries="${2-}" spec="${3-}" n=0 rc=0 start g base gap i
  local -a gaps=()
  if [ "$#" -lt 5 ] || [ "${4}" != "--" ]; then
    printf 'retry: usage: retry_named NAME TRIES BACKOFF -- COMMAND [ARGS...]\n' >&2
    return 2
  fi
  shift 4
  case "${name}" in '' | *[!ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-]*)
    printf 'retry: bad name %q\n' "${name}" >&2; return 2 ;; esac
  case "${tries}" in '' | *[!0123456789]*) printf 'retry: tries must be 1..10: %q\n' "${tries}" >&2; return 2 ;; esac
  if [ "${tries}" -lt 1 ] || [ "${tries}" -gt 10 ]; then printf 'retry: tries must be 1..10: %s\n' "${tries}" >&2; return 2; fi
  case "${spec}" in
    double:*)
      base="${spec#double:}"
      case "${base}" in '' | *[!0123456789]*) printf 'retry: bad doubling base %q\n' "${spec}" >&2; return 2 ;; esac
      gap="${base}"
      for ((i = 1; i < tries; i++)); do gaps+=("${gap}"); gap=$((gap * 2)); done
      ;;
    *)
      read -r -a gaps <<< "${spec}" || true
      for g in "${gaps[@]}"; do
        case "${g}" in '' | *[!0123456789]*) printf 'retry: bad backoff %q\n' "${spec}" >&2; return 2 ;; esac
      done
      [ "${#gaps[@]}" -gt 0 ] || gaps=(0)
      ;;
  esac
  for ((n = 1; n <= tries; n++)); do
    start="${SECONDS}"
    rc=0
    "$@" || rc=$?
    [ "${RETRY_QUIET:-0}" = 1 ] || printf 'retry: name=%s attempt=%d/%d rc=%d elapsed=%ds\n' "${name}" "${n}" "${tries}" "${rc}" "$((SECONDS - start))" >&2
    [ "${rc}" -ne 0 ] || return 0
    if [ "${n}" -lt "${tries}" ]; then
      i=$((n - 1))
      sleep "${gaps[${i}]:-${gaps[${#gaps[@]} - 1]}}"
    fi
  done
  return "${rc}"
}
