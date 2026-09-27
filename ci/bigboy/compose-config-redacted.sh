#!/usr/bin/env bash
# compose-config-redacted.sh -- the ONLY sanctioned way to read `docker compose config`
# output on this host (team-lead, CHAOS-6987, 2026-09-27): bare `docker compose config`
# (piped to grep/cat -A/anything else) can print a resolved secret value straight into a
# terminal or a report -- it already happened three times this session. This script is the
# single redacting filter: every line that looks like a compose-rendered `  NAME: value` or
# `      NAME: value` environment entry is rewritten to `NAME=<byte-length-of-value>`; nothing
# else about the value ever reaches stdout. Callers (bigboy-cut.sh's drift checks, ad-hoc
# debugging) pipe through THIS script only -- never `docker compose config` directly.
#
# Usage: compose-config-redacted.sh <compose -f ... args...> -- prints NAME=<len> per env
# entry found in the rendered config, one per line, sorted.
set -euo pipefail
# No cd here: -f/--env-file paths in "$@" are relative to the CALLER's cwd (matching
# bigboy-cut.sh's own convention of `cd $R` before every compose invocation), not to
# this script's own location.
err=$(mktemp)
trap 'rm -f "$err"' EXIT
# compose's own stderr can quote a resolved value in some error shapes, so it is never
# passed through; only the NAME of an unset required variable is surfaced.
set +e
docker compose "$@" config 2>"$err" \
  | awk -F': ' '
      /^[[:space:]]+[A-Za-z_][A-Za-z0-9_]*:/ {
        line = $0
        sub(/^[[:space:]]+/, "", line)
        colon = index(line, ":")
        if (colon == 0) next
        name = substr(line, 1, colon - 1)
        rest = substr(line, colon + 2)
        # docker compose config quotes a value that needs it (leading/trailing space,
        # special YAML chars); strip exactly one layer of matching quotes before measuring,
        # so a quoted vs unquoted rendering of the same value reports the same length.
        if (length(rest) >= 2) {
          first = substr(rest, 1, 1)
          last = substr(rest, length(rest), 1)
          if ((first == "\"" && last == "\"") || (first == "'\''" && last == "'\''")) {
            rest = substr(rest, 2, length(rest) - 2)
          }
        }
        print name "=" length(rest)
      }
    ' \
  | sort -u
rc=${PIPESTATUS[0]}
grep -o 'required variable [A-Za-z_][A-Za-z0-9_]* is missing' "$err" | sort -u >&2 || true
exit "$rc"
