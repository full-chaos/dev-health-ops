#!/usr/bin/env bash
# CHAOS-3499 trial oracle suite.
#
# Deliberately NOT part of `ci/local_validate.sh`: that gate is host-wide
# single-flight and shared by every worktree, and shadow-trial code must not
# be able to destabilise it. The cost is that this suite runs only when
# someone runs it -- which is what this script is for.
#
# Exits non-zero on any failure. A non-zero exit means the oracles are not
# trustworthy and no trial result derived from them may be reported.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
# --extra dev: the plain `uv run pytest` invocation this replaced depended on
# whatever the ambient environment happened to have installed already. On a
# freshly `git worktree add`-ed checkout with no prior sync there is no
# guarantee pytest (or anything else this suite imports transitively) is
# present, and the failure ("No module named pytest") gives no hint that the
# fix is a sync flag. Naming the extra explicitly makes the dependency this
# script actually has on it visible and self-sufficient.
exec uv run --extra dev pytest trials/chaos_3499/tests "$@"
