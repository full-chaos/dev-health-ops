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
exec uv run pytest trials/chaos_3499/tests "$@"
