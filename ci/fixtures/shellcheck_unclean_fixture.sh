#!/usr/bin/env bash
# DELIBERATELY UNCLEAN. Not real code, never executed, never in the linted set.
#
# This is the red-first fixture for the shellcheck pin: ci/check_shellcheck_pin.sh
# asserts the pinned shellcheck FAILS on this file. If it ever passes, the pin
# is not actually running -- a linter wired up wrong is indistinguishable from a
# codebase with no problems, and both report success.
#
# NOTE ON THE SHAPE. The obvious `[ -f x ] && echo yes || echo no` does NOT
# trigger SC2015 in 0.10.0 or 0.11.0 -- measured. SC2015 is selective; it fires
# on a braced group and on `&& true || false`. Using the non-triggering shape
# would have produced a "red-first" fixture that is silently green, which is the
# exact failure this fixture exists to detect.
set -euo pipefail
a=1
[ "$a" = 1 ] && { echo x; } || { echo y; }
[ "$a" = 1 ] && true || false
