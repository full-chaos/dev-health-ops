#!/usr/bin/env bash
# DELIBERATELY UNCLEAN. Not real code, never executed, never in the linted set.
#
# This is the red-first fixture for the shellcheck pin: ci/check_shellcheck_pin.sh
# asserts the pinned shellcheck FAILS on this file. If it ever passes, the pin
# is not actually running -- a linter wired up wrong is indistinguishable from a
# codebase with no problems, and both report success.
#
# NOTE ON THE SHAPE -- MEASURED, NOT DERIVED. The obvious
# `[ -f x ] && echo yes || echo no` does NOT trigger SC2015 in 0.9.0 (the
# version the runner ships), 0.10.0, or 0.11.0 -- measured against all three.
# It fires on a braced group and on `&& true || false`. Using the
# non-triggering shape would have produced a "red-first" fixture that is
# silently green -- the exact failure this fixture exists to detect.
#
# Do not try to reason out which shapes are safe. The trigger set is NOT
# "commands that might fail" (lane-4441's measurement): `true` cannot fail and
# DOES warn, while `echo` can fail and does NOT. Any first-principles rule you
# invent here will be wrong. Run the binary against the shape you intend to use.
#
# The two lines below are verified to fail under 0.9.0, 0.10.0 AND 0.11.0, so
# this control stays valid whichever of them a future pin selects.
set -euo pipefail
a=1
[ "$a" = 1 ] && { echo x; } || { echo y; }
[ "$a" = 1 ] && true || false
