#!/bin/sh
# The python/python3/pip/uv shim ci/python_tripwire.sh installs (CHAOS-7384). @NAME@ and @LOG@ are replaced when
# it is copied. It names the caller, logs the start and exits 97; it never runs Python.
#
# parent = the nearest ancestor that is a Go test binary (*.test), else the direct parent: a start through
# `sh -c` or a helper script is still attributed to the test binary that ran it.
parent="$(tr '\000' ' ' < "/proc/$PPID/cmdline" 2>/dev/null)"
p=$PPID
depth=0
while [ "$p" -gt 1 ] 2>/dev/null && [ "$depth" -lt 12 ]; do
  cmd="$(tr '\000' ' ' < "/proc/$p/cmdline" 2>/dev/null)"
  case "${cmd%% *}" in
    *.test) parent="$cmd"; break ;;
  esac
  p="$(awk '/^PPid:/ { print $2 }' "/proc/$p/status" 2>/dev/null)"
  depth=$((depth + 1))
done
printf 'pid=%s ppid=%s shim=%s argv=%s parent=%s\n' "$$" "$PPID" "@NAME@" "$*" "$parent" >> "@LOG@"
echo "PYTHON TRIPWIRE: @NAME@ invoked with: $* (parent: $parent)" >&2
exit 97
