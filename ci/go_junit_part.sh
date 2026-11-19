#!/usr/bin/env bash
# CHAOS-13766: usage: ci/go_junit_part.sh DEST DIR...
# Convert the go test -json files of every DIR with ci/go_test_json_to_junit.sh and move the JUnit XML into DEST (one
# go-junit-part-<leg> artifact). Best effort like the converter: a missing DIR is skipped and the exit status is always 0.
set -uo pipefail

dest="${1:?usage: ci/go_junit_part.sh DEST DIR...}"
shift
mkdir -p -- "${dest}"
for dir in "$@"; do
  [ -d "${dir}" ] || continue
  bash "$(dirname "${BASH_SOURCE[0]}")/go_test_json_to_junit.sh" "${dir}"
  for xml in "${dir}"/*.xml; do
    [ -f "${xml}" ] || continue
    mv -- "${xml}" "${dest}/${xml##*/}"
  done
done
if [ -z "$(find "${dest}" -maxdepth 1 -name '*.xml')" ]; then
  echo "::warning::go_junit_part: no JUnit part was written for this leg (no go test -json file, or the converter failed); the merged report will lack it"
fi
exit 0
