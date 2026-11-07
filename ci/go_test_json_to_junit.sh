#!/usr/bin/env bash
# CHAOS-13193: write a JUnit XML next to every go test -json file of a directory, so the product's GitHub tests sync (it reads
# .xml and .info members only) sees the results. The .json files stay. Best effort: a converter failure is reported and
# skipped, and the exit status is always 0, so it can never change the result of the test step before it.
set -uo pipefail

JUNIT_REPORT_MODULE="github.com/jstemmer/go-junit-report/v2@v2.1.0"
dir="${1:?usage: ci/go_test_json_to_junit.sh DIR}"

for json in "${dir}"/*.json; do
  [ -f "${json}" ] || continue
  xml="${json%.json}.xml"
  if go run -trimpath "${JUNIT_REPORT_MODULE}" -parser gojson -in "${json}" -out "${xml}"; then
    echo "go_test_json_to_junit: ${json##*/} -> ${xml##*/}"
  else
    rm -f -- "${xml}"
    echo "go_test_json_to_junit: could not convert ${json##*/}; the .json stays" >&2
  fi
done
exit 0
