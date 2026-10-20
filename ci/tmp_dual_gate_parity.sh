#!/usr/bin/env bash
# TEMPORARY (reverted in the next commit): runs main's Python proof gate and the Go gate back to back on the same
# E2E state and prints both outputs' sha256 and every diff. Never fails the job.
set -u
OUT="${TMP_DIR}/dual-gate"
mkdir -p "${OUT}"
FAMILIES="cicd deploy testops_pipeline testops_test testops_coverage dora repo_user_commit team_wellbeing team_cognitive_load compounding_risk compounding_risk_team ic_finalize work_item_issue_type work_item_investment"

echo "==> TEMPORARY dual-gate parity step"
git fetch -q --depth=1 origin main || echo "fetch of origin/main failed"
git show FETCH_HEAD:ci/assert_metrics_executed_proof.py >"${OUT}/py_gate.py" || echo "could not read the Python gate"
echo "python gate source sha256: $(sha256sum "${OUT}/py_gate.py" | cut -d' ' -f1)"

run_py() {
  # shellcheck disable=SC2086
  CLICKHOUSE_URI="${CLICKHOUSE_URI_HTTP}" PYTHONPATH="${ROOT_DIR}/src" python3 "${OUT}/py_gate.py" \
    --org-id "${ORG_ID}" --run-start "${RUN_START}" --families ${FAMILIES} >"$1" 2>&1
  echo "python gate exit $?" >>"$1"
}
run_go() {
  # shellcheck disable=SC2086
  CLICKHOUSE_URI="${CLICKHOUSE_URI_NATIVE}" "${BIN_DIR}/cigate" metrics-proof \
    --org-id "${ORG_ID}" --run-start "${RUN_START}" --families ${FAMILIES} >"$1" 2>&1
  echo "go gate exit $?" >>"$1"
}

run_py "${OUT}/py1.txt"
run_go "${OUT}/go1.txt"
run_py "${OUT}/py2.txt"
run_go "${OUT}/go2.txt"

for f in py1 go1 py2 go2; do
  echo "sha256 ${f}: $(sha256sum "${OUT}/${f}.txt" | cut -d' ' -f1)"
done
for pair in "py1 go1" "py2 go2" "py1 py2" "go1 go2"; do
  set -- ${pair}
  if diff "${OUT}/$1.txt" "${OUT}/$2.txt" >"${OUT}/diff-$1-$2.txt"; then
    echo "DIFF $1 vs $2: none"
  else
    echo "DIFF $1 vs $2:"
    cat "${OUT}/diff-$1-$2.txt"
  fi
done
echo "==> outputs"
for f in py1 go1; do
  echo "----- ${f}"
  cat "${OUT}/${f}.txt"
done
exit 0
