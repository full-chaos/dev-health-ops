"""Coverage-matrix scope guard for the unified documentation initiative (archived program evidence).

The coverage matrix is program evidence, not public product documentation. It was moved out of
the public ``docs/`` tree and is preserved under ``.github/docs-legacy/coverage-matrix.md``. These
tests keep protecting the approved forty-issue scope and per-row completeness against that
archived program-evidence source.
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ARCHIVED_MATRIX_PATH = ROOT / ".github" / "docs-legacy" / "coverage-matrix.md"
EXPECTED_ISSUES = {
    *(f"CHAOS-{number}" for number in range(2310, 2327)),
    *(f"CHAOS-{number}" for number in range(2329, 2347)),
    *(f"CHAOS-{number}" for number in range(2883, 2888)),
}


def coverage_errors(actual_issues: set[str]) -> list[str]:
    missing = sorted(EXPECTED_ISSUES - actual_issues)
    unexpected = sorted(actual_issues - EXPECTED_ISSUES)
    errors: list[str] = []
    if missing:
        errors.append(f"missing matrix issues: {', '.join(missing)}")
    if unexpected:
        errors.append(f"unexpected matrix issues: {', '.join(unexpected)}")
    return errors


def matrix_rows(document: str) -> list[tuple[str, str, str, str, str]]:
    rows: list[tuple[str, str, str, str, str]] = []
    for line in document.splitlines():
        cells = tuple(cell.strip() for cell in line.strip().strip("|").split("|"))
        if len(cells) == 5 and cells[0].startswith("CHAOS-"):
            rows.append(cells)
    return rows


def test_archived_coverage_matrix_matches_the_approved_forty_issue_scope() -> None:
    assert ARCHIVED_MATRIX_PATH.is_file(), (
        f"missing archived coverage matrix: {ARCHIVED_MATRIX_PATH}"
    )

    rows = matrix_rows(ARCHIVED_MATRIX_PATH.read_text(encoding="utf-8"))
    issues = [row[0] for row in rows]

    assert len(rows) == len(EXPECTED_ISSUES)
    assert len(issues) == len(set(issues)), f"duplicate matrix issues: {issues}"
    assert coverage_errors(set(issues)) == []


def test_every_archived_coverage_row_has_disposition_owner_proof_and_completion_action() -> (
    None
):
    assert ARCHIVED_MATRIX_PATH.is_file(), (
        f"missing archived coverage matrix: {ARCHIVED_MATRIX_PATH}"
    )

    rows = matrix_rows(ARCHIVED_MATRIX_PATH.read_text(encoding="utf-8"))

    assert rows
    for issue, disposition, owner_path, proof, completion_action in rows:
        assert disposition, f"{issue} has no disposition"
        assert owner_path, f"{issue} has no owner path"
        assert proof, f"{issue} has no automated proof"
        assert completion_action, f"{issue} has no completion action"
