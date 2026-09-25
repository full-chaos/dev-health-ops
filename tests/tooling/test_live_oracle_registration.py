"""Every Go live-Python oracle test must be run by CI, not merely exist.

A test that skips unless ``DEV_HEALTH_LIVE_PYTHON_ORACLES=1`` proves nothing on
its own: it only counts when ``ci/check_go.sh`` runs it with that variable set
and requires its proof file. This guard enumerates every Go test function whose
own body reads the variable and requires it to be either

* named in one of ``check_go.sh``'s ``-run`` selectors (the
  ``live-python-oracles`` verb), or
* named ``Test*VenueOracle*``, which must be a row of
  ``ci/venue_oracle_registry.d/`` (the ``venue-oracles`` verb runs the
  registry; ``tests/tooling/test_venue_oracle_registry.py`` fails on a
  ``Test*VenueOracle*`` function the registry does not name).

A test that satisfies neither is reported by name, so a new oracle cannot be
added without wiring it.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CHECK_GO = ROOT / "ci" / "check_go.sh"
ORACLE_ENV = "DEV_HEALTH_LIVE_PYTHON_ORACLES"

_RUN_ARGUMENT = re.compile(r"-run\s+'\^?\(?([^']*?)\)?\$'")


def selected_test_names(script: str) -> set[str]:
    """Test names the script's ``go test -run '^(A|B)$'`` selectors pick."""
    names: set[str] = set()
    for match in _RUN_ARGUMENT.finditer(script):
        names.update(part for part in match.group(1).split("|") if part)
    return names


def live_oracle_tests(root: Path) -> list[tuple[str, str]]:
    """(path, test name) of each Go test whose own body reads ORACLE_ENV."""
    found: list[tuple[str, str]] = []
    for base in ("internal", "cmd"):
        for path in sorted((root / base).rglob("*_test.go")):
            text = path.read_text()
            if ORACLE_ENV not in text:
                continue
            chunks = re.split(r"(?m)^func ", text)[1:]
            for chunk in chunks:
                header = re.match(r"(Test\w+)\(", chunk)
                if header and ORACLE_ENV in chunk.split("\n}\n", 1)[0]:
                    found.append((str(path.relative_to(root)), header.group(1)))
    return found


def unregistered(root: Path, script: str) -> list[str]:
    selected = selected_test_names(script)
    return [
        f"{path}: {name}"
        for path, name in live_oracle_tests(root)
        if "VenueOracle" not in name and name not in selected
    ]


def test_every_live_oracle_test_is_run_by_ci() -> None:
    oracles = live_oracle_tests(ROOT)
    # The enumeration itself must not silently go empty (rule: a measurement
    # that did not happen fails).
    assert len(oracles) >= 40, f"only {len(oracles)} live-oracle tests found"
    missing = unregistered(ROOT, CHECK_GO.read_text())
    assert not missing, (
        "live-Python oracle tests that ci/check_go.sh never runs (add the name to "
        "its live-python-oracles -run selector with a proof-file check, or name "
        "it Test*VenueOracle*):\n  " + "\n  ".join(missing)
    )


def test_guard_sees_an_unselected_oracle(tmp_path: Path) -> None:
    """The guard fails on a planted defect: an oracle test named nowhere."""
    package = tmp_path / "internal" / "demo"
    package.mkdir(parents=True)
    (package / "demo_test.go").write_text(
        "package demo\n\n"
        'import "os"\n\n'
        "func TestDemoMatchesLivePython(t *testing.T) {\n"
        f'\tif os.Getenv("{ORACLE_ENV}") != "1" {{\n\t\tt.Skip()\n\t}}\n'
        "}\n"
    )
    script = "go test -run '^(TestOther|TestAnother)$' ./internal/x\n"
    assert unregistered(tmp_path, script) == [
        "internal/demo/demo_test.go: TestDemoMatchesLivePython"
    ]
    selected = script + "go test -run '^TestDemoMatchesLivePython$' ./internal/demo\n"
    assert unregistered(tmp_path, selected) == []


def test_selector_parser_reads_alternations_and_single_names() -> None:
    script = (
        "go test -run '^(TestA|TestB)$' ./a\n"
        "go test -run '^TestC$' ./c\n"
        "go test -run 'NotAnchored' ./d\n"
    )
    assert selected_test_names(script) == {"TestA", "TestB", "TestC"}
