import subprocess
import sys
import textwrap

from dev_health_ops.analytics.complexity import (
    DEFAULT_COMPLEXITY_CONFIG_PATH,
    LANGUAGE_BY_EXTENSION,
    ComplexityScanner,
    FileComplexity,
)


def scanner() -> ComplexityScanner:
    return ComplexityScanner(DEFAULT_COMPLEXITY_CONFIG_PATH)


def complexity_by_path(results: list[FileComplexity]) -> dict[str, FileComplexity]:
    return {result.file_path: result for result in results}


def test_lizard_analyzes_typescript_tsx_and_go_sources() -> None:
    subject = scanner()

    results = complexity_by_path(
        subject.scan_file_contents(
            [
                (
                    "src/app.ts",
                    "export function score(value: number) {\n"
                    "  if (value > 10) { return value; }\n"
                    "  return 0;\n"
                    "}\n",
                ),
                (
                    "src/view.tsx",
                    "export function View(props) {\n"
                    "  if (props.enabled) { return props.title; }\n"
                    "  return null;\n"
                    "}\n",
                ),
                (
                    "src/main.go",
                    "package main\n\n"
                    "func score(value int) int {\n"
                    "  if value > 10 { return value }\n"
                    "  return 0\n"
                    "}\n",
                ),
            ]
        )
    )

    assert results["src/app.ts"].language == "typescript"
    assert results["src/view.tsx"].language == "typescript"
    assert results["src/main.go"].language == "go"
    assert all(result.functions_count > 0 for result in results.values())
    assert all(result.cyclomatic_total > 0 for result in results.values())


def test_python_still_uses_radon_language() -> None:
    subject = scanner()

    results = subject.scan_file_contents(
        [
            (
                "main.py",
                "def score(value: int) -> int:\n"
                "    if value > 10:\n"
                "        return value\n"
                "    return 0\n",
            )
        ]
    )

    assert len(results) == 1
    result = results[0]
    assert result.language == "python"
    assert result.functions_count == 1
    assert result.cyclomatic_total > 0


def test_unsupported_extensions_are_not_analyzed() -> None:
    subject = scanner()

    assert (
        subject.scan_file_contents(
            [("README.md", "# hello\n"), ("notes.txt", "hello\n")]
        )
        == []
    )
    assert subject._analyze_content("# hello\n", "README.md") is None
    assert subject._analyze_content("hello\n", "notes.txt") is None


def test_should_process_uses_multilanguage_globs_and_exclusions() -> None:
    subject = scanner()

    assert not subject.should_process("node_modules/pkg/index.ts")
    assert not subject.should_process("src/generated/types.d.ts")
    assert not subject.should_process("src/app.min.js")
    assert not subject.should_process("dist/main.go")
    assert subject.should_process("src/foo.ts")
    assert subject.should_process("main.py")


def test_include_globs_cover_supported_extensions_exactly() -> None:
    subject = scanner()

    expected_globs = {f"*{extension}" for extension in LANGUAGE_BY_EXTENSION}

    assert set(subject.include_globs) == expected_globs
    assert len(subject.include_globs) == len(expected_globs)


def test_syntactically_broken_source_returns_no_result() -> None:
    subject = scanner()

    assert subject.scan_file_contents([("broken.py", "def broken(:\n")]) == []
    assert subject._analyze_content("def broken(:\n", "broken.py") is None


def test_lizard_scan_does_not_mutate_sys_path() -> None:
    subject = scanner()
    path_before = list(sys.path)

    results = subject.scan_file_contents(
        [("src/app.ts", "export function f(a: number) { return a ? 1 : 0; }\n")]
    )

    assert results, "scan must succeed for the invariant to be meaningful"
    assert sys.path == path_before


def test_lizard_import_does_not_shadow_alembic_under_python_m(tmp_path) -> None:
    """Regression for CHAOS-2863: lizard inserts dirname(sys.argv[0]) at
    sys.path[0] on import. Under ``python -m dev_health_ops.cli`` that is the
    installed package directory, whose ``alembic`` subpackage then shadows the
    real alembic and crashes ``dev-hops migrate``. Reproduce in a fresh
    interpreter with a poisoned argv[0] and assert the shadow never happens."""
    shadow_dir = tmp_path / "fake_pkg"
    (shadow_dir / "alembic").mkdir(parents=True)
    (shadow_dir / "alembic" / "__init__.py").write_text("SHADOW = True\n")

    script = textwrap.dedent(
        """
        import sys
        sys.argv[0] = sys.argv.pop(1) + "/cli.py"
        from dev_health_ops.analytics.complexity import (
            DEFAULT_COMPLEXITY_CONFIG_PATH,
            ComplexityScanner,
        )
        scanner = ComplexityScanner(DEFAULT_COMPLEXITY_CONFIG_PATH)
        results = scanner.scan_file_contents(
            [("src/app.ts", "export function f(a: number) { return a ? 1 : 0; }")]
        )
        assert results, "lizard analysis must still work"
        assert sys.argv[0].rsplit("/", 1)[0] not in sys.path, (
            "argv[0] dirname leaked into sys.path"
        )
        from alembic import command  # must be the real alembic

        assert not hasattr(command, "SHADOW")
        print("OK")
        """
    )

    proc = subprocess.run(
        [sys.executable, "-c", script, str(shadow_dir)],
        capture_output=True,
        text=True,
        timeout=120,
    )

    assert proc.returncode == 0, proc.stderr
    assert "OK" in proc.stdout
