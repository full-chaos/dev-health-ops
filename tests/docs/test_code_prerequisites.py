from pathlib import Path

from scripts.check_code_prerequisites import load_scope, pages_missing_prerequisite_link


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_load_scope_returns_empty_when_the_file_is_absent(tmp_path: Path) -> None:
    assert load_scope(tmp_path / "code-prerequisite-scope.yml") == frozenset()


def test_pages_missing_prerequisite_link_accepts_a_page_with_the_link(
    tmp_path: Path,
) -> None:
    docs_root = tmp_path / "docs"
    _write(
        docs_root / "guide.md",
        "See [Getting Started](../getting-started.md) first.\n\n"
        "```bash\nCLICKHOUSE_URI=clickhouse://ch:ch@localhost:8123/default dev-hops metrics daily\n```\n",
    )
    scope = frozenset({"guide.md"})

    assert pages_missing_prerequisite_link(docs_root, scope) == []


def test_pages_missing_prerequisite_link_rejects_a_credentialed_sample_without_the_link(
    tmp_path: Path,
) -> None:
    docs_root = tmp_path / "docs"
    _write(
        docs_root / "guide.md",
        "```bash\nCLICKHOUSE_URI=clickhouse://ch:ch@localhost:8123/default dev-hops metrics daily\n```\n",
    )
    scope = frozenset({"guide.md"})

    assert pages_missing_prerequisite_link(docs_root, scope) == ["guide.md"]


def test_pages_missing_prerequisite_link_ignores_pages_outside_scope(
    tmp_path: Path,
) -> None:
    docs_root = tmp_path / "docs"
    _write(
        docs_root / "unscoped.md",
        "```bash\nCLICKHOUSE_URI=clickhouse://ch:ch@localhost:8123/default dev-hops metrics daily\n```\n",
    )
    scope: frozenset[str] = frozenset()

    assert pages_missing_prerequisite_link(docs_root, scope) == []


def test_pages_missing_prerequisite_link_ignores_samples_without_credentials(
    tmp_path: Path,
) -> None:
    docs_root = tmp_path / "docs"
    _write(docs_root / "guide.md", "```bash\ndev-hops --help\n```\n")
    scope = frozenset({"guide.md"})

    assert pages_missing_prerequisite_link(docs_root, scope) == []
