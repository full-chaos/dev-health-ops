import re
from pathlib import Path
from typing import Final

import pytest

ROOT = Path(__file__).resolve().parents[2]
PALETTE_PATH = ROOT / "docs-prototype" / "stylesheets" / "logo-palette.css"
EXTRA_CSS_PATH = ROOT / "docs-prototype" / "stylesheets" / "extra.css"
HOME_CSS_PATH = ROOT / "docs-prototype" / "stylesheets" / "home.css"
DESIGN_DIRECTIONS_PATH = ROOT / "docs-prototype" / "design-directions.md"
LEGACY_SHOWCASE_PATH = ROOT / "docs" / "reference" / "primitive-showcase.md"
DOCS_QA_PACKAGE_PATH = ROOT / "docs-qa" / "package.json"
DOCS_QA_CONFIG_PATH = ROOT / "docs-qa" / "playwright.config.ts"
RAW_CSS_VALUE_PATTERN: Final = re.compile(r"#[0-9a-fA-F]{3,8}\b")
TOKEN_PATTERN: Final = re.compile(r"(--[A-Za-z0-9_-]+)\s*:\s*([^;]+);")

REQUIRED_PALETTE = {
    "--fc-void": "#08080A",
    "--fc-ink": "#15171A",
    "--fc-graphite": "#23252A",
    "--fc-silver": "#D1D1D4",
    "--fc-flame": "#FE7501",
    "--fc-crimson": "#A30A06",
    "--fc-aqua": "#04B7C4",
    "--fc-ocean": "#037493",
    "--fc-glacier": "#6CE0E1",
    "--fc-gold": "#FFDB4B",
}


def _tokens(path: Path) -> dict[str, str]:
    return {
        name: value.strip()
        for name, value in TOKEN_PATTERN.findall(path.read_text(encoding="utf-8"))
    }


def _assert_no_undeclared_raw_css_value(source_path: Path) -> None:
    in_root_tokens = False
    for line in source_path.read_text(encoding="utf-8").splitlines():
        if line == ":root {":
            in_root_tokens = True
        elif in_root_tokens and line == "}":
            in_root_tokens = False

        raw_value = RAW_CSS_VALUE_PATTERN.search(line)
        if raw_value is not None and not (
            in_root_tokens and line.lstrip().startswith("--fc-")
        ):
            raise AssertionError(
                f"{source_path}: undeclared raw CSS value {raw_value.group(0)}"
            )


def test_candidate_uses_the_supplied_full_chaos_palette() -> None:
    assert PALETTE_PATH.is_file(), f"missing candidate palette: {PALETTE_PATH}"
    tokens = _tokens(PALETTE_PATH)

    for token, value in REQUIRED_PALETTE.items():
        assert tokens[token] == value


def test_candidate_css_imports_palette_and_protects_accessibility_states() -> None:
    assert EXTRA_CSS_PATH.is_file(), f"missing candidate CSS: {EXTRA_CSS_PATH}"
    assert HOME_CSS_PATH.is_file(), f"missing candidate home CSS: {HOME_CSS_PATH}"

    extra_css = EXTRA_CSS_PATH.read_text(encoding="utf-8")
    home_css = HOME_CSS_PATH.read_text(encoding="utf-8")

    assert '@import url("logo-palette.css");' in extra_css
    assert ":focus-visible" in extra_css
    assert "prefers-reduced-motion" in extra_css
    assert "fc-home-hero" in home_css
    assert "fc-home-card" in home_css


def test_review_only_design_pages_are_not_in_the_public_candidate() -> None:
    assert not DESIGN_DIRECTIONS_PATH.exists()
    assert LEGACY_SHOWCASE_PATH.is_file()
    assert not DOCS_QA_PACKAGE_PATH.exists()
    assert not DOCS_QA_CONFIG_PATH.exists()


@pytest.fixture
def undeclared_raw_css_value_fixture(tmp_path: Path) -> tuple[Path, str]:
    raw_value = "#badc0d"
    source_path = tmp_path / "undeclared.css"
    source_path.write_text(f".fixture {{ color: {raw_value}; }}", encoding="utf-8")
    return source_path, raw_value


def test_rejects_undeclared_token(
    undeclared_raw_css_value_fixture: tuple[Path, str],
) -> None:
    source_path, raw_value = undeclared_raw_css_value_fixture

    with pytest.raises(AssertionError) as error:
        _assert_no_undeclared_raw_css_value(source_path)

    assert str(error.value) == f"{source_path}: undeclared raw CSS value {raw_value}"
