import json
import re
from pathlib import Path
from typing import Final

import pytest

ROOT = Path(__file__).resolve().parents[2]
DESIGN_PATH = ROOT / "DESIGN.md"
OVERRIDES_PATH = ROOT / "docs" / "overrides" / "extra.css"
SHOWCASE_PATH = ROOT / "docs" / "reference" / "primitive-showcase.md"
DOCS_QA_PACKAGE_PATH = ROOT / "docs-qa" / "package.json"
DOCS_QA_CONFIG_PATH = ROOT / "docs-qa" / "playwright.config.ts"
ACCESS_HEADERS_PATH = ROOT / "docs-qa" / "tests" / "support" / "accessHeaders.ts"
SOURCE_PARTIAL_PATH = ROOT / "docs" / "overrides" / "partials" / "source.html"
RAW_CSS_VALUE_PATTERN: Final = re.compile(r"#[0-9a-fA-F]{3,8}\b")

REQUIRED_TOKENS = (
    "--fc-paper",
    "--fc-ink",
    "--fc-orange",
    "--fc-evidence",
    "--fc-rule",
    "--fc-space-4",
    "--fc-reading-measure",
    "--fc-showcase-measure",
    "--fc-evidence-rail-measure",
    "--fc-evidence-rail-extent",
    "--fc-focus-ring",
)


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


def test_documentation_design_contract_declares_the_locked_direction_and_tokens() -> (
    None
):
    assert DESIGN_PATH.is_file(), (
        f"missing documentation design contract: {DESIGN_PATH}"
    )

    design = DESIGN_PATH.read_text(encoding="utf-8")

    for required_text in (
        "## 0. Research Log",
        "evidence-led editorial operations manual",
        "Stripe documentation",
        "charcoal/ivory/orange",
        "evidence-trail rail",
        "prefers-reduced-motion",
        "Accepted Debt",
    ):
        assert required_text in design

    for token in REQUIRED_TOKENS:
        assert token in design


def test_material_override_and_showcase_use_the_declared_docs_tokens() -> None:
    assert OVERRIDES_PATH.is_file(), f"missing Material override: {OVERRIDES_PATH}"
    assert SHOWCASE_PATH.is_file(), f"missing primitive showcase: {SHOWCASE_PATH}"

    overrides = OVERRIDES_PATH.read_text(encoding="utf-8")
    showcase = SHOWCASE_PATH.read_text(encoding="utf-8")

    for token in REQUIRED_TOKENS:
        assert token in overrides

    _assert_no_undeclared_raw_css_value(OVERRIDES_PATH)

    for required_heading in (
        "# Primitive showcase",
        "## Navigation and search",
        "## Evidence trail",
        "## Callouts and states",
        "## Code, tables, and diagrams",
    ):
        assert required_heading in showcase


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


def test_docs_qa_harness_uses_pinned_chrome_and_redacted_access_headers() -> None:
    assert DOCS_QA_PACKAGE_PATH.is_file(), (
        f"missing docs QA manifest: {DOCS_QA_PACKAGE_PATH}"
    )
    assert DOCS_QA_CONFIG_PATH.is_file(), (
        f"missing docs QA config: {DOCS_QA_CONFIG_PATH}"
    )

    package = json.loads(DOCS_QA_PACKAGE_PATH.read_text(encoding="utf-8"))
    config = DOCS_QA_CONFIG_PATH.read_text(encoding="utf-8")
    access_headers = ACCESS_HEADERS_PATH.read_text(encoding="utf-8")

    assert package["devDependencies"]["@playwright/test"] == "1.61.1"
    assert "test:visual" in package["scripts"]
    assert "test:a11y" in package["scripts"]
    assert 'channel: "chrome"' in config
    assert "CF_ACCESS_CLIENT_ID" in access_headers
    assert "CF_ACCESS_CLIENT_SECRET" in access_headers
    assert 'const docsQaPort = process.env["DOCS_QA_PORT"] ?? "8008";' in config
    assert "python3 -m http.server ${docsQaPort} --directory ../.build/site" in config


def test_repository_source_link_does_not_enable_material_github_api_requests() -> None:
    assert SOURCE_PARTIAL_PATH.is_file(), (
        "missing repository source override that prevents Material GitHub API requests"
    )
    source_partial = SOURCE_PARTIAL_PATH.read_text(encoding="utf-8")

    assert 'href="{{ config.repo_url }}"' in source_partial
    assert 'class="md-source"' in source_partial
    assert 'data-md-component="source"' not in source_partial
