import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DESIGN_PATH = ROOT / "DESIGN.md"
OVERRIDES_PATH = ROOT / "docs" / "overrides" / "extra.css"
SHOWCASE_PATH = ROOT / "docs" / "reference" / "primitive-showcase.md"
DOCS_QA_PACKAGE_PATH = ROOT / "docs-qa" / "package.json"
DOCS_QA_CONFIG_PATH = ROOT / "docs-qa" / "playwright.config.ts"
ACCESS_HEADERS_PATH = ROOT / "docs-qa" / "tests" / "support" / "accessHeaders.ts"

REQUIRED_TOKENS = (
    "--fc-paper",
    "--fc-ink",
    "--fc-orange",
    "--fc-evidence",
    "--fc-rule",
    "--fc-space-4",
    "--fc-reading-measure",
    "--fc-focus-ring",
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

    for required_heading in (
        "# Primitive showcase",
        "## Navigation and search",
        "## Evidence trail",
        "## Callouts and states",
        "## Code, tables, and diagrams",
    ):
        assert required_heading in showcase


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
    assert "python3 -m http.server 8008 --directory ../.build/site" in config
