#!/usr/bin/env python3
"""Complete the one-time canonical documentation migration for PR #1256."""

from __future__ import annotations

import json
import re
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
LEGACY = ROOT / ".github" / "docs-legacy"
MAPPING_PATH = (
    ROOT
    / ".github"
    / "documentation-program"
    / "content"
    / "migrated-source-pages.json"
)

MIGRATIONS = {
    "admin/data-sources/incident-response.md": ".github/docs-legacy/user-guide/pagerduty-oauth-app-setup.md",
    "admin/data-sources/jira-atlassian.md": ".github/docs-legacy/providers/jira-service-management.md",
    "contribute/architecture/go-worker-migration-plan.md": ".github/docs-legacy/plans/go-worker-migration-implementation-plan.md",
    "contribute/architecture/go-worker-migration-prd.md": ".github/docs-legacy/product/go-worker-migration-prd.md",
    "contribute/architecture/go-worker-runtime.md": ".github/docs-legacy/architecture/go-worker-runtime-trd.md",
    "contribute/architecture/river-compatibility.md": ".github/docs-legacy/decisions/chaos-3034-river-compatibility.md",
    "integrate/webhooks/pagerduty.md": ".github/docs-legacy/architecture/pagerduty-contract.md",
    "operate/configure/database-connection-pooling.md": ".github/docs-legacy/ops/database-connection-pooling.md",
    "operate/run/workers-and-jobs.md": ".github/docs-legacy/ops/workers.md",
    "reference/cli/index.md": ".github/docs-legacy/ops/cli-reference.md",
}

BINARY_SUFFIXES = {
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
    ".ico",
    ".zip",
    ".gz",
    ".pdf",
    ".woff",
    ".woff2",
    ".ttf",
    ".eot",
}


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _replace(path: Path, replacements: tuple[tuple[str, str], ...]) -> None:
    if not path.is_file():
        return
    text = path.read_text(encoding="utf-8")
    updated = text
    for old, new in replacements:
        updated = updated.replace(old, new)
    if updated != text:
        path.write_text(updated, encoding="utf-8")


def _active_text_files() -> set[Path]:
    roots = [
        ROOT / "docs",
        ROOT / "scripts",
        ROOT / "tests",
        ROOT / "docs-qa",
        ROOT / ".github" / "workflows",
        ROOT / ".github" / "documentation-program" / "content",
        ROOT / ".github" / "documentation-program" / "phase-9",
        ROOT / ".github" / "documentation-program" / "phase-10",
        ROOT / ".github" / "documentation-program" / "phase-11",
    ]
    paths = {
        ROOT / "Makefile",
        ROOT / "mkdocs.yml",
        ROOT / "wrangler.jsonc",
    }
    for root in roots:
        if root.exists():
            paths.update(path for path in root.rglob("*") if path.is_file())
    return paths


def _canonicalize_paths() -> None:
    replacements = (
        (".build/docs-prototype", ".build/docs"),
        ("mkdocs.prototype.yml", "mkdocs.yml"),
        ("docs-prototype", "docs"),
    )
    for path in sorted(_active_text_files()):
        if path.suffix.lower() in BINARY_SUFFIXES:
            continue
        if LEGACY in path.parents:
            continue
        try:
            _replace(path, replacements)
        except UnicodeDecodeError:
            continue


def _sync_current_sources() -> None:
    MAPPING_PATH.write_text(
        json.dumps(MIGRATIONS, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    for target, source in MIGRATIONS.items():
        source_path = ROOT / source
        target_path = DOCS / target
        if not source_path.is_file():
            raise FileNotFoundError(f"Missing current source document: {source}")
        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source_path, target_path)


def _update_makefile() -> None:
    path = ROOT / "Makefile"
    text = path.read_text(encoding="utf-8")
    text = text.replace(".build/site", ".build/docs")
    text = text.replace(
        "docs/external-link-allowlist.yml",
        ".github/docs-legacy/external-link-allowlist.yml",
    )
    text = text.replace("# Documentation v2 lifecycle:", "# Documentation lifecycle:")
    text = text.replace(
        "$(PYTHON) -m mkdocs build --strict --site-dir .build/docs",
        "$(PYTHON) -m mkdocs build --strict --config-file mkdocs.yml",
    )
    path.write_text(text, encoding="utf-8")


def _write_docs_guards_workflow() -> None:
    _write(
        ROOT / ".github" / "workflows" / "docs-guards.yml",
        """name: Docs Guards

on:
  push:
    branches: [main]
    paths:
      - 'docs/**'
      - 'mkdocs.yml'
      - 'requirements-docs.txt'
      - 'scripts/validate_docs_v2_publication.py'
      - 'scripts/check_built_site_links.py'
      - 'scripts/check_docs_candidate_search.py'
      - 'scripts/check_docs_candidate_accessibility.py'
      - 'scripts/check_docs_candidate_facts.py'
      - 'scripts/mkdocs_migrated_source_links.py'
      - '.github/documentation-program/content/migrated-source-pages.json'
      - '.github/documentation-program/ia/**'
      - '.github/documentation-program/phase-9/**'
      - '.github/documentation-program/phase-10/**'
      - '.github/workflows/docs-guards.yml'
  pull_request:
    branches: ['**']
    paths:
      - 'docs/**'
      - 'mkdocs.yml'
      - 'requirements-docs.txt'
      - 'scripts/validate_docs_v2_publication.py'
      - 'scripts/check_built_site_links.py'
      - 'scripts/check_docs_candidate_search.py'
      - 'scripts/check_docs_candidate_accessibility.py'
      - 'scripts/check_docs_candidate_facts.py'
      - 'scripts/mkdocs_migrated_source_links.py'
      - '.github/documentation-program/content/migrated-source-pages.json'
      - '.github/documentation-program/ia/**'
      - '.github/documentation-program/phase-9/**'
      - '.github/documentation-program/phase-10/**'
      - '.github/workflows/docs-guards.yml'
  merge_group:
  workflow_dispatch:

concurrency:
  group: ${{ github.workflow }}-${{ github.event.pull_request.number || github.ref }}
  cancel-in-progress: true

permissions:
  contents: read

jobs:
  changes:
    runs-on: ubuntu-latest
    outputs:
      docs: ${{ steps.filter.outputs.docs }}
    steps:
      - uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0 # v7.0.0
      - uses: dorny/paths-filter@7b450fff21473bca461d4b92ce414b9d0420d706 # v4.0.2
        id: filter
        if: github.event_name != 'workflow_dispatch'
        with:
          filters: |
            docs:
              - 'docs/**'
              - 'mkdocs.yml'
              - 'requirements-docs.txt'
              - 'scripts/validate_docs_v2_publication.py'
              - 'scripts/check_built_site_links.py'
              - 'scripts/check_docs_candidate_search.py'
              - 'scripts/check_docs_candidate_accessibility.py'
              - 'scripts/check_docs_candidate_facts.py'
              - 'scripts/mkdocs_migrated_source_links.py'
              - '.github/documentation-program/content/migrated-source-pages.json'
              - '.github/documentation-program/ia/**'
              - '.github/documentation-program/phase-9/**'
              - '.github/documentation-program/phase-10/**'
              - '.github/workflows/docs-guards.yml'

  docs-guards-job:
    needs: [changes]
    if: >-
      github.event_name == 'workflow_dispatch' ||
      needs.changes.outputs.docs == 'true'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0 # v7.0.0

      - name: Set up Python
        uses: actions/setup-python@ece7cb06caefa5fff74198d8649806c4678c61a1 # v6
        with:
          python-version: '3.12'

      - name: Install documentation dependencies
        run: pip install -r requirements-docs.txt

      - name: Validate publication inventory, IA placement, redirects, and source links
        run: |
          mkdir -p .build
          set -o pipefail
          python scripts/validate_docs_v2_publication.py \
            2>&1 | tee .build/docs-publication-validation.log

      - name: Strict canonical build
        run: |
          set -o pipefail
          python -m mkdocs build --strict --config-file mkdocs.yml \
            2>&1 | tee .build/docs-build.log

      - name: Check rendered internal links, anchors, and assets
        run: |
          set -o pipefail
          python scripts/check_built_site_links.py \
            --site-dir .build/docs \
            2>&1 | tee .build/docs-links.log

      - name: Check task-based search acceptance
        run: |
          set -o pipefail
          python scripts/check_docs_candidate_search.py \
            --site-dir .build/docs \
            --queries .github/documentation-program/phase-10/search-acceptance.json \
            2>&1 | tee .build/docs-search.log

      - name: Check structural accessibility invariants
        run: |
          set -o pipefail
          python scripts/check_docs_candidate_accessibility.py \
            --site-dir .build/docs \
            --css docs/stylesheets/extra.css \
            2>&1 | tee .build/docs-accessibility.log

      - name: Check objective documentation facts
        run: |
          set -o pipefail
          python scripts/check_docs_candidate_facts.py \
            2>&1 | tee .build/docs-facts.log

      - name: Upload concise docs quality evidence
        if: always()
        uses: actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a # v7
        with:
          name: docs-quality-evidence
          path: |
            .build/docs-*.log
            .build/docs-*.tsv
          include-hidden-files: true
          if-no-files-found: warn
          retention-days: 14

  docs-guards:
    name: docs-guards
    if: always()
    needs: [changes, docs-guards-job]
    runs-on: ubuntu-latest
    steps:
      - name: Aggregate docs guards result
        env:
          CHANGES_RESULT: ${{ needs.changes.result }}
          DOCS_GUARDS_RESULT: ${{ needs.docs-guards-job.result }}
        run: |
          echo "changes job result: ${CHANGES_RESULT}"
          echo "docs guards job result: ${DOCS_GUARDS_RESULT}"
          if [ "${CHANGES_RESULT}" != "success" ] && [ "${CHANGES_RESULT}" != "skipped" ]; then
            echo 'docs guards failed: the paths-filter job did not succeed'
            exit 1
          fi
          case "${DOCS_GUARDS_RESULT}" in
            success|skipped)
              echo 'docs guards passed'
              ;;
            failure|cancelled|*)
              echo 'docs guards failed'
              exit 1
              ;;
          esac
""",
    )


def _write_inventory_workflow() -> None:
    _write(
        ROOT / ".github" / "workflows" / "docs-inventory-review.yml",
        """name: Documentation inventory archive review

on:
  push:
    branches:
      - main
    paths:
      - '.github/documentation-program/inventory/**'
      - '.github/documentation-program/ia/**'
      - 'scripts/validate_docs_inventory_review.py'
      - '.github/workflows/docs-inventory-review.yml'
  pull_request:
    branches:
      - main
    paths:
      - '.github/documentation-program/inventory/**'
      - '.github/documentation-program/ia/**'
      - 'scripts/validate_docs_inventory_review.py'
      - '.github/workflows/docs-inventory-review.yml'
  workflow_dispatch:

permissions:
  contents: read

jobs:
  inventory-archive:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0 # v7.0.0

      - uses: actions/setup-python@ece7cb06caefa5fff74198d8649806c4678c61a1 # v6
        with:
          python-version: '3.12'

      - name: Install inventory dependency
        run: python -m pip install 'PyYAML>=6.0.3'

      - name: Validate the frozen Phase 1 inventory and dispositions
        run: |
          mkdir -p .build
          cp .github/documentation-program/inventory/documentation-inventory.json \
            .build/documentation-inventory.json
          python scripts/validate_docs_inventory_review.py \
            --generated-json .build/documentation-inventory.json \
            --inventory-dir .github/documentation-program/inventory \
            --ia-dir .github/documentation-program/ia
""",
    )


def _write_source_link_hook() -> None:
    _write(
        ROOT / "scripts" / "mkdocs_migrated_source_links.py",
        '''"""Resolve links for pages copied from the archived legacy source tree."""

from __future__ import annotations

import json
import posixpath
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

ROOT = Path(__file__).resolve().parents[1]
MAPPING_PATH = (
    ROOT
    / ".github"
    / "documentation-program"
    / "content"
    / "migrated-source-pages.json"
)
REPOSITORY_BLOB_ORIGIN = "https://github.com/full-chaos/dev-health-ops/blob/main"
REPOSITORY_RAW_ORIGIN = "https://raw.githubusercontent.com/full-chaos/dev-health-ops/main"
INLINE_LINK_RE = re.compile(
    r"(?P<prefix>!?\\[[^\\]\\n]*\\]\\()(?P<destination>[^)\\n]+)(?P<suffix>\\))"
)
REFERENCE_LINK_RE = re.compile(
    r"^(?P<prefix>\\s*\\[[^\\]]+\\]:\\s*)(?P<destination>\\S+)(?P<suffix>.*)$"
)
HTML_ATTR_RE = re.compile(
    r'(?P<prefix>\\b(?P<attr>href|src)=(?P<quote>["\\']))'
    r'(?P<destination>[^"\\']+)'
    r'(?P<suffix>(?P=quote))',
    re.IGNORECASE,
)
EMPTY_ANCHOR_RE = re.compile(
    r'<a\\s+id=(?P<quote>["\\'])(?P<id>[^"\\']+)(?P=quote)\\s*></a>',
    re.IGNORECASE,
)


def _load_mapping() -> dict[str, str]:
    loaded = json.loads(MAPPING_PATH.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError(f"Expected an object in {MAPPING_PATH}")
    return {str(key): str(value) for key, value in loaded.items()}


MIGRATED_SOURCE_PAGES = _load_mapping()
SOURCE_TO_CANONICAL = {
    source: target for target, source in MIGRATED_SOURCE_PAGES.items()
}


def _canonical_url(target_path: str) -> str:
    value = target_path.strip("/")
    if value == "index.md":
        return "/"
    if value.endswith("/index.md"):
        return f"/{value[:-len('index.md')]}"
    if value.endswith(".md"):
        value = value[:-3]
    return f"/{value}/"


def _split_destination(value: str) -> tuple[str, str]:
    stripped = value.strip()
    if stripped.startswith("<") and ">" in stripped:
        end = stripped.index(">") + 1
        return stripped[:end], stripped[end:]
    parts = stripped.split(maxsplit=1)
    return parts[0], f" {parts[1]}" if len(parts) == 2 else ""


def _rewrite_url(url: str, source_path: str, *, image: bool) -> str:
    wrapped = url.startswith("<") and url.endswith(">")
    bare = url[1:-1] if wrapped else url
    parsed = urlsplit(bare)
    if (
        parsed.scheme
        or parsed.netloc
        or bare.startswith(("#", "/", "mailto:", "tel:"))
        or not parsed.path
    ):
        return url

    resolved = posixpath.normpath(
        posixpath.join(posixpath.dirname(source_path), parsed.path)
    )
    if resolved.startswith("../"):
        return url

    canonical_target = SOURCE_TO_CANONICAL.get(resolved)
    if canonical_target:
        rewritten = urlunsplit(
            ("", "", _canonical_url(canonical_target), parsed.query, parsed.fragment)
        )
    else:
        origin = REPOSITORY_RAW_ORIGIN if image else REPOSITORY_BLOB_ORIGIN
        origin_parts = urlsplit(origin)
        rewritten = urlunsplit(
            (
                origin_parts.scheme,
                origin_parts.netloc,
                f"{origin_parts.path}/{resolved}",
                parsed.query,
                parsed.fragment,
            )
        )
    return f"<{rewritten}>" if wrapped else rewritten


def _rewrite_markdown(markdown: str, source_path: str) -> str:
    output: list[str] = []
    fence: str | None = None

    for line in markdown.splitlines(keepends=True):
        stripped = line.lstrip()
        if stripped.startswith(("```", "~~~")):
            marker = stripped[:3]
            fence = None if fence == marker else marker if fence is None else fence
            output.append(line)
            continue
        if fence is not None:
            output.append(line)
            continue

        line = EMPTY_ANCHOR_RE.sub(
            lambda match: f'<span id="{match.group("id")}"></span>', line
        )

        reference = REFERENCE_LINK_RE.match(line)
        if reference:
            destination, title = _split_destination(reference.group("destination"))
            rewritten = _rewrite_url(destination, source_path, image=False)
            output.append(
                f"{reference.group('prefix')}{rewritten}{title}"
                f"{reference.group('suffix')}"
            )
            continue

        def replace_inline(match: re.Match[str]) -> str:
            destination, title = _split_destination(match.group("destination"))
            rewritten = _rewrite_url(
                destination,
                source_path,
                image=match.group("prefix").startswith("!["),
            )
            return f"{match.group('prefix')}{rewritten}{title}{match.group('suffix')}"

        line = INLINE_LINK_RE.sub(replace_inline, line)

        def replace_html(match: re.Match[str]) -> str:
            rewritten = _rewrite_url(
                match.group("destination"),
                source_path,
                image=match.group("attr").lower() == "src",
            )
            return f"{match.group('prefix')}{rewritten}{match.group('suffix')}"

        output.append(HTML_ATTR_RE.sub(replace_html, line))

    return "".join(output)


def on_page_markdown(
    markdown: str,
    page: Any,
    config: Any,
    files: Any,
) -> str:
    """Rewrite source-relative links for explicitly migrated pages."""

    del config, files
    source_path = MIGRATED_SOURCE_PAGES.get(page.file.src_path)
    if not source_path:
        return markdown
    return _rewrite_markdown(markdown, source_path)
''',
    )


def _write_link_checker() -> None:
    _write(
        ROOT / "scripts" / "check_docs_links.py",
        '''#!/usr/bin/env python3
"""Check relative Markdown links and anchors under the canonical docs tree."""

from __future__ import annotations

import json
import re
from pathlib import Path
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[1]
DOCS_ROOT = ROOT / "docs"
MIGRATED_SOURCE_MAP = (
    ROOT
    / ".github"
    / "documentation-program"
    / "content"
    / "migrated-source-pages.json"
)

INLINE_LINK_RE = re.compile(r"(?<!!)\\[[^\\]\\n]+\\]\\(([^)\\s]+)(?:\\s+\"[^\"]*\")?\\)")
REFERENCE_DEF_RE = re.compile(r"^\\[[^\\]]+\\]:\\s+(\\S+)", re.MULTILINE)
HTML_ID_RE = re.compile(r"\\bid=[\"']([^\"']+)[\"']")
HEADING_RE = re.compile(r"^(#{1,6})\\s+(.+?)\\s+\\#*\\s*$")


def slugify(heading: str) -> str:
    heading = re.sub(r"<[^>]+>", "", heading)
    heading = re.sub(r"`([^`]*)`", r"\\1", heading)
    heading = heading.strip().lower()
    heading = re.sub(r"[^\\w\\s-]", "", heading)
    heading = re.sub(r"[\\s_-]+", "-", heading).strip("-")
    return heading


def anchors_for(path: Path) -> set[str]:
    anchors = {""}
    text = path.read_text(encoding="utf-8")
    counts: dict[str, int] = {}
    for line in text.splitlines():
        match = HEADING_RE.match(line)
        if not match:
            continue
        base = slugify(match.group(2))
        if not base:
            continue
        count = counts.get(base, 0)
        counts[base] = count + 1
        anchors.add(base if count == 0 else f"{base}-{count}")
    anchors.update(HTML_ID_RE.findall(text))
    return anchors


def iter_links(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8")
    return [*INLINE_LINK_RE.findall(text), *REFERENCE_DEF_RE.findall(text)]


def should_skip(target: str) -> bool:
    if not target or target.startswith(("http://", "https://", "mailto:", "tel:")):
        return True
    if target.startswith("#"):
        return False
    parsed = urlsplit(target)
    return bool(parsed.scheme or parsed.netloc) or target.startswith("/")


def check_link(
    source: Path,
    raw_target: str,
    anchor_cache: dict[Path, set[str]],
    docs_root: Path,
    root: Path,
) -> str | None:
    if should_skip(raw_target):
        return None
    parsed = urlsplit(raw_target)
    target_path = unquote(parsed.path)
    anchor = unquote(parsed.fragment)

    if target_path and not target_path.endswith(".md"):
        return None

    destination = source if not target_path else (source.parent / target_path).resolve()
    try:
        destination.relative_to(docs_root)
    except ValueError:
        return None

    if not destination.exists():
        return f"{source.relative_to(root)} -> {raw_target}: missing file"
    if destination.suffix != ".md":
        return None

    if anchor:
        anchors = anchor_cache.setdefault(destination, anchors_for(destination))
        if anchor not in anchors:
            return f"{source.relative_to(root)} -> {raw_target}: missing anchor"
    return None


def check_docs(docs_root: Path, root: Path) -> list[str]:
    errors: list[str] = []
    anchor_cache: dict[Path, set[str]] = {}
    migrated = set(json.loads(MIGRATED_SOURCE_MAP.read_text(encoding="utf-8")))
    for path in sorted(docs_root.rglob("*.md")):
        relpath = path.relative_to(docs_root).as_posix()
        if relpath in migrated:
            continue
        for target in iter_links(path):
            error = check_link(path, target, anchor_cache, docs_root, root)
            if error:
                errors.append(error)
    return errors


def main() -> int:
    errors = check_docs(DOCS_ROOT, ROOT)
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    print("Docs link check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
''',
    )


def _update_supporting_scripts() -> None:
    _replace(
        ROOT / "scripts" / "build_docs_cloudflare.py",
        (
            ("Build the MkDocs site", "Build the canonical MkDocs site"),
            ("candidate", "canonical documentation"),
        ),
    )

    validator = ROOT / "scripts" / "validate_docs_v2_publication.py"
    _replace(
        validator,
        (
            (
                "Validate the v2 documentation candidate and emit the Phase 9 publication inventory.",
                "Validate the canonical documentation tree and emit its publication inventory.",
            ),
            ("public-candidate", "public-canonical"),
            ("# Documentation v2 publication summary", "# Canonical documentation publication summary"),
            ("Public candidate pages", "Canonical public pages"),
            ("candidate pages", "canonical pages"),
            (
                "The current production documentation remains the WIP baseline. This inventory is the Phase 9 publication candidate for later quality and cutover gates.",
                "This inventory describes the canonical documentation tree published from `docs/`.",
            ),
        ),
    )

    publication = ROOT / "scripts" / "docs_publication.py"
    text = publication.read_text(encoding="utf-8")
    text = text.replace(
        "Classify every ops/docs/**/*.md file as public-nav, public-reference, or\nexcluded-internal per ``docs/publication.yml``.",
        "Classify every canonical docs/**/*.md file while using the archived\nlegacy publication manifest for compatibility checks.",
    )
    text = text.replace(
        'root / "docs", root / "mkdocs.yml", root / "docs" / "publication.yml"',
        'root / "docs",\n            root / "mkdocs.yml",\n            root / ".github" / "docs-legacy" / "publication.yml"',
    )
    publication.write_text(text, encoding="utf-8")

    inventory = ROOT / "scripts" / "docs_inventory_v2.py"
    text = inventory.read_text(encoding="utf-8")
    text = text.replace('"docs/publication.yml"', '".github/docs-legacy/publication.yml"')
    text = text.replace(
        '"docs/freshness-inventory.yml"',
        '".github/docs-legacy/freshness-inventory.yml"',
    )
    text = text.replace(
        '"docs/search-acceptance.json"',
        '".github/docs-legacy/search-acceptance.json"',
    )
    text = text.replace(
        'manifest = _load_yaml(repo_root / "docs" / "publication.yml")',
        'manifest = _load_yaml(\n        repo_root / ".github" / "docs-legacy" / "publication.yml"\n    )',
    )
    text = text.replace(
        'overrides = repo_root / "docs" / "overrides"',
        'overrides = repo_root / ".github" / "docs-legacy" / "overrides"',
    )
    text = text.replace(
        'elif rel.startswith("docs/overrides/"):',
        'elif rel.startswith(".github/docs-legacy/overrides/"):',
    )
    inventory.write_text(text, encoding="utf-8")

    freshness = ROOT / "scripts" / "check_freshness_inventory.py"
    text = freshness.read_text(encoding="utf-8")
    text = text.replace(
        '"""Validate docs/freshness-inventory.yml disposition rows.',
        '"""Validate the archived legacy freshness inventory.',
    )
    text = text.replace(
        'DEFAULT_INVENTORY = ROOT / "docs" / "freshness-inventory.yml"',
        'DEFAULT_INVENTORY = (\n    ROOT / ".github" / "docs-legacy" / "freshness-inventory.yml"\n)',
    )
    text = text.replace(
        'DEFAULT_DOCS_ROOT = ROOT / "docs"',
        'DEFAULT_DOCS_ROOT = ROOT / ".github" / "docs-legacy"',
    )
    text = text.replace(
        'description="Validate docs/freshness-inventory.yml"',
        'description="Validate the archived legacy freshness inventory"',
    )
    freshness.write_text(text, encoding="utf-8")

    drift = ROOT / "scripts" / "check_investment_docs_drift.py"
    text = drift.read_text(encoding="utf-8")
    text = text.replace(
        'TAXONOMY_DOC = ROOT / "docs" / "product" / "investment-taxonomy.md"',
        'LEGACY_DOCS = ROOT / ".github" / "docs-legacy"\nTAXONOMY_DOC = LEGACY_DOCS / "product" / "investment-taxonomy.md"',
    )
    text = text.replace(
        'LLM_CONTRACT_DOC = ROOT / "docs" / "llm" / "categorization-contract.md"',
        'LLM_CONTRACT_DOC = LEGACY_DOCS / "llm" / "categorization-contract.md"',
    )
    text = text.replace(
        'INVESTMENT_MIX_DOC = ROOT / "docs" / "user-guide" / "views" / "investment-mix.md"',
        'INVESTMENT_MIX_DOC = (\n    LEGACY_DOCS / "user-guide" / "views" / "investment-mix.md"\n)',
    )
    text = text.replace(
        'ROOT / "docs", ROOT / "mkdocs.yml", ROOT / "docs" / "publication.yml"',
        'ROOT / "docs",\n        ROOT / "mkdocs.yml",\n        LEGACY_DOCS / "publication.yml"',
    )
    drift.write_text(text, encoding="utf-8")

    taxonomy = ROOT / "scripts" / "gen_taxonomy_docs.py"
    text = taxonomy.read_text(encoding="utf-8")
    text = text.replace(
        'DOC_PATH = ROOT / "docs" / "product" / "investment-taxonomy.md"',
        'DOC_PATH = (\n    ROOT\n    / ".github"\n    / "docs-legacy"\n    / "product"\n    / "investment-taxonomy.md"\n)',
    )
    taxonomy.write_text(text, encoding="utf-8")


def _update_review_inventory_scanner() -> None:
    path = ROOT / "scripts" / "docs_inventory_review.py"
    text = path.read_text(encoding="utf-8")
    text = text.replace(
        "the v2 prototype, internal program evidence, repository entry",
        "the archived legacy tree, internal program evidence, repository entry",
    )
    text = text.replace("PROTOTYPE_ASSET_SUFFIXES", "LEGACY_ASSET_SUFFIXES")

    start = text.find('    prototype_root = repo_root / "docs"')
    if start == -1:
        start = text.find('    prototype_root = repo_root / "docs-prototype"')
    if start != -1:
        end = text.index("\n    program_root =", start)
        block = '''    legacy_root = repo_root / ".github" / "docs-legacy"
    if legacy_root.exists():
        for path in sorted(legacy_root.rglob("*")):
            if not path.is_file():
                continue
            suffix = path.suffix.lower()
            if suffix == ".md":
                artifact_type = "legacy-page"
                content_type = base._infer_content_type(
                    path.relative_to(legacy_root).as_posix(),
                    {},
                )
            elif suffix in LEGACY_ASSET_SUFFIXES:
                artifact_type = "legacy-asset"
                content_type = None
            else:
                artifact_type = "legacy-support"
                content_type = None
            _add_file(
                rows_by_path,
                repository,
                repo_root,
                path,
                artifact_type,
                "archived-source",
                product_area="documentation-legacy",
                content_type=content_type,
                notes="Preserved pre-cutover documentation source; not in the public build.",
            )
'''
        text = text[:start] + block + text[end:]

    config_start = text.find('    prototype_config = repo_root / "mkdocs.yml"')
    if config_start == -1:
        config_start = text.find('    prototype_config = repo_root / "mkdocs.prototype.yml"')
    if config_start != -1:
        config_end = text.index("\n    for pattern in", config_start)
        block = '''    legacy_config = repo_root / ".github" / "docs-legacy" / "mkdocs.yml"
    if legacy_config.is_file():
        _add_file(
            rows_by_path,
            repository,
            repo_root,
            legacy_config,
            "configuration",
            "archived-source",
            product_area="documentation-legacy",
            notes="Archived pre-cutover build configuration.",
        )
'''
        text = text[:config_start] + block + text[config_end:]

    path.write_text(text, encoding="utf-8")


def _write_publication_tests() -> None:
    _write(
        ROOT / "tests" / "docs" / "test_publication_manifest.py",
        '''from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from scripts.docs_publication import (
    PublicationClassificationError,
    classify_all,
    load_nav_paths,
)

ROOT = Path(__file__).resolve().parents[2]
DOCS_DIR = ROOT / "docs"
LEGACY_DOCS_DIR = ROOT / ".github" / "docs-legacy"
MKDOCS_PATH = ROOT / "mkdocs.yml"
MANIFEST_PATH = LEGACY_DOCS_DIR / "publication.yml"
FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "unclassified_page"


def test_every_canonical_markdown_file_is_published_in_navigation() -> None:
    assert DOCS_DIR.is_dir()
    assert LEGACY_DOCS_DIR.is_dir()
    assert MANIFEST_PATH.is_file()

    classification = classify_all(DOCS_DIR, MKDOCS_PATH, MANIFEST_PATH)
    all_files = {
        path.relative_to(DOCS_DIR).as_posix() for path in DOCS_DIR.rglob("*.md")
    }

    assert set(classification) == all_files
    assert set(classification.values()) == {"public-nav"}


def test_legacy_internal_material_is_archived_outside_the_public_tree() -> None:
    for legacy_path in (
        "plans/dead-code-cleanup-plan.md",
        "plans/atlassian-client-integration.md",
        "roadmap.md",
        "project.md",
    ):
        assert (LEGACY_DOCS_DIR / legacy_path).is_file()
        assert not (DOCS_DIR / legacy_path).exists()


def test_home_page_is_public_navigation() -> None:
    classification = classify_all(DOCS_DIR, MKDOCS_PATH, MANIFEST_PATH)
    assert classification["index.md"] == "public-nav"


def test_navigation_never_points_into_the_legacy_tree() -> None:
    nav_paths = load_nav_paths(MKDOCS_PATH)
    assert nav_paths
    assert all("docs-legacy" not in path for path in nav_paths)
    assert all((DOCS_DIR / path).is_file() for path in nav_paths)


def test_rejects_unclassified_page() -> None:
    with pytest.raises(PublicationClassificationError) as excinfo:
        classify_all(
            FIXTURE_ROOT / "docs",
            FIXTURE_ROOT / "mkdocs.yml",
            FIXTURE_ROOT / "publication.yml",
        )

    assert excinfo.value.path == "mystery.md"


def test_strict_build_omits_archived_legacy_pages(tmp_path: Path) -> None:
    site_dir = tmp_path / "site"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "mkdocs",
            "build",
            "--strict",
            "--config-file",
            "mkdocs.yml",
            "--site-dir",
            str(site_dir),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr

    assert not (site_dir / "roadmap" / "index.html").exists()
    assert not (site_dir / "project" / "index.html").exists()
    assert not (site_dir / "plans" / "dead-code-cleanup-plan" / "index.html").exists()
''',
    )


def _update_docs_content() -> None:
    repo_map = ROOT / "docs" / "contribute" / "start" / "repository-map.md"
    text = repo_map.read_text(encoding="utf-8")
    text = text.replace(
        "The public candidate lives under `docs/`. The non-prototype `docs/` tree remains an important source of current product, architecture, provider, deployment, and runbook facts during migration. Useful verified material should be reshaped into one canonical prototype page, not copied into multiple public destinations.",
        "Canonical public documentation lives under `docs/`. The former documentation corpus is preserved under `.github/docs-legacy/` as source evidence and history; it is not part of the public build. Move verified current material into one canonical page rather than creating parallel public truth.",
    )
    repo_map.write_text(text, encoding="utf-8")

    commands = ROOT / "docs" / "contribute" / "development" / "commands.md"
    text = commands.read_text(encoding="utf-8")
    text = text.replace(
        "## Build and preview the v2 documentation",
        "## Build and preview the documentation",
    )
    text = text.replace(
        "The legacy `make docs:build` target still builds the non-prototype documentation tree. Use the v2 targets for the candidate until cutover retires the legacy tree.",
        "`make docs:build` uses the canonical `mkdocs.yml` configuration. The Cloudflare targets add redirect, header, preview-version, deployment, and rollback behavior around that same build.",
    )
    commands.write_text(text, encoding="utf-8")

    for path in (
        ROOT / "docs-qa" / "tests" / "run-search.mjs",
        ROOT / "docs-qa" / "tests" / "audience-search.search.spec.ts",
    ):
        if path.is_file():
            text = path.read_text(encoding="utf-8")
            text = text.replace(
                "../docs/search-acceptance.json",
                "../.github/documentation-program/phase-10/search-acceptance.json",
            )
            text = text.replace(
                "docs/search-acceptance.json",
                ".github/documentation-program/phase-10/search-acceptance.json",
            )
            path.write_text(text, encoding="utf-8")


def _update_health_workflow() -> None:
    path = ROOT / ".github" / "workflows" / "docs-health-report.yml"
    if not path.is_file():
        return
    text = path.read_text(encoding="utf-8")
    text = text.replace("Build the canonical candidate", "Build the canonical documentation")
    text = text.replace(
        "--allowlist docs/external-link-allowlist.yml",
        "--allowlist .github/docs-legacy/external-link-allowlist.yml",
    )
    path.write_text(text, encoding="utf-8")


def main() -> int:
    if not DOCS.is_dir() or not LEGACY.is_dir():
        raise RuntimeError("Canonical or legacy documentation tree is missing")

    shutil.rmtree(ROOT / "docs-prototype", ignore_errors=True)
    (ROOT / "mkdocs.prototype.yml").unlink(missing_ok=True)

    _canonicalize_paths()
    _sync_current_sources()
    _update_makefile()
    _write_docs_guards_workflow()
    _write_inventory_workflow()
    _write_source_link_hook()
    _write_link_checker()
    _update_supporting_scripts()
    _update_review_inventory_scanner()
    _write_publication_tests()
    _update_docs_content()
    _update_health_workflow()

    for temporary in (
        ROOT / ".github" / "documentation-program" / "content" / "pr-1256-migration-trigger.txt",
        ROOT / ".github" / "workflows" / "finish-canonical-docs.yml",
        ROOT / ".github" / "workflows" / "canonical-docs-sync.yml",
        ROOT / ".github" / "workflows" / "promote-docs-canonical.yml",
        ROOT / ".github" / "workflows" / "run-docs-promotion.yml",
        ROOT / ".github" / "workflows" / "ruff-format-probe.yml",
    ):
        temporary.unlink(missing_ok=True)

    print("Canonical documentation finalization edits applied.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
