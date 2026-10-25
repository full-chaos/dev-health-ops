#!/usr/bin/env python3
"""Build the canonical MkDocs site and prepare the static tree used by Wrangler."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SITE_DIR = ROOT / ".build" / "docs"
VALIDATED_STAMP = ROOT / ".build" / "docs-validated.sha"
ASSET_DIR = ROOT / ".build" / "docs-cloudflare"
REDIRECTS = ROOT / "docs-data" / "redirects.tsv"
SEARCH_ACCEPTANCE = ROOT / "docs-data" / "search-acceptance.json"
CIGATE_DOCS = ["go", "run", "-trimpath", "-mod=readonly", "./cmd/cigate", "docs"]


def _run(args: list[str]) -> None:
    """Run one repository command and stop immediately on failure."""
    print(f"+ {' '.join(args)}", flush=True)
    subprocess.run(args, cwd=ROOT, check=True)


def _source_revision() -> str:
    github_sha = os.environ.get("GITHUB_SHA", "").strip()
    if github_sha:
        return github_sha
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            text=True,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def _validate_asset_tree() -> None:
    required = [
        ASSET_DIR / "index.html",
        ASSET_DIR / "404.html",
        ASSET_DIR / "_headers",
        ASSET_DIR / "_redirects",
        ASSET_DIR / "robots.txt",
        ASSET_DIR / "cloudflare-build-manifest.json",
    ]
    missing = [path.relative_to(ROOT) for path in required if not path.is_file()]
    if missing:
        joined = ", ".join(str(path) for path in missing)
        raise RuntimeError(f"prepared Cloudflare asset tree is incomplete: {joined}")


# Checks of the built site, in run order. validate-publication runs before the build, the rest after it. Docs Guards runs
# the same verbs; a parity test (internal/cigates/docs/parity_test.go) fails when either side drifts.
SITE_CHECK_VERBS = (
    "validate-publication",
    "check-built-site-links",
    "check-docs-search",
    "check-docs-accessibility",
    "check-code-prerequisites",
    "check-freshness",
)
PREVIEW_ONLY_VERBS = ("check-candidate-facts",)
VERB_ARGS = {
    "check-built-site-links": ["--site-dir", str(SITE_DIR.relative_to(ROOT))],
    "check-docs-search": [
        "--site-dir",
        str(SITE_DIR.relative_to(ROOT)),
        "--queries",
        str(SEARCH_ACCEPTANCE.relative_to(ROOT)),
    ],
    "check-docs-accessibility": [
        "--site-dir",
        str(SITE_DIR.relative_to(ROOT)),
        "--css",
        "docs/stylesheets/extra.css",
    ],
}


def _check(verb: str) -> None:
    _run([*CIGATE_DOCS, verb, *VERB_ARGS.get(verb, [])])


def _validated_site_is_current() -> bool:
    """True when .build/docs was built and fully checked for this exact revision."""
    if not (SITE_DIR / "index.html").is_file() or not VALIDATED_STAMP.is_file():
        print("No validated site found; building it here.", flush=True)
        return False
    stamped = VALIDATED_STAMP.read_text(encoding="utf-8").strip()
    revision = _source_revision()
    if stamped != revision or revision == "unknown":
        print(
            f"Validated site is for {stamped!r}, not {revision!r}; building it here.",
            flush=True,
        )
        return False
    return True


def build(*, mode: str, full_check: bool, reuse_site: bool = False) -> None:
    """Build MkDocs, run the requested checks, and prepare Wrangler assets."""
    (ROOT / ".build").mkdir(parents=True, exist_ok=True)

    if not (reuse_site and _validated_site_is_current()):
        if full_check:
            _check(SITE_CHECK_VERBS[0])

        _run(
            [
                sys.executable,
                "-m",
                "mkdocs",
                "build",
                "--strict",
                "--config-file",
                "mkdocs.yml",
            ]
        )

        if full_check:
            for verb in SITE_CHECK_VERBS[1:]:
                _check(verb)

    if full_check:
        for verb in PREVIEW_ONLY_VERBS:
            _check(verb)

    _run(
        [
            "go",
            "run",
            "./cmd/cigate",
            "docs-prepare",
            "--source",
            str(SITE_DIR.relative_to(ROOT)),
            "--output",
            str(ASSET_DIR.relative_to(ROOT)),
            "--mode",
            mode,
            "--redirects",
            str(REDIRECTS.relative_to(ROOT)),
            "--source-revision",
            _source_revision(),
        ]
    )
    _validate_asset_tree()


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Build the static documentation asset tree consumed by Wrangler."
    )
    parser.add_argument(
        "--mode",
        choices=("preview", "production"),
        required=True,
        help="Use preview indexing controls or production indexing controls.",
    )
    parser.add_argument(
        "--full-check",
        action="store_true",
        help="Run the complete reader-critical gate before preparing assets.",
    )
    parser.add_argument(
        "--reuse-site",
        action="store_true",
        help=(
            "Preview only: reuse .build/docs when .build/docs-validated.sha names this "
            "revision (Docs Guards wrote it after every site check passed); otherwise "
            "build and check the site here."
        ),
    )
    args = parser.parse_args(argv)
    if args.reuse_site and args.mode != "preview":
        parser.error("--reuse-site is only valid with --mode preview")

    # Production is never prepared without the full publication gate.
    full_check = args.full_check or args.mode == "production"

    try:
        build(mode=args.mode, full_check=full_check, reuse_site=args.reuse_site)
    except FileNotFoundError as exc:
        print(f"ERROR: required command was not found: {exc}", file=sys.stderr)
        return 1
    except subprocess.CalledProcessError as exc:
        print(
            f"ERROR: documentation build command failed with exit code {exc.returncode}.\n"
            "Install the documentation environment with: "
            "python -m pip install -r requirements-docs.txt",
            file=sys.stderr,
        )
        return exc.returncode or 1
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(
        "Cloudflare documentation assets are ready at "
        f"{ASSET_DIR.relative_to(ROOT)} (mode={args.mode}, full_check={full_check}).",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
