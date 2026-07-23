import subprocess
from datetime import date
from pathlib import Path
from typing import Any

import yaml

from scripts.check_built_site_links import check_built_site
from scripts.check_code_prerequisites import pages_missing_prerequisite_link
from scripts.check_docs_links import check_docs
from scripts.check_external_links import FetchResult, check_external_links
from scripts.check_freshness_inventory import check_freshness_inventory, load_inventory


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_broken_source_link_fails() -> None:
    """Given a markdown page linking to a file that does not exist, when the
    source-link guard runs, then it reports exactly that missing target."""
    root = Path(__file__).resolve().parents[2]
    fixture_root = root / "tests" / "docs" / "fixtures" / "broken_source_link"

    errors = check_docs(fixture_root / "docs", fixture_root)

    assert len(errors) == 1
    assert "does-not-exist.md" in errors[0]
    assert "missing file" in errors[0]


def test_broken_built_asset_fails(tmp_path: Path) -> None:
    """Given built HTML referencing a stylesheet that was never emitted, when
    the built-site guard runs, then it reports the missing built asset."""
    site_dir = tmp_path / "site"
    _write(
        site_dir / "index.html",
        '<html><head><link rel="stylesheet" href="assets/theme.css"></head>'
        "<body>Home</body></html>",
    )

    errors = check_built_site(site_dir)

    assert len(errors) == 1
    assert "assets/theme.css" in errors[0]
    assert "missing built asset" in errors[0]


def test_expired_allowlist_fails(tmp_path: Path) -> None:
    """Given an allowlist entry whose expiry date has passed, when the
    external-link guard runs, then it fails even though the URL is
    reachable, naming the URL and its expiry date."""
    site_dir = tmp_path / "site"
    _write(
        site_dir / "index.html", '<a href="https://expired.example.com/">expired</a>'
    )
    allowlist_path = tmp_path / "allowlist.yml"
    allowlist_path.write_text(
        "- url: https://expired.example.com/\n"
        "  owner: docs-content\n"
        "  reason: temporary vendor outage\n"
        "  expires: '2026-01-01'\n",
        encoding="utf-8",
    )

    def always_ok(_url: str) -> FetchResult:
        return True, "status 200"

    errors = check_external_links(
        site_dir, allowlist_path, always_ok, date(2026, 7, 16)
    )

    assert errors == [
        "index.html: allowlist entry for https://expired.example.com/ expired on "
        "2026-01-01 (owner: docs-content, reason: temporary vendor outage)"
    ]


def test_stale_review_date_fails(tmp_path: Path) -> None:
    """Given a freshness-inventory entry last reviewed more than 180 days
    ago, when the freshness guard runs, then it names the page and its
    review age."""
    docs_root = tmp_path / "docs"
    _write(docs_root / "guide.md", "# Guide\n")
    inventory_path = tmp_path / "freshness-inventory.yml"
    inventory_path.write_text(
        "- page: guide.md\n"
        "  disposition: refresh\n"
        "  owner: docs-team\n"
        "  last-reviewed: '2025-01-01'\n",
        encoding="utf-8",
    )

    entries = load_inventory(inventory_path)
    errors = check_freshness_inventory(entries, docs_root, today=date(2026, 7, 16))

    assert len(errors) == 1
    assert "guide.md" in errors[0]
    assert "exceeding the 180-day review cadence" in errors[0]


def test_missing_code_prerequisite_fails(tmp_path: Path) -> None:
    """Given an in-scope page with a credentialed code sample but no link to
    getting-started.md, when the prerequisite guard runs, then it names the
    page."""
    docs_root = tmp_path / "docs"
    _write(
        docs_root / "guide.md",
        "```bash\nCLICKHOUSE_URI=clickhouse://ch:ch@localhost:8123/default dev-hops metrics daily\n```\n",
    )

    missing = pages_missing_prerequisite_link(docs_root, frozenset({"guide.md"}))

    assert missing == ["guide.md"]


CLOUDFLARE_WORKFLOW_PATH = (
    Path(__file__).resolve().parents[2]
    / ".github"
    / "workflows"
    / "docs-cloudflare.yml"
)


def _cloudflare_workflow() -> dict[object, Any]:
    return yaml.safe_load(CLOUDFLARE_WORKFLOW_PATH.read_text(encoding="utf-8"))


def _cloudflare_step_run(job_name: str, step_name: str) -> str:
    workflow = _cloudflare_workflow()
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    job = jobs[job_name]
    assert isinstance(job, dict)
    steps = job["steps"]
    assert isinstance(steps, list)
    step = next(
        candidate
        for candidate in steps
        if isinstance(candidate, dict) and candidate.get("name") == step_name
    )
    run_script = step["run"]
    assert isinstance(run_script, str)
    return run_script


def _run_confirmation(script: str, **env: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", "-c", script],
        check=False,
        env={"PATH": "/usr/bin:/bin", **env},
        capture_output=True,
        text=True,
    )


def test_cloudflare_production_deploy_fails_closed_without_confirmation() -> None:
    """Given the real production-deploy confirmation gate, when the feature
    flag or the typed confirmation is wrong, then the step exits non-zero
    rather than deploying to the canonical domain."""
    script = _cloudflare_step_run(
        "deploy-production", "Confirm production deployment is enabled"
    )

    # Flag disabled -> refuse regardless of confirmation.
    assert (
        _run_confirmation(
            script, ENABLED="false", CONFIRMATION="docs.fullchaos.dev"
        ).returncode
        == 1
    )
    # Flag enabled but confirmation wrong -> refuse.
    assert (
        _run_confirmation(script, ENABLED="true", CONFIRMATION="nope").returncode == 1
    )
    # Both correct -> the guard passes and hands off to the deploy step.
    assert (
        _run_confirmation(
            script, ENABLED="true", CONFIRMATION="docs.fullchaos.dev"
        ).returncode
        == 0
    )


def test_cloudflare_rollback_requires_enable_confirmation_and_version() -> None:
    """The rollback gate refuses unless the flag, the typed confirmation, and
    an explicit Worker version ID are all supplied."""
    script = _cloudflare_step_run("rollback-production", "Validate rollback request")

    good = {
        "ENABLED": "true",
        "CONFIRMATION": "docs.fullchaos.dev",
        "VERSION_ID": "abc123",
    }
    assert _run_confirmation(script, **good).returncode == 0

    for missing in ("ENABLED", "CONFIRMATION", "VERSION_ID"):
        broken = dict(good)
        broken[missing] = ""
        assert _run_confirmation(script, **broken).returncode == 1, missing


def test_cloudflare_preview_build_streams_full_check_under_pipefail() -> None:
    """The pull-request preview build runs the validated Cloudflare build with
    pipefail so a checker failure is not masked by ``tee``."""
    script = _cloudflare_step_run(
        "preview-or-build", "Build and prepare validated preview assets"
    )

    assert "set -o pipefail" in script
    assert "build_docs_cloudflare.py --mode preview --full-check" in script
    assert "| tee " in script


def test_cloudflare_preview_records_a_no_op_when_upload_is_skipped() -> None:
    """When a live Worker preview is not uploaded (fork PR, missing secrets, or
    disabled feature flag), the workflow records why instead of failing or
    silently passing an empty deploy."""
    workflow = _cloudflare_workflow()
    steps = workflow["jobs"]["preview-or-build"]["steps"]
    assert isinstance(steps, list)
    record = next(
        step
        for step in steps
        if isinstance(step, dict)
        and step.get("name") == "Record why a Cloudflare preview was not uploaded"
    )

    condition = str(record.get("if", ""))
    assert "steps.upload.outcome == 'skipped'" in condition
    # It only reports; it must not attempt any deploy/upload itself.
    run_script = str(record.get("run", ""))
    assert "wrangler" not in run_script
    assert "$GITHUB_STEP_SUMMARY" in run_script
