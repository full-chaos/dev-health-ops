"""CHAOS-9731: the go-api-tools runtime image must carry a CA bundle.

debian:bookworm-slim has none, so outbound TLS from the one-off tools pod
failed certificate verification. Cheap structural guard; the built image is
checked by the docker-images workflow.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DOCKERFILE = (ROOT / "docker" / "go-api-tools.Dockerfile").read_text(encoding="utf-8")
WORKFLOW = (ROOT / ".github" / "workflows" / "docker-images.yml").read_text(
    encoding="utf-8"
)


def _runtime_stage() -> str:
    parts = re.split(r"(?m)^FROM .* AS runtime\s*$", DOCKERFILE)
    assert len(parts) == 2, "expected exactly one runtime stage"
    return parts[1]


def test_runtime_stage_installs_ca_certificates() -> None:
    stage = _runtime_stage()
    assert re.search(r"apt-get install[^\n]*(\\\n[^\n]*)*ca-certificates", stage)


def test_workflow_checks_ca_bundle_in_built_image() -> None:
    assert "/etc/ssl/certs/ca-certificates.crt" in WORKFLOW
