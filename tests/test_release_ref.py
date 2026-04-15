from __future__ import annotations

from importlib import import_module
from types import SimpleNamespace

import pytest

get_release_ref_enrichment = import_module(
    "dev_health_ops.processors.release_ref"
).get_release_ref_enrichment


def test_github_release_ref_prefers_release_tag() -> None:
    deployment = SimpleNamespace(id=101, ref="v2.0.0", payload={})

    enrichment = get_release_ref_enrichment(
        deployment,
        "github",
        releases=[SimpleNamespace(tag_name="v2.0.0")],
    )

    assert enrichment.release_ref == "v2.0.0"
    assert enrichment.confidence == pytest.approx(1.0)


def test_github_release_ref_falls_back_to_deployment_id() -> None:
    deployment = {"deployment_id": "gh-deploy-1"}

    enrichment = get_release_ref_enrichment(deployment, "github")

    assert enrichment.release_ref == "gh-deploy-1"
    assert enrichment.confidence == pytest.approx(0.3)


def test_gitlab_release_ref_falls_back_to_deployment_iid() -> None:
    deployment = {"id": 901, "iid": 42}

    enrichment = get_release_ref_enrichment(deployment, "gitlab")

    assert enrichment.release_ref == "42"
    assert enrichment.confidence == pytest.approx(0.3)


def test_generic_release_ref_uses_explicit_value_when_present() -> None:
    deployment = {"deployment_id": "generic-1", "release_ref": "2026.04.14"}

    enrichment = get_release_ref_enrichment(deployment, "generic")

    assert enrichment.release_ref == "2026.04.14"
    assert enrichment.confidence == pytest.approx(1.0)
