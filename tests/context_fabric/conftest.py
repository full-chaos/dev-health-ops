"""Shared fixtures for the CHAOS-3617 graph-arm suite."""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest

from dev_health_ops.api.dev.evidence_service import EvidenceReferenceSigner
from dev_health_ops.context_fabric.graph_arm import build_projection, fixtures
from dev_health_ops.context_fabric.graph_arm.projection import GraphProjection

#: A fixed, obviously-fake signing secret. The signer requires >= 32 bytes.
TEST_SIGNING_SECRET = "chaos-3617-test-signing-secret-not-a-real-key"


@pytest.fixture
def signer() -> EvidenceReferenceSigner:
    return EvidenceReferenceSigner(TEST_SIGNING_SECRET)


@pytest.fixture
def alpha_projection() -> GraphProjection:
    return build_projection(fixtures.alpha_batch())


@pytest.fixture
def beta_projection() -> GraphProjection:
    return build_projection(fixtures.beta_batch())


@pytest.fixture
def signing_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("JWT_SECRET_KEY", TEST_SIGNING_SECRET)
    yield


@pytest.fixture(autouse=True)
def _flags_default_off(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every test starts with both arm flags unset.

    Autouse because a test that enables projection and forgets to clean up
    would make the "default off" assertions in another module pass for the
    wrong reason -- and those assertions are the whole point of the flags.
    """

    for name in (
        "CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED",
        "CONTEXT_FABRIC_GRAPH_READ_ENABLED",
    ):
        monkeypatch.delenv(name, raising=False)
    assert os.getenv("CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED") is None
