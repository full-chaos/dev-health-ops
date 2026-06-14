"""Tests for the work-graph CLI runner (``dev-hops work-graph build``).

These guard the org-scope wiring flagged in CHAOS-2375 round-3: the CLI must
flow ``--org`` into ``BuildConfig.org_id`` so every read/write the builder
performs is tenant-scoped, and must fail closed when ``--org`` is supplied but
blank rather than silently building under the empty org.
"""

from __future__ import annotations

import argparse
from unittest.mock import MagicMock, patch

# Import connectors first to defuse the providers._base <-> connectors circular
# import so this module runs in isolation.
import dev_health_ops.connectors  # noqa: F401
from dev_health_ops.work_graph.runner import run_work_graph_build


def _ns(**overrides) -> argparse.Namespace:
    base = dict(
        db="clickhouse://localhost:9000/default",
        from_date="2024-01-01",
        to_date="2024-02-01",
        repo_id=None,
        heuristic_window=7,
        heuristic_confidence=0.3,
        check_components=False,
        allow_degenerate=False,
    )
    base.update(overrides)
    return argparse.Namespace(**base)


def _patched_builder():
    """Patch WorkGraphBuilder so build() returns trivially and the verification
    edge-count query reports a non-empty graph (so run returns 0)."""
    fake_builder = MagicMock()
    fake_builder.build.return_value = {"pr_commit_edges": 1}
    client = MagicMock()
    client.query.return_value.result_rows = [[1]]
    fake_builder.client = client
    return fake_builder


def test_org_flows_into_build_config():
    """``--org X`` must produce ``BuildConfig.org_id == X``."""
    captured = {}

    def _capture(config):
        captured["config"] = config
        return _patched_builder()

    with patch(
        "dev_health_ops.work_graph.runner.WorkGraphBuilder", side_effect=_capture
    ):
        rc = run_work_graph_build(_ns(org="org-abc"))

    assert rc == 0
    assert captured["config"].org_id == "org-abc"


def test_no_org_builds_full_rebuild_with_empty_org():
    """Omitting ``--org`` is a legitimate full rebuild: org_id is empty, not a
    failure."""
    captured = {}

    def _capture(config):
        captured["config"] = config
        return _patched_builder()

    with patch(
        "dev_health_ops.work_graph.runner.WorkGraphBuilder", side_effect=_capture
    ):
        rc = run_work_graph_build(_ns(org=None))

    assert rc == 0
    assert captured["config"].org_id == ""


def test_blank_org_fails_closed():
    """An explicit but blank ``--org`` must fail closed (return 2) and never
    construct a builder -- otherwise the tenant build silently runs unscoped."""
    with patch("dev_health_ops.work_graph.runner.WorkGraphBuilder") as builder_cls:
        rc = run_work_graph_build(_ns(org="   "))

    assert rc == 2
    builder_cls.assert_not_called()


def test_verification_uses_builder_sink_client_when_builder_has_no_client():
    fake_builder = MagicMock()
    fake_builder.build.return_value = {"pr_commit_edges": 1}
    fake_builder.client = None
    client = MagicMock()
    client.query.return_value.result_rows = [[1]]
    fake_builder.sink.client = client

    with patch(
        "dev_health_ops.work_graph.runner.WorkGraphBuilder",
        return_value=fake_builder,
    ):
        rc = run_work_graph_build(_ns(org="org-abc"))

    assert rc == 0
    client.query.assert_called_once()
