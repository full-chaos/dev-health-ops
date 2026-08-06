"""Unit coverage for ``scripts.acceptance.corpus.world_digest_guard``.

``verify_world_digest`` (the async compute-and-compare half) needs a live,
reachable ClickHouse/Postgres scratch database and is NOT exercised here --
doing so would mean either standing up real infra in a unit test (wrong
tier) or mocking ``compute_world_digest`` so thoroughly the test would only
prove the mock behaves as scripted. It is exercised by the live corpus
runner's own armed-or-throw session setup instead. This suite covers the
pure comparison/assertion half (``require_world_digest_match``), which is
where the actual pass/fail decision logic worth unit-testing lives.
"""

from __future__ import annotations

import pytest

from scripts.acceptance.corpus.world_digest_guard import (
    WorldDigestMismatchError,
    WorldDigestVerification,
    require_world_digest_match,
)


def _verification(
    *, matched: bool, drifted: tuple[str, ...] = ()
) -> WorldDigestVerification:
    return WorldDigestVerification(
        pinned_digest="pinned-abc",
        live_digest="live-abc" if matched else "live-xyz",
        matched=matched,
        drifted_components=drifted,
    )


class TestRequireWorldDigestMatch:
    def test_a_match_is_a_silent_no_op(self) -> None:
        require_world_digest_match(
            _verification(matched=True), digest_path="WORLD_DIGEST"
        )

    def test_a_mismatch_raises(self) -> None:
        with pytest.raises(WorldDigestMismatchError):
            require_world_digest_match(
                _verification(matched=False, drifted=("clickhouse.commits",)),
                digest_path="WORLD_DIGEST",
            )

    def test_mismatch_error_names_the_drifted_components(self) -> None:
        with pytest.raises(WorldDigestMismatchError, match="clickhouse.commits"):
            require_world_digest_match(
                _verification(
                    matched=False, drifted=("clickhouse.commits", "postgres.users")
                ),
                digest_path="WORLD_DIGEST",
            )

    def test_mismatch_error_names_both_digests(self) -> None:
        with pytest.raises(WorldDigestMismatchError, match="pinned-abc") as excinfo:
            require_world_digest_match(
                _verification(matched=False), digest_path="WORLD_DIGEST"
            )
        assert "live-xyz" in str(excinfo.value)
