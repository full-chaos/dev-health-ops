"""CHAOS-3463: pure-Python guards of the world snapshot/restore path.

Everything here is the half of ``fixtures/world_snapshot.py`` that can be
proven without infrastructure -- the safety preconditions, the two oracles'
comparison logic, and the value codec's round trip. The live half (a real
generate -> snapshot -> restore -> digest cycle against the ask-dev
acceptance stack) is proven by the two-boot evidence run recorded on
CHAOS-3463, not here.

Each guard gets a test that OBSERVES IT FAILING, not merely a happy path:
a precondition nobody has watched reject something is not a precondition.
"""

from __future__ import annotations

import gzip
import json
import uuid
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from dev_health_ops.fixtures.world_snapshot import (
    _CLICKHOUSE_LEDGER_TABLES,
    _DUMP_VERIFY_ATTEMPTS,
    _POSTGRES_LEDGER_TABLES,
    ACCEPTANCE_ENVIRONMENT,
    SNAPSHOT_SCHEMA_VERSION,
    RestoreDriftError,
    RestoreRefusedError,
    SnapshotError,
    _assert_content_identity,
    _assert_no_ttl_horizon_rows,
    _assert_round_trip,
    _assert_schema_compatible,
    _changed_tables,
    _decode_value,
    _dump_clickhouse,
    _encode_value,
    _require_acceptance_environment,
    _require_empty_targets,
    _verify_snapshot_files,
    content_hashes_from_manifest,
    content_hashes_to_manifest,
    foreign_key_is_live,
    read_snapshot_manifest,
    reference_alignment_plan,
)

# ---------------------------------------------------------------------------
# Value codec
# ---------------------------------------------------------------------------


class TestValueCodec:
    """A lossy codec would not fail loudly -- it would change a row's content
    hash and surface as an unexplained WORLD_DIGEST drift on every boot. So
    the round trip is asserted on TYPE as well as value."""

    @pytest.mark.parametrize(
        "value",
        [
            None,
            True,
            0,
            -17,
            3.5,
            "",
            "text with 'quotes' and \\backslashes\\",
            uuid.uuid4(),
            datetime(2026, 8, 5, 12, 34, 56, 789012, tzinfo=timezone.utc),
            datetime(2026, 8, 5, 12, 34, 56),
            date(2026, 8, 5),
            time(1, 2, 3),
            timedelta(days=2, seconds=37),
            Decimal("1234.5678"),
            b"\x00\xff\x10binary",
            {"nested": {"a": [1, 2, 3]}, "b": None},
            [1, "two", None],
        ],
    )
    def test_round_trips_value_and_type(self, value) -> None:
        encoded = json.loads(json.dumps(_encode_value(value)))
        decoded = _decode_value(encoded)
        assert decoded == value
        assert type(decoded) is type(value)

    def test_timezone_awareness_survives(self) -> None:
        aware = datetime(2026, 8, 5, tzinfo=timezone.utc)
        decoded = _decode_value(json.loads(json.dumps(_encode_value(aware))))
        assert decoded.tzinfo is not None
        assert decoded == aware

    def test_unencodable_value_fails_loudly(self) -> None:
        """Refusing beats coercing: a value silently str()'d here restores as
        a different type and changes the digest for no visible reason."""

        class Exotic:
            pass

        with pytest.raises(SnapshotError, match="no encoding for value"):
            _encode_value(Exotic())

    def test_plain_dict_payload_is_not_mistaken_for_an_envelope(self) -> None:
        """A JSONB column whose value happens to contain this codec's own tag
        key must still round-trip as data, not be decoded as an envelope."""
        payload = {"__t__": "uuid", "v": "not-a-uuid"}
        decoded = _decode_value(json.loads(json.dumps(_encode_value(payload))))
        assert decoded == payload


# ---------------------------------------------------------------------------
# Completeness derivation
# ---------------------------------------------------------------------------


class TestChangedTables:
    def test_derives_the_written_tables_from_the_baseline_diff(self) -> None:
        changed = _changed_tables(
            source={"git_commits": 300, "organizations": 3, "feature_flags": 32},
            baseline={"git_commits": 0, "organizations": 0, "feature_flags": 32},
            store="postgres",
            ledger=frozenset(),
        )
        assert changed == ["git_commits", "organizations"]

    def test_a_table_absent_from_the_baseline_map_still_counts(self) -> None:
        """A table created by a migration the baseline database has not seen
        must not vanish from the snapshot just because the baseline map has
        no key for it."""
        assert _changed_tables(
            source={"brand_new": 5}, baseline={}, store="clickhouse", ledger=frozenset()
        ) == ["brand_new"]

    def test_migration_seeded_table_the_world_also_writes_is_refused(self) -> None:
        """The unhandled case fails loudly instead of producing a snapshot
        that restores wrong (a full-table dump on top of the baseline's own
        rows double-counts them)."""
        with pytest.raises(SnapshotError, match="already non-empty in the "):
            _changed_tables(
                source={"feature_flags": 40},
                baseline={"feature_flags": 32},
                store="postgres",
                ledger=frozenset(),
            )

    def test_ledger_tables_are_never_snapshotted(self) -> None:
        assert "alembic_version" in _POSTGRES_LEDGER_TABLES
        assert "schema_migrations" in _CLICKHOUSE_LEDGER_TABLES
        assert (
            _changed_tables(
                source={"alembic_version": 2},
                baseline={"alembic_version": 0},
                store="postgres",
                ledger=_POSTGRES_LEDGER_TABLES,
            )
            == []
        )


# ---------------------------------------------------------------------------
# Restore preconditions
# ---------------------------------------------------------------------------


class TestRestorePreconditions:
    def test_non_acceptance_environment_is_refused(self) -> None:
        with pytest.raises(RestoreRefusedError, match="ENVIRONMENT is"):
            _require_acceptance_environment({"ENVIRONMENT": "development"})

    def test_missing_environment_is_refused(self) -> None:
        with pytest.raises(RestoreRefusedError, match="ENVIRONMENT is None"):
            _require_acceptance_environment({})

    def test_acceptance_environment_is_allowed(self) -> None:
        _require_acceptance_environment({"ENVIRONMENT": ACCEPTANCE_ENVIRONMENT})

    def test_non_empty_target_table_is_refused(self) -> None:
        """The predicate that makes a real dev/prod database impossible to
        restore over: it always has organizations."""
        with pytest.raises(RestoreRefusedError, match="organizations"):
            _require_empty_targets(
                store="postgres",
                counts={"organizations": 41, "git_commits": 0},
                tables={"organizations": {}, "git_commits": {}},
            )

    def test_empty_target_is_allowed(self) -> None:
        _require_empty_targets(
            store="postgres",
            counts={"organizations": 0},
            tables={"organizations": {}},
        )

    def test_rows_in_a_table_the_snapshot_does_not_carry_are_not_a_refusal(
        self,
    ) -> None:
        """Migration-seeded rows (feature_flags, permissions, ...) are present
        in every freshly-migrated database and are not part of the snapshot --
        refusing on them would make the guard unsatisfiable."""
        _require_empty_targets(
            store="postgres",
            counts={"organizations": 0, "feature_flags": 32},
            tables={"organizations": {}},
        )


# ---------------------------------------------------------------------------
# Round-trip oracle
# ---------------------------------------------------------------------------


class TestRoundTripOracle:
    def _call(
        self,
        *,
        source: dict[str, int] | None = None,
        baseline: dict[str, int] | None = None,
        target_before: dict[str, int] | None = None,
        target_after: dict[str, int] | None = None,
    ) -> None:
        _assert_round_trip(
            store="postgres",
            source=source
            if source is not None
            else {"git_commits": 300, "alembic_version": 2},
            baseline=baseline
            if baseline is not None
            else {"git_commits": 0, "alembic_version": 2},
            target_before=target_before
            if target_before is not None
            else {"git_commits": 0, "alembic_version": 2},
            target_after=target_after
            if target_after is not None
            else {"git_commits": 300, "alembic_version": 2},
        )

    def test_matching_deltas_pass(self) -> None:
        self._call()

    def test_a_table_the_snapshot_missed_fails(self) -> None:
        """The whole point of comparing over the UNION of every table rather
        than over the snapshotted ones: a table the world wrote but nobody
        snapshotted reads as `expected +N, got +0`."""
        with pytest.raises(
            RestoreDriftError, match=r"work_items: expected \+12, got \+0"
        ):
            self._call(
                source={"git_commits": 300, "work_items": 12},
                baseline={"git_commits": 0, "work_items": 0},
                target_before={"git_commits": 0, "work_items": 0},
                target_after={"git_commits": 300, "work_items": 0},
            )

    def test_a_short_restore_fails(self) -> None:
        with pytest.raises(
            RestoreDriftError, match=r"git_commits: expected \+300, got \+299"
        ):
            self._call(target_after={"git_commits": 299, "alembic_version": 2})

    def test_a_double_restore_fails(self) -> None:
        with pytest.raises(RestoreDriftError, match=r"expected \+300, got \+600"):
            self._call(target_after={"git_commits": 600, "alembic_version": 2})

    def test_migration_seeded_rows_do_not_register_as_drift(self) -> None:
        """Both databases are migrated at the same head, so their seeded rows
        cancel in the delta -- no ignore list needed."""
        self._call(
            source={"feature_flags": 32},
            baseline={"feature_flags": 32},
            target_before={"feature_flags": 32},
            target_after={"feature_flags": 32},
        )


# ---------------------------------------------------------------------------
# Snapshot artifact integrity
# ---------------------------------------------------------------------------


def _write_snapshot(tmp_path: Path, *, payload: bytes = b"world-bytes") -> dict:
    (tmp_path / "clickhouse").mkdir()
    (tmp_path / "postgres").mkdir()
    blob = tmp_path / "clickhouse" / "git_commits.native.gz"
    blob.write_bytes(gzip.compress(payload, mtime=0))
    import hashlib

    document = {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "clickhouse": {
            "tables": {
                "git_commits": {
                    "file": "clickhouse/git_commits.native.gz",
                    "row_count": 1,
                    "sha256": hashlib.sha256(blob.read_bytes()).hexdigest(),
                }
            },
            "source_row_counts": {},
            "baseline_row_counts": {},
        },
        "postgres": {
            "tables": {},
            "source_row_counts": {},
            "baseline_row_counts": {},
        },
    }
    (tmp_path / "manifest.json").write_text(json.dumps(document))
    return document


class TestSnapshotArtifactIntegrity:
    def test_missing_manifest_is_refused(self, tmp_path: Path) -> None:
        with pytest.raises(RestoreRefusedError, match="no snapshot manifest"):
            read_snapshot_manifest(tmp_path)

    def test_wrong_schema_version_is_refused(self, tmp_path: Path) -> None:
        (tmp_path / "manifest.json").write_text(
            json.dumps({"schema_version": "ask_dev_world_snapshot.v0"})
        )
        with pytest.raises(RestoreRefusedError, match="declares schema_version"):
            read_snapshot_manifest(tmp_path)

    def test_intact_snapshot_verifies(self, tmp_path: Path) -> None:
        document = _write_snapshot(tmp_path)
        _verify_snapshot_files(tmp_path, document)

    def test_a_tampered_file_is_refused(self, tmp_path: Path) -> None:
        """Observed failing, not assumed: a snapshot truncated by a bad
        checkout must be rejected here rather than restore short and surface
        as a confusing digest mismatch later."""
        document = _write_snapshot(tmp_path)
        (tmp_path / "clickhouse" / "git_commits.native.gz").write_bytes(
            gzip.compress(b"tampered", mtime=0)
        )
        with pytest.raises(RestoreRefusedError, match="sha256"):
            _verify_snapshot_files(tmp_path, document)

    def test_a_missing_file_is_refused(self, tmp_path: Path) -> None:
        document = _write_snapshot(tmp_path)
        (tmp_path / "clickhouse" / "git_commits.native.gz").unlink()
        with pytest.raises(RestoreRefusedError, match="is missing"):
            _verify_snapshot_files(tmp_path, document)


# ---------------------------------------------------------------------------
# Reference-id alignment
# ---------------------------------------------------------------------------


class TestReferenceAlignmentPlan:
    """`feature_flags` gets a fresh random `id` in every database a migration
    runs in, and `org_feature_overrides.feature_id` -- a column the world
    digest hashes -- points at it. Live-reproduced: restoring the referencing
    rows verbatim raised `ForeignKeyViolationError: Key (feature_id)=(6cd6…)
    is not present in table "feature_flags"`. Translating the referencing
    value instead would have fixed the FK and broken the pin, because the
    hashed column would then differ on every boot. The reference table's ids
    are aligned to the snapshot so every world row restores byte-identical.
    """

    def _spec(self, ids: dict[str, str]) -> dict:
        return {
            "primary_key": "id",
            "natural_key": ["key"],
            "ids": {key: {"__t__": "uuid", "v": value} for key, value in ids.items()},
        }

    def test_plans_an_update_only_where_the_id_differs(self) -> None:
        source = uuid.uuid4()
        same = uuid.uuid4()
        target_current = uuid.uuid4()
        plan = reference_alignment_plan(
            self._spec({'["ask_dev"]': str(source), '["api_access"]': str(same)}),
            {'["ask_dev"]': target_current, '["api_access"]': same},
            table="feature_flags",
        )
        assert plan == [(target_current, source)]

    def test_an_all_matching_target_needs_no_writes(self) -> None:
        same = uuid.uuid4()
        assert (
            reference_alignment_plan(
                self._spec({'["ask_dev"]': str(same)}),
                {'["ask_dev"]': same},
                table="feature_flags",
            )
            == []
        )

    def test_a_natural_key_the_target_lacks_is_refused_not_skipped(self) -> None:
        """Head drift must abort the restore. Skipping the row would leave a
        dangling foreign key and surface as a much less informative database
        error several statements later."""
        with pytest.raises(RestoreRefusedError, match="migrated at a different head"):
            reference_alignment_plan(
                self._spec({'["a_flag_added_later"]': str(uuid.uuid4())}),
                {'["ask_dev"]': uuid.uuid4()},
                table="feature_flags",
            )


class TestForeignKeyIsLive:
    """MATCH SIMPLE: a composite foreign key is not enforced for any row where
    one of its columns is NULL. Getting this wrong does not corrupt anything --
    it refuses a perfectly restorable snapshot, which is how it was found
    (`dev_runs` -> `dev_messages` on (answer_id, org_id, user_id), answer_id
    NULL, org_id/user_id set)."""

    _INDEX = {"answer_id": 0, "org_id": 1, "user_id": 2}

    def test_a_partially_null_composite_key_is_not_live(self) -> None:
        rows = [[None, uuid.uuid4(), uuid.uuid4()], [None, uuid.uuid4(), uuid.uuid4()]]
        assert not foreign_key_is_live(
            rows, self._INDEX, ["answer_id", "org_id", "user_id"]
        )

    def test_a_fully_populated_composite_key_is_live(self) -> None:
        rows = [
            [None, uuid.uuid4(), uuid.uuid4()],
            [uuid.uuid4(), uuid.uuid4(), uuid.uuid4()],
        ]
        assert foreign_key_is_live(
            rows, self._INDEX, ["answer_id", "org_id", "user_id"]
        )

    def test_a_single_column_key_is_live_when_any_row_sets_it(self) -> None:
        rows = [[None, None, None], [uuid.uuid4(), None, None]]
        assert foreign_key_is_live(rows, self._INDEX, ["answer_id"])

    def test_a_single_column_key_that_is_always_null_is_not_live(self) -> None:
        rows = [[None, None, None], [None, None, None]]
        assert not foreign_key_is_live(rows, self._INDEX, ["answer_id"])

    def test_no_rows_at_all_is_not_live(self) -> None:
        assert not foreign_key_is_live([], self._INDEX, ["answer_id"])


class TestClickHouseContentOracle:
    """Replaces the row-count oracle on the ClickHouse side. Counts there are
    collapsed by background merges on their own schedule, so comparing them
    across two databases compares two moving targets -- observed live as
    `teams: expected +16, got +10` for a restore that had inserted all 16
    rows."""

    def test_identical_content_passes(self) -> None:
        _assert_content_identity(
            source={"git_commits": "a" * 64, "teams": "b" * 64},
            target={"git_commits": "a" * 64, "teams": "b" * 64},
        )

    def test_different_content_fails_even_at_the_same_row_count(self) -> None:
        """The strength this buys over counting: the right NUMBER of wrong
        rows is caught."""
        with pytest.raises(RestoreDriftError, match="git_commits"):
            _assert_content_identity(
                source={"git_commits": "a" * 64},
                target={"git_commits": "c" * 64},
            )

    def test_a_table_missing_from_the_target_fails(self) -> None:
        with pytest.raises(RestoreDriftError, match="teams.*ABSENT"):
            _assert_content_identity(
                source={"teams": "b" * 64},
                target={},
            )

    def test_an_unexpected_table_in_the_target_fails(self) -> None:
        with pytest.raises(RestoreDriftError, match="source=ABSENT"):
            _assert_content_identity(
                source={},
                target={"teams": "b" * 64},
            )


class _DumpResult:
    def __init__(self, rows: list[list[Any]]) -> None:
        self.result_rows = rows


class _FakeShortPayloadClient:
    """CHAOS-3602: `raw_query`'s Native payload and a same-session `count()`
    have been observed to silently disagree on a fully idle, unchanging
    table. This fake makes that disagreement DETERMINISTIC: each entry in
    ``payload_row_counts`` is what a real decode of that attempt's payload
    would count (i.e. what a throwaway staging table would report after
    `raw_insert`-ing it), while `source_count` is what the source table's own
    `count()` reports EVERY time -- exactly like the real incident, where
    that query kept saying 1042 even on the attempt whose payload actually
    held only 1041 rows.
    """

    database = "ask_dev_world_scratch"

    def __init__(self, *, payload_row_counts: list[int], source_count: int) -> None:
        self.payload_row_counts = list(payload_row_counts)
        self.source_count = source_count
        self.raw_query_calls = 0
        self.raw_insert_calls = 0
        self.commands: list[str] = []
        self._last_inserted_index: int | None = None

    def raw_query(self, query: str, fmt: str | None = None) -> bytes:
        assert fmt == "Native"
        assert (
            "ORDER BY" not in query and "LIMIT" not in query and "OFFSET" not in query
        )
        index = self.raw_query_calls
        self.raw_query_calls += 1
        # The "payload" is just a tag identifying which attempt produced it --
        # this fake never really encodes/decodes Native bytes, only tracks
        # which attempt's (fake) payload was inserted where.
        return f"payload-{index}".encode()

    def query(self, query: str, parameters: dict | None = None) -> _DumpResult:
        if "system.parts" in query:
            return _DumpResult([[3]])
        if "__snapshot_verify_" in query:
            assert self._last_inserted_index is not None, (
                "decoded a verify table before any raw_insert into it"
            )
            return _DumpResult([[self.payload_row_counts[self._last_inserted_index]]])
        # The SOURCE table's own count() -- observed live to say the FULL
        # count even on an attempt whose payload was short.
        return _DumpResult([[self.source_count]])

    def command(self, cmd: str, parameters: dict | None = None) -> None:
        self.commands.append(cmd)

    def raw_insert(
        self, table: str, insert_block: bytes, fmt: str | None = None
    ) -> None:
        assert fmt == "Native"
        assert table.startswith("__snapshot_verify_")
        self.raw_insert_calls += 1
        index = int(insert_block.decode().removeprefix("payload-"))
        self._last_inserted_index = index


class TestClickHouseDumpPayloadVerification:
    """CHAOS-3602: the mint's own content oracle caught `feature_flag_event`
    missing a row after a real `_dump_clickhouse` call. The manifest it
    produced recorded `raw_source_row_count: 1042` AND `row_count: 1042` --
    both counts agreed -- yet the actual `.native.gz` file, decoded by hand,
    held only 1041 distinct rows. Stress testing against the live, IDLE
    table (zero concurrent writes) reproduced it again directly: repeated
    `raw_query(Native)` dumps of unchanging data intermittently came up one
    row short, at roughly a 2% per-dump rate. Ruled out: query pagination
    (this function issues one unpaginated `SELECT *`, confirmed by reading
    it) and write-visibility timing (reproduced with no writes in flight).
    The only correct fix is to never trust a separate `count()` query --
    decode the payload itself and compare THAT.
    """

    @pytest.mark.asyncio
    async def test_a_short_payload_is_retried_and_the_recovered_attempt_is_what_gets_written(
        self, tmp_path: Path
    ) -> None:
        """Attempt 1's payload decodes to 1041 (short); attempt 2's decodes
        to 1042 (matches). The OLD code -- which never decoded the payload,
        only trusted a same-session count() -- would have accepted attempt 1
        immediately, since that count() said 1042 the whole time. The fix
        must retry past it and write only the verified attempt's bytes.
        """

        client = _FakeShortPayloadClient(
            payload_row_counts=[1041, 1042], source_count=1042
        )
        path = tmp_path / "feature_flag_event.native.gz"

        row_count = await _dump_clickhouse(
            client,
            "feature_flag_event",
            "MergeTree()",
            path,
            raw_source_row_count=1042,
        )

        assert row_count == 1042, "must report the DECODED count, not a trusted count()"
        assert client.raw_query_calls == 2, (
            "must have retried once after the short attempt"
        )
        assert client.raw_insert_calls == 2, (
            "must decode-verify EVERY attempt, not just trust the first"
        )
        assert path.exists()
        # The bytes actually on disk must be attempt 2's (the verified one),
        # never attempt 1's short payload.
        assert path.read_bytes() != b""
        import gzip as _gzip

        written = _gzip.decompress(path.read_bytes())
        assert written == b"payload-1", (
            "the SHORT attempt-1 payload must never reach disk"
        )

    @pytest.mark.asyncio
    async def test_short_on_every_attempt_raises_and_writes_nothing(
        self, tmp_path: Path
    ) -> None:
        """The exhausted-retries path: every attempt's payload decodes short.
        Old code had no such concept -- it would have written the FIRST
        short payload and reported the source table's count() as if it
        described the file. New code must fail loudly and touch no file.
        """

        client = _FakeShortPayloadClient(
            payload_row_counts=[1041] * _DUMP_VERIFY_ATTEMPTS, source_count=1042
        )
        path = tmp_path / "feature_flag_event.native.gz"

        with pytest.raises(SnapshotError, match="CHAOS-3602"):
            await _dump_clickhouse(
                client,
                "feature_flag_event",
                "MergeTree()",
                path,
                raw_source_row_count=1042,
            )

        assert client.raw_query_calls == _DUMP_VERIFY_ATTEMPTS
        assert not path.exists(), "a payload that never verified must never reach disk"

    @pytest.mark.asyncio
    async def test_a_payload_that_verifies_on_the_first_attempt_needs_no_retry(
        self, tmp_path: Path
    ) -> None:
        """The ordinary, overwhelming-majority case: no flake at all."""

        client = _FakeShortPayloadClient(payload_row_counts=[1042], source_count=1042)
        path = tmp_path / "feature_flag_event.native.gz"

        row_count = await _dump_clickhouse(
            client,
            "feature_flag_event",
            "MergeTree()",
            path,
            raw_source_row_count=1042,
        )

        assert row_count == 1042
        assert client.raw_query_calls == 1
        assert path.exists()

    @pytest.mark.asyncio
    async def test_a_malformed_pre_count_result_fails_closed(
        self, tmp_path: Path
    ) -> None:
        """Codex round-2 finding (MEDIUM, confirmed): `int(pre_count.
        result_rows[0][0])` used to be unvalidated -- `result_rows == [[]]`
        (one row, ZERO columns) raised an uncontrolled `IndexError` instead
        of a named `SnapshotError`. Every count() in this retry loop now
        goes through `_scalar_count`.
        """

        class _MalformedPreCountClient:
            def raw_query(self, q: str, fmt: str | None = None) -> bytes:
                return b"payload"

            def query(self, q: str, parameters: dict | None = None) -> _DumpResult:
                return _DumpResult([[]])  # one row, zero columns

        with pytest.raises(SnapshotError, match="malformed"):
            await _dump_clickhouse(
                _MalformedPreCountClient(),
                "feature_flag_event",
                "MergeTree()",
                tmp_path / "feature_flag_event.native.gz",
                raw_source_row_count=1042,
            )


class TestScalarCount:
    """Codex round-2 finding (MEDIUM, confirmed): a bare `len(result_rows)
    != 1` check let `result_rows == [[]]` (one row, ZERO columns) through to
    an unvalidated `[0][0]` -- an uncontrolled IndexError, not a named
    SnapshotError. `[[0, "extra"]]` (an unexpected extra column) passed
    silently. `_scalar_count` is the one function every count() call site in
    this module goes through now.
    """

    def test_a_single_scalar_row_is_accepted(self) -> None:
        from dev_health_ops.fixtures.world_snapshot import _scalar_count

        assert _scalar_count(_DumpResult([[7]]), context="test") == 7

    def test_zero_rows_is_rejected(self) -> None:
        from dev_health_ops.fixtures.world_snapshot import _scalar_count

        with pytest.raises(SnapshotError, match="malformed"):
            _scalar_count(_DumpResult([]), context="test")

    def test_one_row_zero_columns_is_rejected_not_an_indexerror(self) -> None:
        from dev_health_ops.fixtures.world_snapshot import _scalar_count

        with pytest.raises(SnapshotError, match="malformed"):
            _scalar_count(_DumpResult([[]]), context="test")

    def test_one_row_two_columns_is_rejected(self) -> None:
        from dev_health_ops.fixtures.world_snapshot import _scalar_count

        with pytest.raises(SnapshotError, match="malformed"):
            _scalar_count(_DumpResult([[0, "extra"]]), context="test")

    def test_two_rows_is_rejected(self) -> None:
        from dev_health_ops.fixtures.world_snapshot import _scalar_count

        with pytest.raises(SnapshotError, match="malformed"):
            _scalar_count(_DumpResult([[1], [2]]), context="test")

    def test_a_default_is_returned_instead_of_raising_when_given(self) -> None:
        from dev_health_ops.fixtures.world_snapshot import _scalar_count

        assert _scalar_count(_DumpResult([]), context="test", default=-1) == -1


class _FakeTtlCountClient:
    """Reports a controllable row count for whichever table's TTL-horizon
    query is issued -- keyed by table name appearing in the query text,
    which is how `_assert_no_ttl_horizon_rows` actually builds its SQL."""

    def __init__(self, *, counts: dict[str, int]) -> None:
        self.counts = counts
        self.queries: list[str] = []

    def query(self, q: str, parameters: dict | None = None) -> _DumpResult:
        self.queries.append(q)
        for table, count in self.counts.items():
            if f"FROM `{table}`" in q:
                return _DumpResult([[count]])
        return _DumpResult([[0]])


class TestTtlHorizonGuard:
    """CHAOS-3602: a mint over data currently racing a TTL'd table's own
    background merge scheduler is nondeterministic BY DESIGN -- ClickHouse
    applies TTL deletion at merge time, silently, with no error. This guard
    must catch it BEFORE any dump, not after a content-oracle mismatch has
    already meant real data loss.
    """

    @pytest.mark.asyncio
    async def test_a_table_with_no_horizon_adjacent_rows_passes(self) -> None:
        client = _FakeTtlCountClient(counts={"feature_flag_event": 0})
        await _assert_no_ttl_horizon_rows(client, ["feature_flag_event"])

    @pytest.mark.asyncio
    async def test_a_table_with_horizon_adjacent_rows_is_refused(self) -> None:
        client = _FakeTtlCountClient(counts={"feature_flag_event": 3})
        with pytest.raises(SnapshotError, match="feature_flag_event"):
            await _assert_no_ttl_horizon_rows(client, ["feature_flag_event"])

    @pytest.mark.asyncio
    async def test_a_table_with_no_known_ttl_is_never_queried(self) -> None:
        """No TTL, no risk -- and no wasted query for every one of the ~90
        tables a mint dumps that were never at risk in the first place."""
        client = _FakeTtlCountClient(counts={})
        await _assert_no_ttl_horizon_rows(client, ["projects"])
        assert client.queries == []

    @pytest.mark.asyncio
    async def test_multiple_violating_tables_are_all_named_in_one_error(self) -> None:
        client = _FakeTtlCountClient(
            counts={"feature_flag_event": 1, "telemetry_signal_bucket": 5}
        )
        with pytest.raises(SnapshotError) as exc_info:
            await _assert_no_ttl_horizon_rows(
                client, ["feature_flag_event", "telemetry_signal_bucket", "projects"]
            )
        message = str(exc_info.value)
        assert "feature_flag_event" in message
        assert "telemetry_signal_bucket" in message

    @pytest.mark.asyncio
    async def test_a_broken_ttl_vocabulary_fails_closed_before_any_query(
        self, monkeypatch
    ) -> None:
        """This guard's OWN contract: whatever `assert_ttl_vocabulary_is_
        consistent` decides, this function must propagate it as a
        `SnapshotError` before issuing a single per-table query. The
        vocabulary-consistency logic itself (empty registry, partial
        registry, a fifth TTL table the precise parser misses via an
        unmatched syntax variant, a registry extra, a KNOWN_TTL_TABLES
        entry that's gone stale, ...) is exhaustively covered in
        `tests/test_ttl_registry.py::TestVocabularyConsistency` -- this
        test proves this guard actually calls it and wraps its failure
        correctly, not that the check's own logic is correct.
        """
        from dev_health_ops.fixtures import world_snapshot

        def _broken() -> None:
            raise RuntimeError("synthetic vocabulary break: table_x")

        monkeypatch.setattr(
            world_snapshot, "assert_ttl_vocabulary_is_consistent", _broken
        )
        client = _FakeTtlCountClient(counts={"feature_flag_event": 999999})

        with pytest.raises(SnapshotError, match="synthetic vocabulary break"):
            await _assert_no_ttl_horizon_rows(client, ["feature_flag_event"])

        assert client.queries == [], (
            "must fail before issuing a single per-table query once the "
            "vocabulary check itself fails"
        )

    @pytest.mark.asyncio
    async def test_a_malformed_query_result_fails_closed_instead_of_reading_as_zero(
        self,
    ) -> None:
        """Codex finding (HIGH, confirmed): an empty/malformed
        `result.result_rows` was folded into `count = 0` -- a driver
        returning something other than a single scalar count row (a
        transient response-shape defect, a mocking/wiring bug) silently
        read as "no violating rows" and let the mint proceed unchecked.
        """

        class _MalformedResultClient:
            def query(self, q: str, parameters: dict | None = None) -> _DumpResult:
                return _DumpResult([])  # no rows at all -- malformed

        with pytest.raises(SnapshotError, match="malformed"):
            await _assert_no_ttl_horizon_rows(
                _MalformedResultClient(), ["feature_flag_event"]
            )


class TestContentHashManifestShape:
    """The hashes are stored as a LIST of objects, not a `{table: hash}` map.

    Not cosmetic: as a map, `json.dumps(indent=2)` puts
    `"llm_token_usage": "<64 hex chars>"` on one line, which gitleaks'
    single-line `generic-api-key` rule matches -- a key name containing
    "token" next to a high-entropy value. A real scan of the committed
    artifact failed on exactly that. The list shape splits name and digest
    across lines, so no `.gitleaksignore` entry is needed.
    """

    def test_round_trips(self) -> None:
        hashes = {"teams": "a" * 64, "llm_token_usage": "b" * 64}
        assert (
            content_hashes_from_manifest(content_hashes_to_manifest(hashes)) == hashes
        )

    def test_is_sorted_for_a_stable_diff(self) -> None:
        entries = content_hashes_to_manifest({"z": "0" * 64, "a": "1" * 64})
        assert [entry["table"] for entry in entries] == ["a", "z"]

    def test_no_rendered_line_holds_both_a_table_name_and_its_digest(self) -> None:
        """The property that actually keeps the scanner quiet, asserted on the
        rendered JSON rather than on the data structure."""
        rendered = json.dumps(
            content_hashes_to_manifest({"llm_token_usage": "a7e4" + "b" * 60}),
            indent=2,
        )
        for line in rendered.splitlines():
            assert not ("llm_token_usage" in line and "a7e4" in line), line


class TestSchemaFingerprintPreflight:
    """Codex adversarial review (MEDIUM, confirmed): without this, the first
    sign of a migration-head bump or a ClickHouse image change was a raw
    insert failing PART WAY THROUGH the restore -- partial state, and an error
    naming a column rather than the real cause."""

    _LIVE = {"alembic_heads": ["0021", "river_0003"]}

    def test_matching_fingerprint_passes(self) -> None:
        _assert_schema_compatible(
            store="postgres", minted=dict(self._LIVE), live=dict(self._LIVE)
        )

    def test_a_different_migration_head_is_refused(self) -> None:
        with pytest.raises(RestoreRefusedError, match="schema fingerprint mismatch"):
            _assert_schema_compatible(
                store="postgres",
                minted={"alembic_heads": ["0021", "river_0003"]},
                live={"alembic_heads": ["0022", "river_0003"]},
            )

    def test_a_different_clickhouse_server_version_is_refused(self) -> None:
        """`Native` is a server-version-coupled binary format, so an image bump
        must fail loudly rather than be discovered by a corrupt restore."""
        with pytest.raises(RestoreRefusedError, match="schema fingerprint mismatch"):
            _assert_schema_compatible(
                store="clickhouse",
                minted={"migrations": ["001.sql"], "server_version": "24.8.1.1"},
                live={"migrations": ["001.sql"], "server_version": "25.1.0.1"},
            )

    def test_a_snapshot_with_no_fingerprint_is_refused_not_trusted(self) -> None:
        """Absent evidence must not read as compatibility. A snapshot minted
        before this field existed cannot be checked, so it is refused."""
        absent_values: list[dict[str, object] | None] = [None, {}]
        for absent in absent_values:
            with pytest.raises(RestoreRefusedError, match="records no .* schema"):
                _assert_schema_compatible(
                    store="clickhouse", minted=absent, live={"migrations": []}
                )


class TestCatalogFingerprintCatchesDdlDrift:
    """Codex adversarial review round 2 (MEDIUM, confirmed): comparing only
    migration version labels does not fingerprint a schema. Hand-run DDL, an
    edited migration under an unchanged version, a column type change or an
    engine/ORDER BY change all pass a version-only check while making the
    snapshot's bytes no longer match the target."""

    def test_same_versions_but_drifted_catalog_is_refused(self) -> None:
        with pytest.raises(RestoreRefusedError, match="schema fingerprint mismatch"):
            _assert_schema_compatible(
                store="postgres",
                minted={"alembic_heads": ["0086"], "catalog_sha256": "a" * 64},
                live={"alembic_heads": ["0086"], "catalog_sha256": "b" * 64},
            )

    def test_identical_catalog_passes(self) -> None:
        fingerprint = {"alembic_heads": ["0086"], "catalog_sha256": "a" * 64}
        _assert_schema_compatible(
            store="postgres", minted=dict(fingerprint), live=dict(fingerprint)
        )


class TestRefusalHappensBeforeAnyWrite:
    """Rider 2 (team-lead, binding): the empty-target predicate is the
    LOAD-BEARING safety check -- `ENVIRONMENT=acceptance` is belt only, since
    any shell can set it. So the proof that matters is not "it exited
    non-zero" but "it wrote nothing".

    `restore_world` is driven here with fakes that RAISE if any write path is
    reached, so a refusal that had already written something fails this test
    rather than passing on its exit code.
    """

    def _snapshot(self, tmp_path: Path) -> Path:
        import hashlib

        (tmp_path / "clickhouse").mkdir()
        (tmp_path / "postgres").mkdir()
        blob = tmp_path / "clickhouse" / "git_commits.native.gz"
        blob.write_bytes(gzip.compress(b"payload", mtime=0))
        (tmp_path / "manifest.json").write_text(
            json.dumps(
                {
                    "schema_version": SNAPSHOT_SCHEMA_VERSION,
                    "world_digest": "d" * 64,
                    "clickhouse": {
                        "tables": {
                            "git_commits": {
                                "file": "clickhouse/git_commits.native.gz",
                                "row_count": 1,
                                "sha256": hashlib.sha256(blob.read_bytes()).hexdigest(),
                            }
                        },
                        "schema_fingerprint": {"migrations": ["001"], "s": "v"},
                        "source_content_hashes": [],
                        "source_row_counts": {},
                        "baseline_row_counts": {},
                    },
                    "postgres": {
                        "tables": {},
                        "schema_fingerprint": {"alembic_heads": ["0086"]},
                        "reference_tables": {},
                        "source_row_counts": {},
                        "baseline_row_counts": {},
                    },
                }
            )
        )
        return tmp_path

    @pytest.mark.asyncio
    async def test_non_empty_target_refuses_without_writing(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        from dev_health_ops.fixtures import world_snapshot

        writes: list[str] = []

        async def _fake_ch(sink, handler):
            # Row counts are the only ClickHouse read the precondition needs;
            # anything else on this path would be a write.
            name = getattr(handler, "__name__", str(handler))
            if name == "_clickhouse_row_counts":
                return {"git_commits": 41}
            if name == "_clickhouse_schema_fingerprint":
                return {"migrations": ["001"], "s": "v"}
            writes.append(name)
            return {}

        async def _fake_pg(uri, handler):
            name = getattr(handler, "__name__", str(handler))
            if name == "_postgres_row_counts":
                return {}
            if name == "_postgres_schema_fingerprint":
                return {"alembic_heads": ["0086"]}
            writes.append(name)
            return {}

        monkeypatch.setattr(world_snapshot, "_with_clickhouse_client", _fake_ch)
        monkeypatch.setattr(world_snapshot, "_with_postgres_conn", _fake_pg)
        monkeypatch.setattr(
            world_snapshot, "_require_matching_world_manifest", lambda *a, **k: None
        )

        manifest = object()
        with pytest.raises(RestoreRefusedError, match="not a freshly-migrated"):
            await world_snapshot.restore_world(
                sink="clickhouse://x/default",
                postgres_uri="postgresql+asyncpg://x/postgres",
                snapshot_dir=self._snapshot(tmp_path),
                manifest=manifest,  # type: ignore[arg-type]
                env={"ENVIRONMENT": "acceptance"},
            )

        assert writes == [], (
            "the restore reached a write path before refusing a non-empty "
            f"target: {writes}. The refusal must happen first -- this predicate "
            "is what stops a real database being written to."
        )


class TestLossyMintTouchesNothing:
    """Codex adversarial review round 3 (HIGH, confirmed): the lossless-round-
    trip guard originally compared AFTER `write_digest`, so a lossy round trip
    raised only once it had already overwritten WORLD_DIGEST -- leaving the pin
    and the artifact inconsistent, while the surrounding shell stopped before
    copying the new snapshot. The comment beside it claimed the opposite.

    Asserts the FILES are untouched, not merely that it raised.
    """

    @pytest.mark.asyncio
    async def test_a_lossy_round_trip_leaves_pin_and_manifest_untouched(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        import hashlib

        from dev_health_ops.fixtures import world_snapshot

        snapshot = tmp_path / "snapshot"
        (snapshot / "clickhouse").mkdir(parents=True)
        (snapshot / "postgres").mkdir()
        blob = snapshot / "clickhouse" / "git_commits.native.gz"
        blob.write_bytes(gzip.compress(b"payload", mtime=0))
        manifest_doc = {
            "schema_version": SNAPSHOT_SCHEMA_VERSION,
            "world_schema_version": "ask_dev_world.v1",
            "master_seed": 3219000,
            "world_manifest_contract": "c" * 64,
            "clickhouse": {
                "tables": {
                    "git_commits": {
                        "file": "clickhouse/git_commits.native.gz",
                        "row_count": 1,
                        "sha256": hashlib.sha256(blob.read_bytes()).hexdigest(),
                    }
                },
                "schema_fingerprint": {"migrations": ["001"]},
                "source_content_hashes": [],
                "source_row_counts": {},
                "baseline_row_counts": {},
            },
            "postgres": {
                "tables": {},
                "schema_fingerprint": {"alembic_heads": ["0086"]},
                "reference_tables": {},
                "source_row_counts": {},
                "baseline_row_counts": {},
            },
        }
        manifest_path = snapshot / "manifest.json"
        manifest_path.write_text(json.dumps(manifest_doc))
        pin = tmp_path / "WORLD_DIGEST"
        pin.write_text('{"digest": "pre-existing pin, must survive"}')

        manifest_before = manifest_path.read_bytes()
        pin_before = pin.read_bytes()

        async def _fake_ch(sink, handler):
            name = getattr(handler, "__name__", str(handler))
            if name == "_clickhouse_row_counts":
                return {}
            if name == "_clickhouse_schema_fingerprint":
                return {"migrations": ["001"]}
            if name == "_clickhouse_content_hashes":
                return {}
            return {}

        async def _fake_pg(uri, handler):
            name = getattr(handler, "__name__", str(handler))
            if name == "_postgres_schema_fingerprint":
                return {"alembic_heads": ["0086"]}
            return {}

        async def _fake_digest(*args, **kwargs):
            return {"digest": "b" * 64, "components": {}}

        monkeypatch.setattr(world_snapshot, "_with_clickhouse_client", _fake_ch)
        monkeypatch.setattr(world_snapshot, "_with_postgres_conn", _fake_pg)
        monkeypatch.setattr(world_snapshot, "compute_world_digest", _fake_digest)
        monkeypatch.setattr(
            world_snapshot, "_require_matching_world_manifest", lambda *a, **k: None
        )

        with pytest.raises(world_snapshot.RestoreDriftError, match="LOSSY"):
            await world_snapshot.restore_world(
                sink="clickhouse://x/default",
                postgres_uri="postgresql+asyncpg://x/postgres",
                snapshot_dir=snapshot,
                manifest=object(),  # type: ignore[arg-type]
                digest_path=pin,
                mint_digest=True,
                # What `fixtures world` measured -- deliberately different from
                # what the restored database hashes to.
                generated_digest="a" * 64,
                env={"ENVIRONMENT": "acceptance"},
            )

        assert pin.read_bytes() == pin_before, "a lossy mint overwrote WORLD_DIGEST"
        assert manifest_path.read_bytes() == manifest_before, (
            "a lossy mint stamped the snapshot manifest"
        )
