"""Guards for the CHAOS-4766 edge-golden generator's read barrier and replay contract.

The generator is the only thing standing between a shared, live ClickHouse and a
script that drives the deployed producer, and its ``--replay`` mode is the whole
substance of the rot guard. Both properties are NEGATIVE ones -- "it refuses X" --
and a negative property with no test is indistinguishable from an absent one.

Adversarial review round 1 found both gaps in exactly that shape:

* ``ReplaySink.query_dicts`` served frozen rows by ordinal and IGNORED the
  statement, so a producer that had dropped its org predicate would be handed the
  old org-scoped rows and the guard would go green on a cross-org read; and
* ``__getattr__`` refused every attribute except the allowed methods, but
  ``query_dicts`` is an allowed method that forwards arbitrary SQL to the real
  client -- so the "read-only by construction" claim did not hold.

These tests are pure: they import the generator module and exercise its sinks
directly. Nothing here touches ClickHouse, so they run in the ordinary unit suite
rather than needing the stack.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[2]
GENERATOR = (
    ROOT / "tests" / "fixtures" / "generate_workgraph_issue_edges_python_golden.py"
)

ORG_ID = "70d529e0-3c06-4597-8480-794fd02328b6"
SCOPED_READ = (
    "SELECT source_work_item_id, target_work_item_id, relationship_type, "
    "relationship_type_raw, relationship_semantics_version, last_synced "
    f"FROM work_item_dependencies WHERE 1=1 AND org_id = '{ORG_ID}'"
)
# The same read with its tenant predicate removed. In production this is not a
# cosmetic difference: the dependency read would go cross-org while the builder
# still stamps every derived edge with the requested org.
UNSCOPED_READ = SCOPED_READ.replace(f" WHERE 1=1 AND org_id = '{ORG_ID}'", "")


@pytest.fixture(scope="module")
def generator() -> ModuleType:
    assert GENERATOR.is_file(), f"golden generator is missing at {GENERATOR}"
    spec = importlib.util.spec_from_file_location("workgraph_edges_golden", GENERATOR)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class TestReadBarrier:
    """``query_dicts`` forwards to a live client, so the gate must be on the SQL."""

    @pytest.mark.parametrize(
        "statement",
        [
            "ALTER TABLE work_graph_edges DELETE WHERE org_id = 'x'",
            "INSERT INTO work_graph_edges VALUES",
            "TRUNCATE TABLE work_graph_edges",
            "DROP TABLE work_graph_edges",
            "OPTIMIZE TABLE work_graph_edges FINAL",
            "  \n  alter table work_graph_edges delete where 1=1",
        ],
    )
    def test_refuses_a_statement_that_is_not_a_read(self, generator, statement):
        with pytest.raises(AssertionError, match="non-read"):
            generator._refuse_non_read(statement)

    @pytest.mark.parametrize(
        "statement",
        [
            SCOPED_READ,
            "WITH x AS (SELECT 1) SELECT * FROM x",
            "  \n  select edge_id from work_graph_edges FINAL",
            "(SELECT 1)",
        ],
    )
    def test_allows_a_genuine_read(self, generator, statement):
        generator._refuse_non_read(statement)

    def test_refusing_names_the_statement_without_executing_it(self, generator):
        with pytest.raises(AssertionError) as raised:
            generator._refuse_non_read("ALTER TABLE work_graph_edges DELETE WHERE 1=1")
        # The operator needs to see WHICH statement was refused; a bare
        # "refused" would send them reading the producer to find out.
        assert "ALTER TABLE work_graph_edges DELETE" in str(raised.value)


class TestReplayQueryContract:
    """The replay must answer the question the producer actually asked."""

    @staticmethod
    def _sink(generator, queries):
        return generator.ReplaySink([[{"edge_id": "a"}] for _ in queries], queries)

    def test_rejects_a_read_whose_tenant_scope_was_dropped(self, generator):
        frozen = [generator._normalize_query(SCOPED_READ, {})]
        sink = self._sink(generator, frozen)
        with pytest.raises(AssertionError, match="read contract changed"):
            sink.query_dicts(UNSCOPED_READ, {})

    def test_rejects_a_read_that_gained_a_time_bound(self, generator):
        frozen = [generator._normalize_query(SCOPED_READ, {})]
        sink = self._sink(generator, frozen)
        with pytest.raises(AssertionError, match="read contract changed"):
            sink.query_dicts(SCOPED_READ + " AND last_synced >= {from:DateTime64}", {})

    def test_rejects_a_read_whose_parameters_changed(self, generator):
        frozen = [
            generator._normalize_query(SCOPED_READ, {"after": "", "org_id": ORG_ID})
        ]
        sink = self._sink(generator, frozen)
        with pytest.raises(AssertionError, match="read contract changed"):
            sink.query_dicts(SCOPED_READ, {"after": "zzz", "org_id": ORG_ID})

    def test_accepts_the_genuine_read_including_insignificant_whitespace(
        self, generator
    ):
        frozen = [generator._normalize_query(SCOPED_READ, {})]
        sink = self._sink(generator, frozen)
        # The frozen form is whitespace-collapsed, so the producer's own
        # triple-quoted SQL must still match; a guard that reported indentation
        # as drift would be turned off within a week.
        spaced = SCOPED_READ.replace(" ", "\n    ", 1)
        assert sink.query_dicts(spaced, {}) == [{"edge_id": "a"}]

    def test_refuses_more_reads_than_the_golden_froze(self, generator):
        frozen = [generator._normalize_query(SCOPED_READ, {})]
        sink = self._sink(generator, frozen)
        sink.query_dicts(SCOPED_READ, {})
        with pytest.raises(AssertionError, match="more reads than the golden froze"):
            sink.query_dicts(SCOPED_READ, {})

    def test_replay_refuses_every_other_sink_method(self, generator):
        sink = generator.ReplaySink([], [])
        with pytest.raises(AttributeError, match="replay refuses sink."):
            sink.ensure_schema()


class TestReplayConsumesEveryFrozenRead:
    """A dropped read is drift too, and it leaves no trace in the outputs.

    Adversarial review round 2: validating only the reads that HAPPEN lets a
    producer stop performing a terminal read and still go green, provided its
    observable rows are unchanged. The frozen sequence is a contract in both
    directions.
    """

    def test_unconsumed_reads_are_rejected(self, generator):
        frozen = [
            generator._normalize_query(SCOPED_READ, {}),
            generator._normalize_query(
                "SELECT edge_id FROM work_graph_edges FINAL", {}
            ),
        ]
        sink = generator.ReplaySink([[{"edge_id": "a"}], [{"edge_id": "b"}]], frozen)
        sink.query_dicts(SCOPED_READ, {})  # the producer stops here
        with pytest.raises(AssertionError, match="fewer read"):
            sink.assert_fully_consumed()

    def test_the_message_names_what_was_dropped(self, generator):
        frozen = [
            generator._normalize_query(SCOPED_READ, {}),
            generator._normalize_query(
                "SELECT edge_id FROM work_graph_edges FINAL", {}
            ),
        ]
        sink = generator.ReplaySink([[], []], frozen)
        sink.query_dicts(SCOPED_READ, {})
        with pytest.raises(AssertionError) as raised:
            sink.assert_fully_consumed()
        assert "work_graph_edges" in str(raised.value)

    def test_a_fully_consumed_replay_is_accepted(self, generator):
        frozen = [generator._normalize_query(SCOPED_READ, {})]
        sink = generator.ReplaySink([[{"edge_id": "a"}]], frozen)
        sink.query_dicts(SCOPED_READ, {})
        sink.assert_fully_consumed()


class TestTheReadBarrierIsServerSide:
    """The control is ``readonly=1`` on the connection, not the object graph.

    Three review rounds found the same class here, and the third one is why this
    changed shape. Hiding the sink is not achievable in Python: removing
    ``_inner`` still left ``self._read.__self__`` pointing at the live
    ``ClickHouseMetricsSink``, and closing that would leave ``__closure__``, then
    ``gc.get_referrers``. An object-graph barrier in this language is a promise.

    ``readonly=1`` is enforced by ClickHouse itself, so it holds no matter which
    object issues the statement. Verified against the running instance: a DELETE
    through the real sink's own client returns ``Code: 164 ... Cannot execute
    query in readonly mode``, while ordinary reads are unaffected.

    These tests are pure -- they intercept client construction rather than
    connecting -- so the assertion is that the generator ASKS for a read-only
    connection. The server's enforcement of that request is ClickHouse's job.
    """

    def test_the_connection_is_opened_read_only(self, generator, monkeypatch):
        captured: dict = {}

        def fake_get_client(**kwargs):
            captured.update(kwargs)
            return object()

        monkeypatch.setattr(generator.clickhouse_connect, "get_client", fake_get_client)
        sink = generator._read_only_sink(
            "clickhouse://user:pw@localhost:9000/devhealth"
        )

        assert captured.get("settings", {}).get("readonly") == 1, (
            "the golden generator opened a connection that is not read-only; the "
            "object-graph barrier is explicitly NOT relied upon, so this setting is "
            "the only thing preventing a write to the shared stack"
        )
        # The sink must actually be using the intercepted client, or the setting
        # was applied to a connection nobody uses.
        assert sink.client is not None

    def test_the_sql_gate_remains_as_the_near_side_check(self, generator):
        # Defence in depth: the server refusal is authoritative, but a local
        # refusal fails fast with a message naming the statement.
        with pytest.raises(AssertionError, match="non-read"):
            generator._refuse_non_read("ALTER TABLE work_graph_edges DELETE WHERE 1=1")

    def test_an_undefined_attribute_is_still_refused(self, generator):
        sink = generator.RecordingSink(lambda statement, params: [])
        with pytest.raises(AttributeError, match="generator refuses sink."):
            sink.ensure_schema()


class TestCapturedInputsAreSnapshots:
    """The frozen inputs must be frozen AT CAPTURE, not at serialisation time.

    Adversarial review round 5, P1: the sink recorded the same dict objects it
    handed the producer, and the golden is serialised only after the producer
    returns. So a producer that mutated a row in place rewrote the captured
    "input" as well as the output, both halves agreed, and the Go oracle -- which
    derives `evidence` FROM that input -- derived it from the corruption and
    accepted all 3,548 edges.

    This is the load-bearing property of the whole frozen-inputs pattern, not a
    detail of this generator: an oracle is only as trustworthy as the immutability
    of what it derives from. CHAOS-4803 carries the general form.
    """

    class ByReferenceSink:
        """The sink as it behaved BEFORE the fix: records what it hands over.

        Re-created standalone rather than subclassing the real one — it is the
        OLD behaviour, not a variant of the new, and writing it out makes the
        single differing line (no copy) visible instead of buried in an override.

        Standing this up beside the real sink is the point. A guard that only
        asserts "the recorded value survived" passes identically whether the
        snapshot works or whether nothing in the test ever exercised it: it
        checks the implementation of the fix rather than the failure the fix
        exists to prevent. Showing the old behaviour ACCEPTING the corruption is
        what makes the new behaviour's rejection mean something.
        """

        def __init__(self, rows: list[dict]) -> None:
            self._rows = rows
            self.reads: list[list[dict]] = []

        def query_dicts(self, query: str, params: dict) -> list[dict]:
            self.reads.append(self._rows)  # the defect: recorded by reference
            return self._rows

    def test_the_old_by_reference_sink_accepts_a_corruption_the_new_one_rejects(
        self, generator
    ):
        """CHAOS-4803: the demonstration, not just the mechanism.

        A stub producer mutates a row in place — the shape of a Python regression
        that corrupts `evidence` — and we derive the expected value from each
        sink's recorded input, exactly as the Go oracle does. The by-reference
        sink must derive the CORRUPTED value (and so accept the corrupted output);
        the deep-copying sink must derive the ORIGINAL and reject it.
        """

        def stub_producer(sink):
            """Reads, then rewrites the row in place before deriving evidence."""
            rows = sink.query_dicts(SCOPED_READ, {})
            for row in rows:
                row["relationship_type_raw"] = "regression:oracle-blind"
            # What the producer would emit, derived AFTER its own mutation.
            return [
                row["relationship_type_raw"] or row["relationship_type"] or "dependency"
                for row in rows
            ]

        def fresh_row():
            return {
                "source_work_item_id": "linear:A",
                "target_work_item_id": "linear:B",
                "relationship_type": "relates_to",
                "relationship_type_raw": "linear_relation:related",
            }

        # --- the old behaviour: the oracle is blinded ---
        old_rows = [fresh_row()]
        old_sink = self.ByReferenceSink(old_rows)
        old_emitted = stub_producer(old_sink)
        old_derived = [
            row["relationship_type_raw"] or row["relationship_type"] or "dependency"
            for row in old_sink.reads[0]
        ]
        assert old_emitted == old_derived, (
            "the by-reference sink was expected to record the corrupted value, so that "
            "deriving from it reproduces the corruption — if this fails the "
            "demonstration is no longer demonstrating anything"
        )
        assert old_derived == ["regression:oracle-blind"]

        # --- the current behaviour: the corruption is caught ---
        new_rows = [fresh_row()]
        new_sink = generator.RecordingSink(lambda statement, params: new_rows)
        new_emitted = stub_producer(new_sink)
        new_derived = [
            row["relationship_type_raw"] or row["relationship_type"] or "dependency"
            for row in new_sink.reads[0]
        ]
        assert new_derived == ["linear_relation:related"]
        assert new_emitted != new_derived, (
            "the deep-copying sink still derived the producer's corrupted value; the "
            "oracle would accept a regression"
        )

    def test_a_producer_mutation_does_not_reach_the_recorded_input(self, generator):
        row = {
            "source_work_item_id": "linear:A",
            "target_work_item_id": "linear:B",
            "relationship_type": "relates_to",
            "relationship_type_raw": "linear_relation:related",
            "relationship_semantics_version": "canonical-blocks.v2",
        }
        sink = generator.RecordingSink(lambda statement, params: [row])
        handed_over = sink.query_dicts(SCOPED_READ, {})

        # The producer keeps the originals, so its behaviour is unchanged...
        assert handed_over[0] is row
        # ...but the recording is a snapshot it cannot reach.
        assert sink.reads[0][0] is not row

        handed_over[0]["relationship_type_raw"] = "regression:oracle-blind"
        assert sink.reads[0][0]["relationship_type_raw"] == "linear_relation:related", (
            "a producer mutation reached the recorded input; the oracle would derive "
            "its expectation from the corruption and accept it"
        )

    def test_nested_values_are_snapshotted_too(self, generator):
        # A shallow copy would pass the test above and still alias anything nested.
        row = {"source_work_item_id": "linear:A", "labels": ["one"]}
        sink = generator.RecordingSink(lambda statement, params: [row])
        handed_over = sink.query_dicts(SCOPED_READ, {})
        handed_over[0]["labels"].append("two")
        assert sink.reads[0][0]["labels"] == ["one"], (
            "nested values are aliased; the snapshot is shallow"
        )

    def test_the_snapshot_survives_replacing_the_row_list(self, generator):
        rows = [{"source_work_item_id": "linear:A"}]
        sink = generator.RecordingSink(lambda statement, params: rows)
        sink.query_dicts(SCOPED_READ, {})
        rows.clear()
        assert len(sink.reads[0]) == 1


class TestRecordedMutationsAreNotExecuted:
    """The cleanup mutations belong in the golden, never in the database."""

    def test_command_records_and_returns_without_a_client(self, generator):
        client = generator.RecordingClient()
        client.command(
            "ALTER TABLE work_graph_edges DELETE WHERE org_id = {org_id:String}",
            parameters={"org_id": ORG_ID},
        )
        assert len(client.commands) == 1
        recorded = client.commands[0]
        assert recorded["statement"].startswith("ALTER TABLE work_graph_edges DELETE")
        assert recorded["parameters"] == {"org_id": ORG_ID}
        # There is no connection on this object at all -- that is the property,
        # not a policy it chooses to honour.
        assert not hasattr(client, "_client")
