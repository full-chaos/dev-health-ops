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
        with pytest.raises(AssertionError, match="replay refuses sink."):
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


class TestRecordingSinkHoldsNoWritableObject:
    """The barrier must be structural, not a list of names we remembered to refuse.

    Adversarial review round 2: ``__getattr__`` only fires for attributes that do
    NOT exist, so holding the real sink as ``_inner`` left it reachable — a
    producer path ``self.sink._inner.ensure_schema()`` would have reached the real
    ClickHouseMetricsSink, which runs migrations and ``client.command`` writes.
    The sink now holds one bound read callable and nothing else.
    """

    def test_no_attribute_exposes_a_sink_like_object(self, generator):
        calls: list[tuple[str, dict]] = []

        def read(statement: str, params: dict) -> list[dict]:
            calls.append((statement, params))
            return [{"edge_id": "a"}]

        sink = generator.RecordingSink(read)

        # `client` is the recording stub, which exposes `command` on purpose --
        # that is how the cleanup mutations reach the golden instead of the
        # database, and it is proven connectionless by
        # TestRecordedMutationsAreNotExecuted. Every OTHER attribute must be
        # inert. The property is that nothing here can reach ClickHouse, not that
        # nothing here has a method with an alarming name.
        assert isinstance(sink.client, generator.RecordingClient)
        for name, value in vars(sink).items():
            if name == "client":
                continue
            # Sink-shaped names only. Generic ones like "insert" or "query" match
            # ordinary builtins (a list has .insert), which would make this assert
            # something about Python's data model instead of about the barrier.
            for reachable in (
                "ensure_schema",
                "client",
                "_client",
                "write_work_graph_edges",
                "write_work_graph_projection_runs",
            ):
                assert not hasattr(value, reachable), (
                    f"RecordingSink.{name} exposes {reachable}; a producer path through "
                    "it could reach the real sink and write to the shared stack"
                )
        # ...and the one thing it does hold still works.
        assert sink.query_dicts(SCOPED_READ, {}) == [{"edge_id": "a"}]
        assert calls == [(SCOPED_READ, {})]

    def test_an_undefined_attribute_is_still_refused(self, generator):
        sink = generator.RecordingSink(lambda statement, params: [])
        with pytest.raises(AssertionError, match="generator refuses sink."):
            sink.ensure_schema()


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
