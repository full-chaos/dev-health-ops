"""CHAOS-8268: derive the sync_manual_trigger_await_* metric surface from the Python PRODUCTION source, with ast (stdlib only).

The producer modules cannot run alone (prometheus_client, SQLAlchemy and the project models are not in the live-oracle closure), so
the surface is read from their syntax trees, not executed: that is the named limit. Nothing here is hand-written JSON: every value
below is taken from the source text of src/dev_health_ops/metrics/prometheus.py and src/dev_health_ops/sync/execution_trigger.py.
Usage: python3 await_python_source.py <repo root>   -> one JSON object on stdout.
"""

import ast
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
SYMBOLS = {
    "SYNC_MANUAL_TRIGGER_AWAIT_OUTCOME_TOTAL",
    "SYNC_MANUAL_TRIGGER_AWAIT_LATENCY_SECONDS",
}


def literal(node):
    return ast.literal_eval(node)


families = {}
tree = ast.parse((root / "src/dev_health_ops/metrics/prometheus.py").read_text())
for node in ast.walk(tree):
    if (
        isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance(node.targets[0], ast.Name)
        and node.targets[0].id in SYMBOLS
    ):
        call = node.value
        assert isinstance(call, ast.Call), node.targets[0].id
        if not isinstance(call.func, ast.Attribute):
            continue  # the no-op fallback (_noop_counter()) when prometheus_client is absent
        kind = call.func.attr
        entry = {
            "kind": kind,
            "name": literal(call.args[0]),
            "labels": literal(call.args[2]),
        }
        for keyword in call.keywords:
            if keyword.arg == "buckets":
                entry["buckets"] = list(literal(keyword.value))
        families[node.targets[0].id] = entry
assert set(families) == SYMBOLS, sorted(families)

outcomes = set()
used = set()
tree = ast.parse((root / "src/dev_health_ops/sync/execution_trigger.py").read_text())
for node in ast.walk(tree):
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_record"
        and node.args
    ):
        outcomes.add(literal(node.args[0]))
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "labels"
    ):
        # A POSITIONAL label value (`.labels("late")`) is the same write as the keyword form: a constant joins the outcomes, anything else must be
        # the parameter name `outcome`.
        for arg in node.args:
            if isinstance(arg, ast.Constant):
                outcomes.add(arg.value)
            else:
                assert isinstance(arg, ast.Name) and arg.id == "outcome", ast.dump(arg)
        for keyword in node.keywords:
            used.add(keyword.arg)
            if keyword.arg == "outcome":
                if isinstance(keyword.value, ast.Constant):
                    outcomes.add(
                        keyword.value.value
                    )  # a value written with no _record call
                else:
                    assert (
                        isinstance(keyword.value, ast.Name)
                        and keyword.value.id == "outcome"
                    ), ast.dump(keyword.value)
print(
    json.dumps(
        {
            "families": families,
            "record_outcomes": sorted(outcomes),
            "label_keywords_used": sorted(used),
        },
        sort_keys=True,
    )
)
