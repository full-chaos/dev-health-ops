"""CHAOS-6938: the go-sync KEDA scaler sees planned-but-undispatched units.

The chart rendered ONE postgresql trigger per group, over `river.river_job`. A planned unit is a row in
`public.sync_run_units` until the dispatcher publishes it, so on prod 442 planned, due units read as
about 0-5 on that trigger and go-sync stayed at 1 replica for hours (rev 190 backlog measurement). An
optional `autoscaling.extraTriggers` list adds triggers to a group's ScaledObject (KEDA scales to the
max across triggers); the sync group carries the planned-units one. The KEDA login's SELECT on that
table is granted by `dho migrate roles` (the role half of the same ticket).
"""

import json
import re
from pathlib import Path
from subprocess import run

import yaml

_CHART = Path(__file__).parents[1] / "deploy/helm/dev-health"

# prod-ops' proposed query (ticket comment), and the one the roles e2e proof runs as the KEDA login.
PLANNED_QUERY = (
    "SELECT count(*) FROM public.sync_run_units WHERE status = 'planned' "
    "AND (available_at IS NULL OR available_at <= now())"
)


def _values_groups() -> list[dict]:
    return yaml.safe_load((_CHART / "values.yaml").read_text())["goWorkers"]["groups"]


def _template(groups: list[dict] | None = None):
    args = ["helm", "template", "t", str(_CHART)]
    if groups is not None:
        args += ["--set-json", f"goWorkers.groups={json.dumps(groups)}"]
    return run(args, capture_output=True, text=True)


def _scaled(groups: list[dict] | None = None) -> dict[str, dict]:
    result = _template(groups)
    assert result.returncode == 0, result.stderr
    docs = [d for d in yaml.safe_load_all(result.stdout) if d]
    return {
        d["spec"]["scaleTargetRef"]["name"].removeprefix("t-dev-health-go-"): d
        for d in docs
        if d["kind"] == "ScaledObject"
    }


def _sync(scaled: dict[str, dict]) -> dict:
    return scaled["sync"]


def test_the_sync_scaler_has_the_river_trigger_and_the_planned_units_trigger() -> None:
    triggers = _sync(_scaled())["spec"]["triggers"]
    assert len(triggers) == 2, [t["metadata"]["query"] for t in triggers]
    river, planned = triggers
    assert "river_job" in river["metadata"]["query"]
    assert planned["type"] == "postgresql"
    assert planned["name"] == "planned-units"
    assert " ".join(planned["metadata"]["query"].split()) == PLANNED_QUERY
    assert planned["metadata"]["targetQueryValue"] == "150"


def test_the_extra_trigger_connects_exactly_like_the_river_trigger() -> None:
    river, planned = _sync(_scaled())["spec"]["triggers"]
    for key in ("host", "port", "userName", "dbName", "sslmode"):
        assert planned["metadata"][key] == river["metadata"][key], key
    assert planned["authenticationRef"] == river["authenticationRef"]


def test_go_sync_may_scale_to_two_and_no_further() -> None:
    spec = _sync(_scaled())["spec"]
    assert (spec["minReplicaCount"], spec["maxReplicaCount"]) == (1, 2)


def test_other_autoscaled_groups_keep_their_single_river_trigger() -> None:
    for name, obj in _scaled().items():
        if name == "sync":
            continue
        triggers = obj["spec"]["triggers"]
        assert len(triggers) == 1, (name, len(triggers))
        assert "river_job" in triggers[0]["metadata"]["query"], name


def test_the_planned_query_reads_only_the_table_the_keda_login_may_select() -> None:
    """The role half grants SELECT on public.sync_run_units and river_job and nothing else."""
    _, planned = _sync(_scaled())["spec"]["triggers"]
    tables = set(re.findall(r"\b(?:FROM|JOIN)\s+([\w.]+)", planned["metadata"]["query"], re.I))
    assert tables == {"public.sync_run_units"}, tables


def test_an_empty_extra_triggers_list_renders_like_no_list() -> None:
    groups = _values_groups()
    for group in groups:
        if group["name"] == "sync":
            group["autoscaling"] = {**group["autoscaling"], "extraTriggers": []}
    assert len(_sync(_scaled(groups))["spec"]["triggers"]) == 1


def test_extra_triggers_on_a_group_that_does_not_autoscale_are_refused() -> None:
    groups = _values_groups()
    for group in groups:
        if group["name"] == "sync":
            group["autoscaling"] = {
                "enabled": False,
                "extraTriggers": [{"name": "x", "query": "SELECT 1", "targetQueryValue": 5}],
            }
    result = _template(groups)
    assert result.returncode != 0
    assert "extraTriggers" in result.stderr and "autoscaling.enabled" in result.stderr, result.stderr


def test_an_extra_trigger_without_a_query_or_target_is_refused() -> None:
    for bad in (
        {"name": "x", "targetQueryValue": 5},
        {"name": "x", "query": "SELECT 1"},
        {"name": "x", "query": "", "targetQueryValue": 5},
        {"name": "x", "query": "SELECT 1", "targetQueryValue": 0},
    ):
        groups = _values_groups()
        for group in groups:
            if group["name"] == "sync":
                group["autoscaling"] = {**group["autoscaling"], "extraTriggers": [bad]}
        result = _template(groups)
        assert result.returncode != 0, bad


def test_extra_trigger_names_are_unique_within_a_group() -> None:
    groups = _values_groups()
    for group in groups:
        if group["name"] == "sync":
            trigger = {"name": "dup", "query": "SELECT 1", "targetQueryValue": 5}
            group["autoscaling"] = {**group["autoscaling"], "extraTriggers": [trigger, trigger]}
    result = _template(groups)
    assert result.returncode != 0
    assert "dup" in result.stderr, result.stderr
