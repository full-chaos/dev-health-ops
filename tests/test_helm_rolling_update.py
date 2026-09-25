"""CHAOS-6647: the query-api and go-worker Deployments' rollingUpdate parameters are values-driven.

The chart used to hardcode {maxUnavailable: 0, maxSurge: 1} (never below serving capacity, one
surge pod). A node without CPU headroom for the surge pod (prod rev 175 and rev 183: clickhouse-0
Pending) needs {maxUnavailable: 1, maxSurge: 0}. Unset must keep today's behaviour exactly.
"""

from pathlib import Path
from subprocess import CalledProcessError, run

import pytest
import yaml

_CHART = Path(__file__).parents[1] / "deploy/helm/dev-health"
_DEFAULT = {"maxUnavailable": 0, "maxSurge": 1}
_TIGHT = {"maxUnavailable": 1, "maxSurge": 0}


def _render(*sets: str, json_sets: dict[str, str] | None = None) -> list[dict]:
    args = ["helm", "template", "t", str(_CHART), "--set", "queryApi.enabled=true"]
    for value in sets:
        args += ["--set", value]
    for key, value in (json_sets or {}).items():
        args += ["--set-json", f"{key}={value}"]
    out = run(args, check=True, capture_output=True, text=True).stdout
    return [d for d in yaml.safe_load_all(out) if d]


def _strategies(docs: list[dict]) -> dict[str, dict]:
    return {
        d["metadata"]["name"]: d["spec"]["strategy"]["rollingUpdate"]
        for d in docs
        if d["kind"] == "Deployment"
        and (
            d["metadata"]["name"].endswith("query-api")
            or "go-worker"
            in d["metadata"]["labels"].get("app.kubernetes.io/component", "")
        )
    }


def test_default_keeps_the_hardcoded_guarantee_everywhere() -> None:
    strategies = _strategies(_render())
    assert len(strategies) >= 5  # query-api + the go worker groups
    assert all(s == _DEFAULT for s in strategies.values()), strategies


def test_query_api_rolling_update_is_values_driven() -> None:
    strategies = _strategies(
        _render(
            json_sets={"queryApi.rollingUpdate": '{"maxUnavailable":1,"maxSurge":0}'}
        )
    )
    assert strategies["t-dev-health-query-api"] == _TIGHT
    assert all(
        s == _DEFAULT for n, s in strategies.items() if not n.endswith("query-api")
    )


def test_go_workers_default_and_group_override() -> None:
    docs = _render(
        json_sets={
            "goWorkers.rollingUpdate": '{"maxUnavailable":1,"maxSurge":0}',
        }
    )
    workers = {
        n: s for n, s in _strategies(docs).items() if not n.endswith("query-api")
    }
    assert workers and all(s == _TIGHT for s in workers.values()), workers


def test_group_level_wins_over_the_workers_default() -> None:
    values = yaml.safe_load(
        run(
            ["helm", "show", "values", str(_CHART)],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    )
    groups = values["goWorkers"]["groups"]
    groups[0]["rollingUpdate"] = {"maxUnavailable": 2, "maxSurge": 0}
    override = _CHART.parent.parent.parent / "tests" / "_rolling_update_values.tmp.yaml"
    override.write_text(
        yaml.safe_dump({"goWorkers": {"rollingUpdate": _TIGHT, "groups": groups}})
    )
    try:
        out = run(
            ["helm", "template", "t", str(_CHART), "-f", str(override)],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    finally:
        override.unlink(missing_ok=True)
    by_name = {
        d["metadata"]["name"]: d["spec"]["strategy"]["rollingUpdate"]
        for d in yaml.safe_load_all(out)
        if d
        and d["kind"] == "Deployment"
        and d["metadata"]["labels"].get("app.kubernetes.io/component") == "go-worker"
    }
    assert {"maxUnavailable": 2, "maxSurge": 0} in by_name.values()
    assert _TIGHT in by_name.values()  # the other groups take the workers default


def test_schema_rejects_an_unknown_rolling_update_key() -> None:
    with pytest.raises(CalledProcessError):
        run(
            [
                "helm",
                "template",
                "t",
                str(_CHART),
                "--set-json",
                'goWorkers.rollingUpdate={"nonsense":1}',
            ],
            check=True,
            capture_output=True,
            text=True,
        )
