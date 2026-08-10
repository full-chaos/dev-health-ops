"""Live Jira producer-batch oracle (CHAOS-3704).

The row oracle beside this pair proves the Jira normalizer in isolation. This
pair crosses the next boundary: it executes the shipped
``fetch_jira_work_items_with_extras`` function with a recording Jira client and
returns every list that producer emits. The client is only a deterministic
transport fixture; normalization, JQL construction, optional-child handling,
dependency extraction, reopen detection, comment conversion, and sprint cache
behavior are all production code.

The producer's return value is a tuple rather than a named dataclass. The
top-level field names are therefore derived from the two tuple-return AST
nodes in the production function, and the pair fails if those return shapes
drift apart. This keeps the six-list boundary explicit without maintaining a
second hand-written field manifest that could silently omit a new return.
"""

from __future__ import annotations

import ast
import contextlib
import dataclasses
import io
import os
import pathlib
import sys
from datetime import datetime, timezone
from typing import Any

from internal.providersync.testdata import oracle_registry

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
SRC_ROOT = REPO_ROOT / "src"
PRODUCER_SOURCE = REPO_ROOT / "src/dev_health_ops/metrics/work_items.py"
NORMALIZE_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/jira/normalize.py"
CLIENT_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/jira/client.py"

# The pair runs in its own process, but source provenance still matters: an
# editable install or an ambient PYTHONPATH must not silently provide another
# worktree's producer.
sys.path.insert(0, str(SRC_ROOT))


def _source_path(value: Any) -> pathlib.Path:
    return pathlib.Path(str(value)).resolve()


with (
    contextlib.redirect_stdout(io.StringIO()),
    contextlib.redirect_stderr(io.StringIO()),
):
    from dev_health_ops.metrics import work_items as _producer
    from dev_health_ops.models.work_items import Sprint
    from dev_health_ops.providers.identity import load_identity_resolver
    from dev_health_ops.providers.jira.client import build_jira_jql
    from dev_health_ops.providers.jira.normalize import (
        detect_reopen_events,
        extract_jira_issue_dependencies,
        jira_comment_to_interaction_event,
        jira_issue_to_work_item,
        jira_sprint_payload_to_model,
    )
    from dev_health_ops.providers.status_mapping import load_status_mapping

_producer_path = _source_path(_producer.__file__)
if _producer_path != PRODUCER_SOURCE.resolve():
    raise RuntimeError(
        f"Jira producer resolved to {_producer_path}, expected "
        f"{PRODUCER_SOURCE.resolve()}"
    )
if (
    _source_path(jira_issue_to_work_item.__code__.co_filename)
    != NORMALIZE_SOURCE.resolve()
):
    raise RuntimeError("Jira normalizer did not come from this checkout")
if _source_path(build_jira_jql.__code__.co_filename) != CLIENT_SOURCE.resolve():
    raise RuntimeError("Jira JQL builder did not come from this checkout")

_STATUS_MAPPING_PATH = REPO_ROOT / "src/dev_health_ops/config/status_mapping.yaml"
_IDENTITY_MAPPING_PATH = REPO_ROOT / "src/dev_health_ops/config/identity_mapping.yaml"
_NORMALIZED_AT = datetime(2026, 8, 10, 12, 0, 0, 123456, tzinfo=timezone.utc)


def _producer_return_fields() -> tuple[str, ...]:
    """Derive and pin the six named lists from the production return tuples."""
    tree = ast.parse(PRODUCER_SOURCE.read_text(encoding="utf-8"))
    function = next(
        (
            node
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == "fetch_jira_work_items_with_extras"
        ),
        None,
    )
    if function is None:
        raise RuntimeError("Jira producer function disappeared from production source")
    returns: list[tuple[str, ...]] = []
    for node in ast.walk(function):
        if not isinstance(node, ast.Return) or not isinstance(node.value, ast.Tuple):
            continue
        names = tuple(
            element.id for element in node.value.elts if isinstance(element, ast.Name)
        )
        if len(names) == len(node.value.elts):
            returns.append(names)
    if not returns or any(len(candidate) != 6 for candidate in returns):
        raise RuntimeError(f"Jira producer return tuples drifted: {returns!r}")
    # The provider shortcut names its tuple elements ``batch_*`` while the
    # legacy client path names them after the final output lists. Both are the
    # same six-position public return contract; use the tuple whose names are
    # the canonical final output lists and assert every path still returns six
    # lists. AST walk order is deliberately not used as a control-flow claim.
    final_names = next(
        (candidate for candidate in returns if candidate[0] == "work_items"),
        (),
    )
    if not final_names:
        raise RuntimeError(f"Jira producer has no canonical return tuple: {returns!r}")
    if len(set(final_names)) != 6:
        raise RuntimeError(
            f"Jira producer batch fields are not unique: {final_names!r}"
        )
    return final_names


_BATCH_FIELDS = _producer_return_fields()


class _FakeJiraClient:
    """Transport-only client with the protocol used by the real producer."""

    def __init__(self, case: dict[str, Any]) -> None:
        self.issues = list(case["issues"])
        self.comments = {
            str(key): list(value) for key, value in (case.get("comments") or {}).items()
        }
        self.sprints = {
            str(key): value for key, value in (case.get("sprints") or {}).items()
        }
        self.jqls: list[str] = []
        self.sprint_requests: list[str] = []
        self.closed = False

    def iter_issues(self, *, jql: str, **_: Any):
        self.jqls.append(jql)
        yield from self.issues

    def iter_issue_comments(self, *, issue_id_or_key: str, **_: Any):
        yield from self.comments.get(str(issue_id_or_key), [])

    def get_sprint(self, *, sprint_id: str) -> dict[str, Any]:
        self.sprint_requests.append(str(sprint_id))
        if str(sprint_id) not in self.sprints:
            raise RuntimeError(f"unexpected sprint fetch {sprint_id}")
        return self.sprints[str(sprint_id)]

    def close(self) -> None:
        self.closed = True


def _deps():
    """Use the production normalizers while injecting only fake transport."""
    from types import SimpleNamespace

    return SimpleNamespace(
        jira_build_jql=build_jira_jql,
        jira_issue_to_work_item=jira_issue_to_work_item,
        jira_extract_dependencies=extract_jira_issue_dependencies,
        jira_detect_reopen_events=detect_reopen_events,
        jira_comment_to_interaction=jira_comment_to_interaction_event,
        jira_sprint_to_model=jira_sprint_payload_to_model,
    )


def _reference_sprints(case: dict[str, Any], org_id: str) -> list[Sprint]:
    rows: list[Sprint] = []
    for payload in case.get("reference_sprints") or []:
        sprint = jira_sprint_payload_to_model(payload)
        if sprint is None:
            raise ValueError(f"reference sprint did not normalize: {payload!r}")
        rows.append(
            dataclasses.replace(
                sprint,
                org_id=org_id,
                last_synced=_NORMALIZED_AT,
            )
        )
    return rows


def _deterministic_row(value: Any, org_id: str) -> dict[str, Any]:
    """Stamp the same unit boundary Go carries in its effect rows."""
    if dataclasses.is_dataclass(value):
        updates: dict[str, Any] = {}
        names = {field.name for field in dataclasses.fields(value)}
        if "org_id" in names:
            updates["org_id"] = org_id
        if "last_synced" in names:
            updates["last_synced"] = _NORMALIZED_AT
        value = dataclasses.replace(value, **updates)
        return dataclasses.asdict(value)
    raise TypeError(f"Jira producer returned non-dataclass row {type(value)!r}")


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    org_id = str(case["org_id"])
    status_mapping = load_status_mapping(path=_STATUS_MAPPING_PATH)
    identity = load_identity_resolver(path=_IDENTITY_MAPPING_PATH)
    client = _FakeJiraClient(case)

    # The production function reads these two options from the environment.
    # Set them only inside this subprocess so the fake transport exercises the
    # shipped comments-limit and optional-enrichment branches exactly as a
    # sync invocation would.
    env_names = (
        "JIRA_FETCH_COMMENTS",
        "JIRA_COMMENTS_LIMIT",
        "JIRA_STORY_POINTS_FIELD",
        "JIRA_SPRINT_FIELD",
        "JIRA_EPIC_LINK_FIELD",
    )
    old_env = {name: os.environ.get(name) for name in env_names}
    try:
        os.environ["JIRA_FETCH_COMMENTS"] = (
            "1" if case.get("fetch_comments", True) else "0"
        )
        os.environ["JIRA_COMMENTS_LIMIT"] = str(case.get("comments_limit", 0))
        os.environ["JIRA_STORY_POINTS_FIELD"] = "customfield_10016"
        os.environ["JIRA_SPRINT_FIELD"] = "customfield_10020"
        os.environ["JIRA_EPIC_LINK_FIELD"] = "customfield_10014"
        _producer.get_metrics_dependencies = lambda: _deps()
        with (
            contextlib.redirect_stdout(io.StringIO()),
            contextlib.redirect_stderr(io.StringIO()),
        ):
            batch = _producer.fetch_jira_work_items_with_extras(
                since=datetime.fromisoformat(str(case["since"]).replace("Z", "+00:00")),
                until=datetime.fromisoformat(str(case["until"]).replace("Z", "+00:00"))
                if case.get("until")
                else None,
                status_mapping=status_mapping,
                identity=identity,
                project_keys=[str(key) for key in case.get("project_keys") or []]
                or None,
                client=client,
                fetch_all=bool(case.get("fetch_all", False)),
                use_env_query_options=False,
                reference_sprints=_reference_sprints(case, org_id),
            )
    finally:
        for name, value in old_env.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    if not client.closed:
        raise RuntimeError("Jira producer did not close its client")
    if not any(client.jqls):
        raise RuntimeError("Jira producer did not issue a JQL query")
    if not batch[0] or not batch[1]:
        raise RuntimeError(
            "Jira producer oracle case must produce non-empty work and history"
        )

    return {
        field: [_deterministic_row(row, org_id) for row in values]
        for field, values in zip(_BATCH_FIELDS, batch, strict=True)
    }


def _reflected_fields() -> frozenset[str]:
    return frozenset(_BATCH_FIELDS)


oracle_registry.register(
    oracle_registry.PairSpec(
        id="jira/work-items/batch",
        build_row=_build_row,
        reflected_fields=_reflected_fields,
        excluded_fields={},
    )
)
