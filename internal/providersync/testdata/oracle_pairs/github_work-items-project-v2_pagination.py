"""Live Python Projects v2 outer/nested pagination oracle."""

from __future__ import annotations

import contextlib
import io
import pathlib
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import (
    RETURN_LITERAL,
    dict_literal_keys,
)
from internal.providersync.testdata.oracle_pairs._github_work_items_helpers import (
    install_minimal_oracle_imports,
)

install_minimal_oracle_imports(real_client=True)

with (
    contextlib.redirect_stdout(io.StringIO()),
    contextlib.redirect_stderr(io.StringIO()),
):
    from dev_health_ops.connectors.utils.rate_limit_queue import RateLimitGate
    from dev_health_ops.providers.github.client import (
        GitHubWorkClient,
        ProjectItemChanges,
    )

_THIS_FILE = pathlib.Path(__file__)


class _Harness:
    def __init__(self) -> None:
        self.outer_after: list[str] = []
        self.change_after: list[str] = []
        self.outer_pages = [
            {
                "organization": {
                    "projectV2": {
                        "items": {
                            "nodes": [
                                {
                                    "id": "PVTI_1",
                                    "content": {
                                        "__typename": "DraftIssue",
                                        "title": "one",
                                    },
                                    "fieldValues": {"nodes": []},
                                    "changes": {
                                        "nodes": [
                                            {"createdAt": "2026-08-01T08:00:00Z"}
                                        ],
                                        "pageInfo": {
                                            "hasNextPage": True,
                                            "endCursor": "change-1",
                                        },
                                    },
                                }
                            ],
                            "pageInfo": {"hasNextPage": True, "endCursor": "item-1"},
                        }
                    }
                }
            },
            {
                "organization": {
                    "projectV2": {
                        "items": {
                            "nodes": [
                                {
                                    "id": "PVTI_2",
                                    "content": {
                                        "__typename": "DraftIssue",
                                        "title": "two",
                                    },
                                    "fieldValues": {"nodes": []},
                                    "changes": {
                                        "nodes": [],
                                        "pageInfo": {
                                            "hasNextPage": False,
                                            "endCursor": None,
                                        },
                                    },
                                }
                            ],
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                        }
                    }
                }
            },
        ]

    def query(
        self, _operation: str, _query: str, variables: dict[str, Any]
    ) -> dict[str, Any]:
        after = variables.get("after")
        self.outer_after.append(after if isinstance(after, str) else "<none>")
        return self.outer_pages.pop(0)

    def changes(self, *, item_id: str, after: str | None = None) -> ProjectItemChanges:
        self.change_after.append(f"{item_id}:{after or '<none>'}")
        return {
            "nodes": [{"createdAt": "2026-08-02T08:00:00Z"}],
            "pageInfo": {"hasNextPage": False, "endCursor": None},
        }


class _OracleGitHubWorkClient(GitHubWorkClient):
    """Typed adapter that executes the production pagination loop."""

    def __init__(self, harness: _Harness) -> None:
        # The oracle exercises only iter_project_v2_items, whose concrete
        # dependencies are the gate and the two query methods below. Avoid
        # constructing live PyGithub/GraphQL transports while retaining the
        # production class and method dispatch mypy verifies.
        self.gate = RateLimitGate()
        self._harness = harness

    def _query_graphql(
        self,
        operation: str,
        query: str,
        *,
        variables: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._harness.query(operation, query, variables or {})

    def _fetch_item_changes(
        self,
        *,
        item_id: str,
        after: str | None = None,
    ) -> ProjectItemChanges | None:
        return self._harness.changes(item_id=item_id, after=after)


def _build_row(_case: dict[str, Any]) -> dict[str, Any]:
    harness = _Harness()
    client = _OracleGitHubWorkClient(harness)
    items = list(client.iter_project_v2_items(org_login="acme", project_number=3))
    return {
        "item_ids": [item["id"] for item in items],
        "change_counts": [
            len((item.get("changes") or {}).get("nodes") or []) for item in items
        ],
        "outer_after": harness.outer_after,
        "change_after": harness.change_after,
    }


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/work-items-project-v2/pagination",
        build_row=_build_row,
        reflected_fields=lambda: dict_literal_keys(
            _THIS_FILE.read_text(), "_build_row", (RETURN_LITERAL,)
        ),
    )
)
