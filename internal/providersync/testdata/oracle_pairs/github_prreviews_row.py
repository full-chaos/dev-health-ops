"""Live Python producer oracle for GitHub pull-request review rows."""

from __future__ import annotations

import pathlib
from types import SimpleNamespace
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import class_annotated_field_names
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_PROCESSOR_SOURCE = REPO_ROOT / "src/dev_health_ops/processors/github.py"
_MODEL_SOURCE = REPO_ROOT / "src/dev_health_ops/models/git.py"
_FETCH_UTILS_SOURCE = REPO_ROOT / "src/dev_health_ops/processors/fetch_utils.py"


class _Row:
    def __init__(self, **values: Any) -> None:
        self.__dict__.update(values)


class _ReviewClient:
    def __init__(self, review: SimpleNamespace, number: int) -> None:
        self._review = review
        self._number = number

    def iter_pr_reviews_batch(self, **_kwargs: Any):
        yield self._number, (self._review,)


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    github = load_live_module(_PROCESSOR_SOURCE)
    fetch_utils = load_live_module(_FETCH_UTILS_SOURCE)
    review = SimpleNamespace(**case["review"])
    number = int(case["number"])
    client = _ReviewClient(review, number)
    github.GitPullRequestReview = _Row
    github._coerce_datetime = fetch_utils.safe_parse_datetime
    github._github_work_client_from_connector = lambda *_args, **_kwargs: client
    github.drain_provider_usage = lambda _client: []
    pr = SimpleNamespace(number=number, created_at=case["created_at"])
    rows = github._enrich_prs_with_reviews_batch(
        connector=object(),
        owner="octo",
        repo_name="widgets",
        repo_id=case["repo_id"],
        pr_objects=[pr],
        raw_gh_prs=[pr],
        ingestion_sink=object(),
        loop=object(),
        gate=object(),
        usage_sink=[],
    )
    if len(rows) != 1:
        raise RuntimeError(f"expected one non-empty review row, got {len(rows)}")
    return vars(rows[0])


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/prreviews/row",
        build_row=_build_row,
        reflected_fields=lambda: class_annotated_field_names(
            _MODEL_SOURCE.read_text(), "GitPullRequestReview"
        ),
        excluded_fields={
            "last_synced": "SQLAlchemy default is materialized by the sink, not the producer",
        },
    )
)
