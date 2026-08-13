"""Execute the production Python repository selector on a fixed raw-row case."""

from __future__ import annotations

import json

from dev_health_ops.metrics.job_daily import discover_repos

ORG_ID = "00000000-0000-4000-8000-000000000009"
ROWS = [
    ["00000000-0000-4000-8000-000000000001", "first", None, "github"],
    ["00000000-0000-4000-8000-000000000002", "second", "{}", "gitlab"],
]


class QueryResult:
    result_rows = ROWS


class Client:
    query_text = ""
    parameters: dict[str, str] = {}

    def query(self, query: str, *, parameters: dict[str, str]) -> QueryResult:
        self.query_text = query
        self.parameters = parameters
        return QueryResult()


class Sink:
    def __init__(self) -> None:
        self.client = Client()


sink = Sink()
repositories = discover_repos("clickhouse", sink, org_id=ORG_ID)
print(
    json.dumps(
        {
            "ids": [str(repository.repo_id) for repository in repositories],
            "query": sink.client.query_text,
            "parameters": sink.client.parameters,
        },
        sort_keys=True,
    )
)
