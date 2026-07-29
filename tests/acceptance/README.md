# Ask Dev Compose acceptance

This overlay is the deterministic OpenAI-compatible provider boundary required
by the Ask Dev TRD. It does not add a product provider family. The Ops API still
resolves provider family `openai`, source `platform`, and fixed model
`ask-dev-scripted-v1` through its production provider and readiness paths.

Run the canonical launcher from the Ops checkout with the Web checkout it must
build:

```console
scripts/acceptance/run_ask_dev_compose.sh --web-root /path/to/dev-health-web
```

The launcher owns the whole acceptance lifecycle. It resets only its dedicated
`dev-health-ask-dev-acceptance` Compose project, builds and waits for PostgreSQL,
ClickHouse, Ops, and the internal scripted OpenAI service, generates the fixed
graph/metric/evidence fixture, seeds the canonical admin and organization,
enables only the Ask Dev and contextual-entrypoint entitlement overrides
through the admin API, and runs the real organization-admin readiness action.
It then builds Web in
Compose on `127.0.0.1:3002` and invokes Web's fixed
`test:ask-dev-acceptance` browser oracle. No caller-supplied seed or test command
is accepted, and no missing-service or missing-secret condition becomes a skip.

Agent Context Runtime is independently gated and remains disabled. The browser
requires Ask Dev's signed evidence resolver to be ready while
`agent_context_runtime` is false, so this lane cannot mask a forbidden
entitlement coupling.

The browser oracle exercises the authenticated Web BFF, real Ops REST/SSE
surface, production scope and tool services, persisted conversation/run, and
the real HTTP OpenAI-compatible adapter. Its scripted decision sequence calls
`query_metric.v1`, then `search_evidence.v1`, then `data_health.v1`, and only
then synthesizes a final answer. The oracle requires the exact registered
`items_completed` value and comparison, a material observed claim that cites
both that metric reference and a `meridian/web-app` evidence reference, and a
fresh provider-health result. The browser independently reconstructs the exact
answer sentence from the returned metric values, so unrelated non-empty rows or
a generic grounding statement cannot pass. The browser question and required
answer parts come from the versioned `ask-dev-oracle.v1.json`.

The provider has no published host port. It exists only in the
`ask-dev-acceptance` profile on the internal `ask-dev-acceptance` network. A
successful run removes the dedicated acceptance project and volumes; a failed
run retains them and prints recent Ops, provider, and Web logs for diagnosis.
