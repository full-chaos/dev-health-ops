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

## Live provider profiles

Live provider smokes supplement the deterministic oracle above; they never
replace it as the release gate. Each profile starts a disposable Ops stack,
generates the same seeded organization data, enables Ask Dev through the public
admin API, and invokes the real administrator readiness action. Readiness proves
the OpenAI-compatible structured-output turn and its tool-result continuation.
The smoke then creates a conversation and requires a bounded, contract-valid
`answer.completed` event from the real `/api/v1/dev/**` REST/SSE surface with
`provider_source=platform`. A selected profile fails on missing configuration,
provider errors, invalid output, or an error terminal; it never turns those
conditions into a skip.

For the current local LM Studio profile, LM Studio is listening on the host at
`127.0.0.1:1234` with `google/gemma-4-e4b` loaded. The launcher maps the host
endpoint to Compose and deliberately selects the platform `local` environment
bundle used by normal deployments:

```console
scripts/acceptance/run_ask_dev_provider_profile.sh --profile lmstudio-local
```

Override either exact value only when intentionally validating another loaded
model or endpoint:

```console
ASK_DEV_PROVIDER_MODEL=my/model \
ASK_DEV_PROVIDER_BASE_URL=http://host.docker.internal:1234/v1 \
  scripts/acceptance/run_ask_dev_provider_profile.sh --profile lmstudio-local
```

For a local Ollama daemon, provide the exact installed model. Ollama's documented
OpenAI-compatible default is `http://localhost:11434/v1`; the Compose profile
uses the equivalent host bridge address:

```console
ASK_DEV_PROVIDER_MODEL=qwen3:8b \
  scripts/acceptance/run_ask_dev_provider_profile.sh --profile ollama-local
```

Ollama Cloud is opt-in and requires both an exact cloud model and an
`OLLAMA_API_KEY` injected by the operator's shell credential store or CI secret.
Do not put the key in a command-line argument or checked-in environment file.
The profile uses the OpenAI-compatible `https://ollama.com/v1` endpoint unless
`ASK_DEV_PROVIDER_BASE_URL` explicitly overrides it:

```console
export OLLAMA_API_KEY
ASK_DEV_PROVIDER_MODEL=gpt-oss:120b-cloud \
  scripts/acceptance/run_ask_dev_provider_profile.sh --profile ollama-cloud
```

Local Ollama and Ollama Cloud use the `OLLAMA_*` platform aliases; LM Studio's
validated `local` profile uses `LOCAL_LLM_*`. The overlay also sets the generic
`LLM_MODEL` and `LLM_BASE_URL` to the same values and clears unrelated provider
aliases, so the smoke proves the documented source-bound environment resolution
instead of inheriting a developer's unrelated `.env` provider.
