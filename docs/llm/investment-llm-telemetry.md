# Investment LLM Telemetry

Investment categorization and investment-mix explanation emit Prometheus metrics,
OpenTelemetry metrics, and OpenTelemetry events at the request, validation, repair,
parse, and terminal-outcome boundaries. Recording is synchronous in-process; the
OpenTelemetry SDK batches OTLP metric export outside the LLM hot path.

## Label contract

All metric labels are fixed enums or bounded buckets. Model labels are limited to known
families such as `gpt-5-nano`, `gpt-5-mini`, `claude`, and `other`. Validation labels use
the error-code prefix only. Prompt text, source content, evidence quotes, raw error
messages, organization IDs, run IDs, and work-unit IDs are never labels.

Prompt versions identify behavior changes:

- `investment-categorization-v2`: relative-weight normalization and encoded repair input.
- `investment-mix-explain-v2`: strict explanation schema and deterministic numeric evidence.

## Metrics

| Metric | Purpose |
| --- | --- |
| `devhealth_investment_llm_requests_total` | Request outcomes by provider, model bucket, stage, prompt kind, and prompt version. |
| `devhealth_investment_llm_request_duration_seconds` | Request latency histogram. |
| `devhealth_investment_llm_request_errors_total` | Provider failures by bounded error family. |
| `devhealth_investment_llm_tokens_total` | Input and output token counters. |
| `devhealth_investment_llm_output_chars` | Completion-size histogram. |
| `devhealth_investment_llm_validation_total` | Initial and repair validation outcomes. |
| `devhealth_investment_llm_validation_failures_total` | Validation failures by stable family. |
| `devhealth_investment_llm_categorization_outcomes_total` | Final `ok`, `repaired`, invalid, and pre-LLM fallback outcomes. |
| `devhealth_investment_llm_explanation_parse_total` | Explanation parser outcomes. |

## Collection topology

FastAPI continues to expose the Prometheus registry at `/metrics`. API-side explanation
signals therefore remain available to Prometheus scrapers and are also pushed through
OTLP.

Celery prefork children create their own OTLP metric exporter after fork. Each child
pushes the same bounded `devhealth_investment_llm_*` instruments to
`OTEL_EXPORTER_OTLP_ENDPOINT` at `OTEL_METRIC_EXPORT_INTERVAL` milliseconds. Graceful
worker shutdown forces a final collection and closes the exporter. Every child exports a
unique `service.instance.id`, preserving OTLP cumulative-stream single-writer semantics
across recycled and horizontally scaled workers. This process-local topology requires no
shared filesystem or replica sidecar.

A hard-killed child can lose measurements recorded since its last periodic export because
Celery cannot deliver the shutdown signal after `SIGKILL`. Keep the export interval below
the acceptable loss window and preserve the documented graceful worker drain budget.

Set the collector endpoint on every API and worker deployment. The collector must accept
OTLP gRPC metrics on that endpoint and provide durable storage or forward metrics to the
production metrics backend.

Verify worker delivery after running an investment materialization job:

```promql
sum by (service_name, model, prompt_version, status) (
  increase(devhealth_investment_llm_categorization_outcomes_total[15m])
)
```

At least one series with the worker service resource and the current categorization
prompt version confirms the worker-to-collector path. A worker restart followed by a
second materialization should increment the same bounded label set rather than create
process-specific series.

## Before/after queries

Categorization outcome rate by model bucket and prompt version:

```promql
sum by (model, prompt_version, status) (
  rate(devhealth_investment_llm_categorization_outcomes_total[1h])
)
```

Validation error families by attempt and prompt version:

```promql
sum by (model, prompt_version, stage, error_family) (
  rate(devhealth_investment_llm_validation_failures_total[1h])
)
```

Repair success rate is derived from repaired terminal outcomes divided by initial invalid
validations for the same model and prompt version. Explanation fallback rate is derived
from `devhealth_investment_llm_explanation_parse_total` grouped by `status`.
