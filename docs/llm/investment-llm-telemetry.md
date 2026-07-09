# Investment LLM Telemetry

Investment categorization and investment-mix explanation emit Prometheus metrics and
OpenTelemetry events at the request, validation, repair, parse, and terminal-outcome
boundaries. Metric recording is synchronous in-process only; it does not add database or
network I/O to the hot path.

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
