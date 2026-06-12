# LLM Categorization Contract

Rules and specifications for LLM usage in the Dev Health platform.

> For the end-to-end compute flow this contract sits inside, see the
> [Investment Categorization Pipeline](../architecture/investment-categorization-pipeline.md).
> For the canonical keys, see the [Investment Taxonomy](../product/investment-taxonomy.md).

---

## Overview

LLMs are used in two contexts:

| Context | When | Purpose | Constraints |
|---------|------|---------|-------------|
| Compute-time | During data processing | Categorize work into investment themes | Strict schema, persisted |
| UX-time | On user request | Explain persisted categorizations | Read-only, no recomputation |

---

## Compute-Time Categorization

### Purpose

Map messy human text to canonical investment categories with subcategory distributions.

### Strict schema

Output **MUST** be strict JSON validated by `work_graph/investment/llm_schema.py`.
There are exactly three top-level keys — `subcategories`, `evidence_quotes`,
`uncertainty` — and no others.

### Output requirements

| Requirement | Details |
|-------------|---------|
| `subcategories` | Probabilities over the canonical subcategory keys (see [Investment Taxonomy](../product/investment-taxonomy.md)); each value `0–1`; the values must sum to within `[0.98, 1.02]` |
| `evidence_quotes` | A **list** of 1–10 objects, each exactly `{ "quote", "source", "id" }` |
| `quote` | An **exact substring** of the provided source text (≤ 280 chars) |
| `source` | One of `issue`, `pr`, `commit` |
| `uncertainty` | Non-empty string, ≤ 280 chars |

### Example output

```json
{
  "subcategories": {
    "feature_delivery.customer": 0.45,
    "operational.incident_response": 0.25,
    "quality.bugfix": 0.20,
    "maintenance.refactor": 0.10
  },
  "evidence_quotes": [
    { "quote": "requested by customer", "source": "issue", "id": "PROJ-123" },
    { "quote": "hotfix for production outage", "source": "pr", "id": "456" }
  ],
  "uncertainty": "Mixed signals between customer feature work and incident response."
}
```

> The prompt requests a value for **all 15** subcategories. Validation requires every
> provided key to be canonical and the values to sum to within `[0.98, 1.02]`; the
> validation step then fills any missing keys with `0` and renormalizes via
> `ensure_full_subcategory_vector`. Keys must match `investment_taxonomy.py` exactly —
> obsolete keys like `operational.external` or `feature_delivery.platform` are rejected
> as `unknown_subcategory`.

### Two-stage process

1. **LLM stage:** map text → subcategory distribution.
2. **Deterministic stage:** roll subcategories → themes (no LLM).

This separation prevents category drift. The full flow is documented in the
[Investment Categorization Pipeline](../architecture/investment-categorization-pipeline.md).

---

## Validation, repair, and fallback

Each response is validated by `validate_llm_payload`. On failure the categorizer makes
**exactly one** repair attempt, re-prompting with the specific validation errors. If the
repair also fails, a deterministic fallback distribution is applied.

### Validation failures include

- non-JSON or non-object payloads;
- wrong / extra / missing top-level keys;
- non-canonical subcategory keys (`unknown_subcategory`);
- probabilities out of range, or a sum outside `[0.98, 1.02]`;
- quote count outside 1–10, wrong quote keys, or empty / too-long quotes;
- a quote that is **not a literal substring** of the source text
  (`evidence_quote_not_substring`);
- invalid `source` (not `issue` / `pr` / `commit`) or unknown source id;
- missing / too-long `uncertainty`.

### Outcome status

Every run records a `categorization_status`:

| Status | Meaning |
|--------|---------|
| `ok` | Validated on the first attempt |
| `repaired` | Validated after the single repair re-prompt |
| `invalid_llm_output` | Still invalid after repair → deterministic fallback applied |
| `insufficient_evidence` | Too little text to call the LLM → fallback |
| `no_text_sources` | No usable source text → fallback |
| `llm_task_failed` | The async LLM task raised before an outcome was recorded → fallback |

> The fallback is a **neutral prior** (`FALLBACK_PRIOR`), not "unknown". It preserves the
> never-unknown guarantee but means *"insufficient validated evidence"* — pair it with a
> low `evidence_quality` reading in any UX.

---

## Audit Fields

Every WorkUnit row in `work_unit_investments` persists:

| Field | Description |
|-------|-------------|
| `categorization_status` | Outcome status (table above) |
| `categorization_errors_json` | Serialized validation errors (if any) |
| `categorization_model_version` | Model id, or provider name when no model is set |
| `categorization_input_hash` | SHA-256 of the serialized evidence bundle |
| `categorization_run_id` | Per-run UUID |
| `computed_at` | Run timestamp (also the ReplacingMergeTree version) |

See the [Investment Data Model](../architecture/investment-data-model.md) for the full
schema and read semantics.

---

## LLM Provider Options

The system supports multiple LLM backends. Set `LLM_PROVIDER` or let auto-detection pick the first configured provider.

### Provider Configuration

| Provider | Env Var for Selection | Key Env Vars (selection / override) | Default Model |
|----------|----------------------|-------------------|---------------|
| **OpenAI** | `LLM_PROVIDER=openai` | `OPENAI_API_KEY` | `gpt-5-mini` |
| **Anthropic** | `LLM_PROVIDER=anthropic` | `ANTHROPIC_API_KEY` | `claude-3-haiku-20240307` |
| **Gemini** | `LLM_PROVIDER=gemini` | `GEMINI_API_KEY` | `gemini-3` |
| **Qwen (DashScope)** | `LLM_PROVIDER=qwen` | `QWEN_API_KEY` or `DASHSCOPE_API_KEY` | `qwen-plus` |
| **Ollama** | `LLM_PROVIDER=ollama` | `OLLAMA_BASE_URL` or `OLLAMA_MODEL` | `llama3.2` |
| **Local (generic)** | `LLM_PROVIDER=local` | `LOCAL_LLM_BASE_URL` | `llama3.2` |
| **LM Studio** | `LLM_PROVIDER=lmstudio` | `LMSTUDIO_BASE_URL` | `local-model` |
| **Qwen Local** | `LLM_PROVIDER=qwen-local` | `OLLAMA_BASE_URL` | `qwen2.5:7b` |
| **Mock** | `LLM_PROVIDER=mock` | (none) | deterministic mock |

### Auto-Detection Order

When `LLM_PROVIDER` is unset or `auto`, the system checks in order:
1. `OPENAI_API_KEY` -> OpenAI
2. `ANTHROPIC_API_KEY` -> Anthropic
3. `GEMINI_API_KEY` -> Gemini
4. `LOCAL_LLM_BASE_URL` -> Local
5. `DASHSCOPE_API_KEY` / `QWEN_API_KEY` -> Qwen
6. `OLLAMA_MODEL` / `OLLAMA_BASE_URL` -> Ollama
7. Falls back to mock provider

### Common Configuration

| Variable | Description |
|----------|-------------|
| `LLM_PROVIDER` | Explicit provider selection (see table above) |
| `LLM_MODEL` | Override the default model for any provider |
| `OPENAI_BASE_URL` | Custom OpenAI-compatible endpoint |
| `DASHSCOPE_BASE_URL` | Regional DashScope endpoint (default: China; Singapore/US available) |
| `GEMINI_BASE_URL` | Custom Gemini endpoint |

### Provider-Specific Notes

**OpenAI:** GPT-5+ models use the Responses API with structured outputs (json_schema). Legacy GPT-4 and below use Chat Completions. The provider auto-selects the correct API based on model name.

**Anthropic:** Uses the Messages API. Requires `pip install anthropic`.

**Gemini:** Uses Google's OpenAI-compatible endpoint at `generativelanguage.googleapis.com`.

**Qwen:** Supports both official DashScope API (cloud) and local Ollama/LM Studio deployments.

**Local providers:** Any OpenAI-compatible server (Ollama, vLLM, LM Studio) works. Falls back gracefully if the server doesn't support structured outputs.

---

## OpenAI-Specific Handling

### JSON Mode

Include explicit JSON instruction in **both**:
- System message
- User message

### Token Configuration

- Use `max_completion_tokens` (not `max_tokens`)
- Minimum: 512 tokens
- Double on retry

### Observability

Log on every call:
- `finish_reason`
- `content_length`
- Token parameters
- Response time

---

## UX-Time Explanation

### Purpose

Generate human-readable explanations of **persisted** categorizations.

### Constraints

| Allowed | Forbidden |
|---------|-----------|
| Read persisted distributions | Recompute categories |
| Read stored evidence | Change edges/weights |
| Generate narrative text | Introduce new conclusions |
| Cite specific evidence | Modify persisted decisions |

### Required Labeling

All explanation output **MUST be labeled as AI-generated**.

---

## Explanation Prompt

Canonical prompt (use verbatim):

```
You are explaining a precomputed investment view.

You are not allowed to:
- Recalculate scores
- Change categories
- Introduce new conclusions
- Be conversational (no "Hello", "As an AI", or interactive follow-ups)

Explain the investment view in three distinct sections:

1. **SUMMARY**: Provide a high-level narrative (max 3 sentences) using
   probabilistic language (appears, leans, suggests) explaining why
   the work leans toward the primary categories.

2. **REASONS**: List the specific evidence (structural, contextual,
   textual) that contributed most to this interpretation.

3. **UNCERTAINTY**: Disclose where uncertainty exists based on the
   evidence quality and evidence mix.

Always include evidence quality level and limits.
```

---

## Language Rules

### Allowed Language

Use probabilistic, uncertain phrasing:

- appears
- leans
- suggests
- indicates
- may be

### Forbidden Language

Avoid definitive, deterministic phrasing:

- is
- was
- detected
- determined
- definitely
- clearly

### Rationale

The distinction maintains appropriate uncertainty. LLM categorization is inference, not detection.

---

## Evidence Handling

### Extractive Quotes

Evidence quotes MUST be:
- Direct substrings from input text
- Not paraphrased
- Not summarized
- Traceable to source

### Evidence Types

| Type | Source | Example |
|------|--------|---------|
| Textual | Issue/PR title, description, commits | "hotfix for production bug" |
| Structural | Relationships, links | "Linked to incident #123" |
| Contextual | Timing, patterns | "Merged during outage window" |

---

## Forbidden Patterns

### Do Not

- Invent categories not in canonical list
- Use free-form reasoning in output
- Override canonical vocabulary
- Return "unknown" or "uncategorized"
- Hallucinate evidence not in input
- Apply categories based on author identity

### Immediate Failure Conditions

- Output contains non-canonical keys
- Probabilities don't sum correctly
- Evidence quotes not found in input
- Missing required output sections

---

## Testing

### Unit Tests Must Cover

- Valid JSON output parsing
- Probability normalization
- Evidence extraction validation
- Retry logic
- Fallback application
- Audit field persistence

### Mock Requirements

- Mock LLM API responses
- Test various failure modes
- Verify retry behavior
- Test fallback categorization
