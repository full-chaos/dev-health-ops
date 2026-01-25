# Work Graph & Investment Materialization

## Core Contract (Non-Negotiable)

These rules are architectural constraints. Violations are regressions.

| Rule | Explanation |
|------|-------------|
| WorkUnits are evidence containers | Not categories, not classification layers |
| LLM categorizes at compute-time only | Never at query/render time |
| Theme roll-up is deterministic | From subcategories, no LLM involvement |
| UX renders persisted data only | No recomputation of categories/edges/weights |
| LLM explanations are read-only | May not alter persisted decisions |
| Sinks only for persistence | No static files, no export paths |

---

## WorkUnit Definition

### What a WorkUnit Is

- An **evidence container**
- A unit of aggregation
- An attribution boundary

### What a WorkUnit Is NOT

- A category
- A theme
- A subcategory
- A visible classification layer

This distinction is subtle but critical. Treating WorkUnits as categories breaks the normalization model.

---

## Investment Taxonomy (Fixed)

### Themes

The five canonical themes (see [Investment View](investment-view.md)):

- Feature Delivery
- Operational / Support
- Maintenance / Tech Debt
- Quality / Reliability
- Risk / Security

### Subcategories

Fixed set per theme, curated for comparability across organizations.

**Rules:**
- Themes and subcategories are canonical and non-configurable
- Provider labels/types (Jira/GitHub/GitLab) are inputs only
- Provider-native labels are normalized away

---

## Compute-Time LLM Categorization

### Schema Compliance

LLM output **MUST** be strict JSON matching:
```
work_graph/investment/llm_schema.py
```

### Output Requirements

| Requirement | Details |
|-------------|---------|
| Keys | Must come from canonical subcategory registry |
| Probabilities | Must be valid (0-1) and normalized |
| Evidence quotes | Must be **extractive substrings** from provided inputs |
| Sum | Subcategory probs sum to theme probs; theme probs sum to ~1.0 |

### Two-Stage Mapping

1. **LLM outputs subcategories only** — Maps text to subcategory distribution
2. **Theme roll-up is deterministic** — Non-LLM, computed from subcategories

This prevents category drift and maintains normalization.

### Retry Policy

- **One repair attempt only**
- On continued failure: mark invalid, apply deterministic fallback
- Persist audit fields for every categorization run

### Audit Fields

Always persist:
- `categorized_at` — Timestamp of categorization
- `model_version` — LLM model used
- `prompt_hash` — Hash of prompt template
- `raw_response` — Original LLM output (for debugging)
- `fallback_applied` — Boolean if fallback was used

---

## UX-Time LLM Explanation (Allowed)

### Constraints

LLM may generate explanation text **only from**:
- Persisted distributions
- Stored evidence

LLM **must not**:
- Recompute categories
- Change edges
- Modify weights
- Alter distributions

### Labeling

All explanation output **must be labeled as AI-generated**.

### Canonical Explanation Prompt

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

- appears
- leans
- suggests

### Forbidden Language

- is
- was
- detected
- determined

The distinction maintains appropriate uncertainty and avoids false precision.

---

## Persistence Contract

### Required

- All compute outputs persisted via `metrics/sinks/*` only

### Forbidden

- JSON/YAML dumps
- Output file paths
- Debug files under `work_graph/` or investment modules

---

## OpenAI Response Handling

Production bug fix for blank responses (content_length=0, finish_reason=length).

### Implementation Checklist

| Item | Requirement |
|------|-------------|
| JSON mode | Include explicit JSON instruction in system and user message |
| Tokens | Use `max_completion_tokens` (minimum 512) |
| Retry | Automatic single retry on whitespace, truncated, or invalid JSON |
| Retry strategy | Double tokens and simplify/harden JSON prompt on retry |
| Observability | Log `finish_reason`, `content_length`, and token params |

---

## Migration Notes

### Backend Tasks (if touching Investment code)

- Remove any logic treating WorkUnits as categorization layers
- Ensure LLM outputs subcategory distributions only
- Roll subcategories into themes deterministically
- Update schemas and APIs accordingly

### Frontend Tasks (if touching Investment UI)

- Make theme the default and primary view
- Support drill-down: theme → subcategory → evidence
- Remove any visualization treating WorkUnits as peers to categories
- Ensure charts always roll up by theme unless explicitly drilled
