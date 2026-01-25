# LLM Categorization Contract

Rules and specifications for LLM usage in the Dev Health platform.

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

### Schema Compliance

Output **MUST** be strict JSON matching:
```
work_graph/investment/llm_schema.py
```

### Output Requirements

| Requirement | Details |
|-------------|---------|
| Keys | From canonical subcategory registry only |
| Probabilities | Valid (0–1), normalized |
| Evidence | **Extractive substrings** from input text |
| Theme roll-up | Computed deterministically from subcategories |

### Example Output

```json
{
  "subcategories": {
    "operational.external": 0.45,
    "operational.incident": 0.25,
    "feature_delivery.customer": 0.20,
    "maintenance.refactor": 0.10
  },
  "evidence": {
    "operational.external": ["customer-facing issue", "support ticket"],
    "operational.incident": ["incident response", "outage window"],
    "feature_delivery.customer": ["requested by customer"],
    "maintenance.refactor": ["cleanup", "technical debt"]
  },
  "uncertainty": "moderate"
}
```

### Two-Stage Process

1. **LLM Stage:** Map text → subcategory distribution
2. **Deterministic Stage:** Roll subcategories → themes (no LLM)

This separation prevents category drift.

---

## Retry Policy

### When to Retry

- Whitespace/empty response
- Truncated response (`finish_reason=length`)
- Invalid JSON structure
- Missing required keys

### Retry Strategy

1. **First attempt:** Standard prompt, standard tokens
2. **Retry attempt:**
   - Double `max_completion_tokens` (minimum 512)
   - Simplify and harden JSON prompt
   - Add explicit JSON instruction in system AND user message

### Failure Handling

After one retry failure:
1. Mark categorization as invalid
2. Apply deterministic fallback
3. Persist with `fallback_applied=true`

---

## Audit Fields

Every categorization run must persist:

| Field | Description |
|-------|-------------|
| `categorized_at` | Timestamp |
| `model_version` | LLM model identifier |
| `prompt_hash` | Hash of prompt template |
| `raw_response` | Original LLM output |
| `fallback_applied` | Boolean |
| `retry_count` | Number of retries |
| `finish_reason` | OpenAI finish reason |
| `token_usage` | Tokens consumed |

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
