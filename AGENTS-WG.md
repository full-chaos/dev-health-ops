# AGENT INSTRUCTIONS — WORK GRAPH & INVESTMENT MATERIALIZATION

These rules are non-negotiable. Violations are architectural regressions.

## 0. Core Contract

- WorkUnits are evidence containers, not categories.
- LLM decides subcategory distributions at compute-time only.
- Theme roll-up is deterministic from subcategories.
- UX renders only persisted distributions and edges.
- LLM explanation may run on-demand but may not alter persisted decisions.
- No static files or export paths; sinks only.

## 1. Investment Taxonomy (Fixed)

- Themes and subcategories are canonical and non-configurable.
- Provider labels/types (Jira/GitHub/GitLab) are inputs only and normalized away.

## 2. Compute-Time LLM Categorization (Required)

- LLM output MUST be strict JSON matching the schema in `work_graph/investment/llm_schema.py`.
- Keys must come from the canonical subcategory registry.
- Probabilities must be valid and normalized.
- Evidence quotes must be extractive substrings from provided inputs.
- Retry policy: one repair attempt only. Otherwise mark invalid and apply deterministic fallback.
- Persist audit fields for every categorization run.

## 3. UX-Time LLM Explanation (Allowed)

- LLM may generate explanation text only from persisted distributions and stored evidence.
- LLM must not recompute categories, edges, weights, or distributions.
- All explanation output must be labeled as AI-generated.

### Canonical explanation prompt (verbatim)

You are explaining a precomputed investment view.

You are not allowed to:

- Recalculate scores
- Change categories
- Introduce new conclusions
- Be conversational (no "Hello", "As an AI", or interactive follow-ups)

Explain the investment view in three distinct sections:

1. **SUMMARY**: Provide a high-level narrative (max 3 sentences) using probabilistic language (appears, leans, suggests) explaining why the work leans toward the primary categories.
2. **REASONS**: List the specific evidence (structural, contextual, textual) that contributed most to this interpretation.
3. **UNCERTAINTY**: Disclose where uncertainty exists based on the evidence quality and evidence mix.

Always include evidence quality level and limits.

## 4. Language Rules

Allowed language:

- appears
- leans
- suggests

Forbidden language:

- is
- was
- detected
- determined

## 5. Persistence Contract

- All compute outputs are persisted via `metrics/sinks/*` only.
- No JSON/YAML dumps, no output paths, no debug files under `work_graph/` or investment modules.

## Fix: OpenAI Response Handling (Production Bug)

Fixed production-blocking bug where OpenAI responses were blank (content_length=0) with finish_reason=length.

### Implementation Checklist

- **JSON mode**: Must include explicit JSON instruction in system and user message.
- **Tokens**: Uses `max_completion_tokens` (min 512).
- **Retry**: Automatic single retry on whitespace, truncated responses (finish_reason=length), or invalid JSON.
- **Retry Strategy**: Doubles tokens and simplifies/hardens JSON prompt on retry.
- **Observability**: Logs `finish_reason`, `content_length`, and token params.
