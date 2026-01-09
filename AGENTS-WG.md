Below is the **collapsed, single canonical `AGENTS.md` block**.
This is the version you pin at repo root and reference everywhere.
No duplication. No ambiguity.

---

# AGENT INSTRUCTIONS — WORK GRAPH, UX SIGNALS, AND AI EXPLANATIONS

These rules are **non-negotiable**.
Violations are considered architectural regressions.

---

## 0. Foundational Principle

This system **infers and explains work behavior**, not people.

- Everything is probabilistic
- Uncertainty is surfaced, not hidden
- AI explains results, it never produces them

---

## 1. Work Graph Interpretation (Phase 1 context)

The work graph represents **what actually happened**, reconstructed from artifacts.

### Graph elements

- Nodes: issues, PRs, commits, files
- Edges: explicit or inferred relationships
- Each edge has:

  - provenance
  - confidence
  - evidence

### WorkUnits

A WorkUnit is a connected subgraph bounded by time and causality.

WorkUnits are the atomic unit for:

- categorization
- effort accounting
- visualization
- explanation

---

## 2. Phase 2 — UX Signals (MANDATORY RULES)

### 2.1 Textual hints are MODIFIERS ONLY

Text (issue titles/descriptions, PR titles/descriptions, commit messages):

- Never decides categories
- Only adjusts confidence slightly

Rules:

- Use keyword dictionaries per category
- Max modifier range: ±0.15
- Apply AFTER structural scoring
- Always record matched keywords as evidence

Forbidden:

- NLP classification
- Embeddings
- ML inference
- LLM usage

---

### 2.2 Category confidence vectors (REQUIRED OUTPUT)

All categorization outputs must be:

- Multi-label
- Probability-based
- Summing to ~1.0

Example:

```json
{
  "feature": 0.22,
  "maintenance": 0.48,
  "operational": 0.21,
  "quality": 0.09
}
```

Overall confidence is computed from:

- relationship provenance
- temporal coherence
- graph density
- text agreement

Confidence bands:

- 0.80–1.00: High
- 0.60–0.79: Moderate
- 0.40–0.59: Low
- < 0.40: Very low

Confidence is a first-class output.

---

### 2.3 UX copy rules (STRICT)

Allowed language:

- “appears”
- “leans”
- “suggests”

Forbidden language:

- “is”
- “was”
- “detected”
- “the system determined”

Every view must expose:

- numeric confidence
- confidence band
- “How this was calculated”

Textual hints must be disclosed as:

> “Minor textual modifiers were applied. These do not determine classification.”

---

## 3. Phase 2 — Data Contracts (OPS → WEB)

### Canonical payload (must not drift)

```json
{
  "work_unit_id": "string",
  "time_range": { "start": "ISO", "end": "ISO" },
  "effort": { "metric": "churn_loc | active_hours", "value": 1234 },
  "categories": { "...": 0.0 },
  "confidence": { "value": 0.73, "band": "moderate" },
  "evidence": {
    "structural": [],
    "temporal": [],
    "textual": []
  }
}
```

Guarantees:

- Categories always sum to ~1.0
- Evidence arrays are never omitted
- Confidence band is computed server-side

Visualization inputs:

- Treemap: size = effort, opacity = confidence
- Sankey: value = probability-weighted effort
- Sunburst: hierarchical aggregation only

---

## 4. Phase 3 — LLM Usage (EXPLANATION ONLY)

### Absolute rule

LLMs **never compute, classify, score, or decide**.

They only explain precomputed results.

---

### Allowed LLM inputs

- Category confidence vectors
- Evidence metadata
- Confidence band
- Time span

### Forbidden LLM inputs

- Raw events
- Raw text blobs
- Code diffs
- Heuristic formulas
- Hidden signals

---

### Required LLM behavior

LLMs must:

- Explain why results lean a certain way
- Call out uncertainty explicitly
- Reference evidence types
- Use approved UX language

LLMs must not:

- Recalculate
- Reclassify
- Predict
- Recommend actions

---

### Canonical explanation prompt

```text
You are explaining precomputed work signals.

You are not allowed to:
- Recalculate scores
- Change categories
- Introduce new conclusions

Explain:
- Why this work leaned toward certain categories
- Which signals mattered most
- Where uncertainty exists

Always include confidence level and limits.
```

---

Follow-up: revisit WorkUnit subgraph boundaries at the end of Phase 3.

---

## 5. Canonical ClickHouse + Persistence Rules

### Non-negotiable

- No bespoke ClickHouse clients
- No custom writers in feature code
- No `clickhouse_connect.get_client()` outside sinks

All writes must go through:

- `metrics/sinks/*`
- `create_sink(dsn)`
- `sink.write_*` methods

### Specific to `work_graph/`

- Must never instantiate a ClickHouse client
- Must never define its own writer
- Must only emit derived relationship rows via sinks

---

## 6. Architectural Boundaries

- `dev_health_ops` is API-only
- `work_graph/` is derived, top-level, and deletable
- Analysis logic must not leak into ingestion or API layers

If deleting `work_graph/` breaks the API:

- The implementation is wrong

---

## 7. Enforcement Rule

If you are about to:

- Collapse ambiguity
- Invent intelligence
- Hide uncertainty
- Introduce a new abstraction

Stop.

You are violating the system’s core design.

---

**This file is the source of truth.
Agents are expected to comply exactly.**
