---

# PRD: Investment View (Canonical Replacement for Signals)

## Status

- Signals: **POC only, unreleased**
- Investment View: **Canonical model going forward**

---

## 1. Purpose (Re-stated, locked)

### Primary question this product answers

> “Where is human effort actually being invested across the organization, and what is the cost to people when certain kinds of work dominate?”

This is **not** a work taxonomy.
This is **not** a reporting layer.
This is **an organizational mirror for leadership**.

---

## 2. Core Design Principles (Non-negotiable)

1. **Messy human inputs are normalized**

   - Jira labels, issue types, GitHub labels are inputs only
   - They are never surfaced as truth

2. **Investment categories are canonical**

   - Fixed vocabulary
   - Not user-configurable
   - Comparable across teams, tools, and time

3. **Investment categorization is decisive**

   - Always produces a distribution
   - Never returns “unknown”
   - Weak evidence lowers evidence quality, not output existence

4. **Evidence quality ≠ correctness**

   - It indicates corroboration strength
   - It does not block visibility

5. **The output is about people**

   - Effort concentration
   - Support load
   - Long-term cost of neglect

---

## 3. Canonical Investment Categories

These are fixed and enforced:

- Feature Delivery
- Operational / Support
- Maintenance / Tech Debt
- Quality / Reliability
- Risk / Security

No synonyms. No overrides. No per-team customization.

---

## 4. Data Model (Canonical)

### 4.1 Investment Categorization Output (Primary Artifact)

This replaces “Signals” entirely.

```json
{
  "entity_id": "work_unit_id",
  "investment": {
    "feature_delivery": 0.42,
    "operational": 0.31,
    "maintenance": 0.19,
    "quality": 0.08,
    "risk": 0.0
  },
  "evidence_quality": {
    "value": 0.63,
    "band": "moderate"
  },
  "evidence": {
    "textual": ["Matched phrases: 'hotfix', 'incident response'"],
    "structural": ["Linked to on-call incident issue"],
    "contextual": ["PR merged directly to main during outage window"]
  }
}
```

### Guarantees

- Investment vector always sums to ~1.0
- Evidence always present (empty arrays allowed)
- Evidence quality always emitted

---

## 5. Categorization Logic (Authoritative)

### 5.1 Signal Priority Order

1. **Textual intent (primary)**

   - Issue title & description
   - PR title & description
   - Commit messages

2. **Provider metadata (supporting)**

   - Jira issue type
   - Labels
   - Milestones

3. **Structural context (corroboration)**

   - Relationships
   - Timing
   - Repo scope

Text drives the category.
Structure adjusts and corroborates.

---

### 5.2 Role of LLMs (Allowed and Required)

LLMs are used to:

- Normalize messy human language
- Map intent to canonical investment categories
- Produce a category distribution

LLMs MUST:

- Output only the fixed category set
- Cite exact phrases from input text
- Emit uncertainty explicitly

LLMs MUST NOT:

- Invent categories
- Introduce new facts
- Override the category set

---

## 6. Evidence Quality (Renamed and Reframed)

### Definition

> Evidence quality indicates how strongly the investment categorization is corroborated by independent signals.

### Contributors

- Presence of descriptive text
- Agreement between issue, PR, and commits
- Presence of provider metadata
- Structural alignment

Low evidence quality means:

- “This is inferred from weak or sparse signals”

It does **not** mean:

- “This is probably wrong”

---

## 7. Investment Views (UX)

### 7.1 Treemap — Investment Composition

- Nodes: Investment category (primary)
- Optional split: category × repo_scope or team
- Size: probability-weighted effort
- Opacity: evidence quality

**Purpose**
Show where effort is going, regardless of how work was labeled.

---

### 7.2 Sunburst — Investment Distribution

Hierarchy:

```
Investment Category
 └── Repo scope / Team
     └── Work clusters (optional drill-down)
```

**Purpose**
Show concentration of investment and who absorbs it.

---

### 7.3 Sankey — Flow of Investment Pressure

Source:

- Investment category

Target:

- Repo scope / Team

Weight:

- Probability-weighted effort

**Purpose**
Make support load, maintenance drag, and feature pressure visible.

---

## 8. What Is Explicitly Not Shown

- Jira issue types
- GitHub labels
- Raw ticket metadata
- “Bug vs Feature” debates

These are inputs, not outcomes.

---

## 9. Migration Plan (Signals → Investment)

### Signals is retired.

No backward compatibility required.

---

# MIGRATION PROMPTS FOR AGENTS

## Migration Prompt 1 — Backend (Ops)

```text
You are migrating the unreleased “Signals” POC into a canonical Investment model.

Signals must be removed as a concept.
Investment categorization becomes first-class.

TASKS:
- Replace signal outputs with investment category vectors
- Introduce evidence_quality as a separate field
- Implement text-first categorization using LLMs under strict constraints
- Normalize provider metadata into investment categories
- Ensure every WorkUnit emits an investment vector

DO NOT:
- Preserve “signal confidence”
- Emit “unknown”
- Allow category configuration

Refer to AGENTS-INVESTMENT.md for canonical rules.
```

---

## Migration Prompt 2 — Frontend (UX)

```text
You are migrating the UI from “Signals” to the Investment View.

Signals UI must be removed.
Investment is the primary lens.

TASKS:
- Replace Signals charts with Investment-driven treemap, sunburst, Sankey
- Ensure categories are canonical and fixed
- Surface evidence quality, not confidence-in-correctness
- Remove exposure of labels, issue types, or raw metadata
- Frame all views as “where effort is invested”

DO NOT:
- Add configuration
- Surface provider-native categories
- Show uncertainty as absence of signal

Refer to AGENTS-INVESTMENT.md for interpretation rules.
```

---

## Migration Prompt 3 — LLM Contract

```text
You are implementing LLM-based investment categorization.

You must:
- Map messy human text to fixed investment categories
- Output a probability distribution
- Cite exact phrases from input text
- Emit an uncertainty statement

You must not:
- Invent categories
- Use free-form reasoning
- Override the canonical vocabulary

This system exists to make human cost visible to leadership.
```

---

## Final Lock-In

This migration:

- Clarifies the product
- Fixes the confidence deadlock
- Aligns data, views, and purpose
- Makes people visible again

If you want next, I recommend:

- A **leadership-facing walkthrough** using a real dataset
- Or a **calibration pass** on evidence quality bands so they map cleanly to executive intuition
