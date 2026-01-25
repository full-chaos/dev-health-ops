# Investment View — Product Specification

## Status

- **Signals:** POC only, **retired**
- **Investment View:** Canonical model going forward

---

## Primary Question

> "Where is human effort actually being invested across the organization, and what is the cost to people when certain kinds of work dominate?"

This is:
- **NOT** a work taxonomy
- **NOT** a reporting layer
- **An organizational mirror for leadership**

---

## Core Design Principles (Non-Negotiable)

### 1. Messy Human Inputs Are Normalized

- Jira labels, issue types, GitHub labels are **inputs only**
- They are never surfaced as truth
- Provider-native categories are normalized away

### 2. Investment Categories Are Canonical

- Fixed vocabulary (see below)
- **Not user-configurable**
- Comparable across teams, tools, and time

### 3. Investment Categorization Is Decisive

- Always produces a distribution
- **Never returns "unknown"**
- Weak evidence lowers evidence quality, not output existence

### 4. Evidence Quality ≠ Correctness

- Indicates corroboration strength
- Does not block visibility
- Low quality means "inferred from weak signals," not "probably wrong"

### 5. The Output Is About People

- Effort concentration
- Support load
- Long-term cost of neglect

---

## Canonical Investment Categories

### Themes (Fixed, Leadership-Facing)

| Theme | Description |
|-------|-------------|
| **Feature Delivery** | New value, customer-requested, roadmap items |
| **Operational / Support** | External support, internal support, incident response |
| **Maintenance / Tech Debt** | Refactoring, upgrades, cleanup |
| **Quality / Reliability** | Testing, observability, stability work |
| **Risk / Security** | Security fixes, compliance, vulnerability remediation |

**Rules:**
- No synonyms
- No overrides
- No per-team customization

### Subcategories (Fixed Per Theme)

Each theme has a curated subcategory set providing resolution without fragmenting language.

Example structure:
```
Operational
 ├── External-facing
 ├── Internal support
 ├── Incident response
 └── On-call / reactive

Feature Delivery
 ├── Customer-requested
 ├── Strategic / roadmap
 └── Enablement / platform
```

**Rules:**
- Subcategories are **not user-defined**
- Subcategories are **comparable across orgs**
- Subcategories roll up cleanly into themes

---

## Categorization Hierarchy

```
Evidence (WorkUnits)
   ↓
Subcategory (what flavor of work within a theme)
   ↓
Theme (investment category)
```

Where:
- **Theme** answers: "What kind of organizational investment is this?"
- **Subcategory** answers: "What flavor of that investment is consuming people?"
- **Evidence (WorkUnits)** answers: "What concrete activity supports this inference?"

**Critical distinction:**
- WorkUnits are **evidence containers**, NOT categories
- WorkUnits never appear as categorization layers
- WorkUnits never appear as peers to themes/subcategories in UI

---

## Data Model

### Investment Categorization Output

```json
{
  "entity_id": "work_unit_id",
  "investment": {
    "themes": {
      "operational": 0.47,
      "feature_delivery": 0.33,
      "maintenance": 0.15,
      "quality": 0.05,
      "risk": 0.0
    },
    "subcategories": {
      "operational.external": 0.29,
      "operational.internal": 0.18,
      "feature_delivery.customer": 0.21,
      "feature_delivery.platform": 0.12,
      "maintenance.refactor": 0.15,
      "quality.testing": 0.05
    }
  },
  "evidence_quality": {
    "value": 0.64,
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

- Theme probabilities sum to ~1.0
- Subcategory probabilities sum to theme probabilities
- Evidence arrays always present (may be empty)
- Evidence quality always emitted
- Categorization never returns "unknown"
- WorkUnit never appears as a category

---

## Categorization Logic

### Signal Priority Order

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

**Text drives the category. Structure adjusts and corroborates.**

---

## Evidence Quality

### Definition

> Evidence quality indicates how strongly the investment categorization is corroborated by independent signals.

### Quality Bands

| Band | Range | Meaning |
|------|-------|---------|
| High | 0.8+ | Strong corroboration from multiple sources |
| Moderate | 0.5-0.8 | Reasonable corroboration |
| Low | <0.5 | Inferred from sparse signals |

### Contributors

- Presence of descriptive text
- Agreement between issue, PR, and commits
- Presence of provider metadata
- Structural alignment

Low evidence quality means:
- "This is inferred from weak or sparse signals"

It does **NOT** mean:
- "This is probably wrong"

---

## Investment Views (UX)

### 1. Treemap — Investment Composition

- **Nodes:** Investment category (theme-level by default)
- **Optional split:** category × repo_scope or team
- **Size:** Probability-weighted effort
- **Opacity:** Evidence quality

**Purpose:** Show where effort is going, regardless of how work was labeled.

### 2. Sunburst — Investment Distribution

Hierarchy:
```
Investment Category (Theme)
 └── Repo scope / Team
     └── Work clusters (optional drill-down)
```

**Purpose:** Show concentration of investment and who absorbs it.

### 3. Sankey — Flow of Investment Pressure

- **Source:** Investment category
- **Target:** Repo scope / Team
- **Weight:** Probability-weighted effort

**Purpose:** Make support load, maintenance drag, and feature pressure visible.

---

## Drill-Down Contract

1. **Default:** Theme-only (leadership readable)
2. **Drill:** Theme → Subcategory → Evidence (WorkUnits)

**Never show WorkUnits as top-level segments or peers to categories.**

---

## What Is Explicitly Not Shown

- Jira issue types
- GitHub labels
- Raw ticket metadata
- "Bug vs Feature" debates

These are inputs, not outcomes.
