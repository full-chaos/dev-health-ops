## 1. The correction (lock this in)

### ❌ What must be removed

- **WorkUnit is NOT a category**
- **WorkUnit is NOT part of the categorization hierarchy**

### ✅ What a WorkUnit actually is

> A WorkUnit is **evidence**: a connected set of activity from which investment intent is inferred.

It is:

- an evidence container
- a unit of aggregation
- an attribution boundary

It is **never**:

- a category
- a theme
- a subcategory
- a visible classification layer

This mistake is subtle but fatal if left in place.

---

## 2. The correct categorization model (with added layers)

You need **at least two semantic layers above evidence**.

### Canonical hierarchy (minimum viable)

```
Evidence (WorkUnits)
   ↓
Subcategory (what kind of work within a theme)
   ↓
Theme (investment category)
```

Where:

- **Theme** answers:
  _“What kind of organizational investment is this?”_

- **Subcategory** answers:
  _“What flavor of that investment is consuming people?”_

- **Evidence (WorkUnits)** answers:
  _“What concrete activity supports this inference?”_

This preserves normalization **and** expressiveness.

---

## 3. Updated canonical vocabulary (example structure)

You explicitly asked **not verbatim**, so treat this as structural guidance, not final names.

### Theme (fixed, canonical, leadership-facing)

- Operational
- Feature Delivery
- Maintenance
- Quality
- Risk / Security

### Subcategory (fixed set per theme, still canonical)

Each theme has a small, curated subcategory set.

Example shape (illustrative only):

```text
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

Key rules:

- Subcategories are **not user-defined**
- Subcategories are **comparable across orgs**
- Subcategories roll up cleanly into themes

---

## 4. Revised data model (authoritative)

### Investment categorization output (revised)

```json
{
  "entity_id": "work_unit_id",
  "investment": {
    "themes": {
      "operational": 0.47,
      "feature_delivery": 0.33,
      "maintenance": 0.15,
      "quality": 0.05
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
    "textual": [...],
    "structural": [...],
    "contextual": [...]
  }
}
```

### Guarantees

- Subcategory probabilities sum to theme probabilities
- Theme probabilities sum to ~1.0
- WorkUnit never appears as a category

---

## 5. UX implications (important but simple)

### Initial Investment View (default)

- **Theme-level only**
- Clean, high-level, leadership-readable

Treemap / Sunburst / Sankey:

- operate on **themes**, not WorkUnits

### Drill-down

- Theme → Subcategory
- Subcategory → evidence (WorkUnits)

This gives you:

- Executive clarity first
- Operational insight second
- Evidence always available, never dominant

---

## 6. LLM categorization contract (updated)

The LLM now has a **two-stage mapping responsibility**:

1. Map text → subcategory distribution
2. Roll subcategories → themes deterministically

### LLM rules (tightened)

- LLM outputs **subcategories only**
- Theme roll-up is deterministic, non-LLM
- LLM must cite phrases supporting each subcategory
- LLM must emit uncertainty per subcategory cluster

This prevents category drift and keeps normalization intact.

---

## 7. PRD amendment (add this section)

### New PRD Section: Categorization Layers

> Investment categorization is multi-layered.
>
> - WorkUnits are evidence containers, not categories.
> - Categorization operates at two semantic layers:
>
>   - Themes (investment categories)
>   - Subcategories (flavors of investment)
>
> Themes are the primary lens for leadership.
> Subcategories provide necessary resolution without fragmenting language.
>
> Evidence is always inspectable but never the organizing principle.

---

## 8. Migration prompt updates (critical)

### Backend migration prompt — ADDENDUM

```text
IMPORTANT CORRECTION:

WorkUnits are evidence, not categories.

TASKS:
- Remove any logic treating WorkUnits as categorization layers
- Introduce a two-layer categorization:
  - Theme (primary investment category)
  - Subcategory (secondary, rolls up to theme)
- Ensure LLM outputs subcategory distributions only
- Roll subcategories into themes deterministically
- Update schemas and APIs accordingly

DO NOT:
- Expose WorkUnits as categories
- Allow user-defined categories or subcategories
```

---

### Frontend migration prompt — ADDENDUM

```text
IMPORTANT CORRECTION:

WorkUnits are evidence, not categories.

TASKS:
- Make theme the default and primary view
- Support drill-down from theme → subcategory → evidence
- Remove any visualization that treats WorkUnits as peers to categories
- Ensure charts always roll up by theme unless explicitly drilled

DO NOT:
- Show WorkUnits as top-level segments
- Label evidence as categories
```

---

## 9. Why this matters (plain truth)

Without subcategories:

- “Operational” becomes too blunt
- Leadership shrugs
- Action stalls

Without separating evidence from categories:

- UI becomes noisy
- Humans misread structure as meaning
- You lose trust

This change keeps:

- normalization
- comparability
- people-centric insight

while giving the system enough semantic resolution to be **useful**, not just principled.
