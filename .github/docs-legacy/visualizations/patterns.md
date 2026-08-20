# Visualization Patterns

Guidelines for selecting and implementing visualizations in Dev Health.

---

## Chart Selection Matrix

| Need | Chart Type | When to Use |
|------|------------|-------------|
| Pattern detection | Heatmap | Cyclical congestion, fragmentation, risk build-up |
| Single trend/KPI | Line chart | Directional movement, simple comparisons |
| System state | Quadrant | Mode classification, trajectory analysis |
| Single item diagnosis | Flame diagram | Understanding delays in one work item |
| Investment composition | Treemap | Where effort is allocated by theme |
| Investment distribution | Sunburst | Concentration and who absorbs it |
| Investment flow | Sankey | Pressure flows between categories and teams |

---

## Heatmaps

### When to Use

- **Cyclical congestion:** Hour-of-day × weekday patterns
- **Fragmentation:** Work spread across repos/modules/systems
- **Risk build-up:** Hotspots intensifying over time

### When NOT to Use

- Single trend line needed
- Executive summaries without dimensional breakdown
- Cannot trace cells to real artifacts

### Implementation Rules

- Every cell must link to evidence for that bucket
- If you cannot trace a cell to artifacts, use line chart instead
- Use consistent color scales across related views
- Include legends and axis labels

---

## Line Charts

### When to Use

- Clear directional movement over time (e.g., median cycle time)
- Simple before/after comparisons
- Executive-level summaries
- KPI tracking

### When NOT to Use

- Multi-dimensional pattern detection needed
- Averages would hide important patterns
- Need to identify specific problematic periods

---

## Quadrants

### Purpose

Classify system modes without ranking or judging performance.

### Questions They Answer

- What mode is this team/repo operating in right now?
- Which direction are we moving across recent windows?
- Where do effort and outcomes drift out of alignment?

### Usage Rules

- Use when reasoning about system behavior, not single trends
- Quadrants are **hypothesis starters**, not conclusions
- Always link to evidence
- Follow up with heatmaps or flame diagrams for diagnosis

### Required Quadrants

#### 1. Churn × Throughput

| Quadrant | Interpretation |
|----------|----------------|
| High churn + High throughput | Deliberate refactors, major changes |
| High churn + Low throughput | Thrashing, rework loops |
| Low churn + High throughput | Efficient delivery |
| Low churn + Low throughput | Constraints or hidden blocks |

#### 2. Cycle Time × Throughput

| Quadrant | Interpretation |
|----------|----------------|
| Rising cycle + Stable throughput | Coordination debt accumulating |
| Fast cycle + High throughput | Healthy flow |
| Slow cycle + Low throughput | System under stress |
| Fast cycle + Low throughput | Small batch, capacity available |

#### 3. WIP × Throughput

| Quadrant | Interpretation |
|----------|----------------|
| High WIP + Flat throughput | Early saturation warning |
| High WIP + High throughput | At capacity, watch for overload |
| Low WIP + High throughput | Efficient, focused |
| Low WIP + Low throughput | Underutilized or blocked |

#### 4. Review Load × Review Latency

| Quadrant | Interpretation |
|----------|----------------|
| High load + High latency | Structural bottleneck |
| High load + Low latency | Reviewer doing well, watch for burnout |
| Low load + High latency | Disengagement or competing priorities |
| Low load + Low latency | Healthy review process |

### Zone Maps (Experimental)

Optional overlays highlighting fuzzy, overlapping regions with common constraint patterns.

**Rules:**
- Zone maps are heuristics
- If they feel prescriptive, turn them off
- Never use for performance judgment

---

## Flame Diagrams

### When to Use

- Understanding where time was spent in **single work item**
- Visualizing waiting, rework, or handoffs
- Diagnosing whether delays were sequential or overlapped
- Identifying rework loops hidden in aggregate metrics

### When NOT to Use

- Fleet-wide signals needed
- Aggregate trend analysis
- Comparing across many items

### Relationship to Cycle Time

| Tool | Scope | Purpose |
|------|-------|---------|
| Flame diagram | Single item | Diagnostic, explain outlier |
| Cycle time | Portfolio | Strategic, prove trend |

Use flames to explain **why** a single item took long.
Use cycle time to prove the **trend** across the system.

---

## Investment Views

### Treemap

- **Nodes:** Theme-level (default) or theme × scope/team
- **Size:** Probability-weighted effort
- **Opacity:** Evidence quality
- **Purpose:** Show where effort goes, regardless of labels

### Sunburst

Hierarchical drill-down:
```
Theme
 └── Repo scope / Team
     └── Work clusters (optional)
```

**Purpose:** Show concentration and who absorbs investment.

### Sankey

- **Source:** Investment category (theme)
- **Target:** Repo scope / Team
- **Weight:** Probability-weighted effort
- **Purpose:** Make pressure flows visible

---

## Scope Guardrails

### Organization/Team/Repo Views

- Show teams, repos, or services only
- Never show individual rankings
- Aggregation is the default

### Individual Views

- Show **one person** across time windows
- Optionally show as a path (trajectory)
- Purpose: Reflection and coaching
- Never comparative

### Forbidden Patterns

- Person-to-person rankings or comparisons
- "Top performers" / "Bottom performers" lists
- Stack-ranking visualizations
- Public leaderboards

---

## Guardrails Summary

| Rule | Rationale |
|------|-----------|
| No person-to-person rankings | Prevents misuse for judgment |
| Individual views are single-person only | For reflection, not comparison |
| Heatmap cells trace to evidence | Maintains inspectability |
| Quadrants show raw values only | No percentiles or scoring |
| Flames for diagnosis, cycle time for trends | Right tool for right question |

---

## Drill-Down Patterns

### Recommended Flow

```
Quadrant → Heatmap slice → Flame diagram → Evidence
```

1. **Quadrant:** "What state are we in?"
2. **Heatmap:** "Where and when is pressure accumulating?"
3. **Flame:** "Why did this specific item take so long?"
4. **Evidence:** The actual artifacts (PRs, issues, commits)

### Implementation

- Every visualization should support drilling to the next level
- Evidence should always be the terminal destination
- Maintain context as user drills down
