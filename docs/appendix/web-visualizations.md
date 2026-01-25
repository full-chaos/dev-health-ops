# Visualizations Guide

## Heatmaps vs line charts

Use heatmaps when you need to detect patterns that averages hide:

- Cyclical congestion (hour-of-day x weekday) where work accumulates or stalls.
- Fragmentation across repos, modules, or systems where attention is split.
- Risk build-up across files or services where hotspots intensify over time.

Use line charts when you need a single trend line or KPI trajectory:

- Clear directional movement over time (e.g., median cycle time).
- Simple comparisons between time windows without multi-dimensional breakdowns.
- Executive-level summaries where detailed context is unnecessary.

Heatmaps should always link to evidence for the selected bucket. If you cannot
trace a cell to real artifacts, use a line chart instead.

## Quadrants: state and trajectory

Quadrants classify system modes without ranking or judging performance. They
help answer:

- What mode is this team or repo operating in right now?
- Which direction are we moving across recent windows?
- Where do effort and outcomes drift out of alignment?

Use quadrants when you need to reason about system behavior rather than a single
trend line. Quadrants are hypothesis starters; they should always link to
evidence and follow up with heatmaps or flame diagrams for diagnosis.

### Required quadrants

1) Churn × Throughput
   - Detect thrashing vs stable progress.
   - High churn + high throughput can indicate deliberate refactors.
   - Low churn + low throughput can signal constraints or hidden blocks.

2) Cycle Time × Throughput
   - Rising cycle time with stable throughput indicates coordination debt.
   - Slow-and-steady systems can mask accumulating queues.

3) WIP × Throughput
   - Flat throughput with rising WIP is an early saturation warning.
   - Use to spot local optimization before teams feel pain.

4) Review Load × Review Latency
   - Identify structural review bottlenecks and reviewer concentration.
   - Pair with flame diagrams to see handoff loops.

### Zone maps (experimental)

Zone maps are optional overlays that highlight fuzzy, overlapping regions with
common constraint patterns derived from observed metrics. Zone maps are
heuristics. If they feel prescriptive, turn them off.

### Scope guardrails

- Org/team/repo views show teams, repos, or services only.
- Individual views show one person across time windows, optionally as a path.
- Never label quadrants as good/bad, high/low performer, or rankings.

## Flame diagrams vs cycle time

Use flame diagrams when you need to understand where time was spent in a single
work item:

- Visualize waiting, rework, or handoffs inside an issue, PR, or deployment.
- Diagnose whether delays were sequential or overlapped.
- Identify rework loops and review churn hidden in aggregate cycle time.

Use cycle time metrics when you want a fleet-wide signal:

- Track aggregate performance across a team or system.
- Monitor improvement or regression over time.
- Compare before/after windows at a high level.

Flame diagrams are diagnostic and item-scoped; cycle time is strategic and
portfolio-scoped. Use flames to explain a single outlier, and cycle time to
prove the trend.

## Guardrails

- No person-to-person rankings or comparisons in heatmaps or flame diagrams.
- Individual views are single-person only, for reflection and coaching.
- Heatmap cells should remain explorable down to evidence artifacts.
- Quadrants must show raw values only, no percentiles or scoring.

## Relationship map

Quadrants answer: “What state are we in?”

Heatmaps answer: “Where and when is pressure accumulating?”

Flame diagrams answer: “Why did this specific item take so long?”

Use them together: Quadrant → heatmap slice → flame diagram → evidence.
