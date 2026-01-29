# PR Flow

## What it answers
- Where work stalls (pickup, review, merge)?
- Is the bottleneck capacity or latency?

## Implementation

PR Flow is implemented through multiple complementary visualizations:

### 1. Flame Diagram (Single PR Timeline)
**Location**: `/prs/[pr_id]/page.tsx` → `FlameDiagram` component

Shows the lifecycle of a single PR with:
- **States**: active, waiting, blocked, ci
- **Categories**: planned, unplanned, rework
- Timeline visualization with color-coded segments
- Duration breakdown by state

Use case: Diagnose why a specific PR took long to merge.

### 2. State Flow Sankey
**Location**: `/work` → Flow tab → State Flow sub-tab

Shows work item state transitions across the team:
- Source nodes: initial states
- Target nodes: terminal states
- Link width: volume of transitions
- Identifies common flow paths and bottlenecks

Use case: See where work commonly gets stuck across all items.

### 3. Review Heatmap
**Location**: `/work` → Heatmap tab

Shows review wait patterns by time:
- X-axis: time of day
- Y-axis: day of week
- Cell intensity: review wait density

Use case: Identify when review latency is highest.

## Backing computations
- Stage timestamp extraction per provider (pr_created_at, first_review_at, merged_at)
- Windowed aggregation by scope
- State duration calculations in `work_item_state_durations_daily`

## API Endpoints
- `GET /api/v1/flame?entity_type=pr&entity_id={id}` - Single PR timeline
- `POST /api/v1/sankey` with `mode=state` - State transitions
- `GET /api/v1/heatmap?type=temporal_load&metric=review_wait_density` - Review patterns
