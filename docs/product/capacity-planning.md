# Capacity Planning

Forecast work completion using Monte Carlo simulation on historical throughput data.

## Overview

Capacity planning answers the question: **"When will we finish N items?"** or **"How many items can we complete by date X?"**

It uses historical throughput variance to simulate many possible futures, providing completion estimates with confidence intervals (50%, 85%, 95%).

## Core Concepts

### Throughput Distribution

Historical throughput is measured as `items_completed` per day/week from `work_item_metrics_daily`. The distribution captures:

- **Mean throughput**: Average completion rate
- **Variance**: Day-to-day fluctuation
- **Trends**: Acceleration or deceleration over time

### Monte Carlo Simulation

Rather than a single-point estimate, Monte Carlo simulation runs thousands of scenarios:

1. Sample from historical throughput distribution
2. Accumulate completions until target reached
3. Record the completion date
4. Repeat 10,000+ times
5. Calculate percentiles for confidence intervals

### Confidence Intervals

| Percentile | Meaning |
|------------|---------|
| **P50** | 50% chance of completing by this date (optimistic) |
| **P85** | 85% chance - reasonable planning target |
| **P95** | 95% chance - conservative estimate with buffer |

## Data Model

### Input: Historical Throughput

Source: `work_item_metrics_daily`

```sql
SELECT day, team_id, work_scope_id, items_completed
FROM work_item_metrics_daily
WHERE day >= today() - INTERVAL 90 DAY
ORDER BY day
```

### Input: Backlog

Source: `work_items` (open items)

```sql
SELECT COUNT(*) as backlog_size
FROM work_items
WHERE status NOT IN ('done', 'closed', 'cancelled')
  AND team_id = :team_id
```

### Output: Forecast Record

New table: `capacity_forecasts`

| Column | Type | Description |
|--------|------|-------------|
| `forecast_id` | String | Unique identifier |
| `computed_at` | DateTime | When forecast was computed |
| `team_id` | String | Team scope |
| `work_scope_id` | String | Project/board scope |
| `backlog_size` | Int | Items remaining at forecast time |
| `target_items` | Int | Items to complete (if fixed-scope) |
| `target_date` | Date | Target deadline (if fixed-date) |
| `p50_date` | Date | 50th percentile completion date |
| `p85_date` | Date | 85th percentile completion date |
| `p95_date` | Date | 95th percentile completion date |
| `p50_items` | Int | Items completable by target_date (50th) |
| `p85_items` | Int | Items completable by target_date (85th) |
| `p95_items` | Int | Items completable by target_date (95th) |
| `throughput_mean` | Float | Historical mean throughput/day |
| `throughput_stddev` | Float | Historical stddev |
| `simulation_count` | Int | Number of simulations run |
| `history_days` | Int | Days of history used |

## API

### GraphQL Query

```graphql
type CapacityForecast {
  forecastId: ID!
  computedAt: DateTime!
  teamId: String
  workScopeId: String
  backlogSize: Int!
  
  # Fixed-scope: "When will we finish N items?"
  targetItems: Int
  p50Date: Date
  p85Date: Date
  p95Date: Date
  
  # Fixed-date: "How many items by date X?"
  targetDate: Date
  p50Items: Int
  p85Items: Int
  p95Items: Int
  
  # Throughput stats
  throughputMean: Float!
  throughputStddev: Float!
  historyDays: Int!
}

type Query {
  capacityForecast(
    teamId: String
    workScopeId: String
    targetItems: Int
    targetDate: Date
    historyDays: Int = 90
  ): CapacityForecast
  
  capacityForecasts(
    teamId: String
    fromDate: Date
    toDate: Date
    limit: Int = 10
  ): [CapacityForecast!]!
}
```

### CLI Command

```bash
# Compute forecast for a team
python cli.py forecast capacity \
  --team-id team-123 \
  --target-items 50 \
  --history-days 90 \
  --db "$DATABASE_URI"

# Compute for all teams
python cli.py forecast capacity \
  --all-teams \
  --db "$DATABASE_URI"
```

## Algorithm

### Monte Carlo Implementation

```python
def monte_carlo_forecast(
    throughput_history: List[float],  # Daily throughput samples
    target_items: int,
    simulations: int = 10000,
) -> ForecastResult:
    """
    Run Monte Carlo simulation for completion date.
    
    Returns percentile completion dates (p50, p85, p95).
    """
    completion_days = []
    
    for _ in range(simulations):
        remaining = target_items
        days = 0
        
        while remaining > 0:
            # Sample from historical throughput
            daily_throughput = random.choice(throughput_history)
            remaining -= daily_throughput
            days += 1
            
            # Safety: cap at 365 days
            if days > 365:
                break
        
        completion_days.append(days)
    
    return ForecastResult(
        p50_days=np.percentile(completion_days, 50),
        p85_days=np.percentile(completion_days, 85),
        p95_days=np.percentile(completion_days, 95),
    )
```

### Edge Cases

| Scenario | Handling |
|----------|----------|
| Zero throughput days | Include in distribution (realistic) |
| < 14 days history | Return warning, use available data |
| No history | Return error, cannot forecast |
| Extremely high variance | Flag in response, suggest longer history |

## Visualization

Located at `/work?tab=capacity` in dev-health-web.

### Burndown with Confidence Bands

Implemented as `ConfidenceBandChart` component showing three projection lines:

```
Items
  ^
50|████████████████████
  |  ████████████████████
  |    ██████████████████████  <- P95 (conservative - red)
  |      ████████████████████████
  |        ██████████████████████████  <- P85 (target - amber)
  |          ████████████████████████████
  |            ██████████████████████████████  <- P50 (optimistic - green)
  +-----------------------------------------> Days
              Today        P50    P85   P95
```

### Throughput Histogram

Implemented as `ThroughputHistogram` component showing:
- Bar chart of throughput distribution (normal approximation)
- Vertical line marking the mean
- Shaded region for ±1 standard deviation

### Forecast Summary Card

`ForecastCard` component displays:
- Backlog size and completion targets
- P50/P85/P95 completion dates
- Throughput statistics (mean ± stddev)
- Warning indicators for:
  - `insufficientHistory`: < 14 days of data
  - `highVariance`: stddev > mean (unreliable forecast)

## Implementation Phases

### Phase 1: Core Computation (Backend) ✅
- Monte Carlo simulation function
- Throughput history loader
- Backlog size query
- ForecastResult dataclass

### Phase 2: Storage & Persistence ✅
- `capacity_forecasts` table schema
- ClickHouse sink implementation
- SQLAlchemy sink implementation

### Phase 3: CLI & Jobs ✅
- `forecast capacity` CLI command
- Scheduled job for daily forecast refresh
- Team/scope iteration

### Phase 4: GraphQL API ✅
- `CapacityForecast` type
- `capacityForecast` query resolver
- `capacityForecasts` list query

### Phase 5: Frontend UI (dev-health-web) ✅
- **CapacityView component**: Main view at `/work?tab=capacity`
- **ForecastCard**: Summary card with P50/P85/P95 dates and warning indicators
- **ConfidenceBandChart**: ECharts-based burndown with confidence bands
- **ThroughputHistogram**: Distribution chart with mean/stddev overlays
- **GraphQL integration**: Uses `graphqlClient.query()` fetch pattern
- **Sample data**: Test mode fallback for Playwright tests

## Non-Goals (v1)

- Story point weighting (use item count only)
- Sprint/iteration boundaries
- Resource allocation optimization
- Individual contributor forecasts
- Real-time streaming updates

## References

- [Monte Carlo in Agile](https://www.focusedobjective.com/w/monte-carlo-simulation/)
- [Actionable Agile Metrics](https://actionableagile.com/monte-carlo/)
- Original issue: [#269](https://github.com/full-chaos/dev-health-ops/issues/269)
