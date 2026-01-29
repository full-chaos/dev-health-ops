# Capacity Planning View

Monte Carlo forecast for work completion at `/work?tab=capacity`.

## Components

### Forecast Card
Summary showing:
- **Backlog size**: Items remaining
- **P50/P85/P95 dates**: Completion estimates at different confidence levels
- **Throughput stats**: Mean ± standard deviation
- **Warnings**: Insufficient history or high variance indicators

### Confidence Band Chart
Burndown projection with three bands:
- **P50 (green)**: Optimistic - 50% chance
- **P85 (amber)**: Target - 85% confidence (recommended for planning)
- **P95 (red)**: Conservative - 95% confidence

### Throughput Histogram
Distribution of historical throughput with:
- Bar chart showing frequency distribution
- Vertical line at mean
- Shaded ±1σ region

## Interpretation

| Percentile | Use Case |
|------------|----------|
| P50 | Best-case scenario, internal targets |
| P85 | Recommended for external commitments |
| P95 | High-stakes deadlines, contracts |

## Warnings

- **Insufficient History**: Less than 14 days of data; forecast unreliable
- **High Variance**: Standard deviation exceeds mean; consider longer history window

## Data Requirements

Requires `work_item_metrics_daily` with `items_completed` data. Run:
```bash
python cli.py metrics daily --with-work-items
```
