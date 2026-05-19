# Compute AI flow and operating leverage metrics

## Goal

Measure whether AI-assisted work improves delivery flow or creates downstream drag.

## Metrics

- AI-assisted PR ratio
- AI-assisted commit ratio
- Agent-created PR count
- AI cycle-time delta
- AI coding-time compression
- AI review-time expansion
- AI throughput lift
- AI merge confidence
- Net AI Operating Leverage

## Executive metric

```text
AI Operating Leverage = delivery lift - review amplification - rework drag - defect/revert drag - incident drag
```

This must be decomposable. Do not expose it as a black-box score.

## Acceptance criteria

- Metrics can be grouped by org, team, repo, time range, and work type
- AI-assisted work can be compared against non-AI baselines
- Metric outputs include enough raw components to explain the result
- Metrics are trendable over time
- Null and unknown attribution states are handled explicitly
