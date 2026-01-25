# Work Graph

The work graph is the structure that links evidence and supports drill-down.

## What it is
A graph-like model of WorkUnits and relationships:
- work item ↔ PR ↔ commits ↔ files ↔ incidents (when present)

## Why it exists
- Enables explainability without recomputation.
- Provides drill-down paths from aggregates to evidence.
- Supports flow and investment distribution materialization.

## What it is not
- A replacement for provider-native objects.
- A scoring layer.
