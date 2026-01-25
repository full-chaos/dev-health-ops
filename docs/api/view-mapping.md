# View-to-API mapping

This is the contract between UI charts and backend query surfaces.

## Investment Mix (treemap/sunburst)
- GraphQL: analytics → breakdowns
- Backend: `api/graphql/resolvers/analytics.py`
- Compiler: `api/graphql/sql/compiler.py::compile_breakdown`
- Dimensions: THEME, SUBCATEGORY
- Measure: effort/count (depends on chart config)

## Investment Expense (stacked area)
- GraphQL: analytics → timeseries
- Compiler: `compile_timeseries`

## Investment Flows (sankey)
- GraphQL: analytics → sankey
- Compiler: `compile_sankey`
- Limits: `validate_sankey_limits`

## Hotspots
- GraphQL: breakdowns or timeseries depending on visualization
- Backend: analytics resolver + SQL compiler

## Explore: Flame diagrams
- Input: “representative point” selection
- Output: hierarchical breakdown suitable for flame render
- Server: (add explicit endpoint if not already present; otherwise a persisted breakdown query)
