# Repo layout and stability

## dev-health-ops
Current repo layout places Python packages at repo root (e.g., `api/`, `metrics/`, `work_graph/`).

A planned update will move Python code under `./src`. This should **not** impact these docs because:
- Docs reference **logical modules** and stable public entry points (CLI, GraphQL schema, sinks contracts).
- File-path references are used for navigation only; treat them as “as of commit”.

### Key module roots (today)
- `cli.py` (entry point)
- `connectors/` (provider sync jobs)
- `processors/` (normalization)
- `metrics/` (compute + sinks)
- `work_graph/` (investment compute + materialization)
- `api/graphql/` (GraphQL analytics API)

## dev-health-web
- `src/app` (routes/pages)
- `src/lib/graphql` (client provider, hooks)
- `src/components` (charts and UI)
