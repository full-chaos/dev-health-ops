---
page_id: con-repos
summary: Route a change to the repository and module that owns its public contract.
content_type: concept
owner: engineering
source_of_truth:
  - docs/architecture/repo-layout.md
  - current repository roots and AGENTS.md
applicability: current
lifecycle: active
---

# Choose the correct repository

## `dev-health-ops`

Owns Python services and contracts under `src/dev_health_ops/`, including FastAPI and GraphQL, connectors and providers, normalization, storage, metrics, Work Graph and Investment compute, workers, migrations, fixtures, CLI, credentials, and licensing.

## `dev-health-web`

Owns Next.js routes, product navigation, UI state, charts, GraphQL client behavior, accessibility, and browser interactions.

## Documentation boundary

Documentation source and publication currently live with the operations repository while product route and visual truth can live in the web repository. A documentation change may require evidence from both, but it still has one canonical page.

Read the nearest `AGENTS.md` before editing. Repository guidance is authoritative for local workflow but is not automatically public documentation.
