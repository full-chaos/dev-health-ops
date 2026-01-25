# Agent Instructions — Deep Dive Documentation

This directory contains detailed documentation for AI agents and developers working on the Dev Health platform.

**Start here:** [`/AGENTS.md`](../../AGENTS.md) — Canonical summarized agent lookup

---

## Directory Structure

```
agent-instructions/
├── architecture/           # System architecture documentation
│   ├── data-pipeline.md    # Backend pipeline (connectors → sinks)
│   └── frontend-architecture.md  # Next.js frontend patterns
│
├── product/                # Product specifications
│   ├── investment-view.md  # Investment View product spec
│   └── work-graph.md       # Work Graph contract
│
├── metrics/                # Metrics system documentation
│   ├── canonical-metrics.md    # Metric registry and API mapping
│   └── metric-calculations.md  # Calculation formulas and edge cases
│
├── visualizations/         # Visualization guidelines
│   └── visualization-patterns.md  # Chart selection, quadrants, guardrails
│
├── connectors/             # Data connector documentation
│   ├── github-gitlab.md    # GitHub/GitLab connectors
│   ├── jira.md             # Jira connector
│   └── atlassian-graphql.md    # Atlassian GraphQL Gateway client
│
├── llm/                    # LLM usage specifications
│   └── categorization-contract.md  # Compute-time and UX-time LLM rules
│
└── workflows/              # Developer workflows
    └── cli-reference.md    # CLI command reference
```

---

## Document Index

### Architecture

| Document | Description |
|----------|-------------|
| [data-pipeline.md](architecture/data-pipeline.md) | Backend pipeline stages, storage backends, sink interface |
| [frontend-architecture.md](architecture/frontend-architecture.md) | Next.js structure, component patterns, testing |

### Product

| Document | Description |
|----------|-------------|
| [investment-view.md](product/investment-view.md) | Investment View PRD, canonical themes, data model |
| [work-graph.md](product/work-graph.md) | Work Graph contract, WorkUnit definition, materialization rules |

### Metrics

| Document | Description |
|----------|-------------|
| [canonical-metrics.md](metrics/canonical-metrics.md) | Metric registry, API mapping, database schema |
| [metric-calculations.md](metrics/metric-calculations.md) | Formulas, bucketing, edge cases, null handling |

### Visualizations

| Document | Description |
|----------|-------------|
| [visualization-patterns.md](visualizations/visualization-patterns.md) | Chart selection, quadrants, guardrails, drill-down patterns |

### Connectors

| Document | Description |
|----------|-------------|
| [github-gitlab.md](connectors/github-gitlab.md) | Git provider setup, auth, batch processing |
| [jira.md](connectors/jira.md) | Jira Cloud setup, status normalization, team mapping |
| [atlassian-graphql.md](connectors/atlassian-graphql.md) | AGG client architecture, rate limiting, schema evolution |

### LLM

| Document | Description |
|----------|-------------|
| [categorization-contract.md](llm/categorization-contract.md) | Compute-time/UX-time rules, prompts, language constraints |

### Workflows

| Document | Description |
|----------|-------------|
| [cli-reference.md](workflows/cli-reference.md) | Complete CLI command reference |

---

## How to Use This Documentation

### For AI Agents

1. **Start with** [`/AGENTS.md`](../../AGENTS.md) for the summarized contract
2. **Deep dive** into specific topics in this directory as needed
3. **Reference** during implementation for constraints and patterns

### For Developers

1. **Architecture overview** → `architecture/` directory
2. **Metrics implementation** → `metrics/` directory
3. **Connector setup** → `connectors/` directory

### Navigation Pattern

```
/AGENTS.md (summary)
    ↓
docs/agent-instructions/<topic>/<document>.md (deep dive)
    ↓
Sub-project code and tests
```

---

## Relationship to Sub-Project AGENTS.md

Each sub-project has its own `AGENTS.md` with project-specific details:

- `dev-health-ops/AGENTS.md` — Backend specifics
- `dev-health-web/AGENTS.md` — Frontend specifics
- `atlassian/AGENTS.md` — GraphQL client specifics

The root `/AGENTS.md` consolidates cross-cutting concerns and serves as the canonical starting point.

---

## Contributing

When adding documentation:

1. Place in appropriate category directory
2. Update this README index
3. Link from root `/AGENTS.md` if broadly applicable
4. Follow existing formatting patterns
