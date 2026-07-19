# Phase 7 operations evidence

The operator section consolidates supported deployment and runbook source material into the locked `/operate/` IA.

## Source families

- deployment artifacts: `compose.yml`, `deploy/docker-compose/`, `deploy/docker-swarm/`, `deploy/kubernetes/`;
- deployment and self-hosted guidance: `docs/ops/deployment-guide.md`, `docs/self-hosted-quickstart.md`;
- workers and materialization: `docs/ops/workers.md`, `docs/ops/investment-materialization.md`;
- observability and objectives: `docs/ops/observability-tooling.md`, `docs/alerting.md`, `docs/slos.md`;
- sync and budget observability: current architecture and runtime controls;
- security: credential encryption and rotation implementation;
- runbooks: ingestion and report failure source material.

## Safety rule

The public v2 pages describe supported decisions, checks, boundaries, and evidence. They intentionally omit unverified one-line destructive commands. Exact commands, versions, and configuration keys must be generated or checked from the current reviewed release before production execution.
