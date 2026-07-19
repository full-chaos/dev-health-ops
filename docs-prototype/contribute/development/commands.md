---
page_id: con-commands
summary: Use repository-defined commands and inspect help before running state-changing operations.
content_type: task-guide
owner: engineering
source_of_truth:
  - Makefile
  - pyproject.toml
  - package.json
  - current CI workflows
applicability: current
lifecycle: active
---

# Common development commands

Use the repository's current Make targets, package scripts, CLI `--help`, and workflow definitions as the exact command source.

Typical validation families are:

- formatting and lint;
- static type checking;
- unit and integration tests;
- strict documentation build and link checks;
- Docker or deployment build;
- live-backend or production-like E2E where the change requires it;
- governance and security checks.

Run the narrowest relevant command first, then the same aggregate gate CI will run. Do not copy a command from an old issue if the repository target has changed.
