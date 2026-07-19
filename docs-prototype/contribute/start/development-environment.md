---
page_id: con-setup
summary: Install the repository toolchain, start isolated dependencies, migrate stores, and verify a bounded change.
content_type: tutorial
owner: engineering
source_of_truth:
  - pyproject.toml
  - requirements files
  - compose.yml
  - current CI workflows
applicability: current
lifecycle: active
---

# Set up a development environment

1. Check out the intended repository and reviewed base branch.
2. Read the root and nearest directory-level contributor guidance.
3. Install the supported Python or Node toolchain from repository manifests.
4. Start isolated Postgres, ClickHouse, Redis/Valkey, and other required services through the current Compose or test fixture.
5. Set development-only environment values; never reuse production credentials.
6. Apply Postgres and ClickHouse migrations before sync or metrics work when required.
7. Run the narrowest unit, type, lint, build, and live-like checks for the change.
8. Confirm no generated or migration output is left unstaged unintentionally.

For documentation work, install `requirements-docs.txt` and use the strict MkDocs build configured by the repository workflow.
