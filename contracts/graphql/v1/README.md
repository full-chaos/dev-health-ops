# GraphQL SDL contract (v1)

`schema.graphql` in this directory is the **canonical, CI-checked** export
of the Strawberry GraphQL schema (`src/dev_health_ops/api/graphql/schema.py`).
It is generated, never hand-edited.

## Why this exists

CHAOS-4366 (Go API epic Wave 0) requires the invariant

```
Strawberry export == checked-in canonical SDL == gqlgen input SDL == web codegen SDL
```

to be a real CI gate, not a convention. Before this pin existed, the only
drift check lived downstream in the `web` repo's `live-e2e.yml`, and it
silently *skipped* (exit 0) if the Python export step failed for any
reason — the exact "measurement that did not happen must FAIL, loudly"
failure shape root `AGENTS.md` calls out. This pin moves the authoritative
check into this repo's own unmarked unit-test suite
(`tests/api/graphql/test_schema_sdl_pinned.py`), which `ci/local_validate.sh`
runs in full on every push — so drift is caught here, at the source, not
only (optionally) downstream.

## Regenerating

```bash
PYTHONPATH=src .venv/bin/python -m dev_health_ops.api.graphql.export_schema \
  --out contracts/graphql/v1/schema.graphql
```

Commit the regenerated file in the same PR as the schema change that
produced the diff. Review the diff — this file is a contract, not a build
artifact to rubber-stamp.

## Consumers

- **Web codegen** (`dev-health/web/codegen.ts`): GraphQL Code Generator
  points its `schema:` field at a copy of this file
  (`web/src/lib/graphql/schema.graphql`). Web's own CI
  (`live-e2e.yml`) diffs that copy against a fresh export from this repo;
  when this file changes, regenerate and commit the web copy too, and run
  web's codegen to refresh generated TS types.
- **`query-api`** (Go, `ops/cmd/query-api`, gqlgen schema-first): gqlgen's
  code generation takes this same SDL as its input schema. See
  `ops/cmd/query-api/README.md` (or the equivalent docs page once wired)
  for the exact gqlgen config pointing here.

## Known gap tracked separately

Web's `live-e2e.yml` schema-drift step currently does `exit 0` with a
warning if the Python `export_schema` import fails, instead of hard-failing
— so a broken import on that side reads as "no drift" rather than "drift
check did not run." That is a web-repo change, out of scope for this PR;
filed as a follow-up (see CHAOS-4366 PR discussion) rather than fixed here
silently.
