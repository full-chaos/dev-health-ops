# GraphQL SDL contract (v1)

`schema.graphql` in this directory is the **canonical, CI-checked** GraphQL
schema. The **Go plane owns it**: gqlgen generates query-api's executable
schema from this file (schema-first). It is edited directly and is **never
regenerated from the Python (Strawberry) export** -- that would erase every
type Go added that Python never had. The Python schema must be a **subset**
of it until CHAOS-6264 deletes Python.

## Why this exists

CHAOS-4366 (Go API epic Wave 0) requires the invariant

```
Python schema ⊆ checked-in canonical SDL == gqlgen input SDL == web codegen SDL
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

## Changing the schema

1. Edit `schema.graphql`.
2. Regenerate through `go run ./cmd/gqlgen-guard generate` (never `go generate`, which has no directive here; see internal/queryapi/server/README.md "Regeneration" for the expected-drift record) and commit the diff;
   `TestGeneratedSchemaSourceIsTheCheckedInPin` fails until you do.
3. Update `schema-digest.json` and the schema-digest history row (the
   digest is the sha256 of this file's raw bytes; see
   `docs/contribute/architecture/go-api-wave-0-proof-infrastructure.md`,
   "When the schema digest moves"). `test_go_api_schema_digest.py` and
   `ci/check_go_api_routing_digest.py` fail until you do.
4. If Python also exposes the member, it must already be in the file:
   `tests/api/graphql/test_schema_sdl_pinned.py` fails when the Python
   schema has a type, field, argument, enum value, union member or
   interface the file lacks. Extra members in the file are allowed.

Review the diff -- this file is a contract, not a build artifact.

## Consumers

- **Web codegen** (`dev-health/web/codegen.ts`): GraphQL Code Generator
  points its `schema:` field at a copy of this file
  (`web/src/lib/graphql/schema.graphql`). Web's own CI
  (`live-e2e.yml`) diffs that copy against a fresh export from this repo;
  when this file changes, regenerate and commit the web copy too, and run
  web's codegen to refresh generated TS types.
- **`query-api`** (Go, `dho query-api`, `ops/internal/queryapi`, gqlgen schema-first): gqlgen's
  code generation takes this same SDL as its input schema. See
  `ops/internal/queryapi/server/README.md` (or the equivalent docs page once wired)
  for the exact gqlgen config pointing here.

## Known gap tracked separately

Web's `live-e2e.yml` schema-drift step currently does `exit 0` with a
warning if the Python `export_schema` import fails, instead of hard-failing
— so a broken import on that side reads as "no drift" rather than "drift
check did not run." That is a web-repo change, out of scope for this PR;
filed as a follow-up (see CHAOS-4366 PR discussion) rather than fixed here
silently.
