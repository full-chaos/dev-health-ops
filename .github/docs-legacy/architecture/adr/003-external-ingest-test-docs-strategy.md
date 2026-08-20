# ADR-003: External Ingest Test & Docs Strategy

**Status**: DECIDED
**Created**: 2026-07-02
**Updated**: 2026-07-02
**Parent Issue**: CHAOS-2711 (epic: CHAOS-2690 External customer-push ingestion API)

## Context

CHAOS-2690's customer-push ingestion API was implemented across ten sub-issues
(CHAOS-2691 through CHAOS-2700). Two things needed a decision once the implementation had
landed: how the developer/user documentation is authored and kept from drifting off the real
API, and how the single-source-of-truth example payloads are shared between docs, the CLI, and
the (separately owned) end-to-end test.

This ADR records those decisions per the house rule that decisions get documented in the same
changeset that makes them.

---

## Decision 1: Docs are written against merged code, not the pre-implementation brief

### Context

CHAOS-2711 was originally scoped against `docs/superpowers/plans/2026-07-01-chaos-2690-
implementation/briefs/brief-2702-e2e-docs.md`, written before any of CHAOS-2691-2700 existed.
That brief's §2 "pinned contract" was explicitly a placeholder ("if a sibling issue's
implementation drifts from this contract, update this brief's Section 2, not the other way
around" — a note aimed at keeping CHAOS-2702's E2E test buildable before dependencies landed).

By the time CHAOS-2711 was implemented, all ten sibling issues were merged. Several details in
the brief's pinned contract differ from what actually shipped:

- The brief's error example used `code: "missing_external_id"`; the real vocabulary
  (`dev_health_ops.external_ingest.validate`) is `missing_required_field` / `invalid_literal` /
  `invalid_field` / `unknown_kind`.
- The brief's Postgres sketch (`customer_push_sources`, `ingest_tokens`, snake_case tables)
  differs from the real schema (`external_ingest_sources`, `external_ingest_tokens`, real
  migration numbers, `Mapped[...]`/SQLAlchemy models in
  `dev_health_ops/models/ingest_auth.py`).
- The brief's status vocabulary omitted `stream_unavailable`, a real, documented interim status
  between `accepted` and `processing`.
- ADR numbering: the brief assumed a flat `docs/architecture/adr-006-...` naming scheme
  (following siblings' `adr-003`/`adr-004`/`adr-005` files). This ADR instead uses the
  repo's other existing ADR convention — the numbered subdirectory
  `docs/architecture/adr/NNN-*.md` (see ADR-001) — since that's the format this issue was
  scoped to follow. Both numbering schemes now coexist in this repo; a future cleanup could
  unify them, but is out of scope here.

### Decision

**Document the real merged implementation, not the brief.** Every endpoint, field name,
status code, error code, and scope in the shipped docs
(`docs/customer-push-ingestion/*.md`) was verified directly against source
(`src/dev_health_ops/api/external_ingest/*.py`, `src/dev_health_ops/external_ingest/*.py`,
`src/dev_health_ops/models/ingest_auth.py`, `src/dev_health_ops/push/cli.py`) — never
transcribed from the brief. Where the two disagree, source wins.

## Decision 2: Canonical examples are shared via `pymdownx.snippets`, not retyped

### Context

CHAOS-2692 already ships 9 canonical, schema-registry-served example payloads at
`src/dev_health_ops/api/external_ingest/examples/<kind>.v1.json` — these are also what
`GET /schemas/{version}` embeds per-kind and what `dev-hops push sample --kind <kind>` prints.
Hand-retyping them into the schema-reference doc would let the doc drift from the actual
packaged fixtures the first time either changed.

### Decision

`mkdocs.yml`'s `markdown_extensions` gained `pymdownx.snippets`, scoped via `base_path` to
`src/dev_health_ops/api/external_ingest/examples`. `docs/customer-push-ingestion/
schemas-and-idempotency.md` includes each example with `--8<-- "<kind>.v1.json"` inside a
fenced code block, so the doc always renders the exact bytes shipped in the package. CHAOS-2701
(fixture/example drift tests) owns keeping the *package* examples themselves valid; this ADR
only records that the docs reference them by inclusion, never by copy.

`mkdocs.yml` is edited exclusively by this issue for the whole CHAOS-2690 epic (nav section +
this extension) — sibling issues do not touch it.

## Decision 3: Docs scope explicitly excludes the E2E test and CI/CD runnable examples

### Context

CHAOS-2702 (E2E test) and CHAOS-2713 (CI/CD runnable examples) are separate sub-issues in the
same epic. Both touch adjacent ground: CHAOS-2702 exercises the exact lifecycle this issue
documents (`validate → batch → stream → worker → sinks → status → bounded recompute`);
CHAOS-2713 will ship runnable GitHub Actions/GitLab CI examples that go beyond the doc snippets
here.

### Decision

`docs/customer-push-ingestion/*.md` documents the API surface and lifecycle *prose*, and links
out rather than duplicating: CI/CD runnable pipeline examples are marked "see CHAOS-2713" rather
than authored here, and the authorization *design rationale* (why tokens are scoped/bound the
way they are) is left to CHAOS-2712 — these docs describe the resulting credential UX
(register a source, mint a scoped token, rotate it) rather than re-litigating that design.

---

## Summary of Decisions

| Decision | Status |
|---|---|
| Document merged code, not the pre-implementation brief | **DECIDED** |
| Share canonical examples via `pymdownx.snippets`, base_path = the examples package | **DECIDED** |
| Docs link to, not duplicate, CHAOS-2702 (E2E)/CHAOS-2713 (CI/CD examples)/CHAOS-2712 (authz design) | **DECIDED** |

## Changelog

| Date | Change |
|------|--------|
| 2026-07-02 | Initial version, recording the docs-vs-brief and snippet-sharing decisions made while implementing CHAOS-2711. |
