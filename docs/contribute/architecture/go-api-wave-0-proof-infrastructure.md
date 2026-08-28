---
page_id: con-go-api-wave-0-proof-infrastructure
summary: The effective-principal envelope contract and the operation rollout registry + proof ledger that Wave 0 of the Go API epic (CHAOS-4366/CHAOS-4352) builds before any GraphQL resolver is ported to Go.
content_type: architecture
owner: engineering
source_of_truth:
  - .github/docs-legacy/plans/go-api-epic.md (the epic plan; this page documents two of its pieces in the customer-nav-visible docs tree)
  - .github/docs-legacy/plans/chaos-4381-parity-rules-proposal.md (comparator parity rules, pending chris sign-off)
  - src/dev_health_ops/api/graphql/principal_envelope.py (envelope issuer)
  - src/dev_health_ops/models/go_api_registry.py (registry + ledger schema)
  - src/dev_health_ops/alembic/versions/0114_add_go_api_operation_registry.py
  - contracts/graphql/v1/schema.graphql (canonical SDL pin)
applicability: current
lifecycle: active
---

# Go API Wave 0: proof infrastructure

No GraphQL resolver moves from Python to Go until this exists (plan §5,
§6). Wave 0 builds two pieces of durable infrastructure covered here: the
signed effective-principal envelope the Python edge issues to `query-api`,
and the operation rollout registry + proof ledger that will gate every
future cutover. It does not port any resolver, and nothing on a live
request path calls either piece yet.
{: .fc-page-lede }

## Effective-principal envelope

`query-api` (Go) does not independently re-derive auth state from
Postgres/Valkey. chris's ruling (CHAOS-4379, 2026-08-27): the Python edge
issues a short-lived, audience-bound, SIGNED envelope that reproduces the
`graphql/authz.py` + `graphql/app.py` + `services/auth.py` contract —
disabled-user and token-version revocation, org-switch membership, active
impersonation, and tier fallback are all part of it, not a bare JWT
signature check.

```mermaid
sequenceDiagram
    participant Client
    participant Edge as Python edge (FastAPI)
    participant Auth as AuthService.authenticate_access_token<br/>(DB-backed: disabled, token_version)
    participant Envelope as principal_envelope.issue_effective_principal_envelope
    participant QueryAPI as query-api (Go)

    Client->>Edge: request + user access JWT
    Edge->>Auth: authenticate_access_token(token)
    Auth-->>Edge: AuthenticatedUser (or None: reject)
    Edge->>Envelope: issue_effective_principal_envelope(user, tier, licensed_features)
    Envelope-->>Edge: signed envelope (EdDSA/Ed25519, TTL default 60s, aud=query-api)
    Edge->>QueryAPI: proxied/compared request + envelope
    QueryAPI->>QueryAPI: verify signature via JWKS (dev-health-go authverify)
    QueryAPI->>QueryAPI: check aud, exp, v (claim schema version)
```

### Claim schema (versioned, `v`)

The envelope's `v` claim is bumped whenever a claim is added, removed, or
its meaning changes. A verifier must reject an envelope whose `v` it was
not written to handle — the semantics above are expected to evolve before
`query-api` ever verifies a real request.

**v1** (current):

| Claim | Type | Meaning |
|---|---|---|
| `v` | int | Claim schema version (`1`) |
| `sub` | string | User id |
| `org_id` | string | Active org for this request |
| `role` | string | User's role in `org_id` |
| `is_superuser` | bool | Platform superuser flag |
| `is_superuser_verified` | bool | Superuser bit re-verified against the DB this request (mirrors `AuthenticatedUser.is_superuser_verified`) |
| `permissions` | string[] | Full resolved permission set (`services.permissions.get_user_permissions` — impersonation-aware: if impersonating, this is the TARGET role's permissions, not the real user's) |
| `token_version` | int | The token-version value that passed revocation check this request |
| `tier` | string | Resolved license tier (`services.licensing.resolve_org_tier` — `OrgLicense.tier` wins, else `Organization.tier`, else community) |
| `licensed_features` | string[] | Feature keys the org currently has access to |
| `impersonated_by` | string \| null | Real user id, when an admin/superuser is impersonating |
| `impersonation_active` | bool | Whether impersonation is active for this request |
| `iss` | string | `dev-health-ops-edge` (env-overridable) |
| `aud` | string | `query-api` (env-overridable per verifier) |
| `iat` / `exp` | int (unix) | Issued-at / expiry — default TTL 60s |
| `jti` | string | Unique per envelope |

There is deliberately **no `disabled` claim**. A disabled/deactivated user
never reaches the issuer: `authenticate_access_token` already treats a
deactivated user as unauthenticated (returns `None`), so no envelope is
minted for them at all. Absence of a valid envelope IS the disabled
signal — the same shape the existing Python-only contract already uses.

### Key management

EdDSA/Ed25519, asymmetric, and **separate from** the user-facing
access-token HS256 secret (`JWT_SECRET_KEY`). The envelope crosses a
process and language boundary (Python edge → Go `query-api`), so it uses
the same JWKS-based verification shape `acr/internal/auth` already uses for
its own web-assertion verification — `query-api` never holds a secret
capable of forging a user session token, only the public key needed to
verify an envelope. `build_envelope_jwks()` returns the public JWKS
document; the private key is `GO_API_ENVELOPE_PRIVATE_KEY` (PEM, Ed25519),
required at issuance time. Ed25519, not RS256 (reconciled 2026-08-27 per
CHAOS-4377): the dev-health-go `authverify` package's JWKS verifier
(`Ed25519JWKSVerifier`) is Ed25519-only by design.

## Operation rollout registry + proof ledger

Three Postgres tables (alembic `0114`), keyed by
`(schema_digest, document_digest, selected_operation)`:

```mermaid
erDiagram
    CANDIDATE_BUILD {
        string schema_digest PK
        string document_digest PK
        string selected_operation PK
        string candidate_build PK
        timestamp registered_at
    }
    ROUTING_STATE {
        string schema_digest PK
        string document_digest PK
        string selected_operation PK
        string current_candidate_build FK
        string owner "python|go"
        string mode "python|shadow|canary|primary|disabled"
        string eligible_orgs
        int rollout_percentage
        timestamp updated_at
    }
    PROOF_RUN {
        uuid id PK
        string schema_digest FK
        string document_digest FK
        string selected_operation FK
        string candidate_build FK
        string request_identity
        string stage "dual_run|deployed_executed|shadow|canary"
        string terminal_state "match|mismatch|auth_rejected|validation_rejected|dependency_failed|timeout|cancelled|resource_exhausted|fallback|unsupported|proof_failed"
        string data_watermark "required when stage=shadow"
        string org_id
        timestamp observed_at
    }
    CANDIDATE_BUILD ||--o{ ROUTING_STATE : "one becomes current (4-col FK)"
    CANDIDATE_BUILD ||--o{ PROOF_RUN : "proven by exact 4-col tuple"
```

`CANDIDATE_BUILD` is immutable and append-only. `ROUTING_STATE` is the one
mutable row per operation triple a future request router reads on every
call — a rollback mutates `current_candidate_build` in place, never an
image rollback (plan §5). `PROOF_RUN` is pinned to the *exact* candidate
build it proved via a full 4-column composite foreign key, never a bare
`candidate_build` string match, so a proof can never be silently
reattributed to a later build (plan §8.3).

Python access layer: `src/dev_health_ops/api/graphql/go_api_registry.py`
(`lookup_routing_state`, `register_candidate_build`, `record_proof_run`),
instrumented in `go_api_registry_telemetry.py`
(`devhealth_go_api_registry_lookup_total`,
`devhealth_go_api_candidate_build_registered_total`,
`devhealth_go_api_proof_run_recorded_total`).

## Canonical SDL pin

`contracts/graphql/v1/schema.graphql` is the CI-checked export of the
Strawberry schema — see `contracts/graphql/v1/README.md` for how web
codegen and `query-api`'s gqlgen consume it, and the drift gate
(`tests/api/graphql/test_schema_sdl_pinned.py`) that fails on any
divergence.

## Status

As of this page's authoring (CHAOS-4366 Wave 0): the envelope issuer, the
registry/ledger schema, and the SDL pin exist and are tested. The Go
verifier (in `query-api`, using dev-health-go's extracted JWKS
verification mechanisms — CHAOS-4377), the request router that actually
reads `ROUTING_STATE`, and the comparator (CHAOS-4366 deliverable 5,
blocked on parity-rules sign-off — CHAOS-4381) are separate, later pieces
of this same wave.
