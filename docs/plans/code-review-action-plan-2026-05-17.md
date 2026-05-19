# Code Review Action Plan — 2026-05-17

> Author: Sisyphus review pass  
> Scope: repo-wide review of `dev-health-ops` (docs, refactor, security)  
> Linear tickets filed: **CHAOS-1535 → CHAOS-1555** (21 tickets)  
> Working tree: clean, on `main`. No code changed.

This document maps every finding from the 2026-05-17 review pass to its Linear ticket and orders them by priority, category, and effort so work can be picked up directly.

---

## TL;DR — top moves

1. **Harden GraphQL** ([CHAOS-1550](https://linear.app/fullchaos/issue/CHAOS-1550) + [CHAOS-1551](https://linear.app/fullchaos/issue/CHAOS-1551)) — disable GraphiQL/introspection in prod, register depth/alias validation rules. Two small PRs, kills two HIGH severity findings.
2. **Verify credential encryption at rest** ([CHAOS-1552](https://linear.app/fullchaos/issue/CHAOS-1552)) — if Postgres-stored connector secrets are plaintext, this is the worst finding in the set.
3. **Workflow permissions + action SHA pinning** ([CHAOS-1535](https://linear.app/fullchaos/issue/CHAOS-1535) + [CHAOS-1536](https://linear.app/fullchaos/issue/CHAOS-1536)) — supply-chain hardening, both S effort.
4. **Pick a canonical provider pattern before TestOps lands** ([CHAOS-1539](https://linear.app/fullchaos/issue/CHAOS-1539)) — three coexisting patterns will compound with every new TestOps adapter.
5. **God-file splits** ([CHAOS-1540](https://linear.app/fullchaos/issue/CHAOS-1540), [CHAOS-1541](https://linear.app/fullchaos/issue/CHAOS-1541)) — auth/router (1702 lines) and sinks/clickhouse (1996 lines) are the riskiest to ignore.

---

## By Priority

### P1 — Do this milestone (8 tickets)

| Ticket | Category | Effort | Title |
|---|---|---|---|
| [CHAOS-1550](https://linear.app/fullchaos/issue/CHAOS-1550) | SEC | S | GraphiQL & introspection likely enabled in production |
| [CHAOS-1551](https://linear.app/fullchaos/issue/CHAOS-1551) | SEC | M | No GraphQL query depth/complexity/alias limit |
| [CHAOS-1552](https://linear.app/fullchaos/issue/CHAOS-1552) | SEC | M-L | Connector credentials likely stored plaintext (no encryption at rest) |
| [CHAOS-1535](https://linear.app/fullchaos/issue/CHAOS-1535) | SEC | S | Add explicit `permissions: contents: read` to all GitHub workflows |
| [CHAOS-1536](https://linear.app/fullchaos/issue/CHAOS-1536) | SEC | S | Pin all third-party GitHub Actions to commit SHA |
| [CHAOS-1537](https://linear.app/fullchaos/issue/CHAOS-1537) | SEC | S | Add lower-bound version pins to runtime deps in pyproject.toml |
| [CHAOS-1538](https://linear.app/fullchaos/issue/CHAOS-1538) | DOCS | S | Add test-ops-prd.md to mkdocs.yml + SHOUTING_CASE docs cleanup |
| [CHAOS-1539](https://linear.app/fullchaos/issue/CHAOS-1539) | REFACTOR | M-L | Pick a canonical provider pattern before TestOps lands |
| [CHAOS-1540](https://linear.app/fullchaos/issue/CHAOS-1540) | REFACTOR | M | Split api/auth/router.py (1702 lines) into per-flow routers |
| [CHAOS-1541](https://linear.app/fullchaos/issue/CHAOS-1541) | REFACTOR | M | Split metrics/sinks/clickhouse.py (1996 lines) by table family |

### P2 — Next milestone (9 tickets)

| Ticket | Category | Effort | Title |
|---|---|---|---|
| [CHAOS-1553](https://linear.app/fullchaos/issue/CHAOS-1553) | SEC | S | Rate limiter ignores X-Forwarded-For (`key_func=get_remote_address`) |
| [CHAOS-1554](https://linear.app/fullchaos/issue/CHAOS-1554) | SEC | S | Rate limiter silently falls back to in-memory backend |
| [CHAOS-1545](https://linear.app/fullchaos/issue/CHAOS-1545) | SEC | S | Audit `hashlib.md5` calls, add `usedforsecurity=False` |
| [CHAOS-1546](https://linear.app/fullchaos/issue/CHAOS-1546) | SEC | S | Tighten `RegisterRequest.password` Field bounds |
| [CHAOS-1542](https://linear.app/fullchaos/issue/CHAOS-1542) | REFACTOR | M | Split fixtures/generator.py (3475 lines) per domain |
| [CHAOS-1543](https://linear.app/fullchaos/issue/CHAOS-1543) | REFACTOR | M | Split api/main.py (1563 lines) — middleware + handlers |
| [CHAOS-1544](https://linear.app/fullchaos/issue/CHAOS-1544) | REFACTOR | M | Split api/services/settings.py (1622 lines) |
| [CHAOS-1549](https://linear.app/fullchaos/issue/CHAOS-1549) | DOCS | S | Refresh connectors/README.md to acknowledge providers/ |

### P3 — Backlog (3 tickets)

| Ticket | Category | Effort | Title |
|---|---|---|---|
| [CHAOS-1555](https://linear.app/fullchaos/issue/CHAOS-1555) | DOCS | S | Add GraphQL hardening posture doc (introspection, depth, cost limits) |
| [CHAOS-1547](https://linear.app/fullchaos/issue/CHAOS-1547) | REFACTOR | S-M | Decide aiosqlite scope (allow / retire) and update AGENTS.md |
| [CHAOS-1548](https://linear.app/fullchaos/issue/CHAOS-1548) | REFACTOR | M | Collapse processors/github.py and processors/gitlab.py duplication |

---

## By Category

### Security (11 tickets)

| Pri | Effort | Ticket | Title |
|---|---|---|---|
| P1 | S | [CHAOS-1535](https://linear.app/fullchaos/issue/CHAOS-1535) | Add `permissions: contents: read` to all GitHub workflows |
| P1 | S | [CHAOS-1536](https://linear.app/fullchaos/issue/CHAOS-1536) | Pin third-party GitHub Actions to commit SHA |
| P1 | S | [CHAOS-1537](https://linear.app/fullchaos/issue/CHAOS-1537) | Lower-bound version pins in pyproject.toml |
| P1 | S | [CHAOS-1550](https://linear.app/fullchaos/issue/CHAOS-1550) | GraphiQL & introspection likely enabled in production |
| P1 | M | [CHAOS-1551](https://linear.app/fullchaos/issue/CHAOS-1551) | No GraphQL depth/complexity/alias limit |
| P1 | M-L | [CHAOS-1552](https://linear.app/fullchaos/issue/CHAOS-1552) | Connector credentials likely plaintext at rest |
| P2 | S | [CHAOS-1553](https://linear.app/fullchaos/issue/CHAOS-1553) | Rate limiter ignores X-Forwarded-For |
| P2 | S | [CHAOS-1554](https://linear.app/fullchaos/issue/CHAOS-1554) | Rate limiter silent memory:// fallback |
| P2 | S | [CHAOS-1545](https://linear.app/fullchaos/issue/CHAOS-1545) | Audit `hashlib.md5` + `usedforsecurity=False` |
| P2 | S | [CHAOS-1546](https://linear.app/fullchaos/issue/CHAOS-1546) | Tighten RegisterRequest password Field bounds |

### Refactor (7 tickets)

| Pri | Effort | Ticket | Title |
|---|---|---|---|
| P1 | M-L | [CHAOS-1539](https://linear.app/fullchaos/issue/CHAOS-1539) | Pick canonical provider pattern before TestOps |
| P1 | M | [CHAOS-1540](https://linear.app/fullchaos/issue/CHAOS-1540) | Split api/auth/router.py (1702 lines) |
| P1 | M | [CHAOS-1541](https://linear.app/fullchaos/issue/CHAOS-1541) | Split metrics/sinks/clickhouse.py (1996 lines) |
| P2 | M | [CHAOS-1542](https://linear.app/fullchaos/issue/CHAOS-1542) | Split fixtures/generator.py (3475 lines) |
| P2 | M | [CHAOS-1543](https://linear.app/fullchaos/issue/CHAOS-1543) | Split api/main.py (1563 lines) |
| P2 | M | [CHAOS-1544](https://linear.app/fullchaos/issue/CHAOS-1544) | Split api/services/settings.py (1622 lines) |
| P3 | S-M | [CHAOS-1547](https://linear.app/fullchaos/issue/CHAOS-1547) | Decide aiosqlite scope, update AGENTS.md |
| P3 | M | [CHAOS-1548](https://linear.app/fullchaos/issue/CHAOS-1548) | Collapse processors/github.py + gitlab.py duplication |

### Docs (3 tickets)

| Pri | Effort | Ticket | Title |
|---|---|---|---|
| P1 | S | [CHAOS-1538](https://linear.app/fullchaos/issue/CHAOS-1538) | mkdocs nav + SHOUTING_CASE rename |
| P2 | S | [CHAOS-1549](https://linear.app/fullchaos/issue/CHAOS-1549) | connectors/README.md vs providers/ |
| P3 | S | [CHAOS-1555](https://linear.app/fullchaos/issue/CHAOS-1555) | GraphQL hardening posture doc |

---

## By Effort

### Small (S) — 11 tickets, parallel-friendly, ~1 day each

Group these into one or two "spring cleaning" PRs:

- [CHAOS-1535](https://linear.app/fullchaos/issue/CHAOS-1535) workflow permissions
- [CHAOS-1536](https://linear.app/fullchaos/issue/CHAOS-1536) action SHA pinning
- [CHAOS-1537](https://linear.app/fullchaos/issue/CHAOS-1537) dep version floors
- [CHAOS-1538](https://linear.app/fullchaos/issue/CHAOS-1538) mkdocs + SHOUTY renames
- [CHAOS-1545](https://linear.app/fullchaos/issue/CHAOS-1545) `hashlib.md5` `usedforsecurity=False`
- [CHAOS-1546](https://linear.app/fullchaos/issue/CHAOS-1546) password Field bounds
- [CHAOS-1549](https://linear.app/fullchaos/issue/CHAOS-1549) connectors README
- [CHAOS-1550](https://linear.app/fullchaos/issue/CHAOS-1550) disable GraphiQL/introspection
- [CHAOS-1553](https://linear.app/fullchaos/issue/CHAOS-1553) rate-limit X-Forwarded-For
- [CHAOS-1554](https://linear.app/fullchaos/issue/CHAOS-1554) rate-limit fail-loud
- [CHAOS-1555](https://linear.app/fullchaos/issue/CHAOS-1555) GraphQL hardening doc

### Small-Medium (S-M) — 1 ticket

- [CHAOS-1547](https://linear.app/fullchaos/issue/CHAOS-1547) aiosqlite scope decision

### Medium (M) — 6 tickets, 2–5 days each

- [CHAOS-1540](https://linear.app/fullchaos/issue/CHAOS-1540) auth/router split
- [CHAOS-1541](https://linear.app/fullchaos/issue/CHAOS-1541) sinks/clickhouse split
- [CHAOS-1542](https://linear.app/fullchaos/issue/CHAOS-1542) fixtures/generator split
- [CHAOS-1543](https://linear.app/fullchaos/issue/CHAOS-1543) api/main.py split
- [CHAOS-1544](https://linear.app/fullchaos/issue/CHAOS-1544) api/services/settings.py split
- [CHAOS-1548](https://linear.app/fullchaos/issue/CHAOS-1548) processors duplication
- [CHAOS-1551](https://linear.app/fullchaos/issue/CHAOS-1551) GraphQL depth/alias limits

### Medium-Large (M-L) — 2 tickets, ~1 week each

- [CHAOS-1539](https://linear.app/fullchaos/issue/CHAOS-1539) canonical provider pattern (blocks TestOps)
- [CHAOS-1552](https://linear.app/fullchaos/issue/CHAOS-1552) credential encryption at rest (verify + migrate)

---

## Sequencing & Dependencies

### Must-precede TestOps work

These should land **before** Jenkins/Buildkite/CircleCI/Azure DevOps adapters from the TestOps PRD:

```
CHAOS-1539 (provider pattern)
    ├─→ CHAOS-1549 (connectors README refresh)
    └─→ CHAOS-1548 (processors duplication)

CHAOS-1541 (sinks/clickhouse split)
    └─→ TestOps schema sinks (test_suite_runs, test_case_runs, coverage, flakiness)

CHAOS-1538 (mkdocs nav)
    └─→ TestOps PRD becomes discoverable
```

### Natural pairings (do in same PR)

- **CHAOS-1550 + CHAOS-1551** — both GraphQL hardening, both touch `api/graphql/app.py`
- **CHAOS-1553 + CHAOS-1554** — both `api/middleware/rate_limit.py`
- **CHAOS-1545 + CHAOS-1546** — both small security hygiene
- **CHAOS-1535 + CHAOS-1536** — both `.github/workflows/`

### Suggested PR groupings

| PR | Tickets | Estimated |
|---|---|---|
| `sec: GraphQL hardening` | 1550, 1551, 1555 | 2-3 days |
| `sec: workflow + supply-chain hardening` | 1535, 1536, 1537 | 1-2 days |
| `sec: rate-limiter correctness` | 1553, 1554 | 1 day |
| `sec: credential encryption at rest` | 1552 | 1 week |
| `sec: small hygiene` | 1545, 1546 | 0.5 day |
| `docs: nav + rename` | 1538, 1549, 1555 | 0.5 day |
| `refactor: provider pattern decision` | 1539 | 2-3 days (decision + AGENTS.md) |
| `refactor: auth/router split` | 1540 | 2-3 days |
| `refactor: sinks/clickhouse split` | 1541 | 2-3 days |
| `refactor: fixtures generator split` | 1542 | 2-3 days |
| `refactor: api/main.py split` | 1543 | 1-2 days |
| `refactor: api/services/settings split` | 1544 | 2-3 days |
| `refactor: processors dedup` | 1548 | 3-5 days (depends on 1539) |
| `chore: aiosqlite scope` | 1547 | 1-2 days |

---

## Confirmed NOT-a-finding (positive results)

Already verified during the review, no ticket needed:

- ✅ SAML XML parsing uses **defusedxml** — `api/services/sso.py:18,773`
- ✅ OAuth state via **`secrets.token_urlsafe(32)`** — `api/services/oauth.py:143`
- ✅ Webhook HMAC uses **`hmac.compare_digest`** (timing-safe) — `api/ingest/auth.py:78`
- ✅ Celery serializer is **JSON only**, no pickle — `workers/config.py:13-15`
- ✅ No Mongo residue in `src/` (deprecation per AGENTS.md is real)
- ✅ Subprocess calls use argv lists, no `shell=True` — `workers/runner.py`
- ✅ CORS origins come from `CORS_ALLOWED_ORIGINS` env, not wildcarded — `api/main.py:397-408`
- ✅ Generic 500 handler sanitizes responses — `api/main.py`
- ✅ `DUMMY_PASSWORD_HASH` timing-attack mitigation present in auth — `api/auth/router.py:67`
- ✅ Most GitHub Actions ARE SHA-pinned (some still floating — CHAOS-1536)

---

## Open confidence gaps (not yet ticketed)

These were flagged in the review but couldn't be conclusively resolved without longer investigation. File new tickets if any prove out:

1. **bcrypt 72-byte truncation handling** — verify password is normalized/rejected at the schema/service layer before `bcrypt.hashpw`. CHAOS-1546 closes part of this.
2. **Impersonation session lifecycle** — does the impersonation cookie/token get invalidated when the impersonator logs out? `api/admin/impersonation.py` + `api/admin/middleware.py`.
3. **GraphQL field-level authz coverage** — `authz.py` exists in `api/graphql/` but per-resolver coverage was not audited. Spot-check `flow_matrix` and investment resolvers.
4. **GraphQL N+1 DoS** — pairs with CHAOS-1551 once depth limits land; current dataloaders coverage on hot resolvers (flow_matrix, investment) unverified.
5. **Webhook coverage** — confirmed HMAC verify in `api/ingest/auth.py`. Did **not** exhaustively audit Stripe webhook (`api/billing/router.py`) signature path or GitHub App webhook (PR #688 added App auth; webhook path may not be using it yet).
6. **"Signals" POC residue** — quick grep showed no obvious module, but no exhaustive scan. AGENTS.md says Signals is retired.
7. **Broker URL log leakage** — Celery broker URL handling not verified to scrub credentials in logs.

---

## How this plan was assembled

- **First pass** (Sisyphus direct, grep + Read + LSP): 15 findings → CHAOS-1535 to CHAOS-1549.
- **Deep dive** (Sisyphus direct, after 4 oracle agent attempts had output lost): 6 NEW findings → CHAOS-1550 to CHAOS-1555.
- Tickets created via `linear i create` against the `CHAOS` team. All start in `Backlog` state, unassigned.
- No source files were modified during this review.

### Where this came from in the codebase

The full review thread, including the parts that became these tickets, ran against:

- [`AGENTS.md`](../../AGENTS.md) (canonical contract)
- [`pyproject.toml`](../../pyproject.toml) (deps)
- [`mkdocs.yml`](../../mkdocs.yml) (docs nav)
- `src/dev_health_ops/api/auth/*`, `api/admin/*`, `api/graphql/*`, `api/ingest/*`, `api/middleware/*`
- `src/dev_health_ops/connectors/*`, `providers/*`, `processors/*`
- `src/dev_health_ops/metrics/sinks/*`, `storage/*`
- `src/dev_health_ops/workers/*`
- `.github/workflows/*`
- [`docs/product/test-ops-prd.md`](../product/test-ops-prd.md) (upcoming work that informs P1 priorities)
