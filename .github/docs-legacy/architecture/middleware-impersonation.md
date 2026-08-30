# Design Doc: Middleware-Based Impersonation System

_Date: 2026-02-27_
_Status: Accepted_
_Scope: dev-health-ops (backend), dev-health-web (frontend)_

## One Sentence

Replace the JWT token-swap impersonation system with a server-side session + middleware approach so platform admins can operate without org membership and impersonation is transparent to all downstream code.

## Problem

### Current Approach (Token-Swap)

When a superuser impersonates a target user, the backend issues a **new JWT** with the target's identity. The superuser's original identity is preserved only as an `impersonated_by` claim in the new token. The frontend stores the original token in `real_*` JWT fields and swaps back on stop.

```
Admin JWT {user: admin, org: ""}
  → POST /impersonate → NEW JWT {user: target, org: target_org, impersonated_by: admin}
    → POST /stop → ANOTHER NEW JWT {user: admin, org: ???}
```

### Why This Fails

1. **Org-less superusers can't impersonate.** The `/impersonate` endpoint calls `_parse_uuid(current_user.org_id)` which crashes on empty string — the superuser is blocked before the existing fallback logic ever runs.

2. **Stop restores the wrong org.** On stop, `org_id = real_membership.org_id if real_membership else current_user.org_id`. For an org-less superuser, `real_membership` is None and `current_user.org_id` is the *impersonated* user's org — so the superuser gets stuck in someone else's org context.

3. **Frontend JWT gymnastics.** NextAuth callbacks store `real_access_token`, `real_user_id`, `real_role`, `real_org_id` during impersonation and swap them back on stop. Token refresh is disabled during impersonation (`!token.is_impersonating` guard). If the stop endpoint fails, the admin is locked out.

4. **Every endpoint must know about impersonation.** Code checking `impersonated_by`, audit services detecting the claim, permission checks special-casing it — the impersonation concern leaks across the codebase.

## Design: Middleware-Based Impersonation

### Core Principle

**The superuser's JWT never changes.** Impersonation state lives server-side. A middleware layer transparently translates request context when an impersonation session is active.

### Architecture

```
Request arrives
  → Auth: validate JWT → real_user = admin (is_superuser=true, org_id="")
  → ImpersonationMiddleware:
      check DB for active session where admin_user_id = real_user.id
      IF session found:
        set _current_org_id = session.target_org_id
        set _impersonated_user = {user_id: target, org_id: target_org, role: target_role}
        set _real_user = admin (for audit)
        add X-Impersonating: true response header
        add X-Impersonated-User-Id: target_id response header
      ELSE:
        pass through (normal admin request)
  → OrgIdMiddleware: already ran, but ImpersonationMiddleware overrides _current_org_id
  → Downstream code sees: org_id = target_org, user context = target
  → Audit sees: real_user = admin, action performed in target_org context
```

### Data Model

#### New: `impersonation_sessions` table (Postgres)

```sql
CREATE TABLE impersonation_sessions (
    id UUID PRIMARY KEY,
    admin_user_id UUID NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    target_user_id UUID NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    target_org_id UUID NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
    target_role VARCHAR(50) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,
    ip_address VARCHAR(45),
    user_agent TEXT,
    CONSTRAINT no_self_impersonation CHECK (admin_user_id != target_user_id)
);

CREATE INDEX idx_impersonation_active
    ON impersonation_sessions (admin_user_id)
    WHERE ended_at IS NULL;
```

**Key properties:**
- One active session per admin (enforce in application — end previous before starting new)
- TTL-based expiry (default 60 min, configurable via `IMPERSONATION_TTL_MINUTES`)
- `ended_at` NULL = active, non-NULL = stopped
- `ON DELETE RESTRICT` on user/org FKs — sessions are preserved as an audit trail; users/orgs with sessions cannot be deleted until sessions are removed

### API Changes

#### Start Impersonation

```
POST /api/v1/admin/impersonate
Body: { "target_user_id": "uuid" }
Auth: requires is_superuser=true

Response: {
    "status": "active",
    "target_user": { "id", "email", "org_id", "role" },
    "expires_at": "2026-02-27T20:00:00Z"
}
```

**Logic:**
1. Verify caller is superuser
2. Verify target exists, is active, is NOT superuser
3. Find target's membership (first membership if multiple)
4. End any existing active session for this admin
5. Create new `impersonation_sessions` row
6. Audit log: `IMPERSONATION_START` with admin_user_id and target context
7. Return target info (no token — the JWT stays the same)

#### Stop Impersonation

```
POST /api/v1/admin/impersonate/stop
Auth: requires is_superuser=true + active session

Response: { "status": "stopped" }
```

**Logic:**
1. Find active session for this admin
2. Set `ended_at = now()`
3. Audit log: `IMPERSONATION_STOP`
4. Return success

#### Status

```
GET /api/v1/admin/impersonate/status
Auth: any authenticated user

Response: {
    "is_impersonating": true,
    "target_user_id": "uuid",
    "target_email": "...",
    "target_org_id": "uuid",
    "expires_at": "..."
}
```

### Middleware Implementation

#### New: `ImpersonationMiddleware`

**Runs AFTER auth extraction, BEFORE route handlers.**

```python
class ImpersonationMiddleware:
    """Check for active impersonation session and override request context."""

    async def __call__(self, scope, receive, send):
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return

        real_user = extract_authenticated_user(scope)  # From JWT
        if not real_user or not real_user.is_superuser:
            await self.app(scope, receive, send)
            return

        session = await get_active_impersonation_session(real_user.user_id)
        if not session:
            await self.app(scope, receive, send)
            return

        if session.expires_at < now():
            await expire_session(session)
            await self.app(scope, receive, send)
            return

        # Override context
        org_token = set_current_org_id(str(session.target_org_id))
        imp_token = set_impersonation_context(
            target_user_id=session.target_user_id,
            target_org_id=session.target_org_id,
            target_role=session.target_role,
            real_user_id=real_user.user_id,
        )

        # Add response headers
        send = wrap_send_with_headers(send, {
            "X-Impersonating": "true",
            "X-Impersonated-User-Id": str(session.target_user_id),
        })

        try:
            await self.app(scope, receive, send)
        finally:
            _current_org_id.reset(org_token)
            _impersonation_ctx.reset(imp_token)
```

#### Modified: `OrgIdMiddleware`

**No changes needed.** `ImpersonationMiddleware` runs after it and overrides `_current_org_id`. The middleware ordering in FastAPI app setup:

```python
# OrgIdMiddleware extracts org_id from header/JWT
# ImpersonationMiddleware overrides it if session is active
# So ImpersonationMiddleware must run AFTER OrgIdMiddleware (wrap it)
app.add_middleware(OrgIdMiddleware)              # Inner
app.add_middleware(ImpersonationMiddleware)      # Outer (runs after OrgId sets context)
```

(ASGI middleware order is LIFO — last added wraps everything, so `ImpersonationMiddleware` added last makes it the outermost wrapper.)

### Context Variables

#### New contextvars

```python
_impersonation_ctx: ContextVar[ImpersonationContext | None] = ContextVar(
    "impersonation_ctx", default=None
)

@dataclass
class ImpersonationContext:
    target_user_id: str
    target_org_id: str
    target_role: str
    real_user_id: str
    is_active: bool = True

def get_impersonation_context() -> ImpersonationContext | None:
    return _impersonation_ctx.get(None)

def is_impersonating() -> bool:
    ctx = _impersonation_ctx.get(None)
    return ctx is not None and ctx.is_active
```

#### Audit Integration

```python
# In AuditService.log():
imp_ctx = get_impersonation_context()
if imp_ctx:
    metadata["impersonated_by"] = imp_ctx.real_user_id
    metadata["impersonation_target"] = imp_ctx.target_user_id
```

### Login Fix (Independent)

In `src/dev_health_ops/api/auth/router.py`, line 737:

```python
# Before:
needs_onboarding = membership is None

# After:
needs_onboarding = membership is None and not bool(user.is_superuser)
```

This allows superusers to log in without org membership. They land on `/superadmin` (frontend already handles this redirect).

### Frontend Changes

#### Remove from `src/lib/auth.ts`:
- `real_access_token`, `real_user_id`, `real_role`, `real_org_id` JWT fields
- `startImpersonation` / `stopImpersonation` update triggers in JWT callback
- `!token.is_impersonating` guard on token refresh (line ~127)

#### Add to `src/lib/auth.ts`:
- Read `X-Impersonating` header from backend responses (or use `/impersonate/status` on session validate)
- Set `session.user.is_impersonating` based on response header

#### Simplify `src/lib/admin/server.ts`:
- `startImpersonation()`: call `POST /admin/impersonate`, return success/failure (no token handling)
- `stopImpersonation()`: call `POST /admin/impersonate/stop`, return success/failure

#### Simplify `ImpersonateUserButton.tsx`:
- On success: `router.refresh()` instead of `update({ startImpersonation: ... })`

#### Keep as-is:
- `proxy.ts` — still injects `X-Org-Id` when available, skips when not
- Superadmin layout redirect for org-less superusers
- `requireSuperuser` guard

### Session Caching (Performance)

The middleware runs on EVERY request. Querying Postgres per request is expensive.

**Strategy:** In-memory LRU cache with short TTL.

```python
# Cache key: admin_user_id
# Cache value: ImpersonationSession or None
# TTL: 30 seconds
# Invalidate on: start, stop, expire
```

This means after starting/stopping impersonation, there's up to 30s staleness. For a human-driven admin flow, this is acceptable. The `/impersonate/status` endpoint always reads from DB (bypass cache) for accurate UI state.

### Security Guardrails

1. **Only superusers can impersonate** — middleware checks `is_superuser` before DB lookup
2. **Cannot impersonate other superusers** — start endpoint rejects
3. **Cannot self-impersonate** — DB constraint
4. **Session TTL** — auto-expires (default 60 min)
5. **One session per admin** — starting new ends previous
6. **Audit everything** — start, stop, expire events logged with both identities
7. **No write escalation** — impersonated user's role is used for permission checks, not superuser privileges. During impersonation, `is_superuser` is NOT passed to the scoped context.
8. **Session survives restart** — DB-backed, not in-memory

### What This Does NOT Change

- `GraphQLContext` still requires `org_id` — middleware provides it transparently
- `query_dicts` still auto-injects `org_id` — middleware sets `_current_org_id`
- All org-scoped pages still require org context — impersonation IS how superusers get it
- Permission checks still use role-based RBAC — impersonated role applies
- `/superadmin` routes work without impersonation — they don't need org context

### Migration Path

1. Deploy backend with BOTH old and new impersonation endpoints (feature flag)
2. Frontend switches to new flow
3. Remove old endpoints after verification
4. Clean up frontend JWT swap code

## Edge Cases

| Case | Handling |
|------|---------|
| Session expires mid-request | Middleware checks expiry on every request; if expired, passes through without impersonation context (admin sees superadmin view) |
| Multiple browser tabs | All tabs share the same session (same admin JWT) — starting/stopping affects all tabs |
| Admin's JWT expires during impersonation | Normal JWT refresh works (admin's token was never swapped) — the `!is_impersonating` refresh guard is removed |
| Target user deactivated during impersonation | Middleware should check target user `is_active` on each request (or rely on cached session + periodic validation) |
| Target user removed from org during impersonation | Middleware should validate target membership on session start; mid-session, rely on TTL. Could add membership check to cache refresh. |
| Concurrent impersonation requests | Last-write-wins: new session ends the previous one |
| Admin loses superuser status during impersonation | Middleware checks `is_superuser` on every request before looking up session — if false, impersonation is ignored |

## References

- django-impersonate: middleware swaps `request.user`, preserves `request.impersonator`
- Supabase: server-side session with service-role key
- Current code: `src/dev_health_ops/api/admin/impersonation.py`, `src/dev_health_ops/api/middleware/__init__.py`, `src/lib/auth.ts`
