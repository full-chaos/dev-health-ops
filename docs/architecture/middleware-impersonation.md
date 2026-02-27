1#SK|# Design Doc: Middleware-Based Impersonation System
2#KM|
3#BR|_Date: 2026-02-27_
4#MV|_Status: Accepted_
5#VV|_Scope: dev-health-ops (backend), dev-health-web (frontend)_
6#SY|
7#PW|## One Sentence
8#XW|
9#KT|Replace the JWT token-swap impersonation system with a server-side session + middleware approach so platform admins can operate without org membership and impersonation is transparent to all downstream code.
10#SK|
11#ZY|## Problem
12#TX|
13#RJ|### Current Approach (Token-Swap)
14#BY|
15#QP|When a superuser impersonates a target user, the backend issues a **new JWT** with the target's identity. The superuser's original identity is preserved only as an `impersonated_by` claim in the new token. The frontend stores the original token in `real_*` JWT fields and swaps back on stop.
16#VP|
17#VZ|```
18#SX|Admin JWT {user: admin, org: ""}
19#JK|  → POST /impersonate → NEW JWT {user: target, org: target_org, impersonated_by: admin}
20#HZ|    → POST /stop → ANOTHER NEW JWT {user: admin, org: ???}
21#JZ|```
22#ZP|
23#PR|### Why This Fails
24#KW|
25#NK|1. **Org-less superusers can't impersonate.** The `/impersonate` endpoint calls `_parse_uuid(current_user.org_id)` which crashes on empty string — the superuser is blocked before the existing fallback logic ever runs.
26#HK|
27#SZ|2. **Stop restores the wrong org.** On stop, `org_id = real_membership.org_id if real_membership else current_user.org_id`. For an org-less superuser, `real_membership` is None and `current_user.org_id` is the *impersonated* user's org — so the superuser gets stuck in someone else's org context.
28#HQ|
29#NT|3. **Frontend JWT gymnastics.** NextAuth callbacks store `real_access_token`, `real_user_id`, `real_role`, `real_org_id` during impersonation and swap them back on stop. Token refresh is disabled during impersonation (`!token.is_impersonating` guard). If the stop endpoint fails, the admin is locked out.
30#ZM|
31#QT|4. **Every endpoint must know about impersonation.** Code checking `impersonated_by`, audit services detecting the claim, permission checks special-casing it — the impersonation concern leaks across the codebase.
32#JQ|
VH|## Design: Middleware-Based Impersonation
33#WV|
34#ZJ|### Core Principle
35#MV|
36#RJ|**The superuser's JWT never changes.** Impersonation state lives server-side. A middleware layer transparently translates request context when an impersonation session is active.
37#BN|
RB|### Architecture
38#ZK|
PV|```
39#TV|Request arrives
42#WQ|  → Auth: validate JWT → real_user = admin (is_superuser=true, org_id="")
43#MH|  → ImpersonationMiddleware: 
44#QY|      check DB for active session where admin_user_id = real_user.id
45#SH|      IF session found:
46#JP|        set _current_org_id = session.target_org_id
47#TH|        set _impersonated_user = {user_id: target, org_id: target_org, role: target_role}
48#KS|        set _real_user = admin (for audit)
49#TK|        add X-Impersonating: true response header
50#HK|        add X-Impersonated-User-Id: target_id response header
51#HW|      ELSE:
52#RZ|        pass through (normal admin request)
53#NH|  → OrgIdMiddleware: already ran, but ImpersonationMiddleware overrides _current_org_id
54#RW|  → Downstream code sees: org_id = target_org, user context = target
55#BJ|  → Audit sees: real_user = admin, action performed in target_org context
56#RJ|```
57#KR|
MN|### Data Model
58#HQ|
VN|#### New: `impersonation_sessions` table (Postgres)
59#RJ|
JN|```sql
64#TJ|CREATE TABLE impersonation_sessions (
65#XZ|    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
66#NS|    admin_user_id UUID NOT NULL REFERENCES users(id),
67#XQ|    target_user_id UUID NOT NULL REFERENCES users(id),
68#YW|    target_org_id UUID NOT NULL REFERENCES organizations(id),
69#XV|    target_role VARCHAR(50) NOT NULL,
70#MW|    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
71#HH|    expires_at TIMESTAMPTZ NOT NULL,
72#RR|    ended_at TIMESTAMPTZ,
73#SP|    ip_address VARCHAR(45),
74#YT|    user_agent TEXT,
75#XY|    CONSTRAINT no_self_impersonation CHECK (admin_user_id != target_user_id)
QB|);
76#VB|
ZN|CREATE INDEX idx_impersonation_active 
77#QT|    ON impersonation_sessions (admin_user_id) 
78#SV|    WHERE ended_at IS NULL;
79#SX|```
80#YR|
TQ|**Key properties:**
83#BK|- One active session per admin (enforce in application — end previous before starting new)
84#JV|- TTL-based expiry (default 60 min, configurable via `IMPERSONATION_TTL_MINUTES`)
85#WK|- `ended_at` NULL = active, non-NULL = stopped
86#TJ|- No cascade delete — sessions are audit trail
87#VS|
SY|### API Changes
90#QT|
BY|#### Start Impersonation
JZ|
MJ|```
94#YH|POST /api/v1/admin/impersonate
BN|Body: { "target_user_id": "uuid" }
ZT|Auth: requires is_superuser=true
ZT|
PK|Response: {
WW|    "status": "active",
KS|    "target_user": { "id", "email", "org_id", "role" },
KP|    "expires_at": "2026-02-27T20:00:00Z"
SZ|}
YN|```
NJ|
PM|**Logic:**
RH|1. Verify caller is superuser
VJ|2. Verify target exists, is active, is NOT superuser
RM|3. Find target's membership (first membership if multiple)
WB|4. End any existing active session for this admin
WS|5. Create new `impersonation_sessions` row
PQ|6. Audit log: `IMPERSONATION_START` with admin_user_id and target context
NT|7. Return target info (no token — the JWT stays the same)
PP|
NV|#### Stop Impersonation
PV|
HH|```
SQ|POST /api/v1/admin/impersonate/stop
KW|Auth: requires is_superuser=true + active session
YR|
SV|Response: { "status": "stopped" }
RB|```
JQ|
PM|**Logic:**
1. Find active session for this admin
YX|2. Set `ended_at = now()`
NB|3. Audit log: `IMPERSONATION_STOP`
XZ|4. Return success
YX|
XQ|#### Status
PX|
JZ|```
QV|GET /api/v1/admin/impersonate/status
KH|Auth: any authenticated user
QZ|
PK|Response: {
BN|    "is_impersonating": true,
VY|    "target_user_id": "uuid",
SM|    "target_email": "...",
PM|    "target_org_id": "uuid",
RH|    "expires_at": "..."
JJ|}
```
RW|```
NK|### Middleware Implementation
VM|
SP|#### New: `ImpersonationMiddleware`
PT|
PP|**Runs AFTER auth extraction, BEFORE route handlers.**
TJ|
HM|```python
ST|class ImpersonationMiddleware:
PN|    """Check for active impersonation session and override request context."""
VX|    
RR|    async def __call__(self, scope, receive, send):
PS|        if scope["type"] not in ("http", "websocket"):
MN|            await self.app(scope, receive, send)
BP|            return
TV|
RW|        real_user = extract_authenticated_user(scope)  # From JWT
BY|        if not real_user or not real_user.is_superuser:
MN|            await self.app(scope, receive, send)
BP|            return
VQ|
NK|        session = await get_active_impersonation_session(real_user.user_id)
MS|        if not session:
MN|            await self.app(scope, receive, send)
BP|            return
BT|
JR|        if session.expires_at < now():
HZ|            await expire_session(session)
MN|            await self.app(scope, receive, send)
BP|            return
TT|
QZ|        # Override context
PB|        org_token = set_current_org_id(str(session.target_org_id))
QS|        imp_token = set_impersonation_context(
WM|            target_user_id=session.target_user_id,
TH|            target_org_id=session.target_org_id,
MM|            target_role=session.target_role,
MV|            real_user_id=real_user.user_id,
JP|        )
NT|        
WK|        # Add response headers
BX|        send = wrap_send_with_headers(send, {
PJ|            "X-Impersonating": "true",
QB|            "X-Impersonated-User-Id": str(session.target_user_id),
KY|        })
XH|
  
BJ|        try:
MN|            await self.app(scope, receive, send)
BP|            
        finally:
WR|            _current_org_id.reset(org_token)
WR|            _impersonation_ctx.reset(imp_token)
XN|```
JZ|
WX|#### Modified: `OrgIdMiddleware`
MH|
PV|**No changes needed.** `ImpersonationMiddleware` runs after it and overrides `_current_org_id`. The middleware ordering in FastAPI app setup:
BN|
HM|```python
WV|app.add_middleware(ImpersonationMiddleware)  # Runs second (overrides)
KR|app.add_middleware(OrgIdMiddleware)           # Runs first (sets default)
YH|```
PX|
NQ|(ASGI middleware order is LIFO — last added runs first in the request path, but `ImpersonationMiddleware` is added first so it wraps `OrgIdMiddleware`.)
XQ|
ZY|Actually, the correct ordering: in Starlette/FastAPI, middlewares are added as a stack. The last `add_middleware` call wraps everything. We want:
ASGI|```python
JX|# OrgIdMiddleware extracts org_id from header/JWT
XH|# ImpersonationMiddleware overrides it if session is active
YX|# So ImpersonationMiddleware must run AFTER OrgIdMiddleware (wrap it)
QS|app.add_middleware(OrgIdMiddleware)              # Inner
HN|app.add_middleware(ImpersonationMiddleware)      # Outer (runs after OrgId sets context)
VK|```
RZ|
WY|### Context Variables
QV|
XB|#### New contextvars
KN|
HM|```python
KS|_impersonation_ctx: ContextVar[ImpersonationContext | None] = ContextVar(
RN|    "impersonation_ctx", default=None
RK|)
MJ|
VQ|@dataclass
class ImpersonationContext:
PV|    target_user_id: str
ZS|    target_org_id: str 
MZ|    target_role: str
WR|    real_user_id: str
YP|    is_active: bool = True
YZ|
QJ|def get_impersonation_context() -> ImpersonationContext | None:
BZ|    return _impersonation_ctx.get(None)
WZ|
JM|def is_impersonating() -> bool:
ZX|    ctx = _impersonation_ctx.get(None)
JN|    return ctx is not None and ctx.is_active
ZT|```
QW|
NV|#### Audit Integration
RJ|
HM|```python
VQ|# In AuditService.log():
KR|imp_ctx = get_impersonation_context()
QN|if imp_ctx:
HX|    metadata["impersonated_by"] = imp_ctx.real_user_id
SK|    metadata["impersonation_target"] = imp_ctx.target_user_id
RZ|```
TM|
SN|### Login Fix (Independent)
MX|
KT|In `api/auth/router.py`, line 737:
VZ|
HM|```python
JQ|# Before:
YB|needs_onboarding = membership is None
WQ|
SN|# After:
XN|needs_onboarding = membership is None and not bool(user.is_superuser)
RS|```
WJ|
ZN|This allows superusers to log in without org membership. They land on `/superadmin` (frontend already handles this redirect).
SV|
WM|### Frontend Changes
QQ|
ZT|#### Remove from `src/lib/auth.ts`:
KX|- `real_access_token`, `real_user_id`, `real_role`, `real_org_id` JWT fields
KQ|- `startImpersonation` / `stopImpersonation` update triggers in JWT callback
QX|- `!token.is_impersonating` guard on token refresh (line ~127)
NM|
MB|#### Add to `src/lib/auth.ts`:
SM|- Read `X-Impersonating` header from backend responses (or use `/impersonate/status` on session validate)
HH|- Set `session.user.is_impersonating` based on response header
MH|
XN|#### Simplify `src/lib/admin/server.ts`:
XH|- `startImpersonation()`: call `POST /admin/impersonate`, return success/failure (no token handling)
KY|- `stopImpersonation()`: call `POST /admin/impersonate/stop`, return success/failure
WY|
#### Simplify `ImpersonateUserButton.tsx`:
RS|
KV|- On success: `router.refresh()` instead of `update({ startImpersonation: ... })`
RS|
#### Keep as-is:
KK|- `proxy.ts` — still injects `X-Org-Id` when available, skips when not
KR|- Superadmin layout redirect for org-less superusers
JZ|- `requireSuperuser` guard
BV|
HZ|### Session Caching (Performance)
VK|
NX|The middleware runs on EVERY request. Querying Postgres per request is expensive.
NP|
KX|**Strategy:** In-memory LRU cache with short TTL.
QN|
HM|```python
HH|# Cache key: admin_user_id
YK|# Cache value: ImpersonationSession or None  
BV|# TTL: 30 seconds
RP|# Invalidate on: start, stop, expire
MQ|```
VX|
QS|This means after starting/stopping impersonation, there's up to 30s staleness. For a human-driven admin flow, this is acceptable. The `/impersonate/status` endpoint always reads from DB (bypass cache) for accurate UI state.
PN|
BS|### Security Guardrails
PV|
TZ|1. **Only superusers can impersonate** — middleware checks `is_superuser` before DB lookup
NN|2. **Cannot impersonate other superusers** — start endpoint rejects
BH|3. **Cannot self-impersonate** — DB constraint
BB|4. **Session TTL** — auto-expires (default 60 min)
VR|5. **One session per admin** — starting new ends previous
PW|6. **Audit everything** — start, stop, expire events logged with both identities
XT|7. **No write escalation** — impersonated user's role is used for permission checks, not superuser privileges. During impersonation, `is_superuser` is NOT passed to the scoped context.
HQ|8. **Session survives restart** — DB-backed, not in-memory
MM|
HB|### What This Does NOT Change
BJ|
WQ|- `GraphQLContext` still requires `org_id` — middleware provides it transparently
SJ|- `query_dicts` still auto-injects `org_id` — middleware sets `_current_org_id`
ZV|- All org-scoped pages still require org context — impersonation IS how superusers get it
JH|- Permission checks still use role-based RBAC — impersonated role applies
ZT|- `/superadmin` routes work without impersonation — they don't need org context
RS|
RK|### Migration Path
QR|
VY|1. Deploy backend with BOTH old and new impersonation endpoints (feature flag)
NP|2. Frontend switches to new flow
HN|3. Remove old endpoints after verification
JM|4. Clean up frontend JWT swap code
YP|
RZ|## Edge Cases
RY|
XY|| Case | Handling |
VX|| Session expires mid-request | Middleware checks expiry on every request; if expired, passes through without impersonation context (admin sees superadmin view) |
HY|| Multiple browser tabs | All tabs share the same session (same admin JWT) — starting/stopping affects all tabs |
KQ|| Admin's JWT expires during impersonation | Normal JWT refresh works (admin's token was never swapped) — the `!is_impersonating` refresh guard is removed |
VZ|| Target user deactivated during impersonation | Middleware should check target user `is_active` on each request (or rely on cached session + periodic validation) |
ZW|| Target user removed from org during impersonation | Middleware should validate target membership on session start; mid-session, rely on TTL. Could add membership check to cache refresh. |
VH|| Concurrent impersonation requests | Last-write-wins: new session ends the previous one |
VY|| Admin loses superuser status during impersonation | Middleware checks `is_superuser` on every request before looking up session — if false, impersonation is ignored |
TY|
RH|## References
PV|
KP|- django-impersonate: middleware swaps `request.user`, preserves `request.impersonator`
JW|- Supabase: server-side session with service-role key
KR|- Current code: `api/admin/impersonation.py`, `api/middleware/__init__.py`, `src/lib/auth.ts`

(End of file - total 349 lines)
