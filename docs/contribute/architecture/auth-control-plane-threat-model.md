---
page_id: con-auth-control-plane-threat-model
summary: The Wave 0 Auth Control Plane threat model is maintained privately as a Linear document under the fix-first disclosure policy. This page is a stub; sections publish here in full, past tense, once the fix they describe has merged. Two are published now — telemetry (CHAOS-4722) and legacy ingest (CHAOS-4720).
content_type: architecture
owner: engineering
source_of_truth:
  - Linear document "Auth Control Plane threat model (Wave 0)", project Auth Control Plane (full analysis; private)
  - src/dev_health_ops/api/telemetry/router.py (071c27890 closed CHAOS-4722)
  - src/dev_health_ops/api/ingest/auth.py (512c4e77b closed CHAOS-4720)
applicability: current
lifecycle: active
---

# Auth Control Plane: Wave 0 threat model

The Wave 0 threat model is dense with anchored analysis of weaknesses in the
running product. Under the fix-first disclosure policy, a description of a
currently-unfixed weakness does not land in a public repository until the fix
that closes it is merged. The full document is therefore maintained privately
as a Linear document (project **Auth Control Plane**), and stays the source of
truth for everything not yet published here.
{: .fc-page-lede }

!!! info "How this page grows"
    Each threat-model section publishes here, in full, once its fix merges —
    rewritten from the present-tense "this is exploitable" framing of the
    private analysis into a past-tense account tied to the commit that closed
    it. Two sections qualify today. The rest remain in the private document
    until their fixes land; this page does not summarize, count, or otherwise
    characterize what remains held.

## Telemetry: unauthenticated org-scoped and instance-wide disclosure (CHAOS-4722)

**Fixed in `071c27890`, "Verify org identity server-side on telemetry
routes."**

Before the fix, all four `/api/v1/telemetry/*` routes took a raw, unverified
`X-Org-Id` header as their only identity. `OrgIdMiddleware` did not save them:
it performed its cross-org check only for *authenticated* callers, on the
documented assumption that "downstream endpoints that require auth will 401
via their own dependencies." This router had no such dependency, and it read
the header directly rather than the middleware's resolved value, so even the
authenticated path skipped membership resolution.

That let an unauthenticated caller read and mutate any org's telemetry
opt-in state. `collect_usage_stats()` took no `org_id` at all — it was
instance-wide, returning `total_organizations`, `active_organizations`,
`total_users`, `active_users`, `total_repos`, `total_sync_configs`,
`active_syncs_24h`, `tier_distribution`, `feature_usage`, and `version`. The
precondition chain was self-satisfying: a `POST /opt-in` carrying any
`X-Org-Id` opted that org in, and a subsequent `POST /report` passed its own
gate because the gate was keyed on that same caller-supplied id — a gate
whose key the caller controls is not a gate. This was confirmed reachable
through the public edge (an executed probe on 2026-09-01 returned HTTP/2 200
with a body, passed through Cloudflare), which made it an incident-class
exposure rather than a defense-in-depth gap.

The fix requires a real authenticated caller (`get_current_user`, 401 if
absent or invalid) on every telemetry route, and resolves the acting org from
that authenticated state — the caller's own org, an org they hold a
Membership row for, or any org if they are a superuser — never from the raw
header alone. The instance-wide aggregates on `/report` are additionally
gated to a platform-role (superuser) principal, evaluated against the
*effective* identity: a first review round found the initial check read the
real superuser's JWT claim, which stays true while that superuser is
impersonating a (necessarily non-superuser) target, so the fix now consults
the active impersonation context and derives identity from the impersonation
target when a session is impersonating — the same pattern CHAOS-2303 already
established for GraphQL platform-admin checks. A new counter
(`record_telemetry_org_id_rejected`) makes rejections observable.

## Legacy ingest: fail-open when auth env vars were unset (CHAOS-4720)

**Fixed in `512c4e77b`, "fix(ingest): fail closed on missing
INGEST_API_KEYS/INGEST_SIGNING_SECRET."**

Before the fix, the legacy ingest auth dependency treated an unset
`INGEST_API_KEYS` or `INGEST_SIGNING_SECRET` as nothing to check: when both
env vars were unset, validation returned a permissive success context and the
request proceeded unauthenticated. The dependency was genuinely wired to all
six legacy `/api/v1/ingest/*` routes — confirmed by the fact that an
unauthenticated POST returned 422, not 401, proof the dependency executed and
passed rather than never having been wired at all. These routes were
reachable only on the private Docker network, on both local and prod, which
is what kept this a defense-in-depth item rather than an active incident —
but the underlying shape was a genuine fail-open path, and the network
boundary that limited it was an operational fact, not a property of the code.

The fix makes an unset-credential state fail closed (401) unless
`is_development_environment()` is true (`ENVIRONMENT`/`APP_ENV`/`ENV` =
`development`/`dev`/`local`) — a deliberate opt-in, never the default, since
the environment name defaults to `"production"` when unset. Either credential
alone (API key or HMAC secret) still authenticates exactly as before. A new
counter (`record_ingest_legacy_auth_rejected`) makes the fail-closed branch
observable in production.

## Related reading

* [Credential classes](../../reference/auth/credential-classes.md) — the
  closed 30-entry vocabulary and each class's issuer, validator, and
  lifecycle owner.
* [Endpoint authentication profiles](../../reference/auth/endpoint-profiles.md)
  — the ops inventory rows and how they were discovered.
* [Auth Control Plane ADRs](auth-control-plane-adrs.md) — the eleven Proposed
  ADRs the threat model is an input to.
* [Auth Control Plane decision sheet](auth-control-plane-decision-sheet.md) —
  one-pass ratification: recommendation, alternatives, and risk per ADR.
