---
page_id: op-rb-ask-dev-proxy
summary: Recover Ask Dev web proxy, SSE stream, browser-state, and cancellation failures without exposing prompts, tokens, or provider payloads.
content_type: runbook
owner: platform-operations
source_of_truth:
  - dev-health-web src/app/api/v1/dev/_proxy.ts
  - docs/contribute/architecture/contracts.md
applicability: current
lifecycle: active
---

# Ask Dev web proxy or browser failure

Use this runbook when the permanent Ask Dev window or the `/dev` workspace fails to start, stream, or cancel a run, or when a browser report describes lost state, a stuck stream, or an unexpected error surface. Ask Dev's browser surface never talks to Ops directly: every request crosses one same-origin, server-only proxy in `dev-health-web` before it reaches the authenticated `/api/v1/dev/**` Ops routes.
{: .fc-page-lede }

## Preserve the incident context

Record:

- organization, user, conversation ID, and run ID (opaque identifiers only);
- affected surface (permanent window vs. `/dev` workspace) and browser/OS;
- first and latest failure time;
- the safe `dev_error.v1` code shown to the user, if any;
- whether the failure is one run, one user, one organization, or platform-wide;
- whether the failure is on submission, mid-stream, on cancellation, or on history/evidence load.

Do not record prompts, questions, provider request or response payloads, chain-of-thought, tool call arguments or results, access tokens, or provider credentials. The proxy and the Ops runtime are both designed so none of these ever reach browser-visible logs or analytics; if you find one that has, treat it as a security event and follow the security incident process, not this runbook.

## Classify the failure

| Failure | Evidence | Response |
| --- | --- | --- |
| Proxy rejects the request before it reaches Ops | `dev_web_error.v1` with a same-origin/CSRF or payload-limit reason, no upstream call logged | Confirm the request is a genuine same-origin browser call; a stale or replayed request from a proxy/CDN layer is expected to fail here |
| Upstream Ops call never starts | Web logs show no outbound call; readiness or entitlement check failed first | Check the organization's Ask Dev entitlement and readiness state through the admin API (`GET /api/v1/admin/ask-dev`) before assuming the proxy is broken |
| Stream starts, then stalls with no terminal event | SSE connection open, no `done` or error event within the expected window | Check Ops-side run admission (one active run per user, five per organization, 20 user requests per 15 minutes, 100 organization requests per hour) and the runtime ceilings (four model rounds, six tool calls, 45 seconds total); a run that silently exceeds a ceiling should still emit a safe terminal error — a stall with no terminal event at all is a bug, not an admission rejection |
| Provider call fails (400/401/429/5xx from the model provider) | Ops logs show an upstream provider HTTP error; browser shows a generic `provider_unavailable`/`source_unavailable` safe error | The proxy and the Ops adapter never forward raw provider error bodies to the browser — this is by design (see [`safeUpstreamError` boundary](#the-safeupstreamerror-boundary) below). Diagnose the real provider error from Ops-side logs only, never by relaxing the browser-facing error allowlist |
| Cancellation does not stop the run | User closes the panel or clicks stop; a later answer still arrives or the run is not recorded `cancelled` | Confirm the browser actually closed the SSE connection (a backgrounded tab or a proxy/CDN keep-alive can mask this); Ops treats a closed stream as a cancellation signal and waits for the recorder to persist a durable `cancelled` terminal state before releasing the stream task — a run that keeps executing after a genuinely closed connection is a reliability bug, not expected behavior |
| Conversation/history/evidence load fails or returns unexpectedly empty | `GET /api/v1/dev/conversations/{id}/transcript` or an evidence-expansion call fails or 404s | A cross-tenant, cross-user, expired, retention-zero, or deleted conversation/evidence reference intentionally returns the same not-found response as a missing one — this is the correct fail-closed behavior, not a bug, unless the requesting user genuinely owns the referenced conversation |
| Browser state looks wrong after switching organizations | Stale conversation/transcript visible after an org switch or during impersonation | Confirm the web build includes the org-scoped state reset fix (CHAOS-3215 hardening); a build without it is known to leak the previous organization's Ask Dev state in the browser until a full page reload |

## The `safeUpstreamError` boundary

The `dev-health-web` proxy (`src/app/api/v1/dev/_proxy.ts`) enforces `import "server-only"`, checks same-origin/CSRF on every mutation, applies a payload limit, forwards the authenticated session's access token to Ops on the outbound leg only, and always responds `no-store`. Upstream Ops error bodies are never passed through raw: a dedicated allowlist function admits only `code`, `safe_message`, `retryable`, `request_id`, and `limit_reset_at` from an upstream error — any other field, including a raw provider error body, is collapsed to a generic safe message before it reaches the browser. SSE stream bytes are forwarded verbatim up to a byte cap; the client-side stream parser (`dev-health-web` `src/lib/dev/client.ts`) independently validates every event against its declared type before rendering it, rejecting a payload that doesn't match its event type.

When triaging, never work around this boundary by inspecting or forwarding raw upstream error content to a customer or into a browser-visible surface — diagnose from Ops-side logs, which are allowed to contain more detail (still never prompts, tool payloads, or credentials; see [Privacy and data-handling responsibilities](../../admin/security-and-audit/privacy.md#ask-dev-data-boundary)).

## Recover

1. Confirm the affected scope: one run, one user, one organization, or platform-wide.
2. Check Ask Dev entitlement, readiness, and emergency-disable state for the affected organization (`GET /api/v1/admin/ask-dev`).
3. Check Ops-side logs for the actual upstream failure (provider error, admission rejection, storage failure) — never infer this from the browser-visible safe error alone.
4. For a stuck or non-terminating stream, verify the run's server-side terminal state directly; do not tell the customer to retry until Ops confirms the run actually reached a terminal state (or force one operator-side if the recorder itself is stuck).
5. For an organization-wide or platform-wide failure, use the organization emergency disable (or the global feature decision as the last resort) to stop new runs while the root cause is fixed; this does not delete BYO credentials, retained conversations, or their retention/deletion policy.
6. Re-enable normal operation and verify one real run end-to-end before closing the incident.

## Security escalation

A raw prompt, tool payload, chain-of-thought, access token, or provider credential observed in browser state, a URL, a client-visible log, or an analytics event is a security event, not a routine proxy failure — stop, preserve non-secret evidence, and follow the security incident process rather than continuing this runbook.
