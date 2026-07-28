---
page_id: use-permissions
summary: Distinguish missing access, feature availability, and preview-only routes.
content_type: troubleshooting
owner: documentation
applicability: current
lifecycle: active
---

# Missing permission or unavailable view

1. Confirm you are in the intended workspace and signed in with the expected role.
2. Confirm the destination is currently supported rather than preview-only.
3. Confirm any required workspace feature or entitlement is enabled.
4. Ask an administrator to verify membership and access without sharing secrets.
5. Retain the route and visible message for escalation.

AI Attribution, several report subroutes, and some other product routes are currently marked preview in the product navigation source. Their absence from v2 navigation is intentional.

## Ask Dev metrics

Ask Dev metric catalog and query fields require an authenticated workspace
member with metrics-read access and the Ask Dev entitlement. The server derives
the organization and effective permissions from the session and reauthorizes
all requested scope IDs. Supplying another workspace's ID does not reveal
whether that entity exists.

A blank or unavailable metric is not automatically zero. Preserve the selected
scope, dates, metric name, and displayed state when asking an administrator for
help. `NO_MATCH`, `INSUFFICIENT_EVIDENCE`, `PARTIAL`, `STALE`, `UNCONFIGURED`,
and `UNAVAILABLE` each identify a different next check; only `ZERO` represents
matching measured rows whose value is zero.
