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

## Ask Dev named a subject it could not confirm

When you ask Ask Dev about a specific project, repository, team, issue, pull
request, or work unit, the server resolves that name against your workspace's
authorized catalog **before** it looks anything up. Three replies come from
that step, and each means something different:

- **"No matching subject was found."** No entity you are authorized to see
  matches the name. This is also what you see for an entity that exists in a
  different workspace, deliberately: the reply cannot be used to discover
  whether something exists elsewhere. Check the spelling, or the workspace you
  are signed in to.
- **"More than one authorized entity matches..."** The name is ambiguous, or
  it is only part of a name. Ask again with the full name of exactly the one
  you mean — a partial name is never resolved to a best guess.
- **"This answer is temporarily unavailable."** The catalog could not be
  reached. This one is worth retrying in a few minutes; the others are not.

Ask Dev will not answer about your whole organization under a name it could
not confirm. If a question that names something specific comes back with one
of these replies, that is the intended behaviour, not a lookup failure.

Questions that name nothing specific still answer organization-wide as usual.

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
