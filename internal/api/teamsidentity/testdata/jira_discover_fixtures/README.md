# Jira team-discovery fixtures (CHAOS-6374)

Captured live 2026-09-28 by chris (guarded capture line, CHAOS-6374) via the
compose stack's real, active Jira credential, one read-only GET:

- `GET /rest/api/3/project/search?maxResults=100&startAt=0` -> `project_search.json`

Sanitized before being committed as a fixture: the real Atlassian site host
(`chrisgeorge.atlassian.net`, a real person's domain) is replaced throughout
with `acme-corp.atlassian.net`. The two projects whose real name/key named
this org directly (`FC`/"Full Chaos", `FCD`/"Full Chaos Discovery") are
renamed `ACM`/"Acme Corp" and `ACMD`/"Acme Corp Discovery". Every other
project's key/name (generic short codes: API, AUTH, BILL, DATA, ML, NOTIF,
OPS, SEC, SRCH, SSP, SUP, WEB) and every numeric project/avatar id are kept
as captured -- they identify no person or organization. Every field NAME,
JSON TYPE, and the field SET are preserved exactly as the live API returned
them.

`discoverJira`/`discover_jira` (team_discovery.py:299-338) is a single,
unpaginated GET (`maxResults=100`, no `startAt` follow-up even though the
real endpoint supports it) -- this one capture is the complete real response
the route ever reads from, not a truncated sample.
