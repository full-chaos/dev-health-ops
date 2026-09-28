# Linear team-discovery fixtures (CHAOS-6374)

Captured live 2026-09-28 by chris (guarded capture line, CHAOS-6374) via the
compose stack's real, active Linear credential, one read-only POST of the
exact `TEAMS_QUERY` GraphQL document `discoverLinear`/`iter_teams` send
(providers/linear/client.py) -> `teams.json`.

Sanitized before being committed as a fixture (real person and workspace
data): the real team id/key/name, the real
member ids, the real member names (including one real person's name), the
real member emails (including one real person's email address), and the
workspace timezone are all replaced throughout with fictitious
`11111111-.../ACME/Acme Corp`, `22222222-.../33333333-...` ids,
`svc-bot`/`Team Member` names, `...@oauthapp.linear.app` /
`member@example.invalid` emails, and `Etc/UTC`. The `pageInfo.endCursor`
values (which echo the last node's own id) are updated to match their
replaced ids, keeping the response internally consistent. Every field NAME,
JSON TYPE, and the field SET are preserved exactly as the live API returned
them, including the `members` sub-selection `discoverLinear`/`discover_linear`
never read (Go's own `linearTeamsQuery` sends it anyway -- see that file's
doc comment -- so a truthful fixture keeps it rather than trimming the
query response to only the fields the route consumes).
