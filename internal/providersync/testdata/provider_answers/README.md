# Recorded provider answers (CHAOS-9124)

Real answers of Linear and the Atlassian Teams gateway to the reads the per-kind absence provers ask
(`linear_absence.go`, `internal/atlassianteams/absence.go`). Each file is `{case, method, path, status,
content_type, request_shape, body_shape, body_verbatim}`.

How they were made: `tools/record-provider-answers` (one-off recorder, not a product command), run on
2026-10-10 as a one-off container inside the dev-health compose stack of the bigboy host, read-only, through the
product's own credential path (`providerfoundation.CredentialResolver` over the stored integration credential,
`providerfoundation.NewLinearClient` for Linear and `NewJiraClient` + the vendored `atlassian/graph` client for
the gateway). No credential value was read, printed, put on argv or copied by the lane; the recorder reads the
environment by name in its own process.

What a file holds:

- `body_shape`: every key of the answer and the TYPE of each value (`"<string>"`, `"<number>"`, `"<bool>"`);
  an array keeps its first element only. A `*_found` file holds NO value of the organization.
- `body_verbatim`: set only for a `*_not_found` probe. The probe names an id the recorder made up (a random
  UUID, a random team ARI), so the provider's own wording holds nothing of the organization.

A test that needs a `found` answer builds the body from the recorded shape (`recordedFoundBody`): the shape says
which keys exist, the test fills them with placeholder values. That is a body CONSTRUCTED FROM a recorded shape,
and every test that does so says so.

What the answers say (the provers rest on exactly these):

- Linear `team(id: <unknown key>)` and `project(id: <unknown id>)`: HTTP 200, `errors[0].extensions.code =
  "INPUT_ERROR"`, message `Entity not found: Team|Project`, `data: null`. `teams(filter: {key: {eq: <unknown>}})`:
  HTTP 200, `nodes: []`.
- Atlassian `teamworkGraph_teamUsers` and `graphStore_teamConnectedToContainer` of an unknown team: HTTP 200,
  EMPTY edges (the same answer as a team with none: these two reads cannot prove a team gone). `TeamById` of an
  unknown team: HTTP 200, `errors[0].extensions.classification = "NotFound"`, `statusCode = 404`,
  `data.team.teamV2 = null`. `teamworkGraph_userTeams` of an unknown user: HTTP 200, an empty team list.
