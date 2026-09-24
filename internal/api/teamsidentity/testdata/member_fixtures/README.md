# Member-discovery fixtures (CHAOS-6313)

`github_team_members.json` and `github_user_{1,2,3}.json` are the shapes of
`GET .../teams/{team}/members` (a list of simple-user objects) and
`GET /users/{login}` (the complete user) that GitHub returned on 2026-09-24
for three public accounts, read with the host's authenticated `gh` CLI (GET
only, no token printed or stored). Before they were committed every login,
id, node id, name, e-mail, company, blog, location and bio was replaced with
fictitious `member-N` / `example.invalid` values; every field name, JSON
type and the field set are unchanged. The three detail records were then
set to the shapes the code branches on: a name and an e-mail, a name and a
null e-mail, and a null name and null e-mail.

`identities.json` is the identity catalog both planes match discovered members
against; the Python oracle builds `ClickHouseIdentity` objects from it and the
Go oracle builds `Identity` values from the same file.

GitLab, Linear and Jira responses are not captured: those endpoints need a
provider credential this repository does not hold. Their stubs are authored
from each provider's documented field set (see the stubs in
`venue_oracle_members_test.go` and
`internal/apiservice/venue_members_stub_integration_test.go`) and exercise
the branches the code takes (paging, inactive users, null and missing
fields). Sanitized captures from a real Jira, GitLab and Linear workspace
remain open under the ticket that tracks provider fixture capture.
