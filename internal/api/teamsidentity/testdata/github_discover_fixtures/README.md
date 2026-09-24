# GitHub team-discovery fixtures (CHAOS-6311)

Captured live 2026-09-23 via the host's authenticated `gh` CLI (read-only,
GET only, no token printed or stored) to settle whether `GET
/orgs/{org}/teams` (the list endpoint `discoverGitHub` pages through) already
carries `members_count`/`repos_count`, or whether that data is only on `GET
/orgs/{org}/teams/{slug}` (the per-team detail endpoint).

Endpoints captured:
- `gh api /orgs/full-chaos/teams --paginate` -> `teams_list.json`
- `gh api /orgs/full-chaos/teams/ops-team` -> `team_detail.json`
- `gh api /orgs/full-chaos/teams/ops-team/repos` -> `team_repos.json` (real response: `[]`, the team owns no repos -- not sanitized, an empty array carries no org-internal data)
- `gh api /orgs/full-chaos` -> `org_detail.json` (needed for the venue oracle, CHAOS-6311: PyGithub's `get_organization()` lazily completes the Organization object with its own `GET /orgs/{org}` call before `discover_github` ever calls `.get_teams()` -- a real extra request Go's port never makes, since Go never fetches org detail at all; this fixture only exists so the Python oracle run can proceed past that lazy-completion call, its content is otherwise unused by discover_github's own logic)

Result: `teams_list.json`'s field set has NO `members_count`/`repos_count`.
`team_detail.json`'s does. This settles discover_github.go's implementation
as necessary, not merely "safe either way": the per-team detail call
(`githubTeamDetailFor`) is REQUIRED to populate `member_count`, since the
list response this org's real API returned genuinely does not carry it.

Sanitized before being committed as fixtures (org-internal data): the real
org login/name/slug/ids/node_ids/urls/description are replaced throughout
with fictitious `acme-corp`/`platform-team`/`9000001`/`1000001`/`example.invalid`
values. Every field NAME, JSON TYPE, and the field SET are preserved exactly
as the live API returned them -- nothing added, nothing removed, values only
renamed. `created_at`/`updated_at` timestamps are kept as captured (not
org-identifying).
