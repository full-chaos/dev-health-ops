# GitLab team-discovery fixtures (CHAOS-6374)

Captured live 2026-09-28 by chris (guarded capture line, CHAOS-6374) via the
compose stack's real, active GitLab credential, read-only GET requests:

- `GET /api/v4/groups/{group}` -> `group.json`
- `GET /api/v4/groups/{group}/subgroups` -> `subgroups.json` (real response:
  `[]`, the captured group has no subgroups)
- `GET /api/v4/groups/{group}/projects` -> `projects.json`

Sanitized before being committed as fixtures: the real group id/name/path
(`107805027`/`fullchaos`/`full.chaos`), the real project ids/names/paths
(`77145099`/`dev-health-ops`, `71133891`/`chaos-ops`) and the real
`creator_id` (`179182`) are replaced throughout with fictitious
`9100001`/`acme-corp`, `9200001`/`repo-one`, `9200002`/`repo-two`, and
`9000001` values. Every field NAME, JSON TYPE, and the field SET are
preserved exactly as the live API returned them. The `gitlab.com` /
`registry.gitlab.com` hosts are kept (GitLab's own SaaS domains, not
org-identifying). `created_at`/`updated_at` timestamps are kept as captured.

**Coverage gap (deliberate, named):** `subgroups.json` is genuinely empty (the
real group has none), so this fixture set does NOT exercise the
`discoverGitLab`/`discover_gitlab` per-subgroup walk with a non-empty
subgroups page, nor the truncation-warning path
(`maxGitLabDiscoverySubgroups`/`maxGitLabDiscoveryProjects`). The venue oracle
(`venue_oracle_gitlab_test.go`) proves the Go port SENDS the
`GET .../subgroups` request and correctly produces zero extra teams from an
empty response — that request-shape and empty-list-handling parity is real
and pinned. The non-empty-subgroup-page code path and the truncation-warning
strings are NOT pinned by this real-data capture; see that test's own
TEST-EVIDENCE for the explicit statement.

`projects.json` is reused, unchanged, to answer BOTH the per-group
projects walk (`GET .../groups/{id}/projects`) and the separate flat
`include_subgroups=true` walk `discoverGitLab`/`discover_gitlab` also makes:
with zero subgroups, GitLab's real API returns the identical project set
either way, so reusing the one captured response for both requests is what
the real API would answer, not a stand-in.
