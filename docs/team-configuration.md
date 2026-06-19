# Team Configuration

This guide covers the fastest path to mapping repositories and providers to teams.

## Managed vs Unmanaged Sync Modes

The team sync behavior depends on whether the `--org` flag is provided:

- **Managed Mode (org-scoped, `--org ORG`)**: Provider data is projected into PostgreSQL `team_mappings` via the shared `TeamDriftSyncService`. This is a Postgres-first control plane. The sync preserves admin-curated configurations (such as `project_keys` and `repo_patterns`) and flags changes for review if `sync_policy == 1`. The CLI org path never writes ClickHouse directly; instead, it calls `bridge_teams_to_clickhouse(org_id)` to write the resolved teams and members to ClickHouse.
- **Unmanaged Mode (no `--org`)**: Provider data is written directly to ClickHouse, preserving synthetic/local seeding.

## Provider Capability Registry

The shared provider capability registry (`providers/team_capabilities.py`) defines which providers support org-scoped drift discovery. The supported providers are:
- GitHub
- GitLab
- Jira
- Linear
- Microsoft Teams

Both the worker drift path and the CLI org path read from this registry.

## Admin-Curated Configuration Preservation

When syncing teams in managed mode, the sync process preserves admin-curated configurations:
- Empty provider values do not overwrite non-empty curated `project_keys` or `repo_patterns`.
- If `sync_policy == 1`, changes are flagged for admin review rather than being silently merged.

## Recommended path

1. Define your team mapping source (YAML file, Jira, GitHub, GitLab, or synthetic).
2. Run `dev-hops sync teams` with the appropriate provider.
3. Recompute daily metrics so team-scoped views include the latest mappings.

## Command reference

For provider-specific commands and flags, use [CLI Reference - sync teams](./ops/cli-reference.md#sync-teams).

## Next onboarding steps

- Enterprise SSO configuration: [SSO Setup](./sso-setup.md)
- Deployment and operations checks: [Enterprise Features Manual Test Plan](./ops/enterprise-test-plan.md)
