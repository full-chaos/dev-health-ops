# Team Configuration

This guide covers the fastest path to mapping repositories and providers to teams.

## Org-scoped vs No-org Sync (ClickHouse is the system of record)

As of CHAOS-2600 CS5, **ClickHouse is the system of record for the team catalog** — both sync paths write ClickHouse directly via `insert_teams`. There is no Postgres team projection and no Postgres-first control plane. The only difference between the paths is org tagging:

- **Org-scoped (`--org ORG`)**: Provider data is written directly to ClickHouse with each team row tagged with `org_id` (multi-tenant scoping). It does **not** project to PostgreSQL `team_mappings` and does **not** call any Postgres→ClickHouse bridge.
- **No-org (no `--org`)**: Provider data is written directly to ClickHouse with no org tag, preserving synthetic/local seeding.

> The legacy "Managed Mode" that projected into PostgreSQL `team_mappings` via `TeamDriftSyncService` and then bridged into ClickHouse was **removed in CS5**. The Postgres mapping models/services are dropped in CS6.

## Provider Capability Registry

The shared provider capability registry (`providers/team_capabilities.py`) defines which providers are registered for org-scoped drift discovery. The supported providers are:
- GitHub
- GitLab
- Jira
- Linear
- Microsoft Teams

The org-scoped CLI path reads this registry to log whether a provider is registered for drift discovery, then writes ClickHouse directly regardless.

## Admin team/identity configuration

The admin team and identity surface is ClickHouse-native (CHAOS-2600 CS5): admin team CRUD writes the ClickHouse `teams` table, and identity→team membership is stored in the ClickHouse `identities` table with surgical `teams.members` updates. Admin curation (such as `project_keys` and `repo_patterns`) lives in ClickHouse; no Postgres `team_mappings` / `IdentityMapping` rows are written. The Postgres-`team_mappings` drift-review/`sync_policy` flow is disabled in CS5 (the drift-review endpoints return HTTP 501) pending a ClickHouse-backed rebuild in CS6.

## Recommended path

1. Define your team mapping source (YAML file, Jira, GitHub, GitLab, or synthetic).
2. Run `dev-hops sync teams` with the appropriate provider.
3. Recompute daily metrics so team-scoped views include the latest mappings.

## Command reference

For provider-specific commands and flags, use [CLI Reference - sync teams](./ops/cli-reference.md#sync-teams).

## Next onboarding steps

- Enterprise SSO configuration: [SSO Setup](./sso-setup.md)
- Deployment and operations checks: [Enterprise Features Manual Test Plan](./ops/enterprise-test-plan.md)
