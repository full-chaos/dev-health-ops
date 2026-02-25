# Team Configuration

This guide covers the fastest path to mapping repositories and providers to teams.

## Recommended path

1. Define your team mapping source (YAML file, Jira, GitHub, GitLab, or synthetic).
2. Run `dev-hops sync teams` with the appropriate provider.
3. Recompute daily metrics so team-scoped views include the latest mappings.

## Command reference

For provider-specific commands and flags, use [CLI Reference - sync teams](./ops/cli-reference.md#sync-teams).

## Next onboarding steps

- Enterprise SSO configuration: [SSO Setup](./sso-setup.md)
- Deployment and operations checks: [Enterprise Features Manual Test Plan](./ops/enterprise-test-plan.md)
