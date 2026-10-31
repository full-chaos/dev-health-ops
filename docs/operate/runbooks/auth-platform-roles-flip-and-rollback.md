---
page_id: op-rb-auth-platform-roles-flip
summary: Move platform authority from public.users.is_superuser to the auth schema with one values change (authService.platformRolesOwned), and roll it back.
content_type: runbook
owner: platform-operations
applicability: current
lifecycle: active
---

# Auth platform roles flip and rollback

With `authService.platformRolesOwned: true` the chart renders `AUTH_PLATFORM_ROLES_OWNED=true` on auth-service, go-api, query-api, and the auth-migrate, auth-grants and auth-backfill Jobs. Platform authority then comes from `auth.platform_role_assignments` and no longer from `public.users.is_superuser` (CHAOS-11003). The render refuses `true` unless users are owned (`authService.phase: 4` with `usersOwnedStage: owned`).

## Rules

- Each step is one `helm upgrade`, one `dho` run or one read. Any non-zero exit means STOP and report. Do not retry in a loop.
- Do not mix an image roll with this config change in one release.
- `dho auth` verbs print counts and ids only, never an address.

## Before the flip

All of these hold:

1. Users are owned and live (CHAOS-10231, the users-owned runbook), the organizations are owned and live (CHAOS-10998, which comes before platform roles), and CHAOS-10993 is live.
2. The users backfill ran, so every public superuser has an assignment. Run `dho auth users-reconcile -platform-roles-gate`. It must print `gated_diffs=0`, including `diff.no_active_platform_admin=0`. Without the flag the class prints as `info.no_active_platform_admin` and cannot fail the run, so always pass the flag here.
3. Run `dho auth platform-role-list`. It must show at least one `platform_admin` assignment that is active now.

## Flip

1. Set `authService.platformRolesOwned: true` in the deploy values. This is the only change. Upgrade.
2. The auth-migrate hook runs first and grants the readers what they need for the switch. Wait for it to finish. Check its log.
3. auth-service and auth-migrate must run the same ops build. Check the two image digests before the roll.
4. The one release rolls auth-service, go-api and query-api together. While they roll, each reader decides alone from live reads. The two authorities agree because the reconcile diff is 0, so the order of the pods does not matter for authority. Wait for all three rollouts to finish.
5. Run `dho auth users-reconcile -platform-roles-gate` again. It must print `gated_diffs=0`.

## Token window

The flip does not bump `token_version`, because the reconcile diff is 0. A token minted before the flip carries the old `is_superuser` claim for up to 60 minutes. No server decision trusts that claim. It affects only the web UI display and the impersonation start gate. If an operator needs the new value at once, sign out and in again.

## Rollback

1. Before the rollback, run `dho auth platform-role-list`. A role granted with `dho auth platform-role-grant` while the switch was on does not exist in `public.users`. List those assignments, and set `is_superuser` on the matching public users by hand. If you skip this, the rollback removes that authority.
2. Set `authService.platformRolesOwned: false`. Upgrade. The env row is gone from every reader.
3. Run `dho auth users-reconcile -platform-roles-gate` and read `gated_diffs`. A count above 0 names a role that still needs step 1.
