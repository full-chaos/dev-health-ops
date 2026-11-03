---
page_id: op-rb-auth-platform-roles-flip
summary: Move platform authority from public.users.is_superuser to the auth schema with one values change (authService.platformRolesOwned), and roll it back.
content_type: runbook
owner: platform-operations
applicability: current
lifecycle: active
---

# Auth platform roles flip and rollback

With `authService.platformRolesOwned: true` the chart renders `AUTH_PLATFORM_ROLES_OWNED=true` on auth-service, go-api, query-api, the auth-migrate, auth-grants and auth-backfill Jobs, and the River step (the river-migrate hook, or the migrate Job when the hook is off). Platform authority then comes from `auth.platform_role_assignments` and no longer from `public.users.is_superuser` (CHAOS-11003). The render refuses `true` unless users are owned (`authService.phase: 4` with `usersOwnedStage: owned`).

With the switch on, no runtime role writes `public.users.is_superuser` (CHAOS-10993). The River step gives the go-api role INSERT and UPDATE on the `users` columns its writers name, without `is_superuser`, and go-api's readiness check holds that exact set. The auth runtime role never holds a write of the column, in either state. The column stays readable: it is a frozen copy. The table owner and the migration login are not runtime roles and keep the write; only the app guards stop them.

## Rules

- Each step is one `helm upgrade`, one `dho` run or one read. Any non-zero exit means STOP and report. Do not retry in a loop.
- Do not mix an image roll with this config change in one release.
- `dho auth` verbs print counts and ids only, never an address.

## Before the flip

All of these hold:

1. Users are owned and live (CHAOS-10231, the users-owned runbook): `authService.phase: 4` with `usersOwnedStage: owned`. `public.organizations` and `public.memberships` stay the authority (D6651: orgs owned, CHAOS-10998, is cancelled and is not a step before this flip).
2. Every go-api pod runs a build with the column omission of CHAOS-10993 (ops #5531, f1440088) and one full go-api roll of such a build is done. The REVOKE with an older pod live gives that pod 42501 on a user write, and its readiness check fails on the exact posture.
3. The release that removes the auth runtime role's INSERT (`is_superuser`) on `public.users` (CHAOS-10993) is rolled. Its grants pass is a narrowing: it refuses and prints the digest until `authService.migrate.acknowledgeGrantNarrowing` carries it.
4. The users backfill ran, so every public superuser has an assignment. Run `dho auth users-reconcile -platform-roles-gate`. It must print `gated_diffs=0`, including `diff.no_active_platform_admin=0`. Without the flag the class prints as `info.no_active_platform_admin` and cannot fail the run, so always pass the flag here.
5. Run `dho auth platform-role-list`. It must show at least one `platform_admin` assignment that is active now.
6. The restore fence carries the section `auth.platform_role_assignments` (a revoke after the restore target is deleted again; a grant after it is named in `regrant_platform_role` and never inserted; `no_active_platform_admin` names a fence that leaves no active platform administrator; the break-glass is `auth-migrate platform-role-bootstrap -principal <uuid> -confirm`). The fence apply is the only writer. Read these lines in `auth-migrate restore-fence status` after a restore drill before the flip.

## Flip

1. Set `authService.platformRolesOwned: true` in the deploy values. This is the only change. Upgrade.
2. The auth-migrate hook runs first and grants the readers what they need for the switch. Wait for it to finish. Check its log. Then the River step narrows the go-api role's `users` writes to columns.
3. auth-service and auth-migrate must run the same ops build. Check the two image digests before the roll.
4. The one release rolls auth-service, go-api and query-api together. While they roll, each reader decides alone from live reads. The two authorities agree because the reconcile diff is 0, so the order of the pods does not matter for authority. Wait for all three rollouts to finish.
5. Run `dho auth users-reconcile -platform-roles-gate` again. It must print `gated_diffs=0`.

## Token window

The flip does not bump `token_version`, because the reconcile diff is 0. A token minted before the flip carries the old `is_superuser` claim for up to 60 minutes. No server decision trusts that claim. It affects only the web UI display and the impersonation start gate. If an operator needs the new value at once, sign out and in again.

## Rollback

1. Before the rollback, run `dho auth platform-role-list`. A role granted with `dho auth platform-role-grant` while the switch was on does not exist in `public.users`. List those assignments, and set `is_superuser` on the matching public users by hand, with the owner (migration) login: this is the break-glass. While the switch is on, no runtime role can write the column, and `dho users update --superuser` is refused. If you skip this, the rollback removes that authority.
2. Set `authService.platformRolesOwned: false`. Upgrade. The env row is gone from every reader. The River step gives the go-api role the table-wide INSERT and UPDATE on `users` back before go-api rolls; a `helm rollback` runs the same step in its pre-rollback hook. A `helm rollback --no-hooks` leaves the grants narrowed and the rolled-back go-api pods not ready.
3. Run `dho auth users-reconcile -platform-roles-gate` and read `gated_diffs`. A count above 0 names a role that still needs step 1.

## Platform owner

`platform_owner` is a marker role (CHAOS-12294). It holds no action: an owner acts only through a `platform_admin` row it also holds. Only the operator grants or revokes it, with the migration role:

- `auth-migrate platform-owner-grant -principal <uuid> -confirm` grants it. It is permanent, audited with `source` `operator-cli` and no actor, and it emits one outbox event. The first grant makes the first owner.
- `auth-migrate platform-owner-revoke -principal <uuid> -confirm` revokes it. There is no last-owner guard, so a compromised owner is removed at once. With no active owner the owner exemption is off, which is a safe state.

`platform-role-bootstrap` grants `platform_admin` only; it never grants `platform_owner`. The API and `dho auth platform-role-grant` / `platform-role-revoke` refuse the role, and write an audit row `auth.platform_owner.grant_refused` or `auth.platform_owner.revoke_refused` with outcome `denied`. One principal may hold both roles, as two rows with two audit rows. Owner assignments do not exist in `public.users`; a rollback of the switch does not carry them.
