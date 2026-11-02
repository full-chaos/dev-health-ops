---
page_id: op-rb-auth-orgs-owned-flip
summary: Make auth-service the writer of organizations and memberships with one values change (authService.phase 5), and roll it back.
content_type: runbook
owner: platform-operations
applicability: current
lifecycle: active
---

# Auth orgs-owned flip and rollback

At `authService.phase: 5` the chart renders `AUTH_ORGS_OWNED=true` on auth-service, go-api, the migrate Job, the River hook and the backfill Job. go-api then reads `public.memberships` and writes none, inserts no organization, and updates only the organization columns auth-service does not write (not `id`, `slug`, `name`, `is_active`, `created_at`). The organization `DELETE` of the purge stays. The render refuses phase 5 unless `goApi.authProxy.orgProvisioning: true` and `AUTH_MEMBERSHIP_ROUTES` is on, and unless `authService.usersOwnedStage` is empty.

## Rules

- Each step is one `helm upgrade`, one `dho` run or one read. Any non-zero exit means STOP and report. Do not retry in a loop.
- Do not mix an image roll with this config change in one release.
- `dho auth` verbs print counts and ids only, never an address.

## Order

0. auth-service holds the invite keys: `AUTH_INVITE_TOKEN_SECRET` and `AUTH_INVITE_TOKEN_SECRET_PREVIOUS`, set through `authService.extraEnv` by `valueFrom.secretKeyRef` (the membership routes refuse to start without both).
1. `goApi.authProxy.orgProvisioning: true` is live on every go-api pod, and one full roll has finished. The REVOKE with an older pod live gives that pod `42501` on a membership write, and its readiness check fails on the exact posture.
2. `dho auth orgs-backfill` ran, and `dho auth orgs-reconcile -exact` prints 0 for every class.
3. The River hook image is at or above the build that carries the AUTH_ORGS_OWNED posture. Roll it with every domain group whose posture moved since the live anchor (D6446).
4. auth-service and auth-migrate run the same ops build.

## Flip

1. Set `authService.phase: 5`. Remove `authService.usersOwnedStage`. Upgrade. The release is the only change.
2. The auth-migrate hook runs first. The River hook then revokes the api role's memberships writes and narrows its organizations grants. Wait for both. Check their logs.
3. go-api pods roll. Each readiness check holds the narrowed manifest. A pod that reports not ready fails the exact posture check: STOP.
4. Run `dho auth orgs-reconcile -exact`. It must print 0 for every class.

## Rollback

1. Set `authService.phase: 4` with `authService.usersOwnedStage: owned`. Upgrade. The env row is gone from go-api, the migrate Job and the River hook.
2. The River hook runs and grants the api role the full manifest again. A rollback to a revision runs the same hook (`pre-rollback`). Wait for it.
3. go-api pods roll back to the full manifest. Run `dho auth orgs-reconcile -exact` and read the counts.
4. If `authService.platformRolesOwned` was on, switch it off first, in its own release: it needs phase 5 ([platform roles runbook](auth-platform-roles-flip-and-rollback.md)).
