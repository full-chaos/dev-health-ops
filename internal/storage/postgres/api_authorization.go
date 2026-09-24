package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// apiPosture is the dho api Service's declared Postgres privilege manifest
// (CHAOS-6269, spec.md §4.4). It started EMPTY on purpose: S0 provisions the
// role and proves it holds nothing beyond the baseline every runtime role
// must hold (CONNECT, USAGE on the public schema, no CREATE, no ownership,
// no privilege on any other relation, sequence or function in the public or
// River schema -- rolePostureQuery's own catch-all predicates, unconditional
// for every caller of CheckRolePosture). CHAOS-6244 was the first route PR
// to need anything more (four SELECT-only tables: the acr entitlement route
// is a pure read, and it writes no audit row -- see internal/apiservice/acr's
// package doc for why the Python credential-audit path is not ported for
// that internal route). CHAOS-6246 (external-ingest) is the first to need
// writes: accepting a customer push durably records a Postgres status row
// and the raw payload before it is acknowledged (the CC22 accept sequence),
// and bumping a used token's last_used_at/last_used_ip is part of the auth
// path itself. Each route PR adds exactly the grants its own tables need
// directly to this function's RequiredTables/ColumnScoped/RequiredSequences,
// in the SAME PR that ships the route, the same discipline
// domainPosture/coordinatorPosture already follow.
//
// Unlike the three River runtime roles, the api role never opens a River
// pool and is not part of the CHAOS-3033 Option B split -- it is declared
// here, in the same package, because CheckRolePosture/RolePosture is the
// one reusable readiness mechanism this repo has for "prove a login holds
// exactly its declared manifest," not because api is a River role. Passing
// this package's riverSchema parameter through CheckAPIAuthorization still
// matters: it is what makes rolePostureQuery assert the api role holds ZERO
// privilege on the River schema too, exactly as it does for every other
// role's own posture.
//
// The River migration applies this manifest as GRANT statements to the api
// role once the role exists (riverstore.MigrationOptions.APIRole, derived
// from APIPosture in internal/rivermigrate), so the grant side and
// this readiness side are one list.
func apiPosture() RolePosture {
	return RolePosture{
		RequiredTables: []TablePrivilege{
			// Read root for GET /api/v1/internal/acr/entitlements/{org_id}:
			// existence (404 if absent) and the tier fallback when no
			// org_licenses row exists (internal/apiservice/acr/store.go).
			// Also plan area L's self-service profile: read, and the name
			// and description update of PATCH /api/v1/orgs/me. The admin
			// org routes add insert: create_org (update_org shares the
			// self-service UPDATE grant above). Delete: the org row is the
			// LAST of org_deletion.py's 46 purge targets. Widened in
			// place, per the one-entry rule below.
			{"organizations", true, true, true},
			// The acr route reads one feature row (key =
			// "agent_context_runtime"). The admin feature-flag routes list
			// every flag and update is_enabled/is_beta/is_deprecated.
			{"feature_flags", false, true, false},
			// Per-org override for that same feature. CHAOS-6306: a purge
			// target, delete added. The admin override routes create and
			// update rows.
			{"org_feature_overrides", true, true, true},
			// License tier + features_override JSON. The admin org PATCH
			// route syncs an existing row's tier/managed_by on an actual
			// tier change (OrganizationService._sync_license_tier);
			// CHAOS-6306 adds delete (a purge target) on top.
			// org_licenses insert: the Stripe webhook's checkout path creates
			// an org's first license row (CHAOS-6517).
			{"org_licenses", true, true, true},
			// external-ingest (CHAOS-6246): bearer-token auth resolves the
			// token row and bumps last_used_at/last_used_ip on every
			// request that reaches a scope check (auth.go's bumpLastUsed).
			// CHAOS-6319 (customer-push admin writes) mints tokens (INSERT)
			// and rotates/revokes them (UPDATE revoked_at), widened in place.
			{"external_ingest_tokens", true, true, false},
			// The token's bound source, and source-ownership resolution
			// (ownership.go's resolveEffectiveMode). CHAOS-6319 registers
			// sources (INSERT) and patches them (UPDATE), widened in place.
			{"external_ingest_sources", true, true, false},
			// The CC22 accept sequence's status row: created on NEW,
			// updated on RETRY/mark-stream-unavailable, read by
			// GET /batches* and the idempotency NEW/REPLAY/CONFLICT/RETRY
			// resolution.
			{"external_ingest_batches", true, true, false},
			// The raw batch payload, written (or refreshed) in the SAME
			// transaction as the status row (payload.go's upsertPayloadTx),
			// read back only to prove the fail-closed durability
			// precondition before XADD.
			{"external_ingest_batch_payloads", true, true, false},
			// Read-only: GET /batches/{id}'s per-record rejection detail.
			// This route never writes a rejection row -- that is the
			// CHAOS-2697 worker's job, over the domain role, not this one.
			{"external_ingest_rejections", false, false, false},
			// Read-only: GET /batches/{id}'s recompute.jobs (CHAOS-6321
			// round 1 -- the api role held no grant on this table at all
			// until this route actually queried it; listRecomputeJobs
			// joins by org_id/source_system/source_instance/dispatched_at,
			// same as recompute_status.py's get_recompute_jobs). This route
			// never writes a job row -- that is the CHAOS-2699 recompute
			// dispatcher's job, over the domain role, not this one.
			{"external_ingest_recompute_jobs", false, false, false},
			// Managed-sync ownership matching (ownership.go's
			// findActiveManagedOwner): read-only. A purge target, delete
			// added.
			{"integration_sources", false, false, true},
			{"integrations", false, false, true},
			// CHAOS-6319: the customer-push ownership check reads a managed
			// integration's credential row (provider and plain config only;
			// the encrypted payload is never read or decrypted here). A
			// purge target, delete added. CHAOS-6449 (credential admin
			// writes) creates rows (INSERT) and updates them (UPDATE),
			// widened in place.
			{"integration_credentials", true, true, true},
			// The protected-route principal (internal/api/policy): the users
			// row behind every access token, org membership behind
			// X-Org-Id, and the active impersonation session of a superuser.
			// CHAOS-6304 (admin user routes) is the first route area over
			// this principal to WRITE the users row itself: user CRUD and
			// password changes. Widened in place, per the one-entry rule
			// above.
			{"users", true, true, true},
			// CHAOS-6305 (admin org routes) is the first route area to
			// WRITE memberships: add_member, update_member_role,
			// remove_member, transfer_ownership. Widened in place.
			{"memberships", true, true, true},
			// setUserPassword revokes every outstanding refresh token
			// (refresh_tokens.py's revoke_all_for_user) on a password
			// change. A purge target, delete added. The session routes
			// (login, switch-org, social login, refresh rotation) insert a
			// row per issued refresh token. Widened in place.
			{"refresh_tokens", true, true, true},
			// The login route's failed-attempt lockout
			// (services/login_attempts.py): read, insert the first failure,
			// update the count and lock, delete on a successful login.
			{"login_attempts", true, true, true},
			// org_invites: create_org_invite (CHAOS-6391) checks for a
			// pending invite (read) and inserts one; a purge target, delete
			// added. Never updated by this role.
			{"org_invites", true, false, true},
			// CHAOS-6303 (admin impersonation routes) is the first route
			// area over this principal to WRITE the impersonation session
			// it reads: start_impersonation ends any prior open session
			// (UPDATE) and inserts the new one; stop_impersonation ends it
			// (UPDATE). CHAOS-6306 adds delete (a purge target). ONE entry
			// per table is a hard requirement (riverstore.ValidateMigrationOptions
			// rejects a duplicate TableName as a silent generic
			// ErrMigrationConfiguration) -- a later route area needing
			// more on an already-declared table widens this entry in
			// place, never appends a second one.
			{"impersonation_sessions", true, true, true},
			// Plan area A: /health's application schema revision and
			// /health/workers' worker heartbeat presence.
			{"alembic_version", false, false, false},
			{"worker_instances", false, false, false},
			// Plan area K: the org telemetry settings, the instance usage
			// counts /telemetry/report reads, and its audit row. (There is no
			// Postgres repos table: repositories live in ClickHouse.)
			// Delete: settings is also a purge target (encrypted settings
			// included).
			{"settings", true, true, true},
			// A purge target, delete only.
			{"sync_configurations", false, false, true},
			// CHAOS-6437: the sync coverage read serves the stored
			// projection (build_sync_coverage_summary). Read-only; rows go
			// with their config by ON DELETE CASCADE, so no purge grant.
			{"sync_coverage_projections", false, false, false},
			// The generic audit writer (internal/api/audit): impersonation
			// start/stop, password_changed, member_invited, and plan area K's
			// telemetry-report audit row -- one entry, every area. Also a
			// purge target: the org's whole audit trail is deleted along
			// with everything else.
			{"audit_logs", true, false, true},
			// webhook intake (CHAOS-6247): GitHub/GitLab/Jira persist their
			// durable delivery row (INSERT; the (provider, delivery_key)
			// conflict fallback is a SELECT, always implicit) then publish to
			// the job outbox (joboutbox.Producer.Publish, same INSERT-only
			// shape every domain-role producer already has --
			// domain_authorization.go's own worker_job_outbox row).
			{"webhook_deliveries", true, false, false},
			{"worker_job_outbox", true, false, false},
			// PagerDuty's binding lookup is read-only; its one write is the
			// candidate->ready transition on a verified ping
			// (mark_candidate_ready_from_verified_ping). Binding admin CRUD
			// (create/rotate/activate/revoke) is CHAOS-6255, not this route.
			// CHAOS-6306 adds delete (a purge target) on top.
			{"pagerduty_webhook_bindings", false, true, true},

			// CHAOS-6306 (org deletion): org_deletion.py's remaining purge
			// targets, none previously declared. Every entry here is
			// delete-only (the route counts with the implicit SELECT every
			// declared table carries, then deletes rows with a nonzero
			// count) unless a comment says otherwise. Dev/AI-assistant rows.
			{"dev_feedback", false, false, true},
			{"dev_tool_calls", false, false, true},
			{"dev_runs", false, false, true},
			{"dev_messages", false, false, true},
			{"dev_conversations", false, false, true},
			// Reports: report_runs is deleted via a subquery on
			// saved_reports' own org_id (org_deletion.py's report_runs
			// target has no org_id column of its own).
			{"report_runs", false, false, true},
			{"saved_reports", false, false, true},
			// Scheduled jobs: job_runs is deleted via a subquery on
			// scheduled_jobs' own org_id, same shape as report_runs.
			// scheduled_jobs itself needs UPDATE too: the org-deletion
			// route disables every remaining scheduled job (status,
			// is_running, next_run_at) before deleting the org's other
			// rows, mirroring org_deletion.py's own _disable_scheduled_jobs
			// step.
			{"job_runs", false, false, true},
			{"scheduled_jobs", false, true, true},
			{"backfill_jobs", false, false, true},
			// Billing: invoice_line_items/subscription_events are each
			// deleted via a subquery on their own owning row's org_id
			// (invoices/subscriptions respectively). invoices also takes
			// the void route's status write (CHAOS-6257); refunds and
			// line items are only read by the billing routes. The Stripe
			// webhook's subscription events (CHAOS-6518) insert and update
			// subscriptions and insert their subscription_events row, and
			// queue billing_notifications intents (the key-conflict
			// fallback is a SELECT, always implicit).
			{"refunds", false, false, true},
			{"invoice_line_items", false, false, true},
			{"invoices", false, true, true},
			{"subscription_events", true, false, true},
			{"subscriptions", true, true, true},
			{"billing_notifications", true, false, false},
			// Sync state.
			{"metric_checkpoints", false, false, true},
			{"sync_compute_checkpoints", false, false, true},
			{"sync_watermarks", false, false, true},
			{"sync_run_reference_discoveries", false, false, true},
			{"sync_dispatch_outbox", false, false, true},
			{"sync_run_post_dispatches", false, false, true},
			{"sync_run_units", false, false, true},
			{"sync_runs", false, false, true},
			// PagerDuty: provider_oauth_credentials/provider_oauth_revocations
			// also need SELECT (already implicit) to read the encrypted
			// token the org-deletion route revokes before deleting the
			// row; provider_oauth_revocations additionally needs its own
			// DELETE for the single-row cleanup the revoke step performs
			// on a successfully revoked pending record, ahead of the bulk
			// purge pass. pagerduty_webhook_bindings' own delete grant is
			// declared once, above, widened rather than duplicated here.
			{"pagerduty_oauth_authorization_requests", false, false, true},
			{"provider_oauth_credentials", false, false, true},
			{"provider_oauth_revocations", false, false, true},
			// Integrations.
			{"integration_datasets", false, false, true},
			{"github_app_installations", false, false, true},
			// integration_credentials' delete grant is declared once, above
			// (CHAOS-6319's read-only entry, widened rather than duplicated
			// here) -- the one-entry-per-table rule.
			// SSO: encrypted_secrets presence is also read for
			// credentials_deleted's count.
			{"sso_providers", false, false, true},
			// The admin IP-allowlist routes create, update and delete
			// entries.
			{"org_ip_allowlist", true, true, true},
			{"org_retention_policies", true, true, true},
			// Billing plans/subscriptions/checkout/portal (CHAOS-6256): plan
			// create/update/soft-delete and the Stripe id write-back; price
			// replacement inserts, updates and deletes rows; bundle links are
			// cleared and rewritten; bundles are read only. The trial-abuse
			// row checkout writes is the insert on billing_audit_log (its
			// delete is the org-deletion purge, widened in place).
			{"billing_audit_log", true, false, true},
			{"billing_plans", true, true, false},
			{"billing_prices", true, true, true},
			{"plan_feature_bundles", true, false, true},
			{"feature_bundles", false, false, false},
		},
	}
}

// APIPosture exposes apiPosture for callers outside this package -- the
// same reason DomainPosture/CoordinatorPosture/QueuePosture exist: a future
// grant-application step (the api Service's own equivalent of
// internal/storage/river/migrate.go's runtimeGrantStatements, applied at its
// own provisioning/rollout step once dho api exists) derives its GRANT
// statements from this SAME declaration, never a second hand-maintained
// list.
func APIPosture() RolePosture {
	return apiPosture()
}

// CheckAPIAuthorization is the dho api Service's readiness check: it binds
// the active login to the declared api role and proves it holds exactly
// apiPosture's manifest, no more and no less, by any route (direct grant,
// PUBLIC, role membership, column-level, table-level, with or without grant
// option, or ownership) -- see CheckRolePosture's doc comment for the full
// property and why every role in a multi-role deployment must check its own
// posture for the deployment-wide cross-role attribution property to hold.
// dho api registers this behind its own health.Registry entry (internal/
// apiservice/service.go) once that package exists; this function has no
// dependency on it and can be called, and tested, standing alone.
func CheckAPIAuthorization(ctx context.Context, pool *pgxpool.Pool, expectedRole, riverSchema string) error {
	return CheckRolePosture(ctx, pool, expectedRole, riverSchema, apiPosture())
}
