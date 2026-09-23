package admin

// deletionTarget is one row of org_deletion.py's _postgres_targets(): a
// table this route counts (both dry_run and real) and, when not dry_run,
// deletes, scoped to one organization. The list below is 1:1 with that
// function's own list, in the SAME order -- the response's "tables" object
// renders keys in this order (pyjson.Object preserves insertion order,
// matching Python's dict), so the order is part of the contract, not
// cosmetic.
//
// Exactly one of Direct or Via is set. Direct scopes the table by its own
// org_id (or, for "organizations" itself, "id") column. Via scopes it
// through one level of indirection -- a foreign id column whose owning row
// lives in another table that itself has an org_id column -- matching
// org_deletion.py's own saved_report_ids/scheduled_job_ids/invoice_ids/
// subscription_ids helper closures for report_runs/job_runs/
// invoice_line_items/subscription_events.
//
// UUIDColumn on Direct, and UUIDOrgColumn on Via, report whether the
// relevant org_id-shaped column is a native Postgres uuid (bind a
// uuid.UUID) or text/varchar (bind the org_id path parameter's own
// string) -- read directly off org_deletion.py's own predicate choice
// (`== org_uuid` vs `== org_id`), which is the SQLAlchemy model's own
// declared column type.
type deletionTarget struct {
	Table  string
	Direct *directColumn
	Via    *indirectColumn
}

type directColumn struct {
	// Column is the table's own org-scoping column ("org_id" for every
	// target except impersonation_sessions, "target_org_id", and
	// organizations itself, "id").
	Column   string
	UUIDType bool
}

type indirectColumn struct {
	// Column is this table's own foreign-id column (e.g. "report_id").
	Column string
	// SubTable/SubIDColumn/SubOrgColumn locate the owning row: SELECT
	// <SubIDColumn> FROM <SubTable> WHERE <SubOrgColumn> = $1.
	SubTable      string
	SubIDColumn   string
	SubOrgColumn  string
	UUIDOrgColumn bool
}

// orgDeletionTargets is org_deletion.py's _postgres_targets(), verified
// live against the installed interpreter (46 entries; see
// TestOrgDeletionTargetsMatchThePythonList).
var orgDeletionTargets = []deletionTarget{
	{Table: "dev_feedback", Direct: &directColumn{"org_id", true}},
	{Table: "dev_tool_calls", Direct: &directColumn{"org_id", true}},
	{Table: "dev_runs", Direct: &directColumn{"org_id", true}},
	{Table: "dev_messages", Direct: &directColumn{"org_id", true}},
	{Table: "dev_conversations", Direct: &directColumn{"org_id", true}},
	{Table: "report_runs", Via: &indirectColumn{
		Column: "report_id", SubTable: "saved_reports", SubIDColumn: "id", SubOrgColumn: "org_id", UUIDOrgColumn: false}},
	{Table: "saved_reports", Direct: &directColumn{"org_id", false}},
	{Table: "job_runs", Via: &indirectColumn{
		Column: "job_id", SubTable: "scheduled_jobs", SubIDColumn: "id", SubOrgColumn: "org_id", UUIDOrgColumn: false}},
	{Table: "backfill_jobs", Direct: &directColumn{"org_id", false}},
	{Table: "refunds", Direct: &directColumn{"org_id", true}},
	{Table: "invoice_line_items", Via: &indirectColumn{
		Column: "invoice_id", SubTable: "invoices", SubIDColumn: "id", SubOrgColumn: "org_id", UUIDOrgColumn: true}},
	{Table: "invoices", Direct: &directColumn{"org_id", true}},
	{Table: "subscription_events", Via: &indirectColumn{
		Column: "subscription_id", SubTable: "subscriptions", SubIDColumn: "id", SubOrgColumn: "org_id", UUIDOrgColumn: true}},
	{Table: "subscriptions", Direct: &directColumn{"org_id", true}},
	{Table: "metric_checkpoints", Direct: &directColumn{"org_id", false}},
	{Table: "sync_compute_checkpoints", Direct: &directColumn{"org_id", false}},
	{Table: "sync_watermarks", Direct: &directColumn{"org_id", false}},
	{Table: "scheduled_jobs", Direct: &directColumn{"org_id", false}},
	{Table: "sync_configurations", Direct: &directColumn{"org_id", false}},
	{Table: "sync_run_reference_discoveries", Direct: &directColumn{"org_id", false}},
	{Table: "sync_dispatch_outbox", Direct: &directColumn{"org_id", false}},
	{Table: "sync_run_post_dispatches", Direct: &directColumn{"org_id", false}},
	{Table: "sync_run_units", Direct: &directColumn{"org_id", false}},
	{Table: "sync_runs", Direct: &directColumn{"org_id", false}},
	{Table: "pagerduty_webhook_bindings", Direct: &directColumn{"org_id", true}},
	{Table: "pagerduty_oauth_authorization_requests", Direct: &directColumn{"org_id", false}},
	{Table: "provider_oauth_credentials", Direct: &directColumn{"org_id", false}},
	{Table: "provider_oauth_revocations", Direct: &directColumn{"org_id", false}},
	{Table: "integration_datasets", Direct: &directColumn{"org_id", false}},
	{Table: "integration_sources", Direct: &directColumn{"org_id", false}},
	{Table: "integrations", Direct: &directColumn{"org_id", false}},
	{Table: "github_app_installations", Direct: &directColumn{"org_id", false}},
	{Table: "integration_credentials", Direct: &directColumn{"org_id", false}},
	{Table: "settings", Direct: &directColumn{"org_id", false}},
	{Table: "sso_providers", Direct: &directColumn{"org_id", true}},
	{Table: "org_ip_allowlist", Direct: &directColumn{"org_id", true}},
	{Table: "org_feature_overrides", Direct: &directColumn{"org_id", true}},
	{Table: "org_licenses", Direct: &directColumn{"org_id", true}},
	{Table: "org_retention_policies", Direct: &directColumn{"org_id", true}},
	{Table: "org_invites", Direct: &directColumn{"org_id", true}},
	{Table: "refresh_tokens", Direct: &directColumn{"org_id", true}},
	{Table: "impersonation_sessions", Direct: &directColumn{"target_org_id", true}},
	{Table: "memberships", Direct: &directColumn{"org_id", true}},
	{Table: "audit_logs", Direct: &directColumn{"org_id", true}},
	{Table: "billing_audit_log", Direct: &directColumn{"org_id", true}},
	{Table: "organizations", Direct: &directColumn{"id", true}},
}
