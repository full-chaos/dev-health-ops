package writerinventory

// DynamicAllowlist lists each call site whose SQL writes a table the scan cannot name (TestEveryDynamicWriteSiteIsListed):
// one row per watched table the site writes, keyed File+Func+Table+Verb like Allowlist, with its owner in the comment above
// it. A ClickHouse site goes in DynamicClickHouseSites. The three enumerated sites must equal the table sets read from their
// own data (TestDynamicRowsMatchTheirEnumeratedTableSets).
var DynamicAllowlist = []Entry{
	// go-api org deletion (CHAOS-6306, platform admin route and the operator verb): deleteSQL deletes the organization's rows
	// from every orgDeletionTargets table; these are the watched ones.
	{File: "internal/apiservice/admin/orgdeletion.go", Func: "deletionTarget.deleteSQL", Table: "public.audit_logs", Verb: "DELETE"},
	{File: "internal/apiservice/admin/orgdeletion.go", Func: "deletionTarget.deleteSQL", Table: "public.billing_audit_log", Verb: "DELETE"},
	{File: "internal/apiservice/admin/orgdeletion.go", Func: "deletionTarget.deleteSQL", Table: "public.github_app_installations", Verb: "DELETE"},
	{File: "internal/apiservice/admin/orgdeletion.go", Func: "deletionTarget.deleteSQL", Table: "public.impersonation_sessions", Verb: "DELETE"},
	{File: "internal/apiservice/admin/orgdeletion.go", Func: "deletionTarget.deleteSQL", Table: "public.integration_credentials", Verb: "DELETE"},
	{File: "internal/apiservice/admin/orgdeletion.go", Func: "deletionTarget.deleteSQL", Table: "public.memberships", Verb: "DELETE"},
	{File: "internal/apiservice/admin/orgdeletion.go", Func: "deletionTarget.deleteSQL", Table: "public.org_invites", Verb: "DELETE"},
	{File: "internal/apiservice/admin/orgdeletion.go", Func: "deletionTarget.deleteSQL", Table: "public.org_ip_allowlist", Verb: "DELETE"},
	{File: "internal/apiservice/admin/orgdeletion.go", Func: "deletionTarget.deleteSQL", Table: "public.organizations", Verb: "DELETE"},
	{File: "internal/apiservice/admin/orgdeletion.go", Func: "deletionTarget.deleteSQL", Table: "public.pagerduty_webhook_bindings", Verb: "DELETE"},
	{File: "internal/apiservice/admin/orgdeletion.go", Func: "deletionTarget.deleteSQL", Table: "public.provider_oauth_credentials", Verb: "DELETE"},
	{File: "internal/apiservice/admin/orgdeletion.go", Func: "deletionTarget.deleteSQL", Table: "public.provider_oauth_revocations", Verb: "DELETE"},
	{File: "internal/apiservice/admin/orgdeletion.go", Func: "deletionTarget.deleteSQL", Table: "public.refresh_tokens", Verb: "DELETE"},
	{File: "internal/apiservice/admin/orgdeletion.go", Func: "deletionTarget.deleteSQL", Table: "public.sso_providers", Verb: "DELETE"},
	// go-api user delete, force mode (CHAOS-10019, D6051): sets to NULL each nullable NO ACTION / RESTRICT foreign key to
	// users(id) read from the catalog; these are the watched tables of the baseline's set (refunds is not watched).
	{File: "internal/apiservice/admin/userdelete.go", Func: "pgStore.forceDeleteUser", Table: "public.billing_audit_log", Verb: "UPDATE"},
	{File: "internal/apiservice/admin/userdelete.go", Func: "pgStore.forceDeleteUser", Table: "public.impersonation_sessions", Verb: "UPDATE"},
	// auth-service outbox reaper: the statement builder; its one caller, Reap, passes the auth outbox table (Reap's row in
	// Allowlist).
	{File: "internal/audit/reaper.go", Func: "reapStatement", Table: "auth.auth_outbox_events", Verb: "DELETE"},
	// auth-service SAML sign-in store: the auth schema is a runtime %s, the tables are literal.
	{File: "internal/auth/identity/saml/pgstore.go", Func: "PGStore.ConsumeFlow", Table: "auth.sso_flow_states", Verb: "UPDATE"},
	{File: "internal/auth/identity/saml/pgstore.go", Func: "PGStore.SaveFlow", Table: "auth.sso_flow_states", Verb: "INSERT"},
	{File: "internal/auth/identity/saml/pgstore.go", Func: "PGStore.UseAssertion", Table: "auth.saml_assertion_uses", Verb: "INSERT"},
	// auth-migrate restore-fence (migration role, restorefence/doc.go): applySQL writes each section's table with its kind's
	// verb; these are the watched ones (public.integrations is not watched; report sections write nothing).
	{File: "internal/auth/restorefence/specs.go", Func: "section.applySQL", Table: "auth.credential_registry", Verb: "UPDATE"},
	{File: "internal/auth/restorefence/specs.go", Func: "section.applySQL", Table: "auth.external_identities", Verb: "DELETE"},
	{File: "internal/auth/restorefence/specs.go", Func: "section.applySQL", Table: "auth.invites", Verb: "UPDATE"},
	{File: "internal/auth/restorefence/specs.go", Func: "section.applySQL", Table: "auth.link_credentials", Verb: "DELETE"},
	{File: "internal/auth/restorefence/specs.go", Func: "section.applySQL", Table: "auth.link_credentials", Verb: "UPDATE"},
	{File: "internal/auth/restorefence/specs.go", Func: "section.applySQL", Table: "auth.memberships", Verb: "UPDATE"},
	{File: "internal/auth/restorefence/specs.go", Func: "section.applySQL", Table: "auth.organizations", Verb: "UPDATE"},
	{File: "internal/auth/restorefence/specs.go", Func: "section.applySQL", Table: "auth.platform_role_assignments", Verb: "DELETE"},
	{File: "internal/auth/restorefence/specs.go", Func: "section.applySQL", Table: "auth.principals", Verb: "UPDATE"},
	{File: "internal/auth/restorefence/specs.go", Func: "section.applySQL", Table: "auth.refresh_credentials", Verb: "UPDATE"},
	{File: "internal/auth/restorefence/specs.go", Func: "section.applySQL", Table: "auth.resource_grants", Verb: "UPDATE"},
	{File: "internal/auth/restorefence/specs.go", Func: "section.applySQL", Table: "auth.saml_assertion_uses", Verb: "INSERT"},
	{File: "internal/auth/restorefence/specs.go", Func: "section.applySQL", Table: "auth.service_accounts", Verb: "UPDATE"},
	{File: "internal/auth/restorefence/specs.go", Func: "section.applySQL", Table: "auth.sessions", Verb: "UPDATE"},
	{File: "internal/auth/restorefence/specs.go", Func: "section.applySQL", Table: "auth.signing_keys", Verb: "UPDATE"},
	{File: "internal/auth/restorefence/specs.go", Func: "section.applySQL", Table: "auth.users", Verb: "UPDATE"},
	{File: "internal/auth/restorefence/specs.go", Func: "section.applySQL", Table: "auth.workload_bindings", Verb: "UPDATE"},
	{File: "internal/auth/restorefence/specs.go", Func: "section.applySQL", Table: "auth.workloads", Verb: "UPDATE"},
	{File: "internal/auth/restorefence/specs.go", Func: "section.applySQL", Table: "public.email_verification_tokens", Verb: "DELETE"},
	{File: "internal/auth/restorefence/specs.go", Func: "section.applySQL", Table: "public.external_ingest_tokens", Verb: "UPDATE"},
	{File: "internal/auth/restorefence/specs.go", Func: "section.applySQL", Table: "public.github_app_installations", Verb: "UPDATE"},
	{File: "internal/auth/restorefence/specs.go", Func: "section.applySQL", Table: "public.integration_credentials", Verb: "UPDATE"},
	{File: "internal/auth/restorefence/specs.go", Func: "section.applySQL", Table: "public.internal_service_credentials", Verb: "UPDATE"},
	{File: "internal/auth/restorefence/specs.go", Func: "section.applySQL", Table: "public.memberships", Verb: "DELETE"},
	{File: "internal/auth/restorefence/specs.go", Func: "section.applySQL", Table: "public.memberships", Verb: "UPDATE"},
	{File: "internal/auth/restorefence/specs.go", Func: "section.applySQL", Table: "public.org_invites", Verb: "UPDATE"},
	{File: "internal/auth/restorefence/specs.go", Func: "section.applySQL", Table: "public.org_ip_allowlist", Verb: "UPDATE"},
	{File: "internal/auth/restorefence/specs.go", Func: "section.applySQL", Table: "public.organizations", Verb: "UPDATE"},
	{File: "internal/auth/restorefence/specs.go", Func: "section.applySQL", Table: "public.pagerduty_webhook_bindings", Verb: "UPDATE"},
	{File: "internal/auth/restorefence/specs.go", Func: "section.applySQL", Table: "public.password_reset_tokens", Verb: "DELETE"},
	{File: "internal/auth/restorefence/specs.go", Func: "section.applySQL", Table: "public.provider_oauth_credentials", Verb: "DELETE"},
	{File: "internal/auth/restorefence/specs.go", Func: "section.applySQL", Table: "public.refresh_tokens", Verb: "UPDATE"},
	{File: "internal/auth/restorefence/specs.go", Func: "section.applySQL", Table: "public.saml_assertion_replays", Verb: "INSERT"},
	{File: "internal/auth/restorefence/specs.go", Func: "section.applySQL", Table: "public.users", Verb: "UPDATE"},
}

// DynamicClickHouseSites are dynamic INSERT sites that write ClickHouse tables only (a clickhouse-go PrepareBatch in the
// same function, TestDynamicClickHouseSitesArePrepareBatch): no Postgres table, so no row.
var DynamicClickHouseSites = []DynamicSite{
	{File: "internal/jobs/metrics/daily/stale_team_keys.go", Func: "writeStaleKeyZeroRows", Verb: "INSERT"},
	{File: "internal/providerfoundation/sinks.go", Func: "DefaultClickHouseBatch", Verb: "INSERT"},
	{File: "internal/providersync/catalog_evidence_start.go", Func: "writeCatalogEvidenceCarry", Verb: "INSERT"},
	{File: "internal/providersync/ownership_interval_compaction.go", Func: "ownershipCompactionPlan.applyRuns", Verb: "INSERT"},
	{File: "internal/providersync/ownership_interval_compaction.go", Func: "restoreOwnershipOrg", Verb: "INSERT"},
	{File: "internal/providersync/team_id_carry.go", Func: "teamIDCarryRun.write", Verb: "INSERT"},
}
