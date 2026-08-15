package riverstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

func TestPinnedMigrationBundleIsExactPrefix(t *testing.T) {
	t.Parallel()

	migrator, err := rivermigrate.New(riverpgxv5.New(nil), nil)
	if err != nil {
		t.Fatal(err)
	}
	versions := migrator.AllVersions()
	if err := validatePinnedBundle(versions); err != nil {
		t.Fatal(err)
	}
	if versions[len(versions)-1].Version != PinnedSchemaVersion {
		t.Fatalf("latest bundled version = %d, want %d", versions[len(versions)-1].Version, PinnedSchemaVersion)
	}
}

func TestPinnedMigratorRequiresTwoConnectionsForLockAndCommitSeparatedMigrations(t *testing.T) {
	t.Parallel()

	poolConfig, err := pgxpool.ParseConfig("postgres://migration@127.0.0.1:1/app?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	poolConfig.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	_, err = ApplyPinnedMigrations(context.Background(), pool, MigrationOptions{
		Schema:     "river",
		DomainRole: "domain_runtime",
		QueueRole:  "queue_runtime",
	})
	if !errors.Is(err, ErrMigrationConfiguration) {
		t.Fatalf("ApplyPinnedMigrations() error = %v", err)
	}
}

func TestLongRunningCommandsCannotAutoMigrate(t *testing.T) {
	t.Parallel()

	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate test source")
	}
	repositoryRoot := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", "..", ".."))
	for _, command := range []string{
		"dev-health-worker",
		"dev-health-scheduler",
		"dev-health-reconciler",
		"dev-health-stream-runner",
	} {
		directory := filepath.Join(repositoryRoot, "cmd", command)
		entries, err := os.ReadDir(directory)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
				continue
			}
			path := filepath.Join(directory, entry.Name())
			contents, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			text := string(contents)
			for _, forbidden := range []string{"rivermigrate", "ApplyPinnedMigrations", "dev-health-worker-migrate"} {
				if strings.Contains(text, forbidden) {
					t.Fatalf("long-running command %s file %s references migration surface %q", command, entry.Name(), forbidden)
				}
			}
		}
	}
}

func TestPinnedMigrationBundleRejectsPrefixOrSuffixDrift(t *testing.T) {
	t.Parallel()

	tests := [][]rivermigrate.Migration{
		{{Version: 1}},
		{{Version: 1}, {Version: 3}, {Version: 2}, {Version: 4}, {Version: 5}, {Version: 6}, {Version: 7}},
		{{Version: 1}, {Version: 2}, {Version: 3}, {Version: 4}, {Version: 5}, {Version: 6}, {Version: 7}, {Version: 8}},
	}
	for _, versions := range tests {
		if err := validatePinnedBundle(versions); !errors.Is(err, ErrPinnedMigrationMismatch) {
			t.Fatalf("validatePinnedBundle() error = %v", err)
		}
	}
}

func TestMigrationOptionsRequireSeparateSafeIdentifiers(t *testing.T) {
	t.Parallel()

	valid := MigrationOptions{Schema: "river", DomainRole: "dev_health_domain", QueueRole: "dev_health_queue"}
	if err := ValidateMigrationOptions(valid); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []MigrationOptions{
		{},
		{Schema: "River-Bad", DomainRole: valid.DomainRole, QueueRole: valid.QueueRole},
		{Schema: valid.Schema, DomainRole: "same", QueueRole: "same"},
		{Schema: valid.Schema, DomainRole: "domain; DROP SCHEMA public", QueueRole: valid.QueueRole},
	} {
		if err := ValidateMigrationOptions(invalid); !errors.Is(err, ErrMigrationConfiguration) {
			t.Fatalf("ValidateMigrationOptions(%#v) error = %v", invalid, err)
		}
	}
}

// The coordinator arm of MigrationOptions is optional, but every way of
// half-configuring it is rejected: a role with no grants would REVOKE ALL and
// grant nothing back, leaving the coordinator binaries fail-closed forever,
// while grants with no role would silently skip the privileges the caller
// believed it was applying.
func TestMigrationOptionsRejectHalfConfiguredCoordinatorProvisioning(t *testing.T) {
	t.Parallel()

	base := MigrationOptions{Schema: "river", DomainRole: "dev_health_domain", QueueRole: "dev_health_queue"}
	grants := []TableGrant{{TableName: "worker_job_routes", AllowUpdate: true}}

	valid := base
	valid.CoordinatorRole = "dev_health_coordinator"
	valid.CoordinatorGrants = grants
	if err := ValidateMigrationOptions(valid); err != nil {
		t.Fatalf("ValidateMigrationOptions(valid coordinator) error = %v", err)
	}
	// Omitting the coordinator entirely stays valid: pre-split callers.
	if err := ValidateMigrationOptions(base); err != nil {
		t.Fatalf("ValidateMigrationOptions(no coordinator) error = %v", err)
	}
	// A column-scoped grant on a relation held table-wide by nobody else in
	// this option set is the shape CHAOS-3114 introduced, and it must be
	// accepted -- otherwise the coordinator posture could not be provisioned
	// at all.
	withColumns := valid
	withColumns.CoordinatorColumnGrants = []ColumnGrant{
		{TableName: "worker_job_completion_fences", ColumnName: "completion_key", Privilege: "SELECT"},
		{TableName: "worker_job_completion_fences", ColumnName: "completion_key", Privilege: "INSERT"},
	}
	if err := ValidateMigrationOptions(withColumns); err != nil {
		t.Fatalf("ValidateMigrationOptions(column-scoped coordinator) error = %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*MigrationOptions)
	}{
		{"role without grants", func(o *MigrationOptions) { o.CoordinatorGrants = nil }},
		{"grants without role", func(o *MigrationOptions) { o.CoordinatorRole = "" }},
		{"role collides with domain", func(o *MigrationOptions) { o.CoordinatorRole = o.DomainRole }},
		{"role collides with queue", func(o *MigrationOptions) { o.CoordinatorRole = o.QueueRole }},
		{"unsafe role identifier", func(o *MigrationOptions) {
			o.CoordinatorRole = "coordinator; DROP SCHEMA public"
		}},
		{"unsafe table identifier", func(o *MigrationOptions) {
			o.CoordinatorGrants = []TableGrant{{TableName: "routes; DROP TABLE x"}}
		}},
		{"duplicate table grant", func(o *MigrationOptions) {
			o.CoordinatorGrants = []TableGrant{
				{TableName: "worker_job_routes", AllowUpdate: true},
				{TableName: "worker_job_routes"},
			}
		}},
		{"unsafe sequence identifier", func(o *MigrationOptions) {
			o.CoordinatorSequences = []string{"audit_seq; DROP TABLE x"}
		}},
		{"duplicate sequence grant", func(o *MigrationOptions) {
			o.CoordinatorSequences = []string{"worker_operator_audits_id_seq", "worker_operator_audits_id_seq"}
		}},
		// The column-scoped half (CHAOS-3114) is validated with the same
		// strictness. Every rejection below would otherwise provision a role
		// whose readiness check can never pass, or a statement with an
		// unsanitizable fragment in it.
		{"column grants without role", func(o *MigrationOptions) {
			o.CoordinatorRole = ""
			o.CoordinatorGrants = nil
			o.CoordinatorColumnGrants = []ColumnGrant{
				{TableName: "worker_job_completion_fences", ColumnName: "completion_key", Privilege: "INSERT"},
			}
		}},
		{"unsafe column identifier", func(o *MigrationOptions) {
			o.CoordinatorColumnGrants = []ColumnGrant{
				{TableName: "worker_job_completion_fences", ColumnName: "key\"; DROP TABLE x", Privilege: "INSERT"},
			}
		}},
		{"privilege that is not column grantable", func(o *MigrationOptions) {
			o.CoordinatorColumnGrants = []ColumnGrant{
				{TableName: "worker_job_completion_fences", ColumnName: "completion_key", Privilege: "DELETE"},
			}
		}},
		{"privilege carrying trailing SQL", func(o *MigrationOptions) {
			o.CoordinatorColumnGrants = []ColumnGrant{
				{TableName: "worker_job_completion_fences", ColumnName: "completion_key", Privilege: "INSERT, UPDATE"},
			}
		}},
		{"duplicate column grant", func(o *MigrationOptions) {
			o.CoordinatorColumnGrants = []ColumnGrant{
				{TableName: "worker_job_completion_fences", ColumnName: "completion_key", Privilege: "INSERT"},
				{TableName: "worker_job_completion_fences", ColumnName: "completion_key", Privilege: "INSERT"},
			}
		}},
		// Table-wide AND column-scoped on one relation is the combination the
		// readiness posture check refuses outright, so the migration refuses to
		// create it.
		{"relation granted both table-wide and column-scoped", func(o *MigrationOptions) {
			o.CoordinatorGrants = append(o.CoordinatorGrants,
				TableGrant{TableName: "worker_job_completion_fences"})
			o.CoordinatorColumnGrants = []ColumnGrant{
				{TableName: "worker_job_completion_fences", ColumnName: "completion_key", Privilege: "INSERT"},
			}
		}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			candidate := valid
			test.mutate(&candidate)
			if err := ValidateMigrationOptions(candidate); !errors.Is(err, ErrMigrationConfiguration) {
				t.Fatalf("ValidateMigrationOptions() error = %v, want ErrMigrationConfiguration", err)
			}
		})
	}
}

// The coordinator grant statements must be derived from the injected posture
// data, sanitized, and to_regclass-guarded -- and must vanish entirely when no
// coordinator role is configured, so pre-split callers are unaffected.
func TestCoordinatorGrantStatementsDeriveFromTheInjectedPosture(t *testing.T) {
	t.Parallel()

	if statements := coordinatorGrantStatements(MigrationOptions{Schema: "river"}); statements != nil {
		t.Fatalf("coordinator statements emitted without a coordinator role: %v", statements)
	}

	statements := coordinatorGrantStatements(MigrationOptions{
		Schema:          "river",
		CoordinatorRole: "coordinator_runtime",
		CoordinatorGrants: []TableGrant{
			{TableName: "worker_job_routes", AllowUpdate: true},
			{TableName: "worker_operator_audits", AllowInsert: true, AllowUpdate: true},
			{TableName: "sync_run_post_dispatches"},
		},
		CoordinatorColumnGrants: []ColumnGrant{
			{TableName: "worker_job_completion_fences", ColumnName: "completion_key", Privilege: "SELECT"},
			{TableName: "worker_job_completion_fences", ColumnName: "completion_key", Privilege: "INSERT"},
		},
		CoordinatorSequences: []string{"worker_operator_audits_id_seq"},
	})
	joined := strings.Join(statements, "\n")

	// REVOKE ALL must precede the grants so the posture is a function of this
	// migration alone rather than of whatever ran before it.
	revokeIndex := strings.Index(joined, "REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public")
	grantIndex := strings.Index(joined, "GRANT SELECT, UPDATE ON TABLE \"public\".\"worker_job_routes\"")
	if revokeIndex < 0 || grantIndex < 0 || revokeIndex > grantIndex {
		t.Fatalf("REVOKE ALL must precede the per-table grants:\n%s", joined)
	}
	for _, expected := range []string{
		// Flags translate to exactly SELECT plus what the posture allows.
		"GRANT SELECT, UPDATE ON TABLE \"public\".\"worker_job_routes\" TO \"coordinator_runtime\"",
		"GRANT SELECT, INSERT, UPDATE ON TABLE \"public\".\"worker_operator_audits\" TO \"coordinator_runtime\"",
		"GRANT SELECT ON TABLE \"public\".\"sync_run_post_dispatches\" TO \"coordinator_runtime\"",
		// Guarded, so a table that does not exist yet is skipped rather than
		// failing the whole migration.
		"IF to_regclass('public.worker_job_routes') IS NOT NULL THEN",
		// The coordinator is a public-schema control-plane role only.
		"REVOKE ALL PRIVILEGES ON SCHEMA \"river\" FROM \"coordinator_runtime\"",
		// Column-scoped grants carry the column, are sanitized as identifiers,
		// and are guarded the same way (CHAOS-3114).
		"GRANT SELECT (\"completion_key\") ON TABLE \"public\".\"worker_job_completion_fences\" TO \"coordinator_runtime\"",
		"GRANT INSERT (\"completion_key\") ON TABLE \"public\".\"worker_job_completion_fences\" TO \"coordinator_runtime\"",
		"IF to_regclass('public.worker_job_completion_fences') IS NOT NULL THEN",
		"GRANT USAGE ON SEQUENCE \"public\".\"worker_operator_audits_id_seq\" TO \"coordinator_runtime\"",
	} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("coordinator grants missing %q:\n%s", expected, joined)
		}
	}
	// No privilege the posture never allowed may appear. The column-scoped
	// relation must never acquire a table-wide privilege either: that is what
	// would let the coordinator forge the server-owned completed_at, and the
	// readiness check refuses it outright.
	for _, forbidden := range []string{
		"DELETE ON TABLE \"public\".\"worker_job_routes\"", "TRUNCATE", "TRIGGER", "REFERENCES",
		"SELECT ON TABLE \"public\".\"worker_job_completion_fences\"",
		"INSERT ON TABLE \"public\".\"worker_job_completion_fences\"",
	} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("coordinator grants unexpectedly include %q:\n%s", forbidden, joined)
		}
	}
}

func TestRuntimeRolePreflightRequiresSeparateLeastPrivilegeLoginRoles(t *testing.T) {
	t.Parallel()

	options := MigrationOptions{
		Schema:     "river",
		DomainRole: "domain_runtime",
		QueueRole:  "queue_runtime",
	}
	// The coordinator role of the Option B split, checked with the same
	// strictness as the other two but only when it is actually configured.
	coordinatorOptions := MigrationOptions{
		Schema:          options.Schema,
		DomainRole:      options.DomainRole,
		QueueRole:       options.QueueRole,
		CoordinatorRole: "coordinator_runtime",
	}
	tests := []struct {
		name                string
		migrationRole       string
		domainEligible      bool
		queueEligible       bool
		coordinatorEligible bool
		options             MigrationOptions
		wantErr             bool
	}{
		{name: "separate least privilege login roles", migrationRole: "migration_owner", domainEligible: true, queueEligible: true},
		{name: "domain role missing or privileged", migrationRole: "migration_owner", queueEligible: true, wantErr: true},
		{name: "queue role missing or privileged", migrationRole: "migration_owner", domainEligible: true, wantErr: true},
		{name: "migration uses domain role", migrationRole: options.DomainRole, domainEligible: true, queueEligible: true, wantErr: true},
		{name: "migration uses queue role", migrationRole: options.QueueRole, domainEligible: true, queueEligible: true, wantErr: true},
		// An unconfigured coordinator role must not be required to be
		// eligible: pre-split callers pass no coordinator role at all.
		{
			name:          "coordinator unconfigured ignores its eligibility",
			migrationRole: "migration_owner", domainEligible: true, queueEligible: true,
			coordinatorEligible: false, options: options,
		},
		{
			name:          "coordinator configured and eligible",
			migrationRole: "migration_owner", domainEligible: true, queueEligible: true,
			coordinatorEligible: true, options: coordinatorOptions,
		},
		{
			name:          "coordinator role missing or privileged",
			migrationRole: "migration_owner", domainEligible: true, queueEligible: true,
			coordinatorEligible: false, options: coordinatorOptions, wantErr: true,
		},
		{
			name:          "migration uses coordinator role",
			migrationRole: coordinatorOptions.CoordinatorRole, domainEligible: true, queueEligible: true,
			coordinatorEligible: true, options: coordinatorOptions, wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			effective := test.options
			if effective.Schema == "" {
				effective = options
			}
			err := validateRuntimeRolePreflight(
				test.migrationRole,
				test.domainEligible,
				test.queueEligible,
				test.coordinatorEligible,
				effective,
			)
			if test.wantErr && !errors.Is(err, ErrMigrationConfiguration) {
				t.Fatalf("validateRuntimeRolePreflight() error = %v", err)
			}
			if !test.wantErr && err != nil {
				t.Fatalf("validateRuntimeRolePreflight() error = %v", err)
			}
		})
	}
}

func TestRuntimeGrantStatementsMatchProvisionedLeastPrivilegePolicy(t *testing.T) {
	t.Parallel()

	statements := runtimeGrantStatements(MigrationOptions{
		Schema:     "river",
		DomainRole: "domain_runtime",
		QueueRole:  "queue_runtime",
	})
	want := []string{
		"DO $$ BEGIN EXECUTE format('REVOKE TEMPORARY ON DATABASE %I FROM PUBLIC, %I, %I', current_database(), 'domain_runtime', 'queue_runtime'); END $$",
		"GRANT USAGE ON SCHEMA public TO \"domain_runtime\"",
		"REVOKE CREATE ON SCHEMA public FROM \"domain_runtime\"",
		"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM \"domain_runtime\"",
		"REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM \"domain_runtime\"",
		"DO $$ BEGIN IF to_regclass('public.alembic_version') IS NOT NULL THEN REVOKE ALL PRIVILEGES ON TABLE public.alembic_version FROM \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.integrations') IS NOT NULL THEN GRANT SELECT ON TABLE public.integrations TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.integration_sources') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.integration_sources TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.integration_datasets') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.integration_datasets TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.integration_credentials') IS NOT NULL THEN GRANT SELECT ON TABLE public.integration_credentials TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.provider_oauth_credentials') IS NOT NULL THEN GRANT SELECT, UPDATE ON TABLE public.provider_oauth_credentials TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_runs') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.sync_runs TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_dispatch_transport_routes') IS NOT NULL THEN GRANT SELECT ON TABLE public.sync_dispatch_transport_routes TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_run_units') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.sync_run_units TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_run_unit_chunk_checkpoints') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.sync_run_unit_chunk_checkpoints TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_run_unit_effect_chunks') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.sync_run_unit_effect_chunks TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_run_unit_effect_snapshots') IS NOT NULL THEN GRANT SELECT, INSERT, DELETE ON TABLE public.sync_run_unit_effect_snapshots TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_watermarks') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.sync_watermarks TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_dispatch_outbox') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.sync_dispatch_outbox TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.worker_job_outbox') IS NOT NULL THEN GRANT SELECT, INSERT ON TABLE public.worker_job_outbox TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_configurations') IS NOT NULL THEN GRANT SELECT ON TABLE public.sync_configurations TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.scheduled_jobs') IS NOT NULL THEN GRANT SELECT, UPDATE ON TABLE public.scheduled_jobs TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.scheduled_report_occurrences') IS NOT NULL THEN GRANT SELECT ON TABLE public.scheduled_report_occurrences TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.backfill_jobs') IS NOT NULL THEN GRANT SELECT ON TABLE public.backfill_jobs TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_coverage_projections') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.sync_coverage_projections TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.organizations') IS NOT NULL THEN GRANT SELECT ON TABLE public.organizations TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.remaining_metric_runs') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.remaining_metric_runs TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.remaining_metric_partitions') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.remaining_metric_partitions TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.work_graph_execution_requests') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.work_graph_execution_requests TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.work_graph_execution_ledger') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.work_graph_execution_ledger TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.worker_job_completion_fences') IS NOT NULL THEN GRANT SELECT (completion_key), INSERT (completion_key) ON TABLE public.worker_job_completion_fences TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.billing_notifications') IS NOT NULL THEN GRANT SELECT ON TABLE public.billing_notifications TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.daily_metrics_partitions') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.daily_metrics_partitions TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.daily_metrics_runs') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.daily_metrics_runs TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.dev_conversations') IS NOT NULL THEN GRANT SELECT, UPDATE, DELETE ON TABLE public.dev_conversations TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.dev_conversation_tombstones') IS NOT NULL THEN GRANT SELECT, INSERT ON TABLE public.dev_conversation_tombstones TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.external_ingest_batch_payloads') IS NOT NULL THEN GRANT SELECT, DELETE ON TABLE public.external_ingest_batch_payloads TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.external_ingest_batches') IS NOT NULL THEN GRANT SELECT, UPDATE, DELETE ON TABLE public.external_ingest_batches TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.external_ingest_recompute_jobs') IS NOT NULL THEN GRANT SELECT, INSERT ON TABLE public.external_ingest_recompute_jobs TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.external_ingest_rejections') IS NOT NULL THEN GRANT SELECT, INSERT ON TABLE public.external_ingest_rejections TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.external_ingest_sources') IS NOT NULL THEN GRANT SELECT ON TABLE public.external_ingest_sources TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.feature_flags') IS NOT NULL THEN GRANT SELECT ON TABLE public.feature_flags TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.org_feature_overrides') IS NOT NULL THEN GRANT SELECT ON TABLE public.org_feature_overrides TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.org_licenses') IS NOT NULL THEN GRANT SELECT ON TABLE public.org_licenses TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.provider_rate_limit_observations') IS NOT NULL THEN GRANT SELECT, UPDATE, DELETE ON TABLE public.provider_rate_limit_observations TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.report_runs') IS NOT NULL THEN GRANT SELECT, UPDATE ON TABLE public.report_runs TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.saved_reports') IS NOT NULL THEN GRANT SELECT, UPDATE ON TABLE public.saved_reports TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.webhook_deliveries') IS NOT NULL THEN GRANT SELECT ON TABLE public.webhook_deliveries TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.worker_job_runs') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.worker_job_runs TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.worker_concurrency_leases') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.worker_concurrency_leases TO \"domain_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.worker_profile_instances') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.worker_profile_instances TO \"domain_runtime\"; END IF; END $$",
		"GRANT USAGE ON SCHEMA public TO \"queue_runtime\"",
		"REVOKE CREATE ON SCHEMA public FROM \"queue_runtime\"",
		"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM \"queue_runtime\"",
		"REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM \"queue_runtime\"",
		"REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC, \"domain_runtime\", \"queue_runtime\"",
		"DO $$ BEGIN IF to_regclass('public.worker_job_outbox') IS NOT NULL THEN GRANT SELECT, UPDATE, DELETE ON TABLE public.worker_job_outbox TO \"queue_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.worker_job_delivery_abandonments') IS NOT NULL THEN GRANT SELECT, INSERT ON TABLE public.worker_job_delivery_abandonments TO \"queue_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.worker_job_completion_fences') IS NOT NULL THEN GRANT SELECT, UPDATE, DELETE ON TABLE public.worker_job_completion_fences TO \"queue_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_dispatch_outbox') IS NOT NULL THEN GRANT SELECT, UPDATE ON TABLE public.sync_dispatch_outbox TO \"queue_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_dispatch_transport_routes') IS NOT NULL THEN GRANT SELECT ON TABLE public.sync_dispatch_transport_routes TO \"queue_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_runs') IS NOT NULL THEN GRANT SELECT ON TABLE public.sync_runs TO \"queue_runtime\"; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_run_units') IS NOT NULL THEN GRANT SELECT ON TABLE public.sync_run_units TO \"queue_runtime\"; END IF; END $$",
		"REVOKE ALL PRIVILEGES ON SCHEMA \"river\" FROM PUBLIC",
		"REVOKE ALL PRIVILEGES ON SCHEMA \"river\" FROM \"domain_runtime\"",
		"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA \"river\" FROM PUBLIC, \"domain_runtime\"",
		"REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA \"river\" FROM PUBLIC, \"domain_runtime\"",
		"REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA \"river\" FROM PUBLIC, \"domain_runtime\"",
		"GRANT USAGE ON SCHEMA \"river\" TO \"queue_runtime\"",
		"REVOKE CREATE ON SCHEMA \"river\" FROM \"queue_runtime\"",
		"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA \"river\" TO \"queue_runtime\"",
		"GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA \"river\" TO \"queue_runtime\"",
		"GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA \"river\" TO \"queue_runtime\"",
		"ALTER DEFAULT PRIVILEGES IN SCHEMA \"river\" GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO \"queue_runtime\"",
		"ALTER DEFAULT PRIVILEGES IN SCHEMA \"river\" GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO \"queue_runtime\"",
		"ALTER DEFAULT PRIVILEGES IN SCHEMA \"river\" REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC",
		"ALTER DEFAULT PRIVILEGES IN SCHEMA \"river\" GRANT EXECUTE ON FUNCTIONS TO \"queue_runtime\"",
	}
	if !reflect.DeepEqual(statements, want) {
		t.Fatalf("runtime grant statements = %#v, want %#v", statements, want)
	}
	for _, forbidden := range []string{
		"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public",
		"GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public",
	} {
		for _, statement := range statements {
			if strings.Contains(statement, forbidden) {
				t.Fatalf("runtime grant policy contains broad public grant %q", statement)
			}
		}
	}
}
