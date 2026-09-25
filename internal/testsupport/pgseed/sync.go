package pgseed

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Default ids the sync-graph builders use when a test names none. They are fixed so a test that
// seeds only a unit and a run still gets a valid integration and source underneath it.
const (
	DefaultSyncOrgID         = "pgseed-org"
	DefaultSyncIntegrationID = "00000000-0000-4000-8000-00000000a001"
	DefaultSyncSourceID      = "00000000-0000-4000-8000-00000000a002"
	DefaultSyncRunID         = "00000000-0000-4000-8000-00000000a003"
)

func orDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func nullIfEmpty(value string) any {
	if value == "" {
		return nil
	}
	return value
}

// SyncRun describes a sync_runs row. Empty fields take the defaults above; the parent integration
// is created when it does not exist yet.
type SyncRun struct {
	ID             string
	OrgID          string
	IntegrationID  string
	Status         string
	TotalUnits     int
	CompletedUnits int
	FailedUnits    int
}

// EnsureSyncIntegration creates the integration and one enabled source under it when missing (the
// two parents every sync_runs / sync_run_units row needs) and returns their ids.
func EnsureSyncIntegration(ctx context.Context, t testing.TB, pool *pgxpool.Pool, orgID, integrationID, sourceID string) (string, string) {
	t.Helper()
	orgID = orDefault(orgID, DefaultSyncOrgID)
	integrationID = orDefault(integrationID, DefaultSyncIntegrationID)
	sourceID = orDefault(sourceID, DefaultSyncSourceID)
	exec(ctx, t, pool, "sync integration", `
INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
VALUES ($1::uuid, $2, 'github', 'integration-' || $1::text, '{}'::json, true, now(), now())
ON CONFLICT (id) DO NOTHING`, integrationID, orgID)
	exec(ctx, t, pool, "sync integration source", `
INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name,
	metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1::uuid, $2, $3::uuid, 'github', 'repository', $1::text, $1::text, $1::text, '{}'::json, true, now(), now())
ON CONFLICT (id) DO NOTHING`, sourceID, orgID, integrationID)
	return integrationID, sourceID
}

// EnsureSyncRun inserts the sync_runs row (and its integration) unless it exists.
func EnsureSyncRun(ctx context.Context, t testing.TB, pool *pgxpool.Pool, run SyncRun) {
	t.Helper()
	run.ID = orDefault(run.ID, DefaultSyncRunID)
	run.OrgID = orDefault(run.OrgID, DefaultSyncOrgID)
	run.Status = orDefault(run.Status, "running")
	run.IntegrationID, _ = EnsureSyncIntegration(ctx, t, pool, run.OrgID, run.IntegrationID, "")
	exec(ctx, t, pool, "sync run", `
INSERT INTO sync_runs (id, org_id, integration_id, triggered_by, mode, status,
	total_units, completed_units, failed_units, created_at)
VALUES ($1::uuid, $2, $3::uuid, 'pgseed', 'incremental', $4, $5, $6, $7, now())
ON CONFLICT (id) DO NOTHING`,
		run.ID, run.OrgID, run.IntegrationID, run.Status, run.TotalUnits, run.CompletedUnits, run.FailedUnits)
}

// SyncRunUnit describes a sync_run_units row. Empty strings and nil pointers take defaults (or
// NULL for the nullable columns); the run, integration and source parents are created when missing.
type SyncRunUnit struct {
	ID            string
	RunID         string
	OrgID         string
	IntegrationID string
	SourceID      string
	Provider      string
	DatasetKey    string
	CostClass     string
	Mode          string
	Status        string

	AvailableAt           *time.Time
	UpdatedAt             *time.Time
	LeaseExpiresAt        *time.Time
	RateLimitFirstSeenAt  *time.Time
	BudgetFirstDeferredAt *time.Time
	FirstBlockedAt        *time.Time
	LeaseOwner            *string

	// ResultJSON and ProcessorFlagsJSON are raw JSON text; empty means NULL.
	ResultJSON         string
	ProcessorFlagsJSON string
}

// InsertSyncRunUnit inserts the unit with every NOT NULL column filled.
func InsertSyncRunUnit(ctx context.Context, t testing.TB, pool *pgxpool.Pool, u SyncRunUnit) {
	t.Helper()
	u.RunID = orDefault(u.RunID, DefaultSyncRunID)
	u.OrgID = orDefault(u.OrgID, DefaultSyncOrgID)
	u.IntegrationID, u.SourceID = EnsureSyncIntegration(ctx, t, pool, u.OrgID, u.IntegrationID, u.SourceID)
	EnsureSyncRun(ctx, t, pool, SyncRun{ID: u.RunID, OrgID: u.OrgID, IntegrationID: u.IntegrationID})
	updatedAt := time.Now()
	if u.UpdatedAt != nil {
		updatedAt = *u.UpdatedAt
	}
	exec(ctx, t, pool, "sync run unit", `
INSERT INTO sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class,
	mode, status, attempts, result, processor_flags, created_at, updated_at, lease_owner, lease_expires_at,
	available_at, rate_limit_first_seen_at, budget_first_deferred_at, first_blocked_at)
VALUES ($1::uuid, $2, $3::uuid, $4::uuid, $5::uuid, $6, $7, $8, $9, $10, 0, $11::json, $12::json, now(), $13,
	$14, $15, $16, $17, $18, $19)`,
		u.ID, u.OrgID, u.RunID, u.IntegrationID, u.SourceID,
		orDefault(u.Provider, "github"), orDefault(u.DatasetKey, "commits"), orDefault(u.CostClass, "rest_core"),
		orDefault(u.Mode, "incremental"), u.Status,
		nullIfEmpty(u.ResultJSON), nullIfEmpty(u.ProcessorFlagsJSON), updatedAt,
		u.LeaseOwner, u.LeaseExpiresAt, u.AvailableAt, u.RateLimitFirstSeenAt, u.BudgetFirstDeferredAt, u.FirstBlockedAt)
}

// TierLimit upserts a tier_limits row (the migrations may already carry the tier's defaults).
func TierLimit(ctx context.Context, t testing.TB, pool *pgxpool.Pool, tier, key, value string) {
	t.Helper()
	exec(ctx, t, pool, "tier limit "+tier+"/"+key, `
INSERT INTO tier_limits (id, tier, limit_key, limit_value, created_at, updated_at)
VALUES (gen_random_uuid(), $1, $2, $3, now(), now())
ON CONFLICT (tier, limit_key) DO UPDATE SET limit_value = $3`, tier, key, value)
}
