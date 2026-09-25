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
	// ResultJSON is raw JSON text and Error plain text; empty means NULL.
	ResultJSON string
	Error      string
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
	total_units, completed_units, failed_units, result, error, created_at)
VALUES ($1::uuid, $2, $3::uuid, 'pgseed', 'incremental', $4, $5, $6, $7, $8::json, $9, now())
ON CONFLICT (id) DO NOTHING`,
		run.ID, run.OrgID, run.IntegrationID, run.Status, run.TotalUnits, run.CompletedUnits, run.FailedUnits,
		nullIfEmpty(run.ResultJSON), nullIfEmpty(run.Error))
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

	SinceAt               *time.Time
	BeforeAt              *time.Time
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
	available_at, rate_limit_first_seen_at, budget_first_deferred_at, first_blocked_at, since_at, before_at)
VALUES ($1::uuid, $2, $3::uuid, $4::uuid, $5::uuid, $6, $7, $8, $9, $10, 0, $11::json, $12::json, now(), $13,
	$14, $15, $16, $17, $18, $19, $20, $21)`,
		u.ID, u.OrgID, u.RunID, u.IntegrationID, u.SourceID,
		orDefault(u.Provider, "github"), orDefault(u.DatasetKey, "commits"), orDefault(u.CostClass, "rest_core"),
		orDefault(u.Mode, "incremental"), u.Status,
		nullIfEmpty(u.ResultJSON), nullIfEmpty(u.ProcessorFlagsJSON), updatedAt,
		u.LeaseOwner, u.LeaseExpiresAt, u.AvailableAt, u.RateLimitFirstSeenAt, u.BudgetFirstDeferredAt, u.FirstBlockedAt,
		u.SinceAt, u.BeforeAt)
}

// TierLimit upserts a tier_limits row (the migrations may already carry the tier's defaults).
func TierLimit(ctx context.Context, t testing.TB, pool *pgxpool.Pool, tier, key, value string) {
	t.Helper()
	exec(ctx, t, pool, "tier limit "+tier+"/"+key, `
INSERT INTO tier_limits (id, tier, limit_key, limit_value, created_at, updated_at)
VALUES (gen_random_uuid(), $1, $2, $3, now(), now())
ON CONFLICT (tier, limit_key) DO UPDATE SET limit_value = $3`, tier, key, value)
}

// SyncTransportRoute puts the sync_dispatch_transport_routes row for kind into the given state and
// returns its generation. The migrations seed every kind (celery, generation 2, rollback none) and a
// trigger demands a generation increase for any state change, so a changed row gets
// max(generation, current+1) -- generation is a minimum, not an exact value -- and an unchanged row
// keeps its generation.
func SyncTransportRoute(ctx context.Context, t testing.TB, pool *pgxpool.Pool, kind, transport string, generation int64, paused bool, rollbackTransport string) int64 {
	t.Helper()
	var got int64
	if err := pool.QueryRow(ctx, `
INSERT INTO sync_dispatch_transport_routes AS r (kind, transport, generation, paused, paused_at, rollback_transport, created_at, updated_at)
VALUES ($1, $2, $3, $4, CASE WHEN $4 THEN now() END, $5, now(), now())
ON CONFLICT (kind) DO UPDATE SET
	generation = CASE WHEN (r.transport, r.paused, r.rollback_transport) IS NOT DISTINCT FROM ($2, $4, $5)
		THEN r.generation ELSE greatest($3, r.generation + 1) END,
	transport = $2, paused = $4, paused_at = CASE WHEN $4 THEN coalesce(r.paused_at, now()) END,
	rollback_transport = $5, updated_at = now()
RETURNING generation`,
		kind, transport, generation, paused, rollbackTransport).Scan(&got); err != nil {
		t.Fatalf("pgseed sync transport route %s: %v", kind, err)
	}
	return got
}

// SyncDispatchOutbox inserts a sync_dispatch_outbox row for an existing run. A dispatched row
// names its transport and route generation (the table's coherence check requires both);
// other statuses carry neither, whatever the arguments say.
func SyncDispatchOutbox(ctx context.Context, t testing.TB, pool *pgxpool.Pool, id, runID, orgID, kind, status, transport string, generation int64) {
	t.Helper()
	var dispatchedTransport, dispatchedGeneration any
	if status == "dispatched" {
		dispatchedTransport, dispatchedGeneration = transport, generation
	}
	exec(ctx, t, pool, "sync dispatch outbox", `
INSERT INTO sync_dispatch_outbox (id, org_id, sync_run_id, kind, status, available_at, attempts,
	dispatched_transport, dispatched_route_generation, created_at, updated_at)
VALUES ($1::uuid, $2, $3::uuid, $4, $5, now(), 0, $6, $7, now(), now())`,
		id, orgID, runID, kind, status, dispatchedTransport, dispatchedGeneration)
}

// SyncConfiguration inserts a sync_configurations row under an existing integration; optionsJSON
// is the raw JSON text of sync_options.
func SyncConfiguration(ctx context.Context, t testing.TB, pool *pgxpool.Pool, id, orgID, integrationID, optionsJSON string, createdAt time.Time) {
	t.Helper()
	exec(ctx, t, pool, "sync configuration", `
INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, created_at, updated_at, integration_id)
VALUES ($1::uuid, $2, 'config-' || $1::text, 'github', '[]'::json, $3::json, true, $4, $4, $5::uuid)`,
		id, orgID, optionsJSON, createdAt, integrationID)
}

// ScheduledJob inserts a scheduled_jobs row (status 0 = pending) with a name unique per id.
func ScheduledJob(ctx context.Context, t testing.TB, pool *pgxpool.Pool, id, orgID string) {
	t.Helper()
	exec(ctx, t, pool, "scheduled job", `
INSERT INTO scheduled_jobs (id, org_id, name, job_type, schedule_cron, status, created_at, updated_at)
VALUES ($1::uuid, $2, 'job-' || $1::text, 'sync', '0 * * * *', 0, now(), now())`, id, orgID)
}

// JobRun inserts a job_runs row for an existing scheduled job; resultJSON is raw JSON text.
func JobRun(ctx context.Context, t testing.TB, pool *pgxpool.Pool, id, jobID string, status int, resultJSON string) {
	t.Helper()
	exec(ctx, t, pool, "job run", `
INSERT INTO job_runs (id, job_id, status, result, created_at)
VALUES ($1::uuid, $2::uuid, $3, $4::json, now())`, id, jobID, status, nullIfEmpty(resultJSON))
}
