//go:build integration

package domainrolecell_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/domainroledb"
)

// partitionRouteRegistry routes the partition and finalize kinds to River, the
// same override package daily's own integration tests use.
type partitionRouteRegistry struct {
	production *jobruntime.Registry
}

func (registry partitionRouteRegistry) Descriptor(kind string) (jobruntime.Descriptor, bool) {
	descriptor, ok := registry.production.Descriptor(kind)
	if !ok {
		return descriptor, false
	}
	if kind == jobcontract.KindDailyMetricsPartition || kind == jobcontract.KindDailyMetricsFinalize {
		descriptor.MigrationState = "go_default"
		descriptor.Route = "river"
	}
	return descriptor, true
}

// On the migrated schema and the real go-river-migrate grants, the domain role
// claims a run, materializes a run with a repository, hands an empty run to its
// finalize, and refuses to cut partitions on a retry of the empty run. No
// statement is refused.
func TestMaterializeScheduledFanoutRunsAsTheDomainRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	venue := domainroledb.Start(t, ctx, "daily_materialize")
	venue.RequireReady(t, ctx)
	registry, err := jobruntime.Load(filepath.Join("..", "..", "..", "..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := daily.NewPostgresPublisher(venue.Domain, partitionRouteRegistry{production: registry})
	if err != nil {
		t.Fatal(err)
	}
	store, err := daily.NewPostgresStore(venue.Domain)
	if err != nil {
		t.Fatal(err)
	}
	day := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	const org = "00000000-0000-4000-8000-000000141040"
	claim := func(generation string) daily.Run {
		t.Helper()
		runID := uuid.NewString()
		if _, err := venue.Admin.Exec(ctx, `INSERT INTO daily_metrics_runs
    (id, org_id, target_day, generation, status, finalization_status, created_at, updated_at, full_org)
    VALUES ($1::uuid, $2::uuid, $3::date, $4, 'pending', 'pending', now(), now(), true)`,
			runID, org, day.Format("2006-01-02"), generation); err != nil {
			t.Fatal(err)
		}
		claimed, err := store.ClaimDispatch(ctx, runID)
		if err != nil || claimed == nil {
			t.Fatalf("claim dispatch as the domain role: %v (refused: %q)", err, venue.Denials.All())
		}
		return *claimed
	}
	handoffs := func(runID string) (finalize, partitions int) {
		t.Helper()
		if err := venue.Admin.QueryRow(ctx, `SELECT count(*) FROM worker_job_outbox WHERE dedupe_key = $1`, daily.FinalizeHandoffKey(runID)).Scan(&finalize); err != nil {
			t.Fatal(err)
		}
		if err := venue.Admin.QueryRow(ctx, `SELECT count(*) FROM daily_metrics_partitions WHERE run_id = $1::uuid`, runID).Scan(&partitions); err != nil {
			t.Fatal(err)
		}
		return finalize, partitions
	}

	withRepository := claim(daily.ScheduledFanoutGenerationPrefix + "2026-09-15T01:00:00Z")
	created, err := store.MaterializeScheduledFanout(ctx, withRepository, []daily.RepositoryID{"00000000-0000-4000-8000-0000001410a1"}, publisher)
	finalize, partitions := handoffs(withRepository.ID)
	t.Logf("run with a repository: created=%t err=%v finalize_handoffs=%d partitions=%d", created, err, finalize, partitions)
	if err != nil || !created || finalize != 0 || partitions != 1 {
		t.Errorf("run with a repository: created=%t err=%v, %d handoff(s), %d partition(s); want true, nil, 0, 1", created, err, finalize, partitions)
	}

	empty := claim(daily.ScheduledFanoutGenerationPrefix + "2026-09-15T01:01:00Z")
	created, err = store.MaterializeScheduledFanout(ctx, empty, nil, publisher)
	finalize, partitions = handoffs(empty.ID)
	t.Logf("empty run: created=%t err=%v finalize_handoffs=%d partitions=%d", created, err, finalize, partitions)
	if err != nil || !created || finalize != 1 || partitions != 0 {
		t.Errorf("empty run: created=%t err=%v, %d handoff(s), %d partition(s); want true, nil, 1, 0", created, err, finalize, partitions)
	}
	retried, err := store.MaterializeScheduledFanout(ctx, empty, []daily.RepositoryID{"00000000-0000-4000-8000-0000001410a2"}, publisher)
	finalize, partitions = handoffs(empty.ID)
	t.Logf("retry of the empty run: created=%t err=%v finalize_handoffs=%d partitions=%d", retried, err, finalize, partitions)
	if err != nil || retried || finalize != 1 || partitions != 0 {
		t.Errorf("retry of the empty run: created=%t err=%v, %d handoff(s), %d partition(s); want false, nil, 1, 0", retried, err, finalize, partitions)
	}
	if refused := venue.Denials.All(); len(refused) > 0 {
		t.Errorf("the domain role was refused: %s", strings.Join(refused, "; "))
	}
}

// As the domain role, the blocked-run reconcile reads the later runs of a day
// and both reopen provenance tables, and clears the finalize_failed marker of a
// run that a later full-org run (reopened by both a finalize-redrive and a
// partition recompute) covers. No statement is refused.
func TestReconcileBlockedRunsCoverRunsAsTheDomainRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	venue := domainroledb.Start(t, ctx, "daily_blocked_cover")
	venue.RequireReady(t, ctx)
	store, err := daily.NewPostgresStore(venue.Domain)
	if err != nil {
		t.Fatal(err)
	}
	const (
		org = "00000000-0000-4000-8000-000000141290"
		day = "2026-09-20"
	)
	base := time.Date(2026, 9, 20, 6, 0, 0, 0, time.UTC)
	insert := func(status, finalization string, minute int) string {
		t.Helper()
		runID := uuid.NewString()
		at := base.Add(time.Duration(minute) * time.Minute)
		if _, err := venue.Admin.Exec(ctx, `INSERT INTO daily_metrics_runs
    (id, org_id, target_day, generation, status, finalization_status, created_at, updated_at, full_org)
    VALUES ($1::uuid, $2::uuid, $3::date, $4, $5, $6, $7, $7, true)`,
			runID, org, day, "post-sync:cover-"+runID, status, finalization, at); err != nil {
			t.Fatal(err)
		}
		if _, err := venue.Admin.Exec(ctx, `INSERT INTO daily_metrics_partitions
    (id, run_id, ordinal, repo_ids, status, attempt_count, created_at, updated_at)
    VALUES ($1::uuid, $2::uuid, 0, '["00000000-0000-4000-8000-0000001412b1"]'::json, 'succeeded', 1, $3, $3)`,
			uuid.NewString(), runID, at); err != nil {
			t.Fatal(err)
		}
		return runID
	}
	failed := insert("failed", "failed", 0)
	if _, err := store.ReconcileBlockedRuns(ctx, org); err != nil {
		t.Fatalf("first reconcile as the domain role: %v (refused: %q)", err, venue.Denials.All())
	}
	later := insert("running", "pending", 10)
	if _, err := venue.Admin.Exec(ctx, `INSERT INTO daily_metrics_finalize_redrive_events
    (id, run_id, org_id, target_day, prior_status, prior_finalization_status, actor, reason, nonce)
    VALUES ($1::uuid, $2::uuid, $3::uuid, $4::date, 'succeeded', 'succeeded', 'finalize-redrive', 'cover cell', $5)`,
		uuid.NewString(), later, org, day, uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	if _, err := venue.Admin.Exec(ctx, `INSERT INTO daily_metrics_partition_recompute_events
    (id, run_id, org_id, target_day, family, prior_status, prior_generation, actor, reason, nonce)
    VALUES ($1::uuid, $2::uuid, $3::uuid, $4::date, 'cicd', 'succeeded', 'post-sync:cover', 'partition-recompute', 'cover cell', $5)`,
		uuid.NewString(), later, org, day, uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	outcome, err := store.ReconcileBlockedRuns(ctx, org)
	var reason *string
	if readErr := venue.Admin.QueryRow(ctx, `SELECT blocked_reason FROM daily_metrics_runs WHERE id = $1::uuid`, failed).Scan(&reason); readErr != nil {
		t.Fatal(readErr)
	}
	got := ""
	if reason != nil {
		got = *reason
	}
	t.Logf("reconcile as the domain role: err=%v cleared=%d blocked=%d failed run blocked_reason=%q", err, outcome.Cleared, outcome.Blocked, got)
	if err != nil {
		t.Fatalf("reconcile as the domain role: %v (refused: %q)", err, venue.Denials.All())
	}
	if got != "" || outcome.Cleared != 1 || outcome.Blocked != 0 {
		t.Errorf("failed run blocked_reason=%q cleared=%d blocked=%d, want cleared by the reopened later run (\"\", 1, 0)", got, outcome.Cleared, outcome.Blocked)
	}
	if refused := venue.Denials.All(); len(refused) > 0 {
		t.Errorf("the domain role was refused: %s", strings.Join(refused, "; "))
	}
}
