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
