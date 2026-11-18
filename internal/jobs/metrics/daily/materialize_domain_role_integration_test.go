//go:build integration

package daily

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/domainroledb"
)

// The dispatch materializes a run on the worker's domain pool. On the migrated
// schema and the real go-river-migrate grants, the domain role materializes a
// run with repositories, hands an empty run to its finalize, and refuses to cut
// partitions on a retry of the empty run; no statement is refused.
func TestMaterializeScheduledFanoutRunsAsTheDomainRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	venue := domainroledb.Start(t, ctx, "daily_materialize")
	venue.RequireReady(t, ctx)
	registry, err := jobruntime.Load(filepath.Join("..", "..", "..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := NewPostgresPublisher(venue.Domain, dailyTestRegistry{production: registry})
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewPostgresStore(venue.Domain)
	if err != nil {
		t.Fatal(err)
	}
	day := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	const org = "00000000-0000-4000-8000-000000140376"
	claim := func(generation string) Run {
		t.Helper()
		run := newRun(org, day, generation)
		if _, err := venue.Admin.Exec(ctx, `INSERT INTO daily_metrics_runs
    (id, org_id, target_day, generation, status, finalization_status, created_at, updated_at, full_org)
    VALUES ($1::uuid, $2::uuid, $3::date, $4, 'pending', 'pending', now(), now(), true)`,
			run.ID, org, day.Format("2006-01-02"), generation); err != nil {
			t.Fatal(err)
		}
		claimed, err := store.ClaimDispatch(ctx, run.ID)
		if err != nil || claimed == nil {
			t.Fatalf("claim dispatch as the domain role: %v (refused: %q)", err, venue.Denials.All())
		}
		return *claimed
	}
	handoffs := func(runID string) (finalize, partitions int) {
		t.Helper()
		if err := venue.Admin.QueryRow(ctx, `SELECT count(*) FROM worker_job_outbox WHERE dedupe_key = $1`, FinalizeHandoffKey(runID)).Scan(&finalize); err != nil {
			t.Fatal(err)
		}
		if err := venue.Admin.QueryRow(ctx, `SELECT count(*) FROM daily_metrics_partitions WHERE run_id = $1::uuid`, runID).Scan(&partitions); err != nil {
			t.Fatal(err)
		}
		return finalize, partitions
	}

	withRepositories := claim(ScheduledFanoutGenerationPrefix + "2026-09-15T01:00:00Z")
	created, err := store.MaterializeScheduledFanout(ctx, withRepositories, []RepositoryID{"00000000-0000-4000-8000-0000001403a1"}, publisher)
	finalize, partitions := handoffs(withRepositories.ID)
	t.Logf("run with a repository: created=%t err=%v finalize_handoffs=%d partitions=%d", created, err, finalize, partitions)
	if err != nil || !created || finalize != 0 || partitions != 1 {
		t.Errorf("run with a repository: created=%t err=%v, %d handoff(s), %d partition(s); want true, nil, 0, 1", created, err, finalize, partitions)
	}

	empty := claim(ScheduledFanoutGenerationPrefix + "2026-09-15T01:01:00Z")
	created, err = store.MaterializeScheduledFanout(ctx, empty, nil, publisher)
	finalize, partitions = handoffs(empty.ID)
	t.Logf("empty run: created=%t err=%v finalize_handoffs=%d partitions=%d", created, err, finalize, partitions)
	if err != nil || !created || finalize != 1 || partitions != 0 {
		t.Errorf("empty run: created=%t err=%v, %d handoff(s), %d partition(s); want true, nil, 1, 0", created, err, finalize, partitions)
	}
	retried, err := store.MaterializeScheduledFanout(ctx, empty, []RepositoryID{"00000000-0000-4000-8000-0000001403a2"}, publisher)
	finalize, partitions = handoffs(empty.ID)
	t.Logf("retry of the empty run: created=%t err=%v finalize_handoffs=%d partitions=%d", retried, err, finalize, partitions)
	if err != nil || retried || finalize != 1 || partitions != 0 {
		t.Errorf("retry of the empty run: created=%t err=%v, %d handoff(s), %d partition(s); want false, nil, 1, 0", retried, err, finalize, partitions)
	}
	if refused := venue.Denials.All(); len(refused) > 0 {
		t.Errorf("the domain role was refused: %s", strings.Join(refused, "; "))
	}
}
