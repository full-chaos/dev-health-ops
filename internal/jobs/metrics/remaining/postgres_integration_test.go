//go:build integration

package remaining

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresStoreResumesPartitionsAndFencesCancellationAndExpiry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createRemainingTables(t, ctx, pool)

	const orgID = "00000000-0000-4000-8000-000000000109"
	now := time.Date(2026, 7, 23, 20, 0, 0, 0, time.UTC)
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return now }
	run, err := store.StartRun(ctx, StartRunRequest{
		OrganizationID: orgID,
		Family:         "capacity",
		Generation:     "capacity-v1",
		ScopeKey:       "all-teams",
		GenerationSeed: int64Pointer(42),
		Scopes:         []json.RawMessage{json.RawMessage(`{"version":1,"all_teams":true,"history_days":90,"simulations":10000}`), json.RawMessage(`{"version":1,"all_teams":true,"history_days":91,"simulations":10000}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	runID := run.ID
	firstID := deterministicPartitionID(runID, 1)
	secondID := deterministicPartitionID(runID, 2)

	var before time.Time
	if err := pool.QueryRow(ctx, "SELECT updated_at FROM remaining_metric_runs WHERE id=$1::uuid", runID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	if unknown, err := store.ClaimPartition(ctx, uuid.NewString()); err != nil || unknown != nil {
		t.Fatalf("unknown claim=%#v err=%v", unknown, err)
	}
	var afterNoClaim time.Time
	if err := pool.QueryRow(ctx, "SELECT updated_at FROM remaining_metric_runs WHERE id=$1::uuid", runID).Scan(&afterNoClaim); err != nil {
		t.Fatal(err)
	}
	if !afterNoClaim.Equal(before) {
		t.Fatalf("unclaimable partition moved run timestamp: before=%s after=%s", before, afterNoClaim)
	}
	run, err = store.LoadRun(ctx, runID)
	if err != nil || run.Family != "capacity" || run.Seed == nil || *run.Seed != 42 {
		t.Fatalf("run=%#v err=%v", run, err)
	}

	first, err := store.ClaimPartition(ctx, firstID)
	if err != nil || first == nil {
		t.Fatalf("first claim=%#v err=%v", first, err)
	}
	if err := store.CompletePartition(ctx, *first, "rows=1;sha256=golden"); err != nil {
		t.Fatal(err)
	}
	pending, err := store.PendingPartitions(ctx, runID)
	if err != nil || len(pending) != 1 || pending[0].ID != secondID {
		t.Fatalf("pending=%#v err=%v", pending, err)
	}
	if duplicate, err := store.ClaimPartition(ctx, firstID); err != nil || duplicate != nil {
		t.Fatalf("completed partition reclaimed=%#v err=%v", duplicate, err)
	}

	second, err := store.ClaimPartition(ctx, secondID)
	if err != nil || second == nil {
		t.Fatalf("second claim=%#v err=%v", second, err)
	}
	if err := store.CancelRun(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if err := store.RenewPartition(ctx, *second); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("canceled claim renewed: %v", err)
	}
	if err := store.CompletePartition(ctx, *second, "rows=1"); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("canceled claim completed: %v", err)
	}

	// An expired owner loses authority before any replacement claims.
	if _, err := pool.Exec(ctx, "UPDATE remaining_metric_runs SET status='running' WHERE id=$1::uuid", runID); err != nil {
		t.Fatal(err)
	}
	now = now.Add(store.lease + time.Second)
	if err := store.RenewPartition(ctx, *second); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired unreclaimed claim renewed: %v", err)
	}
	if err := store.CompletePartition(ctx, *second, "rows=1"); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired unreclaimed claim completed: %v", err)
	}
	reclaimed, err := store.ClaimPartition(ctx, secondID)
	if err != nil || reclaimed == nil || reclaimed.Token == second.Token {
		t.Fatalf("reclaimed=%#v err=%v", reclaimed, err)
	}
	if err := store.CompletePartition(ctx, *second, "rows=1"); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale owner completed after reclaim: %v", err)
	}
	if err := store.CompletePartition(ctx, *reclaimed, "rows=1;sha256=golden"); err != nil {
		t.Fatal(err)
	}
	if err := store.FinalizeRun(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if err := store.FinalizeRun(ctx, runID); err != nil {
		t.Fatalf("idempotent finalize: %v", err)
	}
	run, err = store.LoadRun(ctx, runID)
	if err != nil || run.Status != "succeeded" {
		t.Fatalf("final run=%#v err=%v", run, err)
	}
}

func TestPostgresStoreStartRunReplaysAtomically(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createRemainingTables(t, ctx, pool)
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return time.Date(2026, 7, 23, 20, 0, 0, 0, time.UTC) }
	request := StartRunRequest{
		OrganizationID: "00000000-0000-4000-8000-000000000119",
		Family:         "capacity",
		Generation:     "capacity-v1",
		ScopeKey:       "all-teams",
		GenerationSeed: int64Pointer(42),
		Scopes:         []json.RawMessage{json.RawMessage(`{"version":1,"all_teams":true,"history_days":90,"simulations":10000}`), json.RawMessage(`{"version":1,"all_teams":true,"history_days":91,"simulations":10000}`)},
	}

	start := make(chan struct{})
	results := make(chan struct {
		run Run
		err error
	}, 2)
	var workers sync.WaitGroup
	for range 2 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			run, err := store.StartRun(ctx, request)
			results <- struct {
				run Run
				err error
			}{run, err}
		}()
	}
	close(start)
	workers.Wait()
	close(results)
	var runs []Run
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent StartRun: %v", result.err)
		}
		runs = append(runs, result.run)
	}
	if len(runs) != 2 || runs[0].ID != runs[1].ID || runs[0].ID != deterministicRunID(request) {
		t.Fatalf("concurrent runs=%#v", runs)
	}
	partitions, err := store.PendingPartitions(ctx, runs[0].ID)
	if err != nil || len(partitions) != 2 {
		t.Fatalf("partitions=%#v err=%v", partitions, err)
	}
	for index, partition := range partitions {
		ordinal := index + 1
		canonical, err := canonicalJSON(partition.Scope)
		if err != nil {
			t.Fatalf("partition %d scope = %q: %v", ordinal, partition.Scope, err)
		}
		expected, err := validateFamilyScope(request.Family, request.Scopes[index])
		expected, err = canonicalJSON(expected)
		if err != nil || partition.ID != deterministicPartitionID(runs[0].ID, ordinal) || partition.Ordinal != ordinal ||
			string(canonical) != string(expected) {
			t.Fatalf("partition %d = %#v", ordinal, partition)
		}
	}

	mismatchedSeed := request
	mismatchedSeed.GenerationSeed = int64Pointer(43)
	if _, err := store.StartRun(ctx, mismatchedSeed); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("mismatched seed replay error = %v", err)
	}
	mismatchedScopes := request
	mismatchedScopes.Scopes = []json.RawMessage{json.RawMessage(`{"version":1,"all_teams":true,"history_days":90,"simulations":10000}`)}
	if _, err := store.StartRun(ctx, mismatchedScopes); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("mismatched scope replay error = %v", err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM remaining_metric_partitions WHERE run_id=$1::uuid AND ordinal=2`, runs[0].ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartRun(ctx, request); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("corrupted ordinal replay error = %v", err)
	}
	if _, err := store.StartRun(ctx, StartRunRequest{
		OrganizationID: request.OrganizationID, Family: "dora", Generation: "dora-v1", ScopeKey: "bad-seed",
		GenerationSeed: int64Pointer(1), Scopes: []json.RawMessage{json.RawMessage(`{}`)},
	}); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("non-capacity seed error = %v", err)
	}
	if _, err := store.StartRun(ctx, StartRunRequest{
		OrganizationID: request.OrganizationID, Family: "capacity", Generation: "capacity-v2", ScopeKey: "missing-seed",
		Scopes: []json.RawMessage{json.RawMessage(`{}`)},
	}); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("capacity missing seed error = %v", err)
	}
	if _, err := store.StartRun(ctx, StartRunRequest{
		OrganizationID: request.OrganizationID, Family: "not-in-inventory", Generation: "v1", ScopeKey: "bad-family",
		Scopes: []json.RawMessage{json.RawMessage(`{}`)},
	}); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("unknown inventory family error = %v", err)
	}

	invalid := request
	invalid.ScopeKey = "invalid-json"
	invalid.Scopes = []json.RawMessage{json.RawMessage(`not-json`)}
	if _, err := store.StartRun(ctx, invalid); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("invalid scope error = %v", err)
	}
	assertRunAndPartitionCounts(t, ctx, pool, deterministicRunID(invalid), 0, 0)

	if _, err := pool.Exec(ctx, `
CREATE FUNCTION reject_remaining_partition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.ordinal = 2 THEN RAISE EXCEPTION 'forced partition failure'; END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER reject_remaining_partition BEFORE INSERT ON remaining_metric_partitions
FOR EACH ROW EXECUTE FUNCTION reject_remaining_partition()`); err != nil {
		t.Fatal(err)
	}
	partial := request
	partial.ScopeKey = "forced-rollback"
	if _, err := store.StartRun(ctx, partial); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("partial insert error = %v", err)
	}
	assertRunAndPartitionCounts(t, ctx, pool, deterministicRunID(partial), 0, 0)
}

func int64Pointer(value int64) *int64 { return &value }

func assertRunAndPartitionCounts(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID string, wantRuns, wantPartitions int) {
	t.Helper()
	var runs, partitions int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM remaining_metric_runs WHERE id=$1::uuid", runID).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM remaining_metric_partitions WHERE run_id=$1::uuid", runID).Scan(&partitions); err != nil {
		t.Fatal(err)
	}
	if runs != wantRuns || partitions != wantPartitions {
		t.Fatalf("persisted counts = runs:%d partitions:%d, want runs:%d partitions:%d", runs, partitions, wantRuns, wantPartitions)
	}
}

func createRemainingTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
CREATE TABLE remaining_metric_runs (
 id uuid PRIMARY KEY, org_id uuid NOT NULL, family text NOT NULL, generation text NOT NULL,
 scope_key text NOT NULL, generation_seed bigint NULL, status text NOT NULL,
 canceled_at timestamptz NULL, created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
 UNIQUE(org_id,family,generation,scope_key)
);
CREATE TABLE remaining_metric_partitions (
 id uuid PRIMARY KEY, run_id uuid NOT NULL REFERENCES remaining_metric_runs(id), ordinal integer NOT NULL CHECK (ordinal >= 1),
 scope jsonb NOT NULL, status text NOT NULL, claim_token uuid NULL, lease_expires_at timestamptz NULL,
 attempt_count integer NOT NULL, output_evidence text NULL, completed_at timestamptz NULL,
 created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL, UNIQUE(run_id,ordinal)
)`)
	if err != nil {
		t.Fatal(err)
	}
}
