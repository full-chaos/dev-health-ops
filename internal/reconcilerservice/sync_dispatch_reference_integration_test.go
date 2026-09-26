//go:build integration

package reconcilerservice

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
	"github.com/full-chaos/dev-health-ops/internal/syncreconciler"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestSyncDispatchReferenceJoinsTraceParentFromSyncRuns proves the
// sync_dispatch_outbox/sync_runs JOIN added for CHAOS-3996 actually resolves
// what it claims to against a real Postgres: the right org/run identity for
// the (outbox id, kind) pair, a non-NULL trace_parent when the run carries
// one, and a NULL trace_parent -- not an error -- for a run planned before
// this column existed (or with tracing off). Nothing upstream of this test
// type-checks the raw SQL string itself, so a join-direction or column-name
// mistake here would only ever surface at runtime.
func TestSyncDispatchReferenceJoinsTraceParentFromSyncRuns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate Postgres: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	// The migrated schema: sync_runs (with trace_parent) and sync_dispatch_outbox with its real
	// columns, foreign keys and constraints.
	pgschema.Apply(ctx, t, pool)

	orgID := uuid.New()
	tracedRun := uuid.New()
	untracedRun := uuid.New()
	tracedOutbox := uuid.New()
	untracedOutbox := uuid.New()
	const wantTraceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

	seed := []struct {
		runID       uuid.UUID
		traceParent *string
	}{
		{tracedRun, ptr(wantTraceParent)},
		{untracedRun, nil},
	}
	for _, row := range seed {
		pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{ID: row.runID.String(), OrgID: orgID.String()})
		if _, err := pool.Exec(ctx,
			`UPDATE public.sync_runs SET trace_parent = $2 WHERE id = $1`,
			row.runID, row.traceParent,
		); err != nil {
			t.Fatal(err)
		}
	}
	outboxRows := []struct {
		id    uuid.UUID
		runID uuid.UUID
	}{
		{tracedOutbox, tracedRun},
		{untracedOutbox, untracedRun},
	}
	for _, row := range outboxRows {
		pgseed.SyncDispatchOutbox(ctx, t, pool, row.id.String(), row.runID.String(), orgID.String(), "dispatch_sync_run", "pending", "", 0)
	}

	t.Run("a run with a captured trace_parent resolves it", func(t *testing.T) {
		got, err := syncDispatchReference(ctx, pool, tracedOutbox.String(), "dispatch_sync_run")
		if err != nil {
			t.Fatal(err)
		}
		want := syncdispatchruntime.DomainReference{
			OrganizationID: orgID.String(), SyncRunID: tracedRun.String(), TraceParent: wantTraceParent,
		}
		if got != want {
			t.Fatalf("got %+v, want %+v", got, want)
		}
	})

	t.Run("a run planned before trace_parent existed resolves an empty one, not an error", func(t *testing.T) {
		got, err := syncDispatchReference(ctx, pool, untracedOutbox.String(), "dispatch_sync_run")
		if err != nil {
			t.Fatal(err)
		}
		want := syncdispatchruntime.DomainReference{
			OrganizationID: orgID.String(), SyncRunID: untracedRun.String(), TraceParent: "",
		}
		if got != want {
			t.Fatalf("got %+v, want %+v", got, want)
		}
	})

	t.Run("a kind that does not match the claimed row is unavailable, not the wrong row", func(t *testing.T) {
		_, err := syncDispatchReference(ctx, pool, tracedOutbox.String(), "finalize_sync_run")
		if err != syncreconciler.ErrUnavailable {
			t.Fatalf("err = %v, want ErrUnavailable", err)
		}
	})

	t.Run("an outbox id with no row is unavailable", func(t *testing.T) {
		_, err := syncDispatchReference(ctx, pool, uuid.New().String(), "dispatch_sync_run")
		if err != syncreconciler.ErrUnavailable {
			t.Fatalf("err = %v, want ErrUnavailable", err)
		}
	})
}

func ptr(value string) *string { return &value }
