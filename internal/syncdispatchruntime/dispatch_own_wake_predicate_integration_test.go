//go:build integration

package syncdispatchruntime

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
)

// The own wake's predicate (syncrunrollup.WakeDispatchForDeliverySQL) against each row state it must tell apart, for
// the pass at delivery attempt 1, and the clause mutants each case kills.

type ownWakeExec func(ctx context.Context, pool *pgxpool.Pool, at time.Time, attempt int64) (bool, error)

type ownWakeCase struct {
	name  string
	setup string
	check func(row atomicFenceRow, availableAt, at time.Time) error
}

var ownWakeCases = []ownWakeCase{
	{
		name:  "newer_delivery_owns_the_row",
		setup: `UPDATE sync_dispatch_outbox SET attempts = 2, available_at = now() - interval '1 minute' WHERE id = $1`,
		check: func(row atomicFenceRow, _, _ time.Time) error {
			if row.status != "dispatched" || row.attempts != 2 {
				return fmt.Errorf("row %s/%d, want dispatched/2 left to the newer delivery", row.status, row.attempts)
			}
			return nil
		},
	},
	{
		name:  "own_dispatched_row",
		setup: `UPDATE sync_dispatch_outbox SET attempts = 1 WHERE id = $1`,
		check: func(row atomicFenceRow, availableAt, at time.Time) error {
			if row.status != "pending" || row.attempts != 1 || availableAt.After(at) {
				return fmt.Errorf("row %s/%d available_at=%s, want pending/1 due by %s", row.status, row.attempts, availableAt, at)
			}
			return nil
		},
	},
	{
		name: "pending_row_at_another_attempt",
		setup: `UPDATE sync_dispatch_outbox SET status = 'pending', attempts = 3, available_at = now() + interval '1 hour',
    dispatched_at = NULL, dispatched_transport = NULL, dispatched_route_generation = NULL WHERE id = $1`,
		check: func(row atomicFenceRow, availableAt, at time.Time) error {
			if row.status != "pending" || row.attempts != 3 || availableAt.After(at) {
				return fmt.Errorf("row %s/%d available_at=%s, want pending/3 moved to %s", row.status, row.attempts, availableAt, at)
			}
			return nil
		},
	},
}

func runOwnWakeCase(t *testing.T, c ownWakeCase, exec ownWakeExec) error {
	t.Helper()
	var result error
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedAtomicPassRun(t, ctx, pool, false, []string{atomicPassPlannedA}, nil)
		if _, err := pool.Exec(ctx, c.setup, dispatchServiceTestOutbox); err != nil {
			t.Fatal(err)
		}
		at := pgNow()
		if _, err := exec(ctx, pool, at, 1); err != nil {
			result = fmt.Errorf("wake statement: %w", err)
			return
		}
		result = c.check(readAtomicFenceRow(t, ctx, pool), dispatchOutboxAvailableAt(t, ctx, pool, discoveryTestRun), at)
	})
	return result
}

// TestOwnWakeForDeliveryPredicate runs the shipped WakeDispatchForDelivery on every case.
func TestOwnWakeForDeliveryPredicate(t *testing.T) {
	shipped := func(ctx context.Context, pool *pgxpool.Pool, at time.Time, attempt int64) (bool, error) {
		return syncrunrollup.WakeDispatchForDelivery(ctx, pool, discoveryTestRun, at, syncrunrollup.DispatchWakePathDispatchPass, attempt), nil
	}
	for _, c := range ownWakeCases {
		t.Run(c.name, func(t *testing.T) {
			if err := runOwnWakeCase(t, c, shipped); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestOwnWakeForDeliveryClauseMutantsAreKilled drops each clause of the predicate once and requires the named case to
// fail on the mutant statement.
func TestOwnWakeForDeliveryClauseMutantsAreKilled(t *testing.T) {
	const statusClause = "public.sync_dispatch_outbox.status = 'dispatched'"
	const attemptClause = "public.sync_dispatch_outbox.attempts <> $4::bigint"
	where := "\nWHERE NOT (" + statusClause + " AND " + attemptClause + ")"
	if syncrunrollup.WakeDispatchForDeliverySQL != syncrunrollup.WakeDispatchSQL+where {
		t.Fatalf("WakeDispatchForDeliverySQL is not WakeDispatchSQL + %q; update the mutants", where)
	}
	withAttempt := func(statement string) ownWakeExec {
		return func(ctx context.Context, pool *pgxpool.Pool, at time.Time, attempt int64) (bool, error) {
			tag, err := pool.Exec(ctx, statement, uuid.New(), discoveryTestRun, at, attempt)
			return err == nil && tag.RowsAffected() > 0, err
		}
	}
	noAttempt := func(ctx context.Context, pool *pgxpool.Pool, at time.Time, _ int64) (bool, error) {
		tag, err := pool.Exec(ctx, syncrunrollup.WakeDispatchSQL, uuid.New(), discoveryTestRun, at)
		return err == nil && tag.RowsAffected() > 0, err
	}
	for _, mutant := range []struct {
		name   string
		exec   ownWakeExec
		killer string
	}{
		{"drop_status_clause", withAttempt(syncrunrollup.WakeDispatchSQL + "\nWHERE NOT (" + attemptClause + ")"), "pending_row_at_another_attempt"},
		{"drop_attempt_clause", withAttempt(syncrunrollup.WakeDispatchSQL + "\nWHERE NOT (" + statusClause + ")"), "own_dispatched_row"},
		{"drop_where", noAttempt, "newer_delivery_owns_the_row"},
	} {
		t.Run(mutant.name, func(t *testing.T) {
			for _, c := range ownWakeCases {
				if c.name != mutant.killer {
					continue
				}
				err := runOwnWakeCase(t, c, mutant.exec)
				t.Logf("mutant %s, case %s: %v", mutant.name, c.name, err)
				if err == nil || strings.HasPrefix(err.Error(), "wake statement:") {
					t.Fatalf("mutant %s survived case %s (err=%v), want the case to fail on the row it writes", mutant.name, c.name, err)
				}
				return
			}
			t.Fatalf("no case named %s", mutant.killer)
		})
	}
}
