//go:build integration

package remaining

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// capacityExecutionFor builds the execution River would hand the capacity handler for one real partition, at a
// given attempt of a three-attempt budget.
func capacityExecutionFor(orgID, partitionID, runID string, attempt, maxAttempts int) *jobruntime.Execution[jobruntime.RemainingCapacityArgs] {
	organizationID := orgID
	args := jobruntime.RemainingCapacityArgs{
		EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.RemainingMetricsPartitionPayload]{
			ContractVersion: jobcontract.ContractVersionV1,
			OrganizationID:  &organizationID,
			CorrelationID:   "remaining:" + runID,
			IdempotencyKey:  "remaining:partition:" + partitionID,
			Domain:          jobcontract.DomainLink{Type: "remaining_metric_partition", ID: partitionID},
			Payload:         jobcontract.RemainingMetricsPartitionPayload{PartitionID: partitionID},
		},
	}
	execution := &jobruntime.Execution[jobruntime.RemainingCapacityArgs]{
		Args: args, Envelope: args.ContractEnvelope(), OrganizationID: &organizationID, Attempt: attempt,
	}
	execution.Definition.MaxAttempts = maxAttempts
	return execution
}

// TestARunIsNotLeftRunningWhenItsLastPartitionExhaustsItsRetries is the CHAOS-8024 state proof against a real
// Postgres: a partition whose compute fails with a RETRYABLE error on every attempt is released to 'failed' each
// time and reclaimed by the next attempt; when the job discards after its last attempt nothing ever came back to
// finalize the run, so the run stayed 'running' forever (the daily work-item-attribution partition on bigboy and
// prod). The run must be 'failed' once the last attempt fails, and only then; and only when no other partition is
// still outstanding.
func TestARunIsNotLeftRunningWhenItsLastPartitionExhaustsItsRetries(t *testing.T) {
	ctx := context.Background()
	pool, store, _ := newRemainingRedriveTestStack(t)
	store.now = func() time.Time { return time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC) }
	executor := &handlerExecutor{computeErr: errors.New("clickhouse: connection reset")}
	handler, err := NewPartitionHandler[jobruntime.RemainingCapacityArgs](store, executor, "capacity")
	if err != nil {
		t.Fatal(err)
	}
	status := func(t *testing.T, table, id string) string {
		t.Helper()
		var got string
		if err := pool.QueryRow(ctx, "SELECT status FROM "+table+" WHERE id = $1::uuid", id).Scan(&got); err != nil {
			t.Fatal(err)
		}
		return got
	}
	const maxAttempts = 3

	t.Run("the only partition", func(t *testing.T) {
		orgID := "00000000-0000-4000-8000-000000008024"
		run, err := store.StartRun(ctx, StartRunRequest{
			OrganizationID: orgID, Family: "capacity", Generation: "exhaust-single", ScopeKey: "all-teams",
			GenerationSeed: int64Pointer(1), Scopes: []json.RawMessage{capacityScopeJSON(90)},
		})
		if err != nil {
			t.Fatal(err)
		}
		partitionID := deterministicPartitionID(run.ID, 1)
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			workErr := handler.Work(ctx, capacityExecutionFor(orgID, partitionID, run.ID, attempt, maxAttempts))
			if workErr == nil {
				t.Fatalf("attempt %d: expected a failure", attempt)
			}
			if got := status(t, "remaining_metric_partitions", partitionID); got != "failed" {
				t.Fatalf("attempt %d: partition status = %q, want failed", attempt, got)
			}
			wantRun := "running" // a retry is still coming: the run must not be finalized underneath it
			if attempt == maxAttempts {
				wantRun = "failed" // the job discards now: nothing else will ever finalize the run
			}
			if got := status(t, "remaining_metric_runs", run.ID); got != wantRun {
				t.Fatalf("attempt %d of %d: run status = %q, want %q", attempt, maxAttempts, got, wantRun)
			}
		}
	})

	t.Run("a sibling partition still pending keeps the run running", func(t *testing.T) {
		orgID := "00000000-0000-4000-8000-000000008025"
		run, err := store.StartRun(ctx, StartRunRequest{
			OrganizationID: orgID, Family: "capacity", Generation: "exhaust-sibling", ScopeKey: "all-teams",
			GenerationSeed: int64Pointer(2), Scopes: []json.RawMessage{capacityScopeJSON(90), capacityScopeJSON(91)},
		})
		if err != nil {
			t.Fatal(err)
		}
		first := deterministicPartitionID(run.ID, 1)
		second := deterministicPartitionID(run.ID, 2)
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			if workErr := handler.Work(ctx, capacityExecutionFor(orgID, first, run.ID, attempt, maxAttempts)); workErr == nil {
				t.Fatalf("attempt %d: expected a failure", attempt)
			}
		}
		if got := status(t, "remaining_metric_partitions", first); got != "failed" {
			t.Fatalf("exhausted partition status = %q, want failed", got)
		}
		if got := status(t, "remaining_metric_partitions", second); got != "pending" {
			t.Fatalf("sibling partition status = %q, want pending", got)
		}
		if got := status(t, "remaining_metric_runs", run.ID); got != "running" {
			t.Fatalf("run status = %q with a pending sibling, want running (still live work)", got)
		}
	})
}
