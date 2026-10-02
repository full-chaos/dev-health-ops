//go:build integration

package remaining

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining/stepcause"
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
	status := func(t *testing.T, table, id string) string {
		t.Helper()
		var got string
		if err := pool.QueryRow(ctx, "SELECT status FROM "+table+" WHERE id = $1::uuid", id).Scan(&got); err != nil {
			t.Fatal(err)
		}
		return got
	}
	const maxAttempts = 3

	// the executor error is tried untagged and step-tagged: after the step-cause change every real executor failure is
	// tagged, so the tagged one is the path a real ClickHouse/Postgres failure takes
	connection := errors.New("clickhouse: connection reset")
	errorsByName := []struct {
		name string
		err  error
	}{
		{"untagged executor error", connection},
		{"step-tagged executor error", stepcause.Failure(stepcause.QueryWorkItems, connection)},
	}
	for index, variant := range errorsByName {
		variant := variant
		executor := &handlerExecutor{computeErr: variant.err}
		handler, err := NewPartitionHandler[jobruntime.RemainingCapacityArgs](store, executor, "capacity")
		if err != nil {
			t.Fatal(err)
		}
		t.Run("the only partition, "+variant.name, func(t *testing.T) {
			orgID := fmt.Sprintf("00000000-0000-4000-8000-00000000802%d", 4+index*2)
			run, err := store.StartRun(ctx, StartRunRequest{
				OrganizationID: orgID, Family: "capacity", Generation: "exhaust-single-" + variant.name, ScopeKey: "all-teams",
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
	}

	executor := &handlerExecutor{computeErr: connection}
	handler, err := NewPartitionHandler[jobruntime.RemainingCapacityArgs](store, executor, "capacity")
	if err != nil {
		t.Fatal(err)
	}
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

// TestRunLifecycleAroundAnExhaustedPartitionAgainstRealPostgres pins the sibling and non-error paths of CHAOS-8024:
// a partition that failed and awaits its River retry must keep the run live (its retry has to be able to claim
// it), an exhausted partition must not leave the run running once the last live sibling is done, and the run is finalized failed when the last live sibling succeeds behind an exhausted one.
func TestRunLifecycleAroundAnExhaustedPartitionAgainstRealPostgres(t *testing.T) {
	ctx := context.Background()
	pool, plainStore, _ := newRemainingRedriveTestStack(t)
	plainStore.now = func() time.Time { return time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC) }
	const maxAttempts = 3
	status := func(t *testing.T, table, id string) string {
		t.Helper()
		var got string
		if err := pool.QueryRow(ctx, "SELECT status FROM "+table+" WHERE id = $1::uuid", id).Scan(&got); err != nil {
			t.Fatal(err)
		}
		return got
	}
	exhausted := func(t *testing.T, partitionID string) bool {
		t.Helper()
		var completed bool
		if err := pool.QueryRow(ctx, "SELECT completed_at IS NOT NULL FROM remaining_metric_partitions WHERE id = $1::uuid", partitionID).Scan(&completed); err != nil {
			t.Fatal(err)
		}
		return completed
	}
	failing := &handlerExecutor{computeErr: stepcause.Failure(stepcause.QueryWorkItems, errors.New("clickhouse: connection reset"))}
	failingHandler, err := NewPartitionHandler[jobruntime.RemainingCapacityArgs](plainStore, failing, "capacity")
	if err != nil {
		t.Fatal(err)
	}
	startTwo := func(t *testing.T, orgSuffix, generation string, seed int64) (runID, first, second, orgID string) {
		t.Helper()
		orgID = "00000000-0000-4000-8000-0000000" + orgSuffix
		run, err := plainStore.StartRun(ctx, StartRunRequest{
			OrganizationID: orgID, Family: "capacity", Generation: generation, ScopeKey: "all-teams",
			GenerationSeed: int64Pointer(seed), Scopes: []json.RawMessage{capacityScopeJSON(90), capacityScopeJSON(91)},
		})
		if err != nil {
			t.Fatal(err)
		}
		return run.ID, deterministicPartitionID(run.ID, 1), deterministicPartitionID(run.ID, 2), orgID
	}
	work := func(h *PartitionHandler[jobruntime.RemainingCapacityArgs], orgID, partitionID, runID string, attempt int) error {
		return h.Work(ctx, capacityExecutionFor(orgID, partitionID, runID, attempt, maxAttempts))
	}

	t.Run("a sibling awaiting its retry keeps the run live until it is exhausted too", func(t *testing.T) {
		runID, first, second, orgID := startTwo(t, "08031", "lifecycle-await", 11)
		if err := work(failingHandler, orgID, first, runID, 1); err == nil { // first: failed once, retry queued
			t.Fatal("expected a failure")
		}
		for attempt := 1; attempt <= maxAttempts; attempt++ { // second: exhausts
			if err := work(failingHandler, orgID, second, runID, attempt); err == nil {
				t.Fatal("expected a failure")
			}
		}
		if got := status(t, "remaining_metric_runs", runID); got != "running" {
			t.Fatalf("run = %q while the first partition still awaits its retry, want running", got)
		}
		if !exhausted(t, second) || exhausted(t, first) {
			t.Fatalf("exhausted markers: second=%v first=%v, want true and false", exhausted(t, second), exhausted(t, first))
		}
		// the queued retry of the first partition must still be able to claim it
		if err := work(failingHandler, orgID, first, runID, 2); err == nil {
			t.Fatal("expected a failure")
		}
		if got := status(t, "remaining_metric_partitions", first); got != "failed" {
			t.Fatalf("first partition = %q after its second attempt, want failed (it was claimable)", got)
		}
		if got := status(t, "remaining_metric_runs", runID); got != "running" {
			t.Fatalf("run = %q before the first partition's last attempt, want running", got)
		}
		if err := work(failingHandler, orgID, first, runID, 3); err == nil {
			t.Fatal("expected a failure")
		}
		if got := status(t, "remaining_metric_runs", runID); got != "failed" {
			t.Fatalf("run = %q after both partitions are exhausted, want failed", got)
		}
	})

	t.Run("the last live partition succeeding while a sibling is exhausted finalizes the run failed", func(t *testing.T) {
		runID, first, second, orgID := startTwo(t, "08032", "lifecycle-complete", 12)
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			if err := work(failingHandler, orgID, first, runID, attempt); err == nil {
				t.Fatal("expected a failure")
			}
		}
		if got := status(t, "remaining_metric_runs", runID); got != "running" {
			t.Fatalf("run = %q with the second partition still pending, want running", got)
		}
		succeeding, err := NewPartitionHandler[jobruntime.RemainingCapacityArgs](plainStore, &handlerExecutor{}, "capacity")
		if err != nil {
			t.Fatal(err)
		}
		if err := work(succeeding, orgID, second, runID, 1); err != nil {
			t.Fatalf("the second partition failed: %v", err)
		}
		if got := status(t, "remaining_metric_partitions", second); got != "succeeded" {
			t.Fatalf("second partition = %q, want succeeded", got)
		}
		if got := status(t, "remaining_metric_runs", runID); got != "failed" {
			t.Fatalf("run = %q after its last live partition succeeded behind an exhausted one, want failed (it can never succeed)", got)
		}
	})

	t.Run("a live partition succeeding while another awaits its retry and a third is exhausted leaves the run live", func(t *testing.T) {
		orgID := "00000000-0000-4000-8000-000000008036"
		run, err := plainStore.StartRun(ctx, StartRunRequest{
			OrganizationID: orgID, Family: "capacity", Generation: "lifecycle-three", ScopeKey: "all-teams",
			GenerationSeed: int64Pointer(16), Scopes: []json.RawMessage{capacityScopeJSON(90), capacityScopeJSON(91), capacityScopeJSON(92)},
		})
		if err != nil {
			t.Fatal(err)
		}
		awaiting, exhaustedOne, live := deterministicPartitionID(run.ID, 1), deterministicPartitionID(run.ID, 2), deterministicPartitionID(run.ID, 3)
		if err := work(failingHandler, orgID, awaiting, run.ID, 1); err == nil { // failed once, its retry is queued
			t.Fatal("expected a failure")
		}
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			if err := work(failingHandler, orgID, exhaustedOne, run.ID, attempt); err == nil {
				t.Fatal("expected a failure")
			}
		}
		succeeding, err := NewPartitionHandler[jobruntime.RemainingCapacityArgs](plainStore, &handlerExecutor{}, "capacity")
		if err != nil {
			t.Fatal(err)
		}
		if err := work(succeeding, orgID, live, run.ID, 1); err != nil {
			t.Fatalf("the live partition failed: %v", err)
		}
		if got := status(t, "remaining_metric_runs", run.ID); got != "running" {
			t.Fatalf("run = %q while one partition still awaits its retry, want running", got)
		}
		for attempt := 2; attempt <= maxAttempts; attempt++ {
			if err := work(failingHandler, orgID, awaiting, run.ID, attempt); err == nil {
				t.Fatal("expected a failure")
			}
		}
		if got := status(t, "remaining_metric_runs", run.ID); got != "failed" {
			t.Fatalf("run = %q after the awaiting partition was exhausted too, want failed", got)
		}
	})

	t.Run("a run with no failed partition is not failed by a completion", func(t *testing.T) {
		runID, first, second, orgID := startTwo(t, "08037", "lifecycle-canceled", 17)
		if _, err := pool.Exec(ctx, "UPDATE remaining_metric_partitions SET status = 'canceled' WHERE id = $1::uuid", first); err != nil {
			t.Fatal(err)
		}
		succeeding, err := NewPartitionHandler[jobruntime.RemainingCapacityArgs](plainStore, &handlerExecutor{}, "capacity")
		if err != nil {
			t.Fatal(err)
		}
		if err := work(succeeding, orgID, second, runID, 1); err != nil {
			t.Fatalf("the live partition failed: %v", err)
		}
		if got := status(t, "remaining_metric_runs", runID); got != "running" {
			t.Fatalf("run = %q with a canceled and a succeeded partition and none failed, want running (unchanged behaviour)", got)
		}
	})

	t.Run("a claim clears the exhausted marker", func(t *testing.T) {
		runID, first, _, orgID := startTwo(t, "08035", "lifecycle-marker", 15)
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			if err := work(failingHandler, orgID, first, runID, attempt); err == nil {
				t.Fatal("expected a failure")
			}
		}
		if !exhausted(t, first) {
			t.Fatal("the exhausted partition carries no marker")
		}
		claim, err := plainStore.ClaimPartition(ctx, first) // a later claim (a redrive) starts a fresh life
		if err != nil || claim == nil {
			t.Fatalf("claim = %#v, %v", claim, err)
		}
		if exhausted(t, first) {
			t.Fatal("a claim did not clear the exhausted marker")
		}
		if got := status(t, "remaining_metric_runs", runID); got != "running" {
			t.Fatalf("run = %q, want running", got)
		}
	})

}
