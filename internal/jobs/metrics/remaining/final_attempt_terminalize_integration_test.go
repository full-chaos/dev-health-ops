//go:build integration

package remaining

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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

type claimFailingStore struct {
	*PostgresStore
	fail bool
}

func (s *claimFailingStore) ClaimPartition(ctx context.Context, id string) (*Claim, error) {
	if s.fail {
		return nil, errors.New("claim partition: connection reset")
	}
	return s.PostgresStore.ClaimPartition(ctx, id)
}

// CHAOS-8024 (probe from gwc-vetter-3) (#3662 delta, "(2) no worse than base" combined with a sibling): partition 1 fails once (ordinary release,
// retry queued), then its attempts 2 and 3 fail in ClaimPartition (r1 residual (2): River discards the job, nothing releases it);
// partition 2 then exhausts its 3 attempts. Nothing can still run for this run: it must end 'failed', not stay 'running'.
func TestAClaimErrorOnTheLastAttemptPlusAnExhaustedSiblingEndsTheRunFailed(t *testing.T) {
	ctx := context.Background()
	pool, store, _ := newRemainingRedriveTestStack(t)
	store.now = func() time.Time { return time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC) }
	orgID := "00000000-0000-4000-8000-000000009301"
	run, err := store.StartRun(ctx, StartRunRequest{OrganizationID: orgID, Family: "capacity", Generation: "vetter-combo", ScopeKey: "all-teams",
		GenerationSeed: int64Pointer(93), Scopes: []json.RawMessage{capacityScopeJSON(90), capacityScopeJSON(91)}})
	if err != nil {
		t.Fatal(err)
	}
	first, second := deterministicPartitionID(run.ID, 1), deterministicPartitionID(run.ID, 2)
	failing := &handlerExecutor{computeErr: errors.New("clickhouse: connection reset")}
	flaky := &claimFailingStore{PostgresStore: store}
	hFirst, _ := NewPartitionHandler[jobruntime.RemainingCapacityArgs](flaky, failing, "capacity")
	hSecond, _ := NewPartitionHandler[jobruntime.RemainingCapacityArgs](store, failing, "capacity")
	_ = hFirst.Work(ctx, capacityExecutionFor(orgID, first, run.ID, 1, 3))
	flaky.fail = true
	_ = hFirst.Work(ctx, capacityExecutionFor(orgID, first, run.ID, 2, 3))
	_ = hFirst.Work(ctx, capacityExecutionFor(orgID, first, run.ID, 3, 3)) // River discards partition 1's job here
	for attempt := 1; attempt <= 3; attempt++ {
		_ = hSecond.Work(ctx, capacityExecutionFor(orgID, second, run.ID, attempt, 3))
	}
	var runStatus, p1, p2 string
	_ = pool.QueryRow(ctx, "SELECT status FROM remaining_metric_runs WHERE id=$1::uuid", run.ID).Scan(&runStatus)
	_ = pool.QueryRow(ctx, "SELECT status FROM remaining_metric_partitions WHERE id=$1::uuid", first).Scan(&p1)
	_ = pool.QueryRow(ctx, "SELECT status FROM remaining_metric_partitions WHERE id=$1::uuid", second).Scan(&p2)
	t.Logf("VETTER-COMBO run=%s partition1=%s partition2=%s", runStatus, p1, p2)
	if runStatus != "failed" {
		t.Fatalf("run = %q with no job left for either partition, want failed (stranded)", runStatus)
	}
}
func TestALastClaimErrorFinalizesTheRunWhenTheSiblingExhaustedFirstOrTheLeaseExpired(t *testing.T) {
	ctx := context.Background()
	pool, store, _ := newRemainingRedriveTestStack(t)
	clock := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return clock }
	failing := &handlerExecutor{computeErr: errors.New("clickhouse: connection reset")}
	flaky := &claimFailingStore{PostgresStore: store}
	hFlaky, _ := NewPartitionHandler[jobruntime.RemainingCapacityArgs](flaky, failing, "capacity")
	hPlain, _ := NewPartitionHandler[jobruntime.RemainingCapacityArgs](store, failing, "capacity")
	runStatus := func(id string) string {
		var s string
		_ = pool.QueryRow(ctx, "SELECT status FROM remaining_metric_runs WHERE id=$1::uuid", id).Scan(&s)
		return s
	}
	start := func(org string, seed int64) (string, string, string) {
		run, err := store.StartRun(ctx, StartRunRequest{OrganizationID: org, Family: "capacity", Generation: "vetter-row5", ScopeKey: "all-teams",
			GenerationSeed: int64Pointer(seed), Scopes: []json.RawMessage{capacityScopeJSON(90), capacityScopeJSON(91)}})
		if err != nil {
			t.Fatal(err)
		}
		return run.ID, deterministicPartitionID(run.ID, 1), deterministicPartitionID(run.ID, 2)
	}
	failed := false
	orgA := "00000000-0000-4000-8000-000000009351"
	runA, a1, a2 := start(orgA, 351)
	flaky.fail = false
	_ = hFlaky.Work(ctx, capacityExecutionFor(orgA, a1, runA, 1, 3))
	for attempt := 1; attempt <= 3; attempt++ {
		_ = hPlain.Work(ctx, capacityExecutionFor(orgA, a2, runA, attempt, 3))
	}
	flaky.fail = true
	_ = hFlaky.Work(ctx, capacityExecutionFor(orgA, a1, runA, 2, 3))
	_ = hFlaky.Work(ctx, capacityExecutionFor(orgA, a1, runA, 3, 3))
	t.Logf("VETTER-ROW5-A run=%s", runStatus(runA))
	if got := runStatus(runA); got != "failed" {
		t.Errorf("A: run = %q after the last partition's last claim failed, want failed", got)
		failed = true
	}
	orgB := "00000000-0000-4000-8000-000000009352"
	runB, b1, b2 := start(orgB, 352)
	flaky.fail = false
	if _, err := store.ClaimPartition(ctx, b1); err != nil { // attempt 1 crashes holding the lease
		t.Fatal(err)
	}
	clock = clock.Add(2 * defaultLease) // the lease expires
	for attempt := 1; attempt <= 3; attempt++ {
		_ = hPlain.Work(ctx, capacityExecutionFor(orgB, b2, runB, attempt, 3))
	}
	flaky.fail = true
	_ = hFlaky.Work(ctx, capacityExecutionFor(orgB, b1, runB, 3, 3))
	t.Logf("VETTER-ROW5-B run=%s", runStatus(runB))
	if got := runStatus(runB); got != "failed" {
		t.Errorf("B: run = %q after the last claim failed on an expired lease, want failed", got)
		failed = true
	}
	_ = failed
}

// ExhaustPartition must leave a partition under a LIVE lease alone: its holder owns the outcome.
func TestExhaustPartitionLeavesALiveLeaseAlone(t *testing.T) {
	ctx := context.Background()
	pool, store, _ := newRemainingRedriveTestStack(t)
	store.now = func() time.Time { return time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC) }
	org := "00000000-0000-4000-8000-000000009361"
	run, err := store.StartRun(ctx, StartRunRequest{OrganizationID: org, Family: "capacity", Generation: "live-lease", ScopeKey: "all-teams",
		GenerationSeed: int64Pointer(361), Scopes: []json.RawMessage{capacityScopeJSON(90)}})
	if err != nil {
		t.Fatal(err)
	}
	id := deterministicPartitionID(run.ID, 1)
	if _, err := store.ClaimPartition(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := store.ExhaustPartition(ctx, id); err != nil {
		t.Fatal(err)
	}
	var status string
	_ = pool.QueryRow(ctx, "SELECT status FROM remaining_metric_partitions WHERE id=$1::uuid", id).Scan(&status)
	if status != "running" {
		t.Fatalf("partition = %q, want running (live lease untouched)", status)
	}
}

type expiringCompleteStore struct {
	*PostgresStore
	clock *time.Time
}

// CompletePartition lets the lease run out and then fails: the shape of a slow complete on the last attempt.
func (s *expiringCompleteStore) CompletePartition(context.Context, Claim, string) error {
	*s.clock = s.clock.Add(2 * defaultLease)
	return errors.New("complete partition: connection reset")
}

// CHAOS-8177 (gwc-review r2 P1-2): on the LAST attempt a complete that fails after the lease expired is fenced out of
// the terminal release (live-lease predicate), and no later attempt exists. The run must still end failed.
func TestALastAttemptCompleteFailureAfterLeaseExpiryEndsTheRunFailed(t *testing.T) {
	ctx := context.Background()
	pool, store, _ := newRemainingRedriveTestStack(t)
	clock := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return clock }
	org := "00000000-0000-4000-8000-000000009811"
	run, err := store.StartRun(ctx, StartRunRequest{OrganizationID: org, Family: "capacity", Generation: "expired-lease", ScopeKey: "all-teams",
		GenerationSeed: int64Pointer(811), Scopes: []json.RawMessage{capacityScopeJSON(90)}})
	if err != nil {
		t.Fatal(err)
	}
	partition := deterministicPartitionID(run.ID, 1)
	handler, _ := NewPartitionHandler[jobruntime.RemainingCapacityArgs](&expiringCompleteStore{PostgresStore: store, clock: &clock}, &handlerExecutor{}, "capacity")
	if workErr := handler.Work(ctx, capacityExecutionFor(org, partition, run.ID, 3, 3)); workErr == nil {
		t.Fatal("work error = nil, want the retryable complete failure")
	}
	var runStatus, partitionStatus string
	var exhausted bool
	_ = pool.QueryRow(ctx, "SELECT status FROM remaining_metric_runs WHERE id=$1::uuid", run.ID).Scan(&runStatus)
	_ = pool.QueryRow(ctx, "SELECT status, completed_at IS NOT NULL FROM remaining_metric_partitions WHERE id=$1::uuid", partition).Scan(&partitionStatus, &exhausted)
	if runStatus != "failed" || partitionStatus != "failed" || !exhausted {
		t.Fatalf("run=%s partition=%s exhausted=%t, want failed/failed/true", runStatus, partitionStatus, exhausted)
	}
}

type loadRunInvalidStore struct{ *PostgresStore }

func (s *loadRunInvalidStore) LoadRun(context.Context, string) (Run, error) {
	return Run{}, ErrInvalidState
}

// CHAOS-8176 (gwc-review r2 P1-1): a Permanent exit that holds a claim must leave the partition exhausted and the
// run failed when nothing else can progress. Both exits, real Postgres, first attempt (no retry follows a Permanent).
func TestPermanentExitsTerminalizeTheRunAgainstRealPostgres(t *testing.T) {
	ctx := context.Background()
	pool, store, _ := newRemainingRedriveTestStack(t)
	store.now = func() time.Time { return time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC) }
	for _, tc := range []struct {
		name    string
		org     string
		execOrg string // the organization the EXECUTION carries (differs from the run's: a mismatching run)
		seed    int64
		handler func() *PartitionHandler[jobruntime.RemainingCapacityArgs]
	}{
		{"run organization does not match the execution", "00000000-0000-4000-8000-000000009801", "00000000-0000-4000-8000-000000009899", 981, func() *PartitionHandler[jobruntime.RemainingCapacityArgs] {
			h, _ := NewPartitionHandler[jobruntime.RemainingCapacityArgs](store, &handlerExecutor{}, "capacity")
			return h
		}},
		{"load run invalid state", "00000000-0000-4000-8000-000000009802", "00000000-0000-4000-8000-000000009802", 982, func() *PartitionHandler[jobruntime.RemainingCapacityArgs] {
			h, _ := NewPartitionHandler[jobruntime.RemainingCapacityArgs](&loadRunInvalidStore{store}, &handlerExecutor{}, "capacity")
			return h
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run, err := store.StartRun(ctx, StartRunRequest{OrganizationID: tc.org, Family: "capacity", Generation: "permanent-exit", ScopeKey: "all-teams",
				GenerationSeed: int64Pointer(tc.seed), Scopes: []json.RawMessage{capacityScopeJSON(90)}})
			if err != nil {
				t.Fatal(err)
			}
			partition := deterministicPartitionID(run.ID, 1)
			workErr := tc.handler().Work(ctx, capacityExecutionFor(tc.execOrg, partition, run.ID, 1, 3))
			if workErr == nil || !strings.Contains(workErr.Error(), string(jobruntime.CategoryPermanent)) {
				t.Fatalf("work error = %v, want Permanent", workErr)
			}
			var runStatus, partitionStatus string
			var exhausted bool
			if err := pool.QueryRow(ctx, "SELECT status FROM remaining_metric_runs WHERE id=$1::uuid", run.ID).Scan(&runStatus); err != nil {
				t.Fatal(err)
			}
			if err := pool.QueryRow(ctx, "SELECT status, completed_at IS NOT NULL FROM remaining_metric_partitions WHERE id=$1::uuid", partition).Scan(&partitionStatus, &exhausted); err != nil {
				t.Fatal(err)
			}
			if runStatus != "failed" || partitionStatus != "failed" || !exhausted {
				t.Fatalf("run=%s partition=%s exhausted=%t, want failed/failed/true", runStatus, partitionStatus, exhausted)
			}
		})
	}
}

type transientTerminalReleaseStore struct{ *PostgresStore }

// ReleasePartitionTerminally fails like a transient database error: it touches nothing, so the claim's lease stays live.
func (s *transientTerminalReleaseStore) ReleasePartitionTerminally(context.Context, Claim) error {
	return errors.New("terminal release: connection reset")
}

// CHAOS-8177 (r1 P1): the terminal release of the LAST attempt fails with a transient error while the attempt's own
// lease is still LIVE. The fallback must take the partition under the attempt's own claim; leaving it to the lease
// expiry strands the run, because no later attempt exists.
func TestALastAttemptTransientTerminalReleaseFailureWithItsOwnLeaseLiveEndsTheRunFailed(t *testing.T) {
	ctx := context.Background()
	pool, store, _ := newRemainingRedriveTestStack(t)
	store.now = func() time.Time { return time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC) }
	org := "00000000-0000-4000-8000-000000009821"
	run, err := store.StartRun(ctx, StartRunRequest{OrganizationID: org, Family: "capacity", Generation: "own-lease", ScopeKey: "all-teams",
		GenerationSeed: int64Pointer(821), Scopes: []json.RawMessage{capacityScopeJSON(90)}})
	if err != nil {
		t.Fatal(err)
	}
	partition := deterministicPartitionID(run.ID, 1)
	failing := &handlerExecutor{computeErr: errors.New("clickhouse: connection reset")}
	handler, _ := NewPartitionHandler[jobruntime.RemainingCapacityArgs](&transientTerminalReleaseStore{store}, failing, "capacity")
	if workErr := handler.Work(ctx, capacityExecutionFor(org, partition, run.ID, 3, 3)); workErr == nil {
		t.Fatal("work error = nil, want the retryable compute failure")
	}
	var runStatus, partitionStatus string
	var exhausted bool
	_ = pool.QueryRow(ctx, "SELECT status FROM remaining_metric_runs WHERE id=$1::uuid", run.ID).Scan(&runStatus)
	_ = pool.QueryRow(ctx, "SELECT status, completed_at IS NOT NULL FROM remaining_metric_partitions WHERE id=$1::uuid", partition).Scan(&partitionStatus, &exhausted)
	if runStatus != "failed" || partitionStatus != "failed" || !exhausted {
		t.Fatalf("run=%s partition=%s exhausted=%t, want failed/failed/true", runStatus, partitionStatus, exhausted)
	}
}

// A claim whose token is not the partition's current one (another job took the partition and holds a live lease) must
// not be exhausted by the claimed variant: the holder owns the outcome.
func TestExhaustClaimedPartitionLeavesAnotherClaimantsLiveLeaseAlone(t *testing.T) {
	ctx := context.Background()
	pool, store, _ := newRemainingRedriveTestStack(t)
	store.now = func() time.Time { return time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC) }
	org := "00000000-0000-4000-8000-000000009822"
	run, err := store.StartRun(ctx, StartRunRequest{OrganizationID: org, Family: "capacity", Generation: "other-claimant", ScopeKey: "all-teams",
		GenerationSeed: int64Pointer(822), Scopes: []json.RawMessage{capacityScopeJSON(90)}})
	if err != nil {
		t.Fatal(err)
	}
	partition := deterministicPartitionID(run.ID, 1)
	claim, err := store.ClaimPartition(ctx, partition)
	if err != nil || claim == nil {
		t.Fatalf("claim = %v, %v", claim, err)
	}
	stale := *claim
	stale.Token = "00000000-0000-4000-8000-0000000000aa"
	if err := store.ExhaustClaimedPartition(ctx, stale); err != nil {
		t.Fatal(err)
	}
	var status string
	_ = pool.QueryRow(ctx, "SELECT status FROM remaining_metric_partitions WHERE id=$1::uuid", partition).Scan(&status)
	if status != "running" {
		t.Fatalf("partition = %q after a stale-token exhaust, want running (another claimant's live lease untouched)", status)
	}
	if err := store.ExhaustClaimedPartition(ctx, *claim); err != nil {
		t.Fatal(err)
	}
	_ = pool.QueryRow(ctx, "SELECT status FROM remaining_metric_partitions WHERE id=$1::uuid", partition).Scan(&status)
	if status != "failed" {
		t.Fatalf("partition = %q after the owner's exhaust, want failed", status)
	}
}

// CHAOS-8177 (r2 P1). Invariant: the claimed fallback may only end a partition that is still the caller's own claim
// (running under its token) or a running partition whose lease has expired; a partition a replacement attempt already
// released for retry (failed, completed_at NULL) is no longer the caller's, so the fallback must leave it reclaimable.
func TestAnOldAttemptsClaimedFallbackLeavesAReplacementsRetryReleaseReclaimable(t *testing.T) {
	ctx := context.Background()
	pool, store, _ := newRemainingRedriveTestStack(t)
	clock := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return clock }
	org := "00000000-0000-4000-8000-000000009823"
	run, err := store.StartRun(ctx, StartRunRequest{OrganizationID: org, Family: "capacity", Generation: "replacement-released", ScopeKey: "all-teams",
		GenerationSeed: int64Pointer(823), Scopes: []json.RawMessage{capacityScopeJSON(90)}})
	if err != nil {
		t.Fatal(err)
	}
	partition := deterministicPartitionID(run.ID, 1)
	old, err := store.ClaimPartition(ctx, partition)
	if err != nil || old == nil {
		t.Fatalf("old claim = %v, %v", old, err)
	}
	clock = clock.Add(old.LeaseDuration + time.Minute) // the old attempt's lease expires
	replacement, err := store.ClaimPartition(ctx, partition)
	if err != nil || replacement == nil {
		t.Fatalf("replacement claim = %v, %v", replacement, err)
	}
	if err := store.ReleasePartition(ctx, *replacement); err != nil { // ordinary retryable release
		t.Fatal(err)
	}
	if err := store.ExhaustClaimedPartition(ctx, *old); err != nil { // the old final attempt's late fallback
		t.Fatal(err)
	}
	var runStatus, partitionStatus string
	var exhausted bool
	_ = pool.QueryRow(ctx, "SELECT status FROM remaining_metric_runs WHERE id=$1::uuid", run.ID).Scan(&runStatus)
	_ = pool.QueryRow(ctx, "SELECT status, completed_at IS NOT NULL FROM remaining_metric_partitions WHERE id=$1::uuid", partition).Scan(&partitionStatus, &exhausted)
	if runStatus != "running" || partitionStatus != "failed" || exhausted {
		t.Fatalf("run=%s partition=%s exhausted=%t, want running/failed/false (replacement's retry must stay reclaimable)", runStatus, partitionStatus, exhausted)
	}
	if again, err := store.ClaimPartition(ctx, partition); err != nil || again == nil {
		t.Fatalf("the replacement's retry cannot reclaim: %v, %v", again, err)
	}
}

// CHAOS-8177 (vetter-2 HOLD): the replacement crashed without a release and its lease expired. The OLD attempt's claimed
// fallback must still leave it alone (the partition is the replacement's, not the old claim's); only the claim-less
// path or the replacement's own outcome may end it.
func TestAnOldAttemptsClaimedFallbackLeavesACrashedReplacementsExpiredLeaseAlone(t *testing.T) {
	ctx := context.Background()
	pool, store, _ := newRemainingRedriveTestStack(t)
	clock := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return clock }
	org := "00000000-0000-4000-8000-000000009824"
	run, err := store.StartRun(ctx, StartRunRequest{OrganizationID: org, Family: "capacity", Generation: "replacement-crashed", ScopeKey: "all-teams",
		GenerationSeed: int64Pointer(824), Scopes: []json.RawMessage{capacityScopeJSON(90)}})
	if err != nil {
		t.Fatal(err)
	}
	partition := deterministicPartitionID(run.ID, 1)
	old, err := store.ClaimPartition(ctx, partition)
	if err != nil || old == nil {
		t.Fatalf("old claim = %v, %v", old, err)
	}
	clock = clock.Add(old.LeaseDuration + time.Minute)
	replacement, err := store.ClaimPartition(ctx, partition)
	if err != nil || replacement == nil {
		t.Fatalf("replacement claim = %v, %v", replacement, err)
	}
	clock = clock.Add(replacement.LeaseDuration + time.Minute) // the replacement dies; its lease expires
	if err := store.ExhaustClaimedPartition(ctx, *old); err != nil {
		t.Fatal(err)
	}
	var runStatus, partitionStatus, token string
	_ = pool.QueryRow(ctx, "SELECT status FROM remaining_metric_runs WHERE id=$1::uuid", run.ID).Scan(&runStatus)
	_ = pool.QueryRow(ctx, "SELECT status, COALESCE(claim_token::text,'') FROM remaining_metric_partitions WHERE id=$1::uuid", partition).Scan(&partitionStatus, &token)
	if runStatus != "running" || partitionStatus != "running" || token != replacement.Token {
		t.Fatalf("run=%s partition=%s token=%s, want running/running under the replacement's token %s", runStatus, partitionStatus, token, replacement.Token)
	}
	clock = clock.Add(time.Second)
	if again, err := store.ClaimPartition(ctx, partition); err != nil || again == nil {
		t.Fatalf("the crashed replacement's partition cannot be reclaimed: %v, %v", again, err)
	}
}
