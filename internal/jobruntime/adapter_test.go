package jobruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

func TestAdapterMiddlewareOutcomesAreSafeAndDeterministic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		attempt         int
		parent          func() (context.Context, context.CancelFunc)
		claimState      ClaimState
		handler         HandlerFunc[RetentionCleanupArgs]
		wantResult      Result
		wantCategory    ErrorCategory
		wantTerminal    bool
		wantCancelError bool
		wantPanic       bool
		wantDomain      bool
	}{
		{
			name: "success", attempt: 1, claimState: ClaimProceed,
			handler: func(ctx context.Context, execution *Execution[RetentionCleanupArgs]) error {
				if correlation, ok := CorrelationID(ctx); !ok || correlation != execution.CorrelationID {
					t.Fatalf("correlation context missing: %q %v", correlation, ok)
				}
				return nil
			},
			wantResult: ResultSuccess, wantCategory: CategoryNone,
		},
		{
			name: "retry", attempt: 1, claimState: ClaimProceed,
			handler: func(context.Context, *Execution[RetentionCleanupArgs]) error {
				return Retryable(errors.New("credential=do-not-log"))
			},
			wantResult: ResultRetry, wantCategory: CategoryRetryable,
		},
		{
			name: "budget contention snooze at attempt ceiling", attempt: 3,
			claimState: ClaimProceed,
			handler: func(context.Context, *Execution[RetentionCleanupArgs]) error {
				return BudgetContention(errors.New("provider budget contended"), 1500*time.Millisecond)
			},
			wantResult: ResultRetry, wantCategory: CategoryBudget,
		},
		{
			name: "retryable contention snooze at attempt ceiling", attempt: 3,
			claimState: ClaimProceed,
			handler: func(context.Context, *Execution[RetentionCleanupArgs]) error {
				return RetryableAfter(errors.New("lease still active"), 2*time.Second)
			},
			wantResult: ResultRetry, wantCategory: CategoryRetryable,
		},
		{
			name: "discard", attempt: 3, claimState: ClaimProceed,
			handler: func(context.Context, *Execution[RetentionCleanupArgs]) error {
				return Retryable(errors.New("credential=do-not-log"))
			},
			wantResult: ResultDiscard, wantCategory: CategoryRetryable,
		},
		{
			name: "panic", attempt: 1, claimState: ClaimProceed,
			handler: func(context.Context, *Execution[RetentionCleanupArgs]) error {
				panic("panic-secret")
			},
			wantResult: ResultRetry, wantCategory: CategoryPanic, wantPanic: true,
		},
		{
			name: "timeout", attempt: 1, claimState: ClaimProceed,
			parent: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 10*time.Millisecond)
			},
			handler: func(ctx context.Context, _ *Execution[RetentionCleanupArgs]) error {
				<-ctx.Done()
				return ctx.Err()
			},
			wantResult: ResultRetry, wantCategory: CategoryTimeout,
		},
		{
			name: "cancel", attempt: 1, claimState: ClaimProceed,
			parent: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, func() {}
			},
			handler: func(ctx context.Context, _ *Execution[RetentionCleanupArgs]) error {
				return ctx.Err()
			},
			// A drain must leave the run retryable, never terminal.
			wantResult: ResultCancel, wantCategory: CategoryCancelled, wantTerminal: false,
		},
		{
			// CHAOS-3865: a drain or budget-lease loss landing between the
			// handler's successful return and classification must not rewrite
			// the outcome -- the work is already done.
			name: "success survives late cancellation", attempt: 1, claimState: ClaimProceed,
			parent: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, func() {}
			},
			handler: func(context.Context, *Execution[RetentionCleanupArgs]) error {
				return nil
			},
			wantResult: ResultSuccess, wantCategory: CategoryNone,
		},
		{
			name: "terminal domain", attempt: 1, claimState: ClaimTerminal,
			handler: func(context.Context, *Execution[RetentionCleanupArgs]) error {
				t.Fatal("terminal domain claim reached handler")
				return nil
			},
			wantResult: ResultCancel, wantCategory: CategoryTerminalDomain,
			wantCancelError: true, wantDomain: true,
		},
		{
			name: "duplicate checkpoint", attempt: 1, claimState: ClaimAlreadyComplete,
			handler: func(context.Context, *Execution[RetentionCleanupArgs]) error {
				t.Fatal("completed duplicate reached handler")
				return nil
			},
			wantResult: ResultDuplicate, wantCategory: CategoryNone,
		},
		{
			name: "unclassified failure cancels", attempt: 1, claimState: ClaimProceed,
			handler: func(context.Context, *Execution[RetentionCleanupArgs]) error {
				return errors.New("unclassified-secret")
			},
			wantResult: ResultCancel, wantCategory: CategoryPermanent, wantTerminal: true, wantCancelError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var logs bytes.Buffer
			observer := &recordingObserver{}
			claim := &recordingClaim{state: test.claimState}
			lease := &recordingLease{}
			adapter := newRetentionAdapter(t, test.handler, observer, claim, lease, &logs)
			job := retentionJob(t, test.attempt)
			ctx, cancel := context.WithCancel(context.Background())
			if test.parent != nil {
				ctx, cancel = test.parent()
			}
			defer cancel()

			err := adapter.Work(ctx, job)
			if test.wantResult == ResultSuccess || test.wantResult == ResultDuplicate {
				if err != nil {
					t.Fatalf("Work: %v", err)
				}
			} else if err == nil {
				t.Fatal("expected safe error")
			}
			var cancelErr *river.JobCancelError
			if errors.As(err, &cancelErr) != test.wantCancelError {
				t.Fatalf("cancel wrapper = %v, want %v (err=%v)", errors.As(err, &cancelErr), test.wantCancelError, err)
			}
			var snoozeErr *rivertype.JobSnoozeError
			wantSnooze := time.Duration(0)
			switch test.name {
			case "budget contention snooze at attempt ceiling":
				wantSnooze = 1500 * time.Millisecond
			case "retryable contention snooze at attempt ceiling":
				wantSnooze = 2 * time.Second
			}
			if wantSnooze > 0 {
				if !errors.As(err, &snoozeErr) || snoozeErr.Duration != wantSnooze {
					t.Fatalf("snooze error = %#v, want %s River snooze", err, wantSnooze)
				}
			} else if errors.As(err, &snoozeErr) {
				t.Fatalf("unexpected River snooze: %v", err)
			}
			if observer.result != test.wantResult || observer.category != test.wantCategory {
				t.Fatalf("observed %s/%s, want %s/%s", observer.result, observer.category, test.wantResult, test.wantCategory)
			}
			if observer.cancelled != (test.wantResult == ResultCancel) {
				t.Fatalf("cancellation observation = %v, result = %s", observer.cancelled, test.wantResult)
			}
			if observer.panicked != test.wantPanic || observer.domainMismatch != test.wantDomain {
				t.Fatalf("panic/domain observations = %v/%v", observer.panicked, observer.domainMismatch)
			}
			if test.claimState == ClaimProceed {
				if len(claim.completions) != 1 || claim.completions[0].Result != test.wantResult {
					t.Fatalf("claim completions: %+v", claim.completions)
				}
				if claim.completions[0].Terminal != test.wantTerminal {
					t.Fatalf("completion terminal = %v, want %v (%+v)",
						claim.completions[0].Terminal, test.wantTerminal, claim.completions[0])
				}
				if claim.finishContextErr != nil {
					t.Fatalf("claim finalized with cancelled context: %v", claim.finishContextErr)
				}
			} else if len(claim.completions) != 0 {
				t.Fatalf("non-proceed claim was completed: %+v", claim.completions)
			}
			if !lease.released {
				t.Fatal("budget lease was not released")
			}
			combined := logs.String()
			if err != nil {
				combined += err.Error()
			}
			for _, secret := range []string{"do-not-log", "panic-secret", "unclassified-secret"} {
				if strings.Contains(combined, secret) {
					t.Fatalf("secret leaked in logs/error: %s", combined)
				}
			}
		})
	}
}

// TestAdapterObservesJobWaitFromRiverScheduledAt is CHAOS-3118 evidence for
// worker_job_wait_seconds: it drives the real Adapter.Work() path (not a
// setter call) with a real *MetricsCollector as the Observer, then reads the
// collector's own Prometheus exposition to prove a non-zero series exists at
// the exact bucket a ~750ms wait belongs in.
func TestAdapterObservesJobWaitFromRiverScheduledAt(t *testing.T) {
	t.Parallel()
	registry, err := newRegistry(testContractRegistry(), testMigrationState())
	if err != nil {
		t.Fatalf("newRegistry: %v", err)
	}
	spec, ok := registry.Descriptor(jobcontract.KindRetentionCleanup)
	if !ok {
		t.Fatal("missing retention descriptor")
	}
	collector, err := NewMetricsCollector(MetricDimensions{
		Jobs: []JobLabels{{Queue: spec.Queue, Kind: spec.Kind}},
	})
	if err != nil {
		t.Fatalf("NewMetricsCollector: %v", err)
	}

	var logs bytes.Buffer
	claim := &recordingClaim{state: ClaimProceed}
	lease := &recordingLease{}
	handler := HandlerFunc[RetentionCleanupArgs](func(context.Context, *Execution[RetentionCleanupArgs]) error {
		return nil
	})
	adapter := newRetentionAdapter(t, handler, collector, claim, lease, &logs)

	job := retentionJob(t, 1)
	job.JobRow.ScheduledAt = time.Now().Add(-750 * time.Millisecond)

	if err := adapter.Work(context.Background(), job); err != nil {
		t.Fatalf("Work: %v", err)
	}

	text := collector.PrometheusText()
	labels := `queue="` + spec.Queue + `",kind="` + spec.Kind + `"`
	if !strings.Contains(text, "worker_job_wait_seconds_count{"+labels+"} 1") {
		t.Fatalf("expected a non-zero worker_job_wait_seconds series, got:\n%s", text)
	}
	// A near-zero rounding artifact would land under le="0.5"; the real
	// ~750ms wait must not.
	if strings.Contains(text, `worker_job_wait_seconds_bucket{`+labels+`,le="0.5"} 1`) {
		t.Fatalf("wait was bucketed under 0.5s, want at least 0.5s:\n%s", text)
	}
	if !strings.Contains(text, `worker_job_wait_seconds_bucket{`+labels+`,le="1"} 1`) {
		t.Fatalf("wait was not bucketed at 1s as expected:\n%s", text)
	}
}

func TestAdapterRejectsRawContractAndExecutionPolicyDrift(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*river.Job[RetentionCleanupArgs])
	}{
		{
			name: "unknown encoded field",
			mutate: func(job *river.Job[RetentionCleanupArgs]) {
				var value map[string]any
				if err := json.Unmarshal(job.EncodedArgs, &value); err != nil {
					t.Fatal(err)
				}
				value["credential"] = "raw-secret"
				job.EncodedArgs, _ = json.Marshal(value)
			},
		},
		{name: "wrong queue", mutate: func(job *river.Job[RetentionCleanupArgs]) { job.Queue = "other" }},
		{name: "attempt policy reduced", mutate: func(job *river.Job[RetentionCleanupArgs]) { job.MaxAttempts = 2 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var logs bytes.Buffer
			called := false
			observer := &recordingObserver{}
			adapter := newRetentionAdapter(t, HandlerFunc[RetentionCleanupArgs](func(context.Context, *Execution[RetentionCleanupArgs]) error {
				called = true
				return nil
			}), observer, &recordingClaim{state: ClaimProceed}, &recordingLease{}, &logs)
			job := retentionJob(t, 1)
			test.mutate(job)
			err := adapter.Work(context.Background(), job)
			var cancelErr *river.JobCancelError
			if err == nil || !errors.As(err, &cancelErr) || observer.category != CategoryValidation {
				t.Fatalf("expected validation cancellation, got err=%v category=%s", err, observer.category)
			}
			if called {
				t.Fatal("invalid job reached handler")
			}
			if strings.Contains(logs.String()+err.Error(), "raw-secret") {
				t.Fatal("encoded argument leaked")
			}
		})
	}
}

func TestAdapterAllowsAuditedManualRetryAttemptCeiling(t *testing.T) {
	t.Parallel()
	observer := &recordingObserver{}
	adapter := newRetentionAdapter(t, HandlerFunc[RetentionCleanupArgs](func(context.Context, *Execution[RetentionCleanupArgs]) error {
		return nil
	}), observer, &recordingClaim{state: ClaimProceed}, &recordingLease{}, &bytes.Buffer{})
	job := retentionJob(t, 4)
	job.MaxAttempts = 4
	if err := adapter.Work(context.Background(), job); err != nil {
		t.Fatalf("manual retry: %v", err)
	}
}

func TestAdapterNextRetryIsBoundedAndDeterministic(t *testing.T) {
	t.Parallel()
	adapter := newRetentionAdapter(t, HandlerFunc[RetentionCleanupArgs](func(context.Context, *Execution[RetentionCleanupArgs]) error { return nil }), &recordingObserver{}, &recordingClaim{state: ClaimProceed}, &recordingLease{}, &bytes.Buffer{})
	job := retentionJob(t, 2)
	attempted := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	job.AttemptedAt = &attempted
	first := adapter.NextRetry(job)
	second := adapter.NextRetry(job)
	if !first.Equal(second) {
		t.Fatalf("retry time is not deterministic: %v != %v", first, second)
	}
	delay := first.Sub(attempted)
	if delay < 9*time.Second || delay > 11*time.Second {
		t.Fatalf("retry delay outside jitter bound: %v", delay)
	}
}

func newRetentionAdapter(t *testing.T, handler Handler[RetentionCleanupArgs], observer Observer, claim IdempotencyClaim, lease BudgetLease, logs *bytes.Buffer) *Adapter[RetentionCleanupArgs] {
	t.Helper()
	registry, err := newRegistry(testContractRegistry(), testMigrationState())
	if err != nil {
		t.Fatalf("newRegistry: %v", err)
	}
	spec, _ := registry.Descriptor(jobcontract.KindRetentionCleanup)
	adapter, err := NewAdapter(registry, spec, handler, Dependencies{
		Logger:   slog.New(slog.NewJSONHandler(logs, nil)),
		Observer: observer,
		TenantScope: tenantScopeFunc(func(ctx context.Context, _ ScopeRequest) (context.Context, error) {
			return ctx, nil
		}),
		Budget: budgetFunc(func(context.Context, BudgetRequest) (BudgetLease, error) {
			return lease, nil
		}),
		Idempotency: idempotencyFunc(func(context.Context, ClaimRequest) (IdempotencyClaim, error) {
			return claim, nil
		}),
	})
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	return adapter
}

func retentionJob(t *testing.T, attempt int) *river.Job[RetentionCleanupArgs] {
	t.Helper()
	envelope := jobcontract.Envelope{
		ContractVersion: 1,
		CorrelationID:   "corr-test-1",
		IdempotencyKey:  "retention:2026-07-21",
		Domain: jobcontract.DomainLink{
			Type: "maintenance_run",
			ID:   "11111111-1111-4111-8111-111111111111",
		},
		Payload: jobcontract.RetentionCleanupPayload{
			BatchSize:       100,
			DeleteBefore:    "2026-07-01T00:00:00Z",
			RetentionPolicy: jobcontract.RetentionWorkerTerminal,
		},
	}
	raw, err := jobcontract.MarshalCanonical(envelope)
	if err != nil {
		t.Fatalf("MarshalCanonical: %v", err)
	}
	typed := RetentionCleanupArgs{EnvelopeArgs: EnvelopeArgs[jobcontract.RetentionCleanupPayload]{
		ContractVersion: envelope.ContractVersion,
		CorrelationID:   envelope.CorrelationID,
		IdempotencyKey:  envelope.IdempotencyKey,
		Domain:          envelope.Domain,
		Payload:         envelope.Payload.(jobcontract.RetentionCleanupPayload),
	}}
	return &river.Job[RetentionCleanupArgs]{
		JobRow: &rivertype.JobRow{
			ID: 42, Attempt: attempt, CreatedAt: time.Now().UTC(), EncodedArgs: raw,
			Kind: jobcontract.KindRetentionCleanup, MaxAttempts: 3, Priority: 3,
			Queue: "retention", ScheduledAt: time.Now().UTC(), State: rivertype.JobStateRunning,
		},
		Args: typed,
	}
}

type tenantScopeFunc func(context.Context, ScopeRequest) (context.Context, error)

func (tenantScopeFunc) Supports(string) bool { return true }

func (function tenantScopeFunc) Resolve(ctx context.Context, request ScopeRequest) (context.Context, error) {
	return function(ctx, request)
}

type budgetFunc func(context.Context, BudgetRequest) (BudgetLease, error)

func (budgetFunc) Supports(string, int) bool { return true }

func (function budgetFunc) Acquire(ctx context.Context, request BudgetRequest) (BudgetLease, error) {
	return function(ctx, request)
}

type idempotencyFunc func(context.Context, ClaimRequest) (IdempotencyClaim, error)

func (idempotencyFunc) Supports(string) bool { return true }

func (function idempotencyFunc) Begin(ctx context.Context, request ClaimRequest) (IdempotencyClaim, error) {
	return function(ctx, request)
}

type recordingLease struct{ released bool }

func (lease *recordingLease) Release() { lease.released = true }

type recordingClaim struct {
	state            ClaimState
	completions      []Completion
	finishContextErr error
}

func (claim *recordingClaim) State() ClaimState { return claim.state }
func (claim *recordingClaim) Finish(ctx context.Context, completion Completion) error {
	claim.completions = append(claim.completions, completion)
	claim.finishContextErr = ctx.Err()
	return nil
}

type recordingObserver struct {
	result         Result
	category       ErrorCategory
	panicked       bool
	domainMismatch bool
	cancelled      bool
	jobWaits       []time.Duration
}

func (*recordingObserver) RuntimeRegistered(context.Context, RuntimeInfo) {}
func (*recordingObserver) JobStarted(context.Context, JobLabels)          {}
func (observer *recordingObserver) JobFinished(_ context.Context, _ JobLabels, result Result, category ErrorCategory, _ time.Duration) {
	observer.result, observer.category = result, category
}
func (observer *recordingObserver) JobPanicked(context.Context, JobLabels) { observer.panicked = true }
func (observer *recordingObserver) JobCancelled(context.Context, JobLabels, ErrorCategory) {
	observer.cancelled = true
}
func (observer *recordingObserver) DomainMismatch(context.Context, string) {
	observer.domainMismatch = true
}
func (*recordingObserver) BudgetWait(context.Context, JobLabels, time.Duration, string) {}
func (observer *recordingObserver) JobWait(_ context.Context, _ JobLabels, wait time.Duration) {
	observer.jobWaits = append(observer.jobWaits, wait)
}

// TestAdapterAcceptsEnvelopeCarryingTraceParent is the CHAOS-4093 regression.
//
// CHAOS-3993 added trace_parent to the wire envelope and jobcontract.Decode
// populates jobcontract.Envelope.TraceParent from it. EnvelopeArgs -- the
// typed River form every kind embeds -- did not carry the field, so
// ContractEnvelope() always returned an empty TraceParent while the decoded
// envelope carried the real one. Adapter.execute compares the two with
// reflect.DeepEqual and cancels on drift, so in production EVERY job whose
// producer captured a traceparent was cancelled with error_category
// "validation" at attempt 1, terminally, before the handler or the
// idempotency claim ran. For sync.provider_unit that stranded the unit in
// 'dispatching' forever: the outbox row was already 'delivered', so the
// unreclaimable sweep's published-outbox filter skipped it, and the pair was
// routable, so its routability filter skipped it too.
//
// The job is built the way River builds one: the typed args are unmarshalled
// from the SAME canonical bytes that land in encoded_args.
func TestAdapterAcceptsEnvelopeCarryingTraceParent(t *testing.T) {
	t.Parallel()

	const traceParent = "00-9edef9ef79314c405d4cb812ba493826-46eef2b618f33ca0-03"
	envelope := jobcontract.Envelope{
		ContractVersion: 1,
		CorrelationID:   "corr-test-1",
		IdempotencyKey:  "retention:2026-07-21",
		TraceParent:     traceParent,
		Domain: jobcontract.DomainLink{
			Type: "maintenance_run",
			ID:   "11111111-1111-4111-8111-111111111111",
		},
		Payload: jobcontract.RetentionCleanupPayload{
			BatchSize:       100,
			DeleteBefore:    "2026-07-01T00:00:00Z",
			RetentionPolicy: jobcontract.RetentionWorkerTerminal,
		},
	}
	raw, err := jobcontract.MarshalCanonical(envelope)
	if err != nil {
		t.Fatalf("MarshalCanonical: %v", err)
	}
	var typed RetentionCleanupArgs
	if err := json.Unmarshal(raw, &typed); err != nil {
		t.Fatalf("unmarshal typed args: %v", err)
	}
	if got := typed.ContractEnvelope().TraceParent; got != traceParent {
		t.Fatalf("ContractEnvelope().TraceParent = %q, want %q", got, traceParent)
	}

	reached := false
	var logs bytes.Buffer
	observer := &recordingObserver{}
	adapter := newRetentionAdapter(t, HandlerFunc[RetentionCleanupArgs](func(context.Context, *Execution[RetentionCleanupArgs]) error {
		reached = true
		return nil
	}), observer, &recordingClaim{state: ClaimProceed}, &recordingLease{}, &logs)
	job := &river.Job[RetentionCleanupArgs]{
		JobRow: &rivertype.JobRow{
			ID: 42, Attempt: 1, CreatedAt: time.Now().UTC(), EncodedArgs: raw,
			Kind: jobcontract.KindRetentionCleanup, MaxAttempts: 3, Priority: 3,
			Queue: "retention", ScheduledAt: time.Now().UTC(), State: rivertype.JobStateRunning,
		},
		Args: typed,
	}
	if err := adapter.Work(context.Background(), job); err != nil {
		t.Fatalf("Work rejected an envelope carrying trace_parent: %v", err)
	}
	if !reached {
		t.Fatal("handler never ran for an envelope carrying trace_parent")
	}
	if observer.result != ResultSuccess || observer.category != CategoryNone {
		t.Fatalf("observed %s/%s, want success/none", observer.result, observer.category)
	}
}
