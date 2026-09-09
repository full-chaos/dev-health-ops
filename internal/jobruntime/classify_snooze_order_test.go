package jobruntime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/riverqueue/river/rivertype"
)

// CHAOS-5455 regression suite.
//
// classify() used to consult the LIVE context -- errors.Is(ctx.Err(),
// context.Canceled) and its context.DeadlineExceeded sibling -- before it
// ever looked for a snooze marker. A handler that deliberately asked for a
// River snooze (RetryableAfter/BudgetContention/RateLimited) while the
// worker was draining therefore had that snooze silently replaced by the
// cancellation branch, whatever error it returned: no handler can influence
// a check made against a context it does not own.
//
// That is not a billing-only defect (it was found there, CHAOS-5399 r3) --
// it is every job kind's shutdown-retry semantics, because classify() is the
// single decision point for all of them (adapter.go:361/363/416/471/478).
//
// A snooze is strictly the SAFER of the two outcomes during a drain: like
// the cancellation branch it leaves the row retryable and non-terminal, but
// it also honours the delay the handler asked for and does not consume the
// job's bounded attempt budget. The ordering below is therefore: a
// deliberate snooze first, then the context branches exactly as before for
// everything else.

func TestClassifyHonoursASnoozeMarkerAheadOfTheLiveContext(t *testing.T) {
	t.Parallel()

	drained := func() context.Context {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx
	}
	expired := func() context.Context {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Hour))
		t.Cleanup(cancel)
		return ctx
	}

	// The exact shape internal/jobs/operational/billinghandler.go returns for
	// FenceOutcomeAmbiguous: a RetryableAfter whose delay is chosen to land
	// past the 15-minute stale-claim threshold, so the follow-up attempt
	// deterministically reaches the operator-visible staleness alert.
	billingAmbiguousSnooze := RetryableAfter(errors.New("resend API response uncertain"), 16*time.Minute)

	tests := []struct {
		name         string
		ctx          context.Context
		err          error
		attempt      int
		maxAttempts  int
		wantResult   Result
		wantCategory ErrorCategory
		wantSnooze   time.Duration
		wantCancel   bool
		wantReason   Reason
	}{
		{
			// The reported defect, on a real job kind's path
			// (operational.billing_notification, CHAOS-5399/CHAOS-5455).
			name: "billing ambiguous snooze survives a draining worker",
			ctx:  drained(), err: billingAmbiguousSnooze, attempt: 1, maxAttempts: 4,
			wantResult: ResultRetry, wantCategory: CategoryRetryable, wantSnooze: 16 * time.Minute,
		},
		{
			name: "billing ambiguous snooze survives a draining worker on the final attempt",
			ctx:  drained(), err: billingAmbiguousSnooze, attempt: 4, maxAttempts: 4,
			wantResult: ResultRetry, wantCategory: CategoryRetryable, wantSnooze: 16 * time.Minute,
		},
		{
			name: "budget contention snooze survives a draining worker",
			ctx:  drained(), err: BudgetContention(errors.New("provider budget contended"), 2*time.Second),
			attempt: 1, maxAttempts: 3,
			wantResult: ResultRetry, wantCategory: CategoryBudget, wantSnooze: 2 * time.Second,
		},
		{
			name: "rate-limit snooze survives a draining worker",
			ctx:  drained(), err: RateLimited(errors.New("provider 429"), 30*time.Minute),
			attempt: 1, maxAttempts: 3,
			wantResult: ResultRetry, wantCategory: CategoryRateLimited, wantSnooze: 30 * time.Minute,
		},
		{
			// The cause chain, not just the live context: an ambiguous
			// provider call that was itself cut short by the drain wraps
			// context.Canceled.
			name: "snooze marker wrapping a cancelled cause still snoozes",
			ctx:  context.Background(),
			err: RetryableAfter(fmt.Errorf("Post %q: %w",
				"https://api.resend.com/emails", context.Canceled), 16*time.Minute),
			attempt: 1, maxAttempts: 4,
			wantResult: ResultRetry, wantCategory: CategoryRetryable, wantSnooze: 16 * time.Minute,
		},
		{
			// The DeadlineExceeded sibling of the same ordering defect
			// (CHAOS-5399 r2). The handler works around it today by severing
			// the cause chain before returning; this makes the workaround
			// unnecessary rather than load-bearing.
			name: "snooze marker wrapping a deadline cause still snoozes",
			ctx:  context.Background(),
			err: RetryableAfter(fmt.Errorf("resend API response uncertain: %w",
				context.DeadlineExceeded), 16*time.Minute),
			attempt: 1, maxAttempts: 4,
			wantResult: ResultRetry, wantCategory: CategoryRetryable, wantSnooze: 16 * time.Minute,
		},
		{
			name: "snooze marker survives an expired context deadline",
			ctx:  expired(), err: billingAmbiguousSnooze, attempt: 1, maxAttempts: 4,
			wantResult: ResultRetry, wantCategory: CategoryRetryable, wantSnooze: 16 * time.Minute,
		},
		{
			// WithReason's own contract names RetryableAfter/BudgetContention/
			// RateLimited among the markers it decorates, so the bounded Reason
			// must survive the snooze branch -- including the branch's new
			// position ahead of the context checks. Without this case the
			// suite is a false negative: deleting `reason: marked.reason` from
			// classify's snooze return leaves every other assertion green
			// (r1 P3-1, proven by that exact mutation).
			name: "a reasoned snooze keeps its bounded reason while draining",
			ctx:  drained(),
			err: WithReason(RetryableAfter(errors.New("pids budget still contended"), 45*time.Second),
				ReasonCapacityExhausted),
			attempt: 1, maxAttempts: 3,
			wantResult: ResultRetry, wantCategory: CategoryRetryable, wantSnooze: 45 * time.Second,
			wantReason: ReasonCapacityExhausted,
		},
		{
			// The non-snooze marked path's reason must keep flowing too: the
			// fix split one `errors.As` block into two, and this is the half
			// that still runs after the context branches.
			name:    "a reasoned permanent failure keeps its reason on a live context",
			ctx:     context.Background(),
			err:     WithReason(Permanent(errors.New("scope is malformed")), ReasonInvalidState),
			attempt: 1, maxAttempts: 3,
			wantResult: ResultCancel, wantCategory: CategoryPermanent, wantCancel: true,
			wantReason: ReasonInvalidState,
		},

		// ------------------------------------------------------------------
		// Ordering that must NOT change. Only a deliberate snooze overtakes
		// the context branches; every other error keeps the behaviour it had
		// before CHAOS-5455.
		// ------------------------------------------------------------------
		{
			name: "an unmarked deadline error is still a timeout retry",
			ctx:  context.Background(), err: context.DeadlineExceeded, attempt: 1, maxAttempts: 3,
			wantResult: ResultRetry, wantCategory: CategoryTimeout,
		},
		{
			name: "an expired context still wins over a plain retryable marker",
			ctx:  expired(), err: Retryable(errors.New("transient")), attempt: 1, maxAttempts: 3,
			wantResult: ResultRetry, wantCategory: CategoryTimeout,
		},
		{
			name: "an expired context still discards a plain retryable on the final attempt",
			ctx:  expired(), err: Retryable(errors.New("transient")), attempt: 3, maxAttempts: 3,
			wantResult: ResultDiscard, wantCategory: CategoryTimeout,
		},
		{
			name: "a draining worker still cancels a plain retryable marker",
			ctx:  drained(), err: Retryable(errors.New("transient")), attempt: 1, maxAttempts: 3,
			wantResult: ResultCancel, wantCategory: CategoryCancelled,
		},
		{
			// A drain must never turn a job terminal: this is the branch's
			// whole reason for preceding the marker block, and a Permanent
			// marker is exactly the case that would flip to a durable
			// river.JobCancel if the marker block were simply hoisted above
			// it wholesale.
			name: "a draining worker still cancels non-terminally through a permanent marker",
			ctx:  drained(), err: Permanent(errors.New("deterministic")), attempt: 1, maxAttempts: 3,
			wantResult: ResultCancel, wantCategory: CategoryCancelled, wantCancel: false,
		},
		{
			name: "a draining worker still cancels an unmarked error",
			ctx:  drained(), err: errors.New("unclassified"), attempt: 1, maxAttempts: 3,
			wantResult: ResultCancel, wantCategory: CategoryCancelled,
		},
		{
			name: "a cancelled cause with no marker is still a cancellation",
			ctx:  context.Background(), err: fmt.Errorf("wrapped: %w", context.Canceled),
			attempt: 1, maxAttempts: 3,
			wantResult: ResultCancel, wantCategory: CategoryCancelled,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			choice := classify(test.ctx, test.err, test.attempt, test.maxAttempts)
			if choice.result != test.wantResult || choice.category != test.wantCategory {
				t.Fatalf("classify() = %s/%s, want %s/%s",
					choice.result, choice.category, test.wantResult, test.wantCategory)
			}
			if choice.snooze != test.wantSnooze {
				t.Fatalf("classify() snooze = %v, want %v -- a deliberate snooze request "+
					"must not be replaced by a decision made against the live context",
					choice.snooze, test.wantSnooze)
			}
			if choice.cancel != test.wantCancel {
				t.Fatalf("classify() cancel = %v, want %v", choice.cancel, test.wantCancel)
			}
			if choice.reason != test.wantReason {
				t.Fatalf("classify() reason = %q, want %q -- the bounded Reason must "+
					"survive whichever branch answered", choice.reason.String(), test.wantReason.String())
			}
			// A snoozed decision must reach River as a real JobSnoozeError,
			// not as a safe error or a JobCancel wrapper.
			transported := transportError(choice)
			var snoozeErr *rivertype.JobSnoozeError
			if errors.As(transported, &snoozeErr) != (test.wantSnooze > 0) {
				t.Fatalf("transportError(%+v) = %v, want a River snooze = %v",
					choice, transported, test.wantSnooze > 0)
			}
			if test.wantSnooze > 0 && snoozeErr.Duration != test.wantSnooze {
				t.Fatalf("River snooze duration = %v, want %v", snoozeErr.Duration, test.wantSnooze)
			}
		})
	}
}

// TestClassifyBudgetWaitStillCancelsOnADrainingWorker pins the deliberate
// asymmetry with classify(): classifyBudgetWait sees only budget.Acquire's
// own failure, which no handler ever marks -- there is no snooze request to
// honour there, so its context branches keep running first, unchanged.
func TestClassifyBudgetWaitStillCancelsOnADrainingWorker(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	choice := classifyBudgetWait(ctx, errors.New("budget wait failed"), 1, 3)
	if choice.result != ResultCancel || choice.category != CategoryCancelled {
		t.Fatalf("classifyBudgetWait() = %s/%s, want %s/%s",
			choice.result, choice.category, ResultCancel, CategoryCancelled)
	}
	if choice.snooze != 0 {
		t.Fatalf("classifyBudgetWait() snooze = %v, want 0", choice.snooze)
	}
}

// TestAdapterSnoozesADrainingJobInsteadOfCancellingIt drives the whole
// Adapter.Work path for a real job kind (retention_cleanup) with a worker
// context that is already done, and proves River receives the handler's
// snooze rather than a cancellation -- the end-to-end shape of CHAOS-5455.
// The attempt is deliberately the job's LAST: a snooze does not consume the
// bounded attempt budget, so draining on the final attempt must still
// schedule the follow-up rather than discard the work.
func TestAdapterSnoozesADrainingJobInsteadOfCancellingIt(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	observer := &recordingObserver{}
	claim := &recordingClaim{state: ClaimProceed}
	lease := &recordingLease{}
	adapter := newRetentionAdapter(t, HandlerFunc[RetentionCleanupArgs](
		func(context.Context, *Execution[RetentionCleanupArgs]) error {
			return RetryableAfter(errors.New("ambiguous downstream result"), 16*time.Minute)
		}), observer, claim, lease, &logs)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the worker is draining: a deploy/shutdown is in progress

	err := adapter.Work(ctx, retentionJob(t, 3))
	var snoozeErr *rivertype.JobSnoozeError
	if !errors.As(err, &snoozeErr) {
		t.Fatalf("Work() = %v, want a River snooze -- the handler's deliberate "+
			"RetryableAfter was replaced by the live context's cancellation", err)
	}
	if snoozeErr.Duration != 16*time.Minute {
		t.Fatalf("River snooze duration = %v, want %v", snoozeErr.Duration, 16*time.Minute)
	}
	if observer.result != ResultRetry || observer.category != CategoryRetryable {
		t.Fatalf("observed %s/%s, want %s/%s",
			observer.result, observer.category, ResultRetry, CategoryRetryable)
	}
	if observer.cancelled {
		t.Fatal("a snoozed job was observed as a cancellation")
	}
	if len(claim.completions) != 1 || claim.completions[0].Result != ResultRetry ||
		claim.completions[0].Terminal {
		t.Fatalf("claim completions: %+v, want one non-terminal retry", claim.completions)
	}
	if !lease.released {
		t.Fatal("budget lease was not released")
	}
}
