//go:build integration

package jobruntime

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresIdempotencyPreservesDuplicateAndCrashRecoverySemantics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := instance.Close(context.Background()); err != nil {
			t.Errorf("close PostgreSQL: %v", err)
		}
	}()

	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createIdempotencyTable(t, ctx, pool)

	store, err := NewPostgresIdempotency(pool, nil)
	if err != nil {
		t.Fatal(err)
	}
	store.leaseDuration = time.Minute
	request := idempotencyRequest("retention:worker_job_terminal:2026-07-14")

	first, err := store.Begin(ctx, request)
	if err != nil || first.State() != ClaimProceed {
		t.Fatalf("first Begin = %v, %v", first, err)
	}
	duplicate, err := store.Begin(ctx, request)
	if err != nil || duplicate.State() != ClaimAlreadyComplete {
		t.Fatalf("duplicate Begin = %v, %v", duplicate, err)
	}

	// A process can die after claiming but before completion. The later River
	// attempt may reclaim only after the persisted lease, never concurrently.
	if _, err := pool.Exec(ctx, "UPDATE public.worker_job_runs SET lease_expires_at = statement_timestamp() - interval '1 second'"); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := store.Begin(ctx, request)
	if err != nil || reclaimed.State() != ClaimProceed {
		t.Fatalf("reclaimed Begin = %v, %v", reclaimed, err)
	}
	if err := reclaimed.Finish(ctx, Completion{Result: ResultSuccess, Category: CategoryNone}); err != nil {
		t.Fatalf("finish reclaimed claim: %v", err)
	}
	completed, err := store.Begin(ctx, request)
	if err != nil || completed.State() != ClaimAlreadyComplete {
		t.Fatalf("completed Begin = %v, %v", completed, err)
	}

	retryRequest := idempotencyRequest("retention:worker_job_terminal:2026-07-15")
	retrying, err := store.Begin(ctx, retryRequest)
	if err != nil || retrying.State() != ClaimProceed {
		t.Fatalf("retrying Begin = %v, %v", retrying, err)
	}
	if err := retrying.Finish(ctx, Completion{Result: ResultRetry, Category: CategoryRetryable}); err != nil {
		t.Fatalf("finish retryable claim: %v", err)
	}
	nextAttempt, err := store.Begin(ctx, retryRequest)
	if err != nil || nextAttempt.State() != ClaimProceed {
		t.Fatalf("next retry Begin = %v, %v", nextAttempt, err)
	}
	// A drain or budget-lease loss also arrives as ResultCancel but is not
	// terminal: the run must stay reclaimable so River's retry can execute it
	// (CHAOS-3865). classify() sets Terminal from the same cancel flag.
	if err := nextAttempt.Finish(ctx, Completion{Result: ResultCancel, Category: CategoryCancelled}); err != nil {
		t.Fatalf("finish drained claim: %v", err)
	}
	afterDrain, err := store.Begin(ctx, retryRequest)
	if err != nil || afterDrain.State() != ClaimProceed {
		t.Fatalf("post-drain Begin = %v, %v", afterDrain, err)
	}
	// An explicit domain-terminal outcome still fences every later claim.
	if err := afterDrain.Finish(ctx, Completion{
		Result: ResultCancel, Category: CategoryPermanent, Terminal: true,
	}); err != nil {
		t.Fatalf("finish terminal claim: %v", err)
	}
	terminal, err := store.Begin(ctx, retryRequest)
	if err != nil || terminal.State() != ClaimTerminal {
		t.Fatalf("terminal Begin = %v, %v", terminal, err)
	}
}

func createIdempotencyTable(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
		CREATE TABLE public.worker_job_runs (
			id uuid PRIMARY KEY,
			job_kind text NOT NULL,
			idempotency_key text NOT NULL,
			org_id uuid NULL,
			domain_type text NOT NULL,
			domain_id uuid NOT NULL,
			status text NOT NULL,
			claim_token uuid NULL,
			lease_expires_at timestamptz NULL,
			attempt_count integer NOT NULL,
			started_at timestamptz NOT NULL,
			finished_at timestamptz NULL,
			result text NULL,
			error_category text NULL,
			created_at timestamptz NOT NULL,
			updated_at timestamptz NOT NULL,
			UNIQUE (job_kind, idempotency_key)
		)`)
	if err != nil {
		t.Fatal(err)
	}
}

func idempotencyRequest(key string) ClaimRequest {
	return ClaimRequest{
		Kind:           jobcontract.KindRetentionCleanup,
		IdempotencyKey: key,
		Domain: jobcontract.DomainLink{
			Type: "maintenance_run",
			ID:   "00000000-0000-4000-8000-000000000002",
		},
		Policy:  "maintenance_run_checkpoint",
		JobID:   42,
		Attempt: 1,
	}
}

// TestPostgresIdempotencyRenewsLeaseWhileTheRunIsHealthy is CHAOS-3866
// evidence. The lease (10 minutes in production) is shorter than registered
// timeouts (up to 2 hours), and Begin's running-with-expired-lease branch
// hands a duplicate a concurrent claim on the same run -- the double execution
// this store exists to fence. Renewal must keep a healthy run's lease ahead of
// the clock for as long as it executes.
func TestPostgresIdempotencyRenewsLeaseWhileTheRunIsHealthy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := instance.Close(context.Background()); err != nil {
			t.Errorf("close PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createIdempotencyTable(t, ctx, pool)

	store, err := NewPostgresIdempotency(pool, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Renewal ticks every leaseDuration/3, so the run outlives its original
	// lease several times over during this test.
	store.leaseDuration = 1500 * time.Millisecond
	request := idempotencyRequest("retention:worker_job_long_run:2026-07-16")

	claim, err := store.Begin(ctx, request)
	if err != nil || claim.State() != ClaimProceed {
		t.Fatalf("Begin = %v, %v", claim, err)
	}

	// Well past the original lease: without renewal the row would now be
	// claimable by a duplicate running concurrently with this one.
	time.Sleep(3 * store.leaseDuration)

	var leaseAhead bool
	if err := pool.QueryRow(ctx,
		`SELECT lease_expires_at > statement_timestamp() FROM public.worker_job_runs`,
	).Scan(&leaseAhead); err != nil {
		t.Fatal(err)
	}
	if !leaseAhead {
		t.Fatal("lease was not renewed while the run was still executing")
	}
	duplicate, err := store.Begin(ctx, request)
	if err != nil || duplicate.State() != ClaimAlreadyComplete {
		t.Fatalf("duplicate claimed a healthy long-running job: %v, %v", duplicate, err)
	}
	if err := claim.Finish(ctx, Completion{Result: ResultSuccess, Category: CategoryNone}); err != nil {
		t.Fatalf("finish renewed claim: %v", err)
	}
}

// TestPostgresIdempotencyRetiresRenewalLoudlyWhenRenewalKeepsFailing drives the
// terminal branch of postgresClaim.renew with a real fault rather than a tuned
// sleep: the pool the renewal goroutine uses is closed, so every sub-second
// renewal Exec from that point on errors, which is the shape a database blip
// outlasting one lease takes. Roughly three consecutive failed ticks later the
// renewal goroutine gives up.
//
// The assertions are the two outcomes that matter to a worker: it LEARNS the
// lease is gone, and the loss is real -- durable state has moved on far enough
// that a second worker may legitimately claim the same job, which is the
// duplicate execution this store exists to fence.
func TestPostgresIdempotencyRetiresRenewalLoudlyWhenRenewalKeepsFailing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := instance.Close(context.Background()); err != nil {
			t.Errorf("close PostgreSQL: %v", err)
		}
	}()

	// Two pools on one container: the store renews through renewPool, which
	// the test kills, while verifyPool stays live to read durable truth.
	renewPool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	verifyPool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer verifyPool.Close()
	createIdempotencyTable(t, ctx, verifyPool)

	const lease = 1500 * time.Millisecond
	retirements := &recordingRetirements{}
	store, err := NewPostgresIdempotency(renewPool, retirements)
	if err != nil {
		t.Fatal(err)
	}
	store.leaseDuration = lease
	request := idempotencyRequest("retention:worker_job_renewal_retired:2026-08-23")

	claim, err := store.Begin(ctx, request)
	if err != nil || claim.State() != ClaimProceed {
		t.Fatalf("Begin = %v, %v", claim, err)
	}

	// A handler context wired the way the adapter wires it, so the assertion
	// below is what a running handler actually observes.
	handlerContext, cancelLoss := withIdempotencyClaimLoss(context.Background(), claim)
	defer cancelLoss()

	renewPool.Close()

	lost, ok := claim.(interface{ Lost() <-chan struct{} })
	if !ok || lost.Lost() == nil {
		t.Fatal("claim exposes no lost signal: renewal can retire while the handler runs on, and nothing tells the worker")
	}
	select {
	case <-lost.Lost():
	case <-time.After(10 * lease):
		t.Fatal("renewal retired without signaling that the lease was lost")
	}
	select {
	case <-handlerContext.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("handler context was not canceled after the lease was lost")
	}
	if got := retirements.reason(); got != IdempotencyRenewalTransientExhausted {
		t.Fatalf("retirement reason = %q, want %q", got, IdempotencyRenewalTransientExhausted)
	}

	// The renewal goroutine really is gone, not merely quiet.
	if internal, ok := claim.(*postgresClaim); ok {
		select {
		case <-internal.renewalDone:
		case <-time.After(10 * time.Second):
			t.Fatal("renewal goroutine still running after signaling loss")
		}
	}

	// Durable truth agrees the lease lapsed. This is the honest outcome, not a
	// defect: the lease IS gone. What changed is that the first worker now
	// knows and has stopped, so the duplicate below is a takeover rather than
	// a second worker running concurrently with a handler that never noticed.
	var leaseAhead bool
	if err := verifyPool.QueryRow(ctx,
		`SELECT lease_expires_at > statement_timestamp() FROM public.worker_job_runs`,
	).Scan(&leaseAhead); err != nil {
		t.Fatal(err)
	}
	if leaseAhead {
		t.Fatal("loss was signaled while the lease was still live")
	}

	time.Sleep(2 * lease)
	duplicateStore, err := NewPostgresIdempotency(verifyPool, nil)
	if err != nil {
		t.Fatal(err)
	}
	duplicateStore.leaseDuration = lease
	duplicate, err := duplicateStore.Begin(ctx, request)
	if err != nil || duplicate.State() != ClaimProceed {
		t.Fatalf("duplicate could not take over the abandoned run: %v, %v", duplicate, err)
	}
	if err := duplicate.Finish(ctx, Completion{Result: ResultSuccess, Category: CategoryNone}); err != nil {
		t.Fatalf("finish duplicate claim: %v", err)
	}
}

// recordingRetirements captures the bounded reason the renewal goroutine
// reported, so the test asserts the exported signal an alert would bind to and
// not merely that renewal stopped.
type recordingRetirements struct {
	mu      sync.Mutex
	reasons []IdempotencyRenewalRetiredReason
}

func (recorder *recordingRetirements) ObserveIdempotencyRenewalRetired(
	reason IdempotencyRenewalRetiredReason,
) error {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.reasons = append(recorder.reasons, reason)
	return nil
}

func (recorder *recordingRetirements) all() []IdempotencyRenewalRetiredReason {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return append([]IdempotencyRenewalRetiredReason(nil), recorder.reasons...)
}

func (recorder *recordingRetirements) reason() IdempotencyRenewalRetiredReason {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if len(recorder.reasons) != 1 {
		return IdempotencyRenewalRetiredReason(fmt.Sprintf("%d retirements", len(recorder.reasons)))
	}
	return recorder.reasons[0]
}

// TestPostgresIdempotencyRetiresRenewalLoudlyWhenFencedOut covers the OTHER
// non-transient arm: the renewal UPDATE succeeds as a query but matches zero
// rows, which is proof the run is no longer ours. The database is healthy
// throughout, so nothing here is a blip -- a second claimant simply took the
// row. The original claimant must still learn and stop, and the reason must be
// distinguishable from a database outage, because the two demand opposite
// operator responses.
func TestPostgresIdempotencyRetiresRenewalLoudlyWhenFencedOut(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := instance.Close(context.Background()); err != nil {
			t.Errorf("close PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createIdempotencyTable(t, ctx, pool)

	const lease = 1500 * time.Millisecond
	retirements := &recordingRetirements{}
	store, err := NewPostgresIdempotency(pool, retirements)
	if err != nil {
		t.Fatal(err)
	}
	store.leaseDuration = lease
	request := idempotencyRequest("retention:worker_job_fenced:2026-08-23")

	claim, err := store.Begin(ctx, request)
	if err != nil || claim.State() != ClaimProceed {
		t.Fatalf("Begin = %v, %v", claim, err)
	}
	handlerContext, cancelLoss := withIdempotencyClaimLoss(context.Background(), claim)
	defer cancelLoss()

	// Fence the claim the way a real takeover does: rotate the claim token so
	// the renewal UPDATE's token predicate stops matching.
	if _, err := pool.Exec(ctx,
		`UPDATE public.worker_job_runs SET claim_token = $1`, uuid.New()); err != nil {
		t.Fatal(err)
	}

	lost, ok := claim.(interface{ Lost() <-chan struct{} })
	if !ok || lost.Lost() == nil {
		t.Fatal("claim exposes no lost signal after being fenced out")
	}
	select {
	case <-lost.Lost():
	case <-time.After(10 * lease):
		t.Fatal("fenced claim never learned it had lost the run")
	}
	select {
	case <-handlerContext.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("handler context was not canceled after the claim was fenced out")
	}
	if got := retirements.reason(); got != IdempotencyRenewalFenced {
		t.Fatalf("retirement reason = %q, want %q", got, IdempotencyRenewalFenced)
	}
}

// TestPostgresIdempotencyOrdinaryCompletionIsSilent is the counter-weight to the
// two retirement tests. Loud retirement is only an improvement if it stays
// quiet when nothing is wrong: a loss signal on an ordinary Finish would cancel
// a healthy handler, and a retirement counted on every completion would bury
// the real signal under normal traffic. Cancelling on a false positive is the
// CHAOS-3866 regression, which is worse than the hole this change closes.
func TestPostgresIdempotencyOrdinaryCompletionIsSilent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := instance.Close(context.Background()); err != nil {
			t.Errorf("close PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createIdempotencyTable(t, ctx, pool)

	const lease = 1500 * time.Millisecond
	retirements := &recordingRetirements{}
	store, err := NewPostgresIdempotency(pool, retirements)
	if err != nil {
		t.Fatal(err)
	}
	store.leaseDuration = lease
	request := idempotencyRequest("retention:worker_job_quiet_finish:2026-08-23")

	claim, err := store.Begin(ctx, request)
	if err != nil || claim.State() != ClaimProceed {
		t.Fatalf("Begin = %v, %v", claim, err)
	}
	handlerContext, cancelLoss := withIdempotencyClaimLoss(context.Background(), claim)
	defer cancelLoss()

	// Long enough that renewal ticks several times against a healthy database,
	// then finish the way a successful handler does.
	time.Sleep(2 * lease)
	if err := claim.Finish(ctx, Completion{Result: ResultSuccess, Category: CategoryNone}); err != nil {
		t.Fatalf("finish healthy claim: %v", err)
	}

	lost, ok := claim.(interface{ Lost() <-chan struct{} })
	if !ok {
		t.Fatal("claim lost the lost-signal capability")
	}
	select {
	case <-lost.Lost():
		t.Fatal("an ordinary completion signaled lease loss: a healthy handler would have been canceled")
	default:
	}
	select {
	case <-handlerContext.Done():
		t.Fatal("an ordinary completion canceled the handler context")
	default:
	}
	if got := len(retirements.all()); got != 0 {
		t.Fatalf("ordinary completion recorded %d retirements, want 0", got)
	}
}
