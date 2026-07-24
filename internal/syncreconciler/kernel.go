package syncreconciler

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	minimumLeaseDuration = time.Second
	maximumLeaseDuration = 15 * time.Minute
)

var (
	// ErrLeaseLost means the persisted route or claim changed before this
	// transaction's terminal write. The transaction is rolled back so the work
	// remains retryable under its current owner.
	ErrLeaseLost = errors.New("sync dispatch transport lease lost")
	// ErrPublisherRequired means mutation reached an at-least-once claim without
	// an injected same-transaction River publisher.
	ErrPublisherRequired = errors.New("sync dispatch transport publisher required")
)

// KernelMode selects a strictly bounded reconciler behavior. Shadow is the
// only mode suitable for the current runtime. Mutation is intentionally
// unreferenced until a future cutover has an audited activation path.
type KernelMode string

const (
	KernelModeShadow   KernelMode = "shadow"
	KernelModeMutation KernelMode = "mutation"
)

// TransportClaim is the minimum non-sensitive state a transport publisher
// needs. The claim token and route generation are persisted before delivery.
type TransportClaim struct {
	ID              string
	Kind            string
	ClaimToken      string
	RouteGeneration int64
	AvailableAt     time.Time
	Attempts        int64
}

// AtLeastOncePublisher inserts one River job using the supplied fresh
// transaction. The returned identifier is optional audit metadata. Returning
// an error rolls back the River insert; the already committed claim is then
// released through the bounded persisted failure recorder.
type AtLeastOncePublisher func(context.Context, pgx.Tx, TransportClaim) (string, error)

// PostSyncHandoff is retained as an inert source-compatibility parameter for
// dormant compositions. All post_sync delivery goes through AtLeastOncePublisher
// and this callback has no dispatch, retry, or readiness effect.
type PostSyncHandoff func(context.Context, TransportClaim) error

// KernelResult is bounded to one requested claim window. Shadow includes the
// existing read-only observation; mutation reports only aggregate counts.
type KernelResult struct {
	Mode        KernelMode
	Observation Observation
	Claimed     int
	Dispatched  int
	Retried     int
	LeaseLost   int
}

type beginFunc func(context.Context) (pgx.Tx, error)

// Kernel keeps a dormant mutation implementation beside the observer without
// coupling it to command wiring. A future activation must explicitly construct
// it in KernelModeMutation and provide the delivery seams to Step.
type Kernel struct {
	mode        KernelMode
	observer    Stepper
	begin       beginFunc
	failures    *PublishFailureRecorder
	descriptors map[string]syncdispatchcontract.Descriptor
	riverKinds  []string
}

// NewKernel constructs an unactivated kernel across the two runtime database
// trust boundaries. Observation stays on the domain pool; mutation begins only
// on the least-privilege queue-control pool. No transaction or route mutation
// occurs during construction.
func NewKernel(
	domainPool *pgxpool.Pool,
	queueControlPool *pgxpool.Pool,
	registry Registry,
	mode KernelMode,
) (*Kernel, error) {
	if domainPool == nil || (mode == KernelModeMutation && queueControlPool == nil) {
		return nil, ErrInvalidConfiguration
	}
	observer, err := NewObserver(domainPool, registry)
	if err != nil {
		return nil, err
	}
	var begin beginFunc
	if queueControlPool != nil {
		begin = queueControlPool.Begin
	}
	return newKernel(registry, mode, observer, begin)
}

func newKernel(
	registry Registry,
	mode KernelMode,
	observer Stepper,
	begin beginFunc,
) (*Kernel, error) {
	descriptors, err := fixedDescriptors(registry)
	if err != nil || observer == nil || (mode != KernelModeShadow && mode != KernelModeMutation) ||
		(mode == KernelModeMutation && begin == nil) {
		return nil, ErrInvalidConfiguration
	}
	kernel := &Kernel{
		mode:        mode,
		observer:    observer,
		begin:       begin,
		descriptors: make(map[string]syncdispatchcontract.Descriptor, len(descriptors)),
	}
	if mode == KernelModeMutation {
		failures, failureErr := newPublishFailureRecorder(func(ctx context.Context) (failureTransaction, error) {
			return begin(ctx)
		})
		if failureErr != nil {
			return nil, ErrInvalidConfiguration
		}
		kernel.failures = failures
	}
	for _, descriptor := range descriptors {
		kernel.descriptors[descriptor.Kind] = descriptor
		if descriptor.Route == syncdispatchcontract.RouteRiver {
			kernel.riverKinds = append(kernel.riverKinds, descriptor.Kind)
		}
	}
	sort.Strings(kernel.riverKinds)
	return kernel, nil
}

// Step runs exactly one bounded unit of work. Shadow delegates to the existing
// read-only observer and never begins a transaction. Mutation first commits one
// bounded set of River claims. Each at-least-once claim is then inserted and
// terminally marked in its own fresh transaction, including post_sync.
func (kernel *Kernel) Step(
	ctx context.Context,
	now time.Time,
	limit int,
	leaseDuration time.Duration,
	publish AtLeastOncePublisher,
	postSyncHandoff PostSyncHandoff,
) (KernelResult, error) {
	if kernel == nil || kernel.observer == nil || ctx == nil || now.IsZero() ||
		limit < minimumStepLimit || limit > maximumStepLimit ||
		leaseDuration < minimumLeaseDuration || leaseDuration > maximumLeaseDuration {
		return KernelResult{}, ErrInvalidConfiguration
	}
	if err := ctx.Err(); err != nil {
		return KernelResult{}, err
	}
	now = now.UTC()
	_ = postSyncHandoff
	if kernel.mode == KernelModeShadow {
		observation, err := kernel.observer.Step(ctx, now, limit)
		return KernelResult{Mode: KernelModeShadow, Observation: observation}, err
	}
	if kernel.mode != KernelModeMutation || kernel.begin == nil || kernel.failures == nil {
		return KernelResult{}, ErrInvalidConfiguration
	}
	// The checked-in contract is currently Celery-only. Avoid even opening a
	// write-capable transaction until a later activation changes a frozen kind
	// to River and explicitly wires this kernel.
	if len(kernel.riverKinds) == 0 {
		return KernelResult{Mode: KernelModeMutation}, nil
	}
	if publish == nil {
		return KernelResult{}, ErrPublisherRequired
	}

	claims, err := kernel.commitClaims(ctx, now, limit, leaseDuration)
	if err != nil {
		return KernelResult{}, err
	}
	result := KernelResult{Mode: KernelModeMutation, Claimed: len(claims)}
	for _, claim := range claims {
		descriptor, known := kernel.descriptors[claim.Kind]
		if !known || descriptor.Route != syncdispatchcontract.RouteRiver {
			return result, ErrLeaseLost
		}
		if descriptor.Delivery != syncdispatchcontract.DeliveryAtLeastOnce {
			return result, ErrInvalidConfiguration
		}
		outcome, err := kernel.deliverAtLeastOnce(ctx, claim, now, publish)
		switch outcome {
		case deliverySucceeded:
			result.Dispatched++
		case deliveryRetried:
			result.Retried++
		case deliveryLeaseLost:
			result.LeaseLost++
		}
		// A lease loss is a normal concurrent-owner outcome. Do not mask a
		// compound failure from an unavailable dependency as a lease loss.
		if err == ErrLeaseLost {
			continue
		}
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

type deliveryOutcome uint8

const (
	deliveryNoResult deliveryOutcome = iota
	deliverySucceeded
	deliveryRetried
	deliveryLeaseLost
)

func (kernel *Kernel) commitClaims(
	ctx context.Context,
	now time.Time,
	limit int,
	leaseDuration time.Duration,
) ([]TransportClaim, error) {
	tx, err := kernel.begin(ctx)
	if err != nil || tx == nil {
		return nil, ErrUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()
	claims, err := claimRiverRoutes(ctx, tx, now, limit, leaseDuration, kernel.riverKinds)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, ErrUnavailable
	}
	return claims, nil
}

func (kernel *Kernel) deliverAtLeastOnce(
	ctx context.Context,
	claim TransportClaim,
	now time.Time,
	publish AtLeastOncePublisher,
) (deliveryOutcome, error) {
	tx, err := kernel.begin(ctx)
	if err != nil || tx == nil {
		return deliveryNoResult, ErrUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := lockRiverClaim(ctx, tx, claim, now); err != nil {
		if err == ErrLeaseLost {
			return deliveryLeaseLost, err
		}
		return deliveryNoResult, err
	}
	transportJobID, publishErr := publish(ctx, tx, claim)
	if publishErr != nil {
		_ = tx.Rollback(ctx)
		recordErr := kernel.failures.Record(ctx, claim, now, publishErr)
		switch {
		case recordErr == nil:
			return deliveryRetried, nil
		case recordErr == ErrLeaseLost:
			return deliveryLeaseLost, nil
		default:
			return deliveryNoResult, recordErr
		}
	}
	if err := markRiverDispatched(ctx, tx, claim, now, transportJobID); err != nil {
		if err == ErrLeaseLost {
			return deliveryLeaseLost, err
		}
		return deliveryNoResult, err
	}
	if err := tx.Commit(ctx); err != nil {
		// Commit failure is outcome-unknown. The exact-CAS recorder may prove
		// that the terminal transaction did not commit by successfully rearming
		// the claim, but Step still fails closed because it cannot prove whether
		// a River job became durable.
		_ = tx.Rollback(ctx)
		recordErr := kernel.failures.Record(ctx, claim, now, ErrUnavailable)
		if recordErr != nil {
			return deliveryNoResult, errors.Join(ErrUnavailable, recordErr)
		}
		return deliveryNoResult, ErrUnavailable
	}
	return deliverySucceeded, nil
}

func lockRiverClaim(
	ctx context.Context,
	tx pgx.Tx,
	claim TransportClaim,
	now time.Time,
) error {
	command, err := tx.Exec(ctx, lockRiverClaimSQL,
		claim.ID, claim.ClaimToken, claim.RouteGeneration, now)
	if err != nil {
		return ErrUnavailable
	}
	if command.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	return nil
}

func claimRiverRoutes(
	ctx context.Context,
	tx pgx.Tx,
	now time.Time,
	limit int,
	leaseDuration time.Duration,
	riverKinds []string,
) ([]TransportClaim, error) {
	if tx == nil || len(riverKinds) == 0 {
		return []TransportClaim{}, nil
	}
	rows, err := tx.Query(
		ctx,
		claimRiverRoutesSQL,
		now,
		limit,
		now.Add(leaseDuration),
		riverKinds,
	)
	if err != nil || rows == nil {
		return nil, ErrUnavailable
	}
	defer rows.Close()
	claims := make([]TransportClaim, 0, limit)
	for rows.Next() {
		var claim TransportClaim
		if err := rows.Scan(
			&claim.ID,
			&claim.Kind,
			&claim.ClaimToken,
			&claim.RouteGeneration,
			&claim.AvailableAt,
			&claim.Attempts,
		); err != nil {
			return nil, ErrUnavailable
		}
		if !uuidPattern.MatchString(claim.ID) || !uuidPattern.MatchString(claim.ClaimToken) ||
			claim.RouteGeneration < 1 || claim.AvailableAt.IsZero() || claim.Attempts < 1 {
			return nil, ErrUnavailable
		}
		claims = append(claims, claim)
	}
	if err := rows.Err(); err != nil || len(claims) > limit {
		return nil, ErrUnavailable
	}
	sort.Slice(claims, func(left, right int) bool {
		if claims[left].AvailableAt.Equal(claims[right].AvailableAt) {
			return claims[left].ID < claims[right].ID
		}
		return claims[left].AvailableAt.Before(claims[right].AvailableAt)
	})
	return claims, nil
}

func markRiverDispatched(
	ctx context.Context,
	tx pgx.Tx,
	claim TransportClaim,
	now time.Time,
	transportJobID string,
) error {
	command, err := tx.Exec(ctx, markRiverDispatchedSQL,
		claim.ID, claim.ClaimToken, claim.RouteGeneration, now, transportJobID)
	if err != nil {
		return ErrUnavailable
	}
	if command.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	return nil
}

// claimRiverRoutesSQL is intentionally not reachable from current command
// wiring. The queue role has UPDATE on the outbox but only SELECT on routes.
// PostgreSQL row-locking clauses require mutation authority on every locked
// relation, so only each claimed outbox row is locked here. A route change
// committed before markRiverDispatched is safe: the live generation recheck
// after River InsertTx rolls the entire transaction back on mismatch. Route
// mutation activation still requires an external serialization/quiescence
// barrier for the post-terminal, pre-commit window. The explicit returned
// AvailableAt is sorted again in Go because UPDATE ... RETURNING does not
// promise result ordering.
const claimRiverRoutesSQL = `
WITH candidates AS (
	SELECT outbox.id, route.generation
	FROM public.sync_dispatch_outbox AS outbox
	JOIN public.sync_dispatch_transport_routes AS route
		ON route.kind = outbox.kind
	WHERE outbox.status = 'pending'
		AND outbox.available_at <= $1
		AND (outbox.claim_expires_at IS NULL OR outbox.claim_expires_at <= $1)
		AND outbox.kind = ANY($4::text[])
		AND route.transport = 'river'
		AND route.paused = FALSE
	ORDER BY outbox.available_at, outbox.id
	FOR UPDATE OF outbox SKIP LOCKED
	LIMIT $2
)
UPDATE public.sync_dispatch_outbox AS outbox
SET claim_token = gen_random_uuid()::text,
	claim_expires_at = $3,
	claim_transport = 'river',
	claim_route_generation = candidates.generation,
	attempts = outbox.attempts + 1,
	updated_at = $1
FROM candidates
WHERE outbox.id = candidates.id
RETURNING outbox.id::text, outbox.kind, outbox.claim_token::text,
	outbox.claim_route_generation, outbox.available_at, outbox.attempts
`

// lockRiverClaimSQL reacquires the committed claim before River InsertTx. The
// queue role cannot lock the route relation, so it locks only the outbox row
// and verifies the live route snapshot. The terminal update repeats this CAS;
// the operator's outbox-table barrier serializes route mutation across the
// remaining insert-and-mark transaction window.
const lockRiverClaimSQL = `
SELECT outbox.id
FROM public.sync_dispatch_outbox AS outbox
JOIN public.sync_dispatch_transport_routes AS route
	ON route.kind = outbox.kind
WHERE outbox.id = $1
	AND outbox.claim_token = $2
	AND outbox.status = 'pending'
	AND outbox.claim_expires_at > $4
	AND outbox.claim_transport = 'river'
	AND outbox.claim_route_generation = $3
	AND route.transport = 'river'
	AND route.paused = FALSE
	AND route.generation = outbox.claim_route_generation
FOR UPDATE OF outbox
`

// markRiverDispatchedSQL rechecks the live route generation and pause state in
// the terminal write while the transaction holds the outbox lock. It catches a
// route change committed before this statement; route mutation activation must
// separately serialize the remaining post-terminal, pre-commit window.
const markRiverDispatchedSQL = `
UPDATE public.sync_dispatch_outbox AS outbox
SET status = 'dispatched',
	dispatched_at = $4,
	dispatched_transport = outbox.claim_transport,
	dispatched_route_generation = outbox.claim_route_generation,
	transport_job_id = NULLIF($5, ''),
	claim_token = NULL,
	claim_expires_at = NULL,
	claim_transport = NULL,
	claim_route_generation = NULL,
	last_error = NULL,
	updated_at = $4
FROM public.sync_dispatch_transport_routes AS route
WHERE outbox.id = $1
	AND outbox.claim_token = $2
	AND outbox.status = 'pending'
	AND outbox.claim_expires_at > $4
	AND outbox.claim_transport = 'river'
	AND outbox.claim_route_generation = $3
	AND route.kind = outbox.kind
	AND route.transport = 'river'
	AND route.paused = FALSE
	AND route.generation = outbox.claim_route_generation
`
