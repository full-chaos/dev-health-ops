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
	// ErrPostSyncMarkerRequired means mutation reached the special at-most-once
	// post_sync kind without its explicit mark-before seam.
	ErrPostSyncMarkerRequired = errors.New("sync dispatch post-sync mark-before seam required")
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
// needs. It is valid only inside the pgx transaction passed to the publisher.
type TransportClaim struct {
	ID              string
	Kind            string
	ClaimToken      string
	RouteGeneration int64
	AvailableAt     time.Time
	Attempts        int64
}

// AtLeastOncePublisher inserts one River job using the supplied transaction.
// The returned identifier is optional audit metadata. Returning an error rolls
// back both the River insert and the claim, making the attempt retry-safe.
type AtLeastOncePublisher func(context.Context, pgx.Tx, TransportClaim) (string, error)

// PostSyncMarkBefore is deliberately separate from AtLeastOncePublisher.
// The kernel persists the terminal dispatched mark first, then invokes this
// transaction-local seam before commit. It must not perform an external effect;
// post_sync's external at-most-once work belongs to a later, explicit phase.
type PostSyncMarkBefore func(context.Context, pgx.Tx, TransportClaim) error

// KernelResult is bounded to one requested claim window. Shadow includes the
// existing read-only observation; mutation reports only aggregate counts.
type KernelResult struct {
	Mode         KernelMode
	Observation  Observation
	Claimed      int
	Dispatched   int
	PostSyncMark int
}

type beginFunc func(context.Context) (pgx.Tx, error)

// Kernel keeps a dormant mutation implementation beside the observer without
// coupling it to command wiring. A future activation must explicitly construct
// it in KernelModeMutation and provide the delivery seams to Step.
type Kernel struct {
	mode        KernelMode
	observer    Stepper
	begin       beginFunc
	descriptors map[string]syncdispatchcontract.Descriptor
	riverKinds  []string
}

// NewKernel constructs an unactivated kernel backed by the semantic database.
// No transaction or route mutation occurs during construction.
func NewKernel(
	pool *pgxpool.Pool,
	registry Registry,
	mode KernelMode,
) (*Kernel, error) {
	if pool == nil {
		return nil, ErrInvalidConfiguration
	}
	observer, err := NewObserver(pool, registry)
	if err != nil {
		return nil, err
	}
	return newKernel(registry, mode, observer, pool.Begin)
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
// read-only observer and never begins a transaction. Mutation claims only rows
// whose persisted route is River and unpaused; both the outbox and route are
// locked in deterministic order before any publisher is called.
func (kernel *Kernel) Step(
	ctx context.Context,
	now time.Time,
	limit int,
	leaseDuration time.Duration,
	publish AtLeastOncePublisher,
	postSyncMark PostSyncMarkBefore,
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
	if kernel.mode == KernelModeShadow {
		observation, err := kernel.observer.Step(ctx, now, limit)
		return KernelResult{Mode: KernelModeShadow, Observation: observation}, err
	}
	if kernel.mode != KernelModeMutation || kernel.begin == nil {
		return KernelResult{}, ErrInvalidConfiguration
	}
	// The checked-in contract is currently Celery-only. Avoid even opening a
	// write-capable transaction until a later activation changes a frozen kind
	// to River and explicitly wires this kernel.
	if len(kernel.riverKinds) == 0 {
		return KernelResult{Mode: KernelModeMutation}, nil
	}

	tx, err := kernel.begin(ctx)
	if err != nil || tx == nil {
		return KernelResult{}, ErrUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()

	claims, err := claimRiverRoutes(ctx, tx, now, limit, leaseDuration, kernel.riverKinds)
	if err != nil {
		return KernelResult{}, err
	}
	result := KernelResult{Mode: KernelModeMutation, Claimed: len(claims)}
	for _, claim := range claims {
		descriptor, known := kernel.descriptors[claim.Kind]
		if !known || descriptor.Route != syncdispatchcontract.RouteRiver {
			return KernelResult{}, ErrLeaseLost
		}
		if descriptor.Delivery == syncdispatchcontract.DeliveryAtMostOnceMarkBefore {
			if err := markRiverDispatched(ctx, tx, claim, now, ""); err != nil {
				return KernelResult{}, err
			}
			if postSyncMark == nil {
				return KernelResult{}, ErrPostSyncMarkerRequired
			}
			if err := postSyncMark(ctx, tx, claim); err != nil {
				return KernelResult{}, err
			}
			result.Dispatched++
			result.PostSyncMark++
			continue
		}
		if descriptor.Delivery != syncdispatchcontract.DeliveryAtLeastOnce {
			return KernelResult{}, ErrInvalidConfiguration
		}
		if publish == nil {
			return KernelResult{}, ErrPublisherRequired
		}
		transportJobID, err := publish(ctx, tx, claim)
		if err != nil {
			return KernelResult{}, err
		}
		if err := markRiverDispatched(ctx, tx, claim, now, transportJobID); err != nil {
			return KernelResult{}, err
		}
		result.Dispatched++
	}
	if err := tx.Commit(ctx); err != nil {
		return KernelResult{}, ErrUnavailable
	}
	return result, nil
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
// wiring. It takes both route and outbox row locks so a route change cannot
// race a persisted River claim. The explicit returned AvailableAt is sorted
// again in Go because UPDATE ... RETURNING does not promise result ordering.
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
	FOR UPDATE OF outbox, route SKIP LOCKED
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

// markRiverDispatchedSQL rechecks the live route generation and pause state in
// the terminal write. The transaction already holds both locks, but preserving
// this predicate makes an accidental future call without the claim lock fail
// closed rather than dispatching across a route ownership change.
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
