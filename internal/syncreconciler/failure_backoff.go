package syncreconciler

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const transportPublishFailureEvidence = "transport_publish_failed"

type failureTransaction interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Commit(context.Context) error
	Rollback(context.Context) error
}

type failureBegin func(context.Context) (failureTransaction, error)

// PublishFailureRecorder persists a retry after a publisher fails outside the
// transaction that committed its claim. The dormant Kernel invokes it after
// rolling back the failed per-claim delivery transaction; command activation
// remains a separate reviewed boundary.
type PublishFailureRecorder struct {
	begin failureBegin
}

// NewPublishFailureRecorder constructs the dormant persistence seam.
func NewPublishFailureRecorder(pool *pgxpool.Pool) (*PublishFailureRecorder, error) {
	if pool == nil {
		return nil, ErrInvalidConfiguration
	}
	return newPublishFailureRecorder(func(ctx context.Context) (failureTransaction, error) {
		return pool.Begin(ctx)
	})
}

func newPublishFailureRecorder(begin failureBegin) (*PublishFailureRecorder, error) {
	if begin == nil {
		return nil, ErrInvalidConfiguration
	}
	return &PublishFailureRecorder{begin: begin}, nil
}

// Record publishes no external effect. It atomically releases one still-live
// River claim back to pending with Python-parity retry timing. failure is
// required to make accidental success-path use fail closed, but raw exception
// text is never persisted; the bounded stable evidence code is sufficient for
// operators without retaining credentials or payload fragments.
func (recorder *PublishFailureRecorder) Record(
	ctx context.Context,
	claim TransportClaim,
	now time.Time,
	failure error,
) error {
	if recorder == nil || recorder.begin == nil || ctx == nil || now.IsZero() || failure == nil ||
		!uuidPattern.MatchString(claim.ID) || !uuidPattern.MatchString(claim.ClaimToken) ||
		claim.Kind == "" ||
		claim.RouteGeneration < 1 || claim.Attempts < 1 {
		return ErrInvalidConfiguration
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	now = now.UTC()
	tx, err := recorder.begin(ctx)
	if err != nil || tx == nil {
		return ErrUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()

	command, err := tx.Exec(
		ctx,
		recordTransportPublishFailureSQL,
		claim.ID,
		claim.ClaimToken,
		claim.RouteGeneration,
		claim.Kind,
		now,
		now.Add(transportPublishBackoff(claim.Attempts)),
		transportPublishFailureEvidence,
	)
	if err != nil {
		return ErrUnavailable
	}
	if command.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	if err := tx.Commit(ctx); err != nil {
		return ErrUnavailable
	}
	return nil
}

// transportPublishBackoff matches dispatch_outbox.backoff_seconds:
// min(60 * 2^(attempt-1 capped at exponent 4), 900) seconds.
func transportPublishBackoff(attempt int64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	exponent := attempt - 1
	if exponent > 4 {
		exponent = 4
	}
	seconds := int64(60) * (int64(1) << exponent)
	if seconds > 900 {
		seconds = 900
	}
	return time.Duration(seconds) * time.Second
}

// recordTransportPublishFailureSQL is an exact CAS over the committed claim
// and its live route. A stale lease, competing replica, pause, or route change
// updates no rows and is reported as ErrLeaseLost. post_sync is never passed to
// this at-least-once-only seam.
const recordTransportPublishFailureSQL = `
UPDATE public.sync_dispatch_outbox AS outbox
SET status = 'pending',
	available_at = $6,
	claim_token = NULL,
	claim_expires_at = NULL,
	claim_transport = NULL,
	claim_route_generation = NULL,
	last_error = $7,
	updated_at = $5
FROM public.sync_dispatch_transport_routes AS route
WHERE outbox.id = $1
	AND outbox.claim_token = $2
	AND outbox.kind = $4
	AND outbox.status = 'pending'
	AND outbox.claim_expires_at > $5
	AND outbox.claim_transport = 'river'
	AND outbox.claim_route_generation = $3
	AND route.kind = outbox.kind
	AND route.transport = outbox.claim_transport
	AND route.transport = 'river'
	AND route.paused = FALSE
	AND route.generation = outbox.claim_route_generation
`

var _ failureTransaction = (pgx.Tx)(nil)
