package jobruntime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultConcurrencyLease = 15 * time.Minute
	concurrencyRetryDelay   = 100 * time.Millisecond
)

var errConcurrencyBudgetUnavailable = errors.New("concurrency budget store is unavailable")

// ConcurrencyBudgetObserver records bounded fleet-budget state. It receives
// policy identity only; organization IDs, payloads, credentials, and lease
// tokens never enter metrics.
type ConcurrencyBudgetObserver interface {
	SetConcurrencyBudgetCapacity(ConcurrencyBudgetLabels, int) error
	SetConcurrencyBudgetLeased(ConcurrencyBudgetLabels, int) error
	ObserveConcurrencyBudgetWait(ConcurrencyBudgetLabels, time.Duration) error
	ObserveConcurrencyBudgetExpiry(ConcurrencyBudgetLabels, string) error
}

// PostgresConcurrencyBudget replaces process-local semaphores with durable
// leases in the domain PostgreSQL pool. The advisory transaction lock makes
// count-and-insert atomic across independent worker processes without adding
// a broker or exposing job payloads to the coordination record.
type PostgresConcurrencyBudget struct {
	pool          *pgxpool.Pool
	leaseDuration time.Duration
	now           func() time.Time
	observer      ConcurrencyBudgetObserver
}

func NewPostgresConcurrencyBudget(
	pool *pgxpool.Pool,
	observer ConcurrencyBudgetObserver,
) (*PostgresConcurrencyBudget, error) {
	if pool == nil {
		return nil, errConcurrencyBudgetUnavailable
	}
	return &PostgresConcurrencyBudget{
		pool:          pool,
		leaseDuration: defaultConcurrencyLease,
		now:           time.Now,
		observer:      observer,
	}, nil
}

func (store *PostgresConcurrencyBudget) Supports(scope string, limit int) bool {
	return store != nil && store.pool != nil &&
		(scope == "organization" || scope == "fleet") && limit > 0 && limit <= 32
}

func (store *PostgresConcurrencyBudget) Acquire(
	ctx context.Context,
	request BudgetRequest,
) (BudgetLease, error) {
	if ctx == nil || !store.Supports(request.ConcurrencyScope, request.ConcurrencyLimit) {
		return nil, errConcurrencyBudgetUnavailable
	}
	key, err := concurrencyBudgetKey(request)
	if err != nil || store.now == nil || store.leaseDuration < time.Second ||
		store.leaseDuration > time.Hour {
		return nil, errConcurrencyBudgetUnavailable
	}
	labels := ConcurrencyBudgetLabels{Kind: request.Kind, Scope: request.ConcurrencyScope}
	started := store.now()
	if err := store.observeCapacity(labels, request.ConcurrencyLimit); err != nil {
		return nil, errConcurrencyBudgetUnavailable
	}
	for {
		lease, leased, expired, acquireErr := store.tryAcquire(ctx, request, key)
		if acquireErr != nil {
			return nil, acquireErr
		}
		if expired > 0 {
			_ = store.observeExpiry(labels, "expired", expired)
		}
		if leased >= 0 {
			_ = store.observeLeased(labels, leased)
		}
		if lease != nil {
			if expired > 0 {
				_ = store.observeExpiry(labels, "recovered", expired)
			}
			_ = store.observeWait(labels, store.now().Sub(started))
			return lease, nil
		}
		timer := time.NewTimer(concurrencyRetryDelay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func (store *PostgresConcurrencyBudget) tryAcquire(
	ctx context.Context,
	request BudgetRequest,
	key string,
) (BudgetLease, int, int64, error) {
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return nil, -1, 0, fmt.Errorf("begin concurrency lease transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, key); err != nil {
		return nil, -1, 0, fmt.Errorf("lock concurrency lease key: %w", err)
	}
	deleted, err := tx.Exec(ctx, `
		DELETE FROM public.worker_concurrency_leases
		WHERE budget_key = $1 AND lease_expires_at <= statement_timestamp()`, key)
	if err != nil {
		return nil, -1, 0, fmt.Errorf("expire concurrency leases: %w", err)
	}
	var leased int
	if err := tx.QueryRow(ctx, `
		SELECT count(*)
		FROM public.worker_concurrency_leases
		WHERE budget_key = $1 AND lease_expires_at > statement_timestamp()`, key).Scan(&leased); err != nil {
		return nil, -1, 0, fmt.Errorf("count concurrency leases: %w", err)
	}
	if leased >= request.ConcurrencyLimit {
		if err := tx.Commit(ctx); err != nil {
			return nil, -1, 0, errConcurrencyBudgetUnavailable
		}
		return nil, leased, deleted.RowsAffected(), nil
	}
	id := uuid.New()
	token := uuid.New()
	var organization any
	if request.OrganizationID != nil {
		organization, err = uuid.Parse(*request.OrganizationID)
		if err != nil {
			return nil, -1, 0, errConcurrencyBudgetUnavailable
		}
	}
	// Captured before the INSERT so the locally-tracked expiry can only be
	// EARLIER than the row's statement_timestamp()-based one: renewal retries
	// must give up before the real lease expires, never after.
	acquiredAt := time.Now()
	if _, err := tx.Exec(ctx, `
		INSERT INTO public.worker_concurrency_leases (
			id, budget_key, job_kind, concurrency_scope, organization_id,
			owner_token, lease_expires_at, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6,
			statement_timestamp() + ($7::bigint * interval '1 second'),
			statement_timestamp(), statement_timestamp())`,
		id, key, request.Kind, request.ConcurrencyScope, organization,
		token, int64(store.leaseDuration/time.Second)); err != nil {
		return nil, -1, 0, fmt.Errorf("insert concurrency lease: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, -1, 0, errConcurrencyBudgetUnavailable
	}
	lease := &postgresConcurrencyLease{
		pool:          store.pool,
		id:            id,
		token:         token,
		leasedAt:      acquiredAt,
		leaseDuration: store.leaseDuration,
		stopRenewal:   make(chan struct{}),
		renewalDone:   make(chan struct{}),
		lost:          make(chan struct{}),
	}
	go lease.renew()
	return lease, leased + 1, deleted.RowsAffected(), nil
}

type postgresConcurrencyLease struct {
	pool          *pgxpool.Pool
	id            uuid.UUID
	token         uuid.UUID
	leasedAt      time.Time
	leaseDuration time.Duration
	stopRenewal   chan struct{}
	renewalDone   chan struct{}
	lost          chan struct{}
	lostOnce      sync.Once
	once          sync.Once
}

// Release fences the owner token and stops renewal before deleting the row.
// Renewal only extends a still-live row, so a crashed worker's expired lease
// cannot be resurrected by a delayed renewal attempt.
func (lease *postgresConcurrencyLease) Release() {
	if lease == nil || lease.pool == nil || lease.id == uuid.Nil || lease.token == uuid.Nil {
		return
	}
	lease.once.Do(func() {
		if lease.stopRenewal != nil {
			close(lease.stopRenewal)
			<-lease.renewalDone
		}
		_, _ = lease.pool.Exec(context.Background(), `
			DELETE FROM public.worker_concurrency_leases
			WHERE id = $1 AND owner_token = $2`, lease.id, lease.token)
	})
}

// Lost closes when the lease can no longer be renewed. Consumers must cancel
// the running handler context when this signal fires.
func (lease *postgresConcurrencyLease) Lost() <-chan struct{} {
	if lease == nil {
		return nil
	}
	return lease.lost
}

func (lease *postgresConcurrencyLease) markLost() {
	if lease == nil || lease.lost == nil {
		return
	}
	lease.lostOnce.Do(func() { close(lease.lost) })
}

func (lease *postgresConcurrencyLease) renew() {
	interval := lease.leaseDuration / 3
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer close(lease.renewalDone)
	// The instant the lease stops being ours if no renewal succeeds. Renewal
	// retries until this passes rather than surrendering on the first error.
	expiry := lease.leasedAt.Add(lease.leaseDuration)
	for {
		select {
		case <-lease.stopRenewal:
			return
		case <-ticker.C:
			attemptedAt := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), renewalQueryTimeout(interval))
			result, err := lease.pool.Exec(ctx, `
				UPDATE public.worker_concurrency_leases
				SET lease_expires_at = statement_timestamp() + ($3::bigint * interval '1 second'),
					updated_at = statement_timestamp()
				WHERE id = $1 AND owner_token = $2 AND lease_expires_at > statement_timestamp()`,
				lease.id, lease.token, int64(lease.leaseDuration/time.Second))
			cancel()
			switch {
			case err == nil && result.RowsAffected() == 1:
				expiry = attemptedAt.Add(lease.leaseDuration)
			case err == nil:
				// The UPDATE only matches a live row we still own, so zero rows
				// means the lease is provably gone -- another owner took it or
				// it already expired. Nothing to wait for.
				lease.markLost()
				return
			case time.Now().Before(expiry):
				// A transient failure -- a query timeout, a PgBouncer restart,
				// a failover -- is not lease loss. The interval is a third of
				// the TTL, so there are further attempts left before the lease
				// actually expires; cancelling the handler on the first error
				// terminalized 2-hour jobs on any DB blip (CHAOS-3866).
			default:
				lease.markLost()
				return
			}
		}
	}
}

func renewalQueryTimeout(interval time.Duration) time.Duration {
	if interval < 2*time.Second {
		return interval
	}
	return 2 * time.Second
}

func (store *PostgresConcurrencyBudget) observeCapacity(labels ConcurrencyBudgetLabels, capacity int) error {
	if store.observer == nil {
		return nil
	}
	return store.observer.SetConcurrencyBudgetCapacity(labels, capacity)
}

func (store *PostgresConcurrencyBudget) observeLeased(labels ConcurrencyBudgetLabels, leased int) error {
	if store.observer == nil || leased < 0 {
		return nil
	}
	return store.observer.SetConcurrencyBudgetLeased(labels, leased)
}

func (store *PostgresConcurrencyBudget) observeWait(labels ConcurrencyBudgetLabels, wait time.Duration) error {
	if store.observer == nil || wait < 0 {
		return nil
	}
	return store.observer.ObserveConcurrencyBudgetWait(labels, wait)
}

func (store *PostgresConcurrencyBudget) observeExpiry(labels ConcurrencyBudgetLabels, result string, count int64) error {
	if store.observer == nil || count <= 0 {
		return nil
	}
	for i := int64(0); i < count; i++ {
		if err := store.observer.ObserveConcurrencyBudgetExpiry(labels, result); err != nil {
			return err
		}
	}
	return nil
}

func concurrencyBudgetKey(request BudgetRequest) (string, error) {
	if request.Kind == "" || len(request.Kind) > 96 || request.ConcurrencyScope == "" {
		return "", errConcurrencyBudgetUnavailable
	}
	if request.ConcurrencyScope == "fleet" {
		if request.OrganizationID != nil {
			return "", errConcurrencyBudgetUnavailable
		}
		return fmt.Sprintf("%s|fleet|global", request.Kind), nil
	}
	if request.ConcurrencyScope != "organization" || request.OrganizationID == nil {
		return "", errConcurrencyBudgetUnavailable
	}
	organization, err := uuid.Parse(*request.OrganizationID)
	if err != nil {
		return "", errConcurrencyBudgetUnavailable
	}
	return fmt.Sprintf("%s|organization|%s", request.Kind, organization), nil
}
