// Package jobroute owns durable, per-kind transport selection for bounded
// River jobs. Checked-in migration policy defines allowed targets; PostgreSQL
// records the active choice and an operator generation.
package jobroute

import (
	"context"
	"errors"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrInvalidConfiguration    = errors.New("job route configuration is invalid")
	ErrUnknownRoute            = errors.New("job route is unknown")
	ErrDrift                   = errors.New("job route drifts from checked-in policy")
	ErrPaused                  = errors.New("job route is paused")
	ErrLiveClaims              = errors.New("job route still has live claims")
	ErrPendingOutbox           = errors.New("job route still has pending outbox work")
	ErrCeleryQuiescenceMissing = errors.New("celery route quiescence is not configured")
	ErrUnavailable             = errors.New("job route store is unavailable")
	ErrOutcomeUnknown          = errors.New("job route mutation outcome is unknown")
)

type State struct {
	Kind       string    `json:"kind"`
	Transport  string    `json:"transport"`
	Paused     bool      `json:"paused"`
	Generation int64     `json:"generation"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type Registry interface {
	Descriptor(string) (jobruntime.Descriptor, bool)
	Descriptors() []jobruntime.Descriptor
}

type Quiescer interface {
	Quiesce(context.Context, string) error
}

type Controller struct {
	pool           *pgxpool.Pool
	registry       Registry
	quiescer       Quiescer
	celeryQuiescer Quiescer
	now            func() time.Time
}

func NewController(pool *pgxpool.Pool, registry Registry, quiescer Quiescer) (*Controller, error) {
	if pool == nil || registry == nil || quiescer == nil {
		return nil, ErrInvalidConfiguration
	}
	return &Controller{pool: pool, registry: registry, quiescer: quiescer, now: time.Now}, nil
}

// NewControllerWithCeleryQuiescer enables forward activation only when a
// concrete Celery ownership probe is supplied. The default constructor
// deliberately leaves future Celery-to-River transitions fail closed.
func NewControllerWithCeleryQuiescer(
	pool *pgxpool.Pool,
	registry Registry,
	riverQuiescer Quiescer,
	celeryQuiescer Quiescer,
) (*Controller, error) {
	controller, err := NewController(pool, registry, riverQuiescer)
	if err != nil || celeryQuiescer == nil {
		return nil, ErrInvalidConfiguration
	}
	controller.celeryQuiescer = celeryQuiescer
	return controller, nil
}

func (controller *Controller) Inspect(ctx context.Context, kind string) (State, error) {
	descriptor, ok := controller.descriptor(kind)
	if !ok {
		return State{}, ErrUnknownRoute
	}
	state, err := controller.read(ctx, kind)
	if err != nil {
		return State{}, err
	}
	if !allowed(descriptor, state.Transport) {
		return State{}, ErrDrift
	}
	return state, nil
}

// Resolve is used by producers and relays. Paused or drifted routes fail
// closed; callers must not silently fall back to either transport.
func (controller *Controller) Resolve(ctx context.Context, kind string) (string, error) {
	state, err := controller.Inspect(ctx, kind)
	if err != nil {
		return "", err
	}
	if state.Paused {
		return "", ErrPaused
	}
	return state.Transport, nil
}

// DeferredKinds returns routes whose current durable transport remains Celery.
// The relay refreshes this list for every bounded step so rollback immediately
// prevents new River inserts without a process restart.
func (controller *Controller) DeferredKinds(ctx context.Context) ([]string, error) {
	descriptors := controller.registry.Descriptors()
	deferred := make([]string, 0, len(descriptors))
	for _, descriptor := range descriptors {
		transport, err := controller.Resolve(ctx, descriptor.Kind)
		if err != nil {
			return nil, err
		}
		if transport == "celery" {
			deferred = append(deferred, descriptor.Kind)
		}
	}
	return deferred, nil
}

// ApplyCheckedIn moves a kind to the exact route committed in migration
// policy. It cannot select an arbitrary transport. The row lock serializes the
// transition with producer FOR SHARE reads.
func (controller *Controller) ApplyCheckedIn(ctx context.Context, kind string) (State, error) {
	descriptor, ok := controller.descriptor(kind)
	if !ok {
		return State{}, ErrUnknownRoute
	}
	tx, err := controller.pool.Begin(ctx)
	if err != nil {
		return State{}, ErrUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()
	state, err := readState(ctx, tx, kind, true)
	if err != nil {
		return State{}, err
	}
	if !allowed(descriptor, state.Transport) {
		return State{}, ErrDrift
	}
	if state.Transport == descriptor.Route && !state.Paused {
		return state, nil
	}
	if state.Transport == descriptor.RollbackRoute && descriptor.Route != descriptor.RollbackRoute {
		if controller.celeryQuiescer == nil {
			return State{}, ErrCeleryQuiescenceMissing
		}
		if err := controller.celeryQuiescer.Quiesce(ctx, kind); err != nil {
			return State{}, ErrLiveClaims
		}
	}
	now := controller.now().UTC()
	err = tx.QueryRow(ctx, `
		UPDATE public.worker_job_routes
		SET transport = $2, paused = FALSE, generation = generation + 1, updated_at = $3
		WHERE job_kind = $1 AND generation = $4
		RETURNING job_kind, transport, paused, generation, updated_at`,
		kind, descriptor.Route, now, state.Generation,
	).Scan(&state.Kind, &state.Transport, &state.Paused, &state.Generation, &state.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return State{}, ErrDrift
	}
	if err != nil {
		return State{}, ErrUnavailable
	}
	if err := tx.Commit(ctx); err != nil {
		return State{}, ErrOutcomeUnknown
	}
	return state, nil
}

// Rollback is a single authenticated operator action. It first prevents new
// producer/relay transactions, proves there is no live domain or River work,
// then changes only this kind to its checked-in rollback transport.
func (controller *Controller) Rollback(ctx context.Context, kind string) (State, error) {
	descriptor, ok := controller.descriptor(kind)
	if !ok || descriptor.RollbackRoute == "none" {
		return State{}, ErrUnknownRoute
	}
	tx, err := controller.pool.Begin(ctx)
	if err != nil {
		return State{}, ErrUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()
	state, err := readState(ctx, tx, kind, true)
	if err != nil {
		return State{}, err
	}
	if !allowed(descriptor, state.Transport) {
		return State{}, ErrDrift
	}
	if _, err := tx.Exec(ctx, "LOCK TABLE public.worker_job_outbox IN SHARE ROW EXCLUSIVE MODE"); err != nil {
		return State{}, ErrUnavailable
	}
	var pending int64
	if err := tx.QueryRow(ctx, `
		SELECT count(*) FROM public.worker_job_outbox
		WHERE job_kind = $1 AND status IN ('pending', 'claimed')`,
		kind,
	).Scan(&pending); err != nil {
		return State{}, ErrUnavailable
	}
	if pending != 0 {
		return State{}, ErrPendingOutbox
	}
	var live int64
	if err := tx.QueryRow(ctx, `
		SELECT count(*) FROM public.worker_job_runs
		WHERE job_kind = $1 AND status = 'running'`,
		kind,
	).Scan(&live); err != nil {
		return State{}, ErrUnavailable
	}
	if live != 0 {
		return State{}, ErrLiveClaims
	}
	if state.Transport != descriptor.RollbackRoute {
		if err := controller.quiescer.Quiesce(ctx, kind); err != nil {
			return State{}, ErrLiveClaims
		}
	}
	now := controller.now().UTC()
	err = tx.QueryRow(ctx, `
		UPDATE public.worker_job_routes
		SET transport = $2, paused = FALSE, generation = generation + 1, updated_at = $3
		WHERE job_kind = $1 AND generation = $4
		RETURNING job_kind, transport, paused, generation, updated_at`,
		kind, descriptor.RollbackRoute, now, state.Generation,
	).Scan(&state.Kind, &state.Transport, &state.Paused, &state.Generation, &state.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return State{}, ErrDrift
	}
	if err != nil {
		return State{}, ErrUnavailable
	}
	if err := tx.Commit(ctx); err != nil {
		return State{}, ErrOutcomeUnknown
	}
	return state, nil
}

func (controller *Controller) descriptor(kind string) (jobruntime.Descriptor, bool) {
	if controller == nil || controller.pool == nil || controller.registry == nil || controller.now == nil || kind == "" {
		return jobruntime.Descriptor{}, false
	}
	return controller.registry.Descriptor(kind)
}

func (controller *Controller) read(ctx context.Context, kind string) (State, error) {
	return readState(ctx, controller.pool.QueryRow(ctx, `
		SELECT job_kind, transport, paused, generation, updated_at
		FROM public.worker_job_routes WHERE job_kind = $1`, kind), kind, false)
}

func readState(ctx context.Context, source any, kind string, lock bool) (State, error) {
	var row pgx.Row
	switch typed := source.(type) {
	case pgx.Tx:
		suffix := ""
		if lock {
			suffix = " FOR UPDATE"
		}
		row = typed.QueryRow(ctx, `
			SELECT job_kind, transport, paused, generation, updated_at
			FROM public.worker_job_routes WHERE job_kind = $1`+suffix, kind)
	case pgx.Row:
		row = typed
	default:
		return State{}, ErrUnavailable
	}
	var state State
	if err := row.Scan(&state.Kind, &state.Transport, &state.Paused, &state.Generation, &state.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return State{}, ErrUnknownRoute
		}
		return State{}, ErrUnavailable
	}
	if state.Kind != kind || state.Generation < 1 || state.UpdatedAt.IsZero() {
		return State{}, ErrDrift
	}
	return state, nil
}

func allowed(descriptor jobruntime.Descriptor, transport string) bool {
	return transport != "" && (transport == descriptor.Route || transport == descriptor.RollbackRoute)
}
