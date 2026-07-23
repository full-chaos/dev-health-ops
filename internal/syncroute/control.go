package syncroute

import (
	"context"
	"errors"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	maximumQuiescenceTimeout = 30 * time.Second
	routeMutationLockSQL     = `LOCK TABLE public.sync_dispatch_outbox IN SHARE ROW EXCLUSIVE MODE`
)

var (
	ErrUnknownRoute       = errors.New("sync dispatch route is not registered")
	ErrRouteStateConflict = errors.New("sync dispatch route state conflict")
	ErrLiveClaims         = errors.New("sync dispatch route has live claims")
	ErrCapabilityMissing  = errors.New("sync dispatch transport capability is missing")
	ErrQuiescenceMissing  = errors.New("sync dispatch external quiescence capability is missing")
	// ErrMutationOutcomeUnknown means PostgreSQL returned a commit error after
	// a route mutation. The generation change may be durable, so callers must
	// inspect rather than retrying the operation.
	ErrMutationOutcomeUnknown = errors.New("sync dispatch route mutation outcome is unknown")
)

// RouteState is the bounded, payload-free route control projection.
type RouteState struct {
	Kind              string     `json:"kind"`
	Transport         string     `json:"transport"`
	Generation        int64      `json:"generation"`
	Paused            bool       `json:"paused"`
	PausedAt          *time.Time `json:"paused_at,omitempty"`
	RollbackTransport string     `json:"rollback_transport"`
	LiveClaims        int64      `json:"live_claims"`
}

// QuiescenceRequest identifies the old generation whose external handoffs
// must be unable to issue another publish before a paused route can resume.
type QuiescenceRequest struct {
	Kind       string
	Transport  string
	Generation int64
}

type Quiescer interface {
	Quiesce(context.Context, QuiescenceRequest) error
}

// Capability is registered only by composition code that has a concrete
// publisher/handler. Quiescer is retained for generic route-control callers,
// but sync dispatch has no external-effect window outside its transaction.
type Capability struct {
	Kind      string
	Transport string
	Quiescer  Quiescer
}

type Capabilities interface {
	Lookup(kind, transport string) (Capability, bool)
}

type capabilitySet map[string]Capability

// NewCapabilities returns an immutable exact-match capability registry.
func NewCapabilities(values []Capability) (Capabilities, error) {
	result := make(capabilitySet, len(values))
	for _, value := range values {
		if value.Kind == "" ||
			(value.Transport != syncdispatchcontract.RouteRiver && value.Transport != syncdispatchcontract.RouteCelery) {
			return nil, ErrInvalidConfiguration
		}
		key := value.Kind + "\x00" + value.Transport
		if _, duplicate := result[key]; duplicate {
			return nil, ErrInvalidConfiguration
		}
		result[key] = value
	}
	return result, nil
}

func (values capabilitySet) Lookup(kind, transport string) (Capability, bool) {
	value, ok := values[kind+"\x00"+transport]
	return value, ok
}

type routeRegistry interface {
	Lookup(string) (syncdispatchcontract.Descriptor, bool)
}

type beginControlTransaction func(context.Context) (pgx.Tx, error)

// Controller owns explicit, audited-by-caller route mutations. It never
// changes a route automatically and has no dispatch or handler activation path.
type Controller struct {
	begin        beginControlTransaction
	registry     routeRegistry
	capabilities Capabilities
	now          func() time.Time
}

func NewController(pool *pgxpool.Pool, registry routeRegistry, capabilities Capabilities) (*Controller, error) {
	if pool == nil {
		return nil, ErrInvalidConfiguration
	}
	return newController(pool.Begin, registry, capabilities, time.Now)
}

func newController(
	begin beginControlTransaction,
	registry routeRegistry,
	capabilities Capabilities,
	now func() time.Time,
) (*Controller, error) {
	if begin == nil || registry == nil || capabilities == nil || now == nil {
		return nil, ErrInvalidConfiguration
	}
	return &Controller{begin: begin, registry: registry, capabilities: capabilities, now: now}, nil
}

func (controller *Controller) Inspect(ctx context.Context, kind string) (RouteState, error) {
	if !controller.validKind(kind) {
		return RouteState{}, ErrUnknownRoute
	}
	tx, err := controller.begin(ctx)
	if err != nil || tx == nil {
		return RouteState{}, ErrUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()
	state, err := readRouteState(ctx, tx, kind, controller.now().UTC(), false)
	if err != nil {
		return RouteState{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return RouteState{}, ErrUnavailable
	}
	return state, nil
}

// Pause serializes behind every outbox terminal transaction, increments the
// generation, and prevents new claims before releasing the lock.
func (controller *Controller) Pause(ctx context.Context, kind string) (RouteState, error) {
	if !controller.validKind(kind) {
		return RouteState{}, ErrUnknownRoute
	}
	now := controller.now().UTC()
	tx, state, err := controller.beginRouteMutation(ctx, kind, now)
	if err != nil {
		return RouteState{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if state.Paused {
		return RouteState{}, ErrRouteStateConflict
	}
	descriptor, _ := controller.registry.Lookup(kind)
	if state.Transport != descriptor.Route {
		return RouteState{}, ErrDrift
	}
	row := tx.QueryRow(ctx, `
UPDATE public.sync_dispatch_transport_routes
SET paused = TRUE, paused_at = $2, generation = generation + 1, updated_at = $2
WHERE kind = $1 AND generation = $3 AND paused = FALSE
RETURNING generation, paused_at`, kind, now, state.Generation)
	if err := row.Scan(&state.Generation, &state.PausedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RouteState{}, ErrRouteStateConflict
		}
		return RouteState{}, ErrUnavailable
	}
	state.Paused = true
	state.LiveClaims, err = countLiveClaims(ctx, tx, kind, now)
	if err != nil {
		return RouteState{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return RouteState{}, ErrMutationOutcomeUnknown
	}
	return state, nil
}

// Drain proves whether a paused route has any unexpired database claims.
func (controller *Controller) Drain(ctx context.Context, kind string) (RouteState, error) {
	if !controller.validKind(kind) {
		return RouteState{}, ErrUnknownRoute
	}
	tx, err := controller.begin(ctx)
	if err != nil || tx == nil {
		return RouteState{}, ErrUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()
	state, err := readRouteState(ctx, tx, kind, controller.now().UTC(), true)
	if err != nil {
		return RouteState{}, err
	}
	if !state.Paused {
		return RouteState{}, ErrRouteStateConflict
	}
	if err := tx.Commit(ctx); err != nil {
		return RouteState{}, ErrUnavailable
	}
	return state, nil
}

// Resume holds the outbox table lock while rechecking claims and changing the
// generation. This prevents an old-generation terminal update from committing
// after the route becomes active. River requires an exact capability. The same
// durable claim and route-generation terminal fence applies to every kind.
func (controller *Controller) Resume(
	ctx context.Context,
	kind string,
	transport string,
	_ time.Duration, // Kept for CLI compatibility; the durable claim fence is sufficient.
) (RouteState, error) {
	descriptor, known := controller.registry.Lookup(kind)
	if !known {
		return RouteState{}, ErrUnknownRoute
	}
	if transport != descriptor.Route {
		return RouteState{}, ErrCapabilityMissing
	}
	if transport == syncdispatchcontract.RouteRiver {
		capability, ok := controller.capabilities.Lookup(kind, transport)
		if !ok || capability.Kind != kind || capability.Transport != transport {
			return RouteState{}, ErrCapabilityMissing
		}
	}

	now := controller.now().UTC()
	tx, state, err := controller.beginRouteMutation(ctx, kind, now)
	if err != nil {
		return RouteState{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if !state.Paused {
		return RouteState{}, ErrRouteStateConflict
	}
	if state.LiveClaims != 0 {
		return RouteState{}, ErrLiveClaims
	}
	if err := resumeCapability(
		controller.capabilities, kind, state.Transport, transport,
	); err != nil {
		return RouteState{}, err
	}
	row := tx.QueryRow(ctx, `
UPDATE public.sync_dispatch_transport_routes
SET transport = $2, paused = FALSE, paused_at = NULL,
    generation = generation + 1, updated_at = $3
WHERE kind = $1 AND generation = $4 AND paused = TRUE
RETURNING generation`, kind, transport, now, state.Generation)
	if err := row.Scan(&state.Generation); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RouteState{}, ErrRouteStateConflict
		}
		return RouteState{}, ErrUnavailable
	}
	state.Transport = transport
	state.Paused = false
	state.PausedAt = nil
	if err := tx.Commit(ctx); err != nil {
		return RouteState{}, ErrMutationOutcomeUnknown
	}
	return state, nil
}

func validQuiescenceTimeout(timeout time.Duration) bool {
	return timeout > 0 && timeout <= maximumQuiescenceTimeout
}

func resumeCapability(
	capabilities Capabilities,
	kind string,
	_ string,
	targetTransport string,
) error {
	if targetTransport == syncdispatchcontract.RouteRiver {
		capability, ok := capabilities.Lookup(kind, targetTransport)
		if !ok || capability.Kind != kind || capability.Transport != targetTransport {
			return ErrCapabilityMissing
		}
	}
	return nil
}

// beginRouteMutation follows the producer lock order: route row first, then
// the outbox table barrier. An in-flight Celery publisher can therefore finish
// its terminal UPDATE and commit before this transaction acquires the route
// row. An outbox-only Go terminal transaction is then waited out by the table
// lock. State and claims are re-read only after both barriers are held.
func (controller *Controller) beginRouteMutation(
	ctx context.Context,
	kind string,
	now time.Time,
) (pgx.Tx, RouteState, error) {
	tx, err := controller.begin(ctx)
	if err != nil || tx == nil {
		return nil, RouteState{}, ErrUnavailable
	}
	if _, err := readRouteRecord(ctx, tx, kind, true); err != nil {
		_ = tx.Rollback(ctx)
		return nil, RouteState{}, err
	}
	if _, err := tx.Exec(ctx, routeMutationLockSQL); err != nil {
		_ = tx.Rollback(ctx)
		return nil, RouteState{}, ErrUnavailable
	}
	state, err := readRouteState(ctx, tx, kind, now, true)
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, RouteState{}, err
	}
	return tx, state, nil
}

func (controller *Controller) validKind(kind string) bool {
	if controller == nil || controller.registry == nil {
		return false
	}
	_, ok := controller.registry.Lookup(kind)
	return ok
}

func readRouteState(ctx context.Context, tx pgx.Tx, kind string, now time.Time, lock bool) (RouteState, error) {
	state, err := readRouteRecord(ctx, tx, kind, lock)
	if err != nil {
		return RouteState{}, err
	}
	state.LiveClaims, err = countLiveClaims(ctx, tx, kind, now)
	return state, err
}

func readRouteRecord(ctx context.Context, tx pgx.Tx, kind string, lock bool) (RouteState, error) {
	suffix := ""
	if lock {
		suffix = " FOR UPDATE"
	}
	var state RouteState
	err := tx.QueryRow(ctx, `
SELECT kind, transport, generation, paused, paused_at, rollback_transport
FROM public.sync_dispatch_transport_routes
WHERE kind = $1`+suffix, kind).Scan(
		&state.Kind, &state.Transport, &state.Generation, &state.Paused,
		&state.PausedAt, &state.RollbackTransport,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return RouteState{}, ErrUnknownRoute
	}
	if err != nil {
		return RouteState{}, ErrUnavailable
	}
	if state.Generation < 1 || state.Paused != (state.PausedAt != nil) ||
		(state.Transport != syncdispatchcontract.RouteCelery && state.Transport != syncdispatchcontract.RouteRiver) ||
		state.RollbackTransport != syncdispatchcontract.RouteCelery {
		return RouteState{}, ErrDrift
	}
	return state, nil
}

func countLiveClaims(ctx context.Context, tx pgx.Tx, kind string, now time.Time) (int64, error) {
	var count int64
	err := tx.QueryRow(ctx, `
SELECT count(*)
FROM public.sync_dispatch_outbox
WHERE kind = $1
  AND claim_token IS NOT NULL
  AND claim_expires_at > $2`, kind, now).Scan(&count)
	if err != nil || count < 0 {
		return 0, ErrUnavailable
	}
	return count, nil
}
