// Package syncroute verifies the database-owned sync-dispatch transport fence.
//
// It is deliberately read-only. Route activation remains outside this package;
// callers use the fence only to keep readiness closed when the persisted
// ownership state differs from the checked-in dispatch contract.
package syncroute

import (
	"context"
	"errors"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	routeFenceReadLimit = 5 // four frozen routes plus one overflow proof
	routeFenceSQL       = `
SELECT kind, transport, generation, paused, paused_at, rollback_transport
FROM public.sync_dispatch_transport_routes
ORDER BY kind
LIMIT $1`
)

var (
	// ErrInvalidConfiguration means the process cannot construct a trustworthy
	// fence. It is distinct from a temporarily unavailable database.
	ErrInvalidConfiguration = errors.New("invalid sync dispatch route fence configuration")
	// ErrUnavailable hides database driver details from the readiness surface.
	ErrUnavailable = errors.New("sync dispatch route fence unavailable")
	// ErrDrift indicates a persisted route state that cannot safely coexist with
	// the checked-in sync-dispatch contract.
	ErrDrift = errors.New("sync dispatch route fence drift")
)

var frozenKinds = [...]string{
	syncdispatchcontract.KindDispatchSyncRun,
	syncdispatchcontract.KindFinalizeSyncRun,
	syncdispatchcontract.KindPostSync,
	syncdispatchcontract.KindReferenceDiscovery,
}

// Checker is the small readiness seam used by the reconciler command.
type Checker interface {
	Check(context.Context) error
}

// Fence checks the complete persisted route set in one bounded, read-only
// query. It contains no mutation or dispatch path.
type Fence struct {
	registry *syncdispatchcontract.Registry
	query    queryFunc
}

type queryFunc func(context.Context) (routeRows, error)

type routeRows interface {
	Next() bool
	Scan(...any) error
	Err() error
	Close()
}

// New constructs a fence backed by the domain pool, where the route table is
// owned. The queue-control pool intentionally has no access to this semantic
// routing state.
func New(pool *pgxpool.Pool, registry *syncdispatchcontract.Registry) (*Fence, error) {
	if pool == nil || !validRegistry(registry) {
		return nil, ErrInvalidConfiguration
	}
	return newFence(registry, func(ctx context.Context) (routeRows, error) {
		return pool.Query(ctx, routeFenceSQL, routeFenceReadLimit)
	})
}

func newFence(registry *syncdispatchcontract.Registry, query queryFunc) (*Fence, error) {
	if query == nil || !validRegistry(registry) {
		return nil, ErrInvalidConfiguration
	}
	return &Fence{registry: registry, query: query}, nil
}

// Check compares each active persisted route to the checked-in contract. A
// pause is coherent only with its timestamp, but is still drift for reconciler
// readiness while an audited operator transition is in progress.
func (fence *Fence) Check(ctx context.Context) error {
	if fence == nil || fence.query == nil || !validRegistry(fence.registry) {
		return ErrInvalidConfiguration
	}
	rows, err := fence.query(ctx)
	if err != nil || rows == nil {
		return ErrUnavailable
	}
	defer rows.Close()

	seen := make(map[string]struct{}, len(frozenKinds))
	for rows.Next() {
		var state routeState
		if err := rows.Scan(
			&state.kind,
			&state.transport,
			&state.generation,
			&state.paused,
			&state.pausedAt,
			&state.rollbackTransport,
		); err != nil {
			return ErrUnavailable
		}
		if len(seen) >= len(frozenKinds) || !state.valid(fence.registry, seen) {
			return ErrDrift
		}
		seen[state.kind] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return ErrUnavailable
	}
	if len(seen) != len(frozenKinds) {
		return ErrDrift
	}
	return nil
}

type routeState struct {
	kind              string
	transport         string
	generation        int64
	paused            bool
	pausedAt          *time.Time
	rollbackTransport string
}

func (state routeState) valid(registry *syncdispatchcontract.Registry, seen map[string]struct{}) bool {
	if _, duplicate := seen[state.kind]; duplicate || state.generation < 1 {
		return false
	}
	descriptor, known := registry.Lookup(state.kind)
	if !known || (state.transport != syncdispatchcontract.RouteCelery && state.transport != syncdispatchcontract.RouteRiver) {
		return false
	}
	if state.rollbackTransport != syncdispatchcontract.RouteCelery {
		return false
	}
	if state.paused != (state.pausedAt != nil) {
		return false
	}
	return !state.paused && state.transport == descriptor.Route
}

func validRegistry(registry *syncdispatchcontract.Registry) bool {
	if registry == nil {
		return false
	}
	for _, kind := range frozenKinds {
		descriptor, ok := registry.Lookup(kind)
		if !ok || descriptor.Route != syncdispatchcontract.RouteCelery && descriptor.Route != syncdispatchcontract.RouteRiver ||
			descriptor.RollbackRoute != syncdispatchcontract.RouteCelery {
			return false
		}
	}
	return true
}
