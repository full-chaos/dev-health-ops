package syncroute

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestFenceChecksCompleteActiveRouteSetWithOneBoundedQuery(t *testing.T) {
	registry := loadRegistry(t)
	queries := 0
	fence, err := newFence(registry, func(context.Context) (routeRows, error) {
		queries++
		return &fakeRows{states: validStates(registry)}, nil
	})
	if err != nil {
		t.Fatalf("newFence() error = %v", err)
	}
	if err := fence.Check(context.Background()); err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	if queries != 1 {
		t.Fatalf("queries = %d, want 1", queries)
	}
	if !strings.Contains(routeFenceSQL, "LIMIT $1") || routeFenceReadLimit != len(frozenKinds)+1 {
		t.Fatalf("route fence query is not bounded to four rows plus overflow proof: %q", routeFenceSQL)
	}
}

func TestFenceClosesReadinessOnRouteDrift(t *testing.T) {
	registry := loadRegistry(t)
	now := time.Now().UTC()
	tests := []struct {
		name   string
		states []routeState
	}{
		{name: "missing", states: validStates(registry)[:3]},
		{name: "extra", states: append(validStates(registry), routeState{kind: "unexpected", transport: syncdispatchcontract.RouteCelery, rollbackTransport: syncdispatchcontract.RouteCelery, generation: 1})},
		{name: "duplicate", states: replaceState(validStates(registry), 3, validStates(registry)[0])},
		{name: "unknown kind", states: replaceState(validStates(registry), 0, routeState{kind: "unexpected", transport: syncdispatchcontract.RouteCelery, rollbackTransport: syncdispatchcontract.RouteCelery, generation: 1})},
		{name: "invalid transport", states: replaceState(validStates(registry), 0, routeState{kind: frozenKinds[0], transport: "other", rollbackTransport: syncdispatchcontract.RouteCelery, generation: 1})},
		{name: "invalid rollback", states: replaceState(validStates(registry), 0, routeState{kind: frozenKinds[0], transport: syncdispatchcontract.RouteCelery, rollbackTransport: syncdispatchcontract.RouteRiver, generation: 1})},
		{name: "zero generation", states: replaceState(validStates(registry), 0, routeState{kind: frozenKinds[0], transport: syncdispatchcontract.RouteCelery, rollbackTransport: syncdispatchcontract.RouteCelery})},
		{name: "paused without timestamp", states: replaceState(validStates(registry), 0, routeState{kind: frozenKinds[0], transport: syncdispatchcontract.RouteCelery, rollbackTransport: syncdispatchcontract.RouteCelery, generation: 1, paused: true})},
		{name: "coherent paused route", states: replaceState(validStates(registry), 0, routeState{kind: frozenKinds[0], transport: syncdispatchcontract.RouteCelery, rollbackTransport: syncdispatchcontract.RouteCelery, generation: 1, paused: true, pausedAt: &now})},
		{name: "unpaused with timestamp", states: replaceState(validStates(registry), 0, routeState{kind: frozenKinds[0], transport: syncdispatchcontract.RouteCelery, rollbackTransport: syncdispatchcontract.RouteCelery, generation: 1, pausedAt: &now})},
		{name: "active transport differs from contract", states: replaceState(validStates(registry), 0, routeState{kind: frozenKinds[0], transport: syncdispatchcontract.RouteRiver, rollbackTransport: syncdispatchcontract.RouteCelery, generation: 1})},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fence, err := newFence(registry, func(context.Context) (routeRows, error) {
				return &fakeRows{states: test.states}, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := fence.Check(context.Background()); !errors.Is(err, ErrDrift) {
				t.Fatalf("Check() error = %v, want ErrDrift", err)
			}
		})
	}
}

func TestFenceClosesReadinessForCoherentPausedRoute(t *testing.T) {
	registry := loadRegistry(t)
	states := validStates(registry)
	now := time.Now().UTC()
	states[0].paused = true
	states[0].pausedAt = &now
	states[0].transport = syncdispatchcontract.RouteRiver
	fence, err := newFence(registry, func(context.Context) (routeRows, error) {
		return &fakeRows{states: states}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := fence.Check(context.Background()); !errors.Is(err, ErrDrift) {
		t.Fatalf("Check() error = %v, want ErrDrift", err)
	}
}

func TestFenceClosesReadinessOnQueryOrScanFailure(t *testing.T) {
	registry := loadRegistry(t)
	tests := []struct {
		name  string
		query queryFunc
	}{
		{name: "query", query: func(context.Context) (routeRows, error) { return nil, errors.New("postgresql://do-not-print") }},
		{name: "scan", query: func(context.Context) (routeRows, error) {
			return &fakeRows{states: validStates(registry), scanErr: errors.New("driver details")}, nil
		}},
		{name: "rows", query: func(context.Context) (routeRows, error) {
			return &fakeRows{states: validStates(registry), err: errors.New("driver details")}, nil
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fence, err := newFence(registry, test.query)
			if err != nil {
				t.Fatal(err)
			}
			if err := fence.Check(context.Background()); !errors.Is(err, ErrUnavailable) {
				t.Fatalf("Check() error = %v, want ErrUnavailable", err)
			}
		})
	}
}

func TestNewRequiresDomainPoolAndFrozenRegistry(t *testing.T) {
	if _, err := New(nil, loadRegistry(t)); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("New(nil, registry) error = %v", err)
	}
	if _, err := newFence(nil, func(context.Context) (routeRows, error) { return nil, nil }); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("newFence(nil, query) error = %v", err)
	}
}

func TestFenceAgainstMigratedPostgres(t *testing.T) {
	databaseURI := os.Getenv("DEV_HEALTH_POSTGRES_TEST_URI")
	if databaseURI == "" {
		t.Skip("DEV_HEALTH_POSTGRES_TEST_URI is not set")
	}
	pool, err := pgxpool.New(t.Context(), databaseURI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	fence, err := New(pool, loadRegistry(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := fence.Check(t.Context()); err != nil {
		t.Fatalf("Check() error = %v", err)
	}
}

func loadRegistry(t *testing.T) *syncdispatchcontract.Registry {
	t.Helper()
	registry, err := syncdispatchcontract.Load(filepath.Join("..", "..", "contracts", "sync-dispatch", "v1"))
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	return registry
}

func validStates(registry *syncdispatchcontract.Registry) []routeState {
	states := make([]routeState, 0, len(frozenKinds))
	for _, kind := range frozenKinds {
		descriptor, _ := registry.Lookup(kind)
		states = append(states, routeState{kind: kind, transport: descriptor.Route, rollbackTransport: descriptor.RollbackRoute, generation: 1})
	}
	return states
}

func replaceState(states []routeState, index int, replacement routeState) []routeState {
	states[index] = replacement
	return states
}

type fakeRows struct {
	states  []routeState
	index   int
	scanErr error
	err     error
	closed  bool
}

func (rows *fakeRows) Next() bool {
	if rows.index >= len(rows.states) {
		return false
	}
	rows.index++
	return true
}

func (rows *fakeRows) Scan(destinations ...any) error {
	if rows.scanErr != nil {
		return rows.scanErr
	}
	state := rows.states[rows.index-1]
	*destinations[0].(*string) = state.kind
	*destinations[1].(*string) = state.transport
	*destinations[2].(*int64) = state.generation
	*destinations[3].(*bool) = state.paused
	*destinations[4].(**time.Time) = state.pausedAt
	*destinations[5].(*string) = state.rollbackTransport
	return nil
}

func (rows *fakeRows) Err() error { return rows.err }

func (rows *fakeRows) Close() { rows.closed = true }
