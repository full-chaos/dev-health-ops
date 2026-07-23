package syncroute

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/jackc/pgx/v5"
)

type controlRegistry map[string]syncdispatchcontract.Descriptor

func (registry controlRegistry) Lookup(kind string) (syncdispatchcontract.Descriptor, bool) {
	value, ok := registry[kind]
	return value, ok
}

func TestCapabilitiesRequireExactConcreteRiverRegistration(t *testing.T) {
	t.Parallel()
	if _, err := NewCapabilities([]Capability{{Kind: "dispatch_sync_run", Transport: "sqs"}}); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("unknown transport capability error=%v", err)
	}
	capabilities, err := NewCapabilities([]Capability{{
		Kind: syncdispatchcontract.KindDispatchSyncRun, Transport: syncdispatchcontract.RouteRiver,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := capabilities.Lookup(syncdispatchcontract.KindDispatchSyncRun, syncdispatchcontract.RouteRiver); !ok {
		t.Fatal("exact River capability is missing")
	}
	if _, ok := capabilities.Lookup(syncdispatchcontract.KindFinalizeSyncRun, syncdispatchcontract.RouteRiver); ok {
		t.Fatal("capability leaked across kinds")
	}
}

func TestResumeFailsBeforeTransactionWithoutMatchingCapabilityOrPostSyncBarrier(t *testing.T) {
	t.Parallel()
	registry := controlRegistry{
		syncdispatchcontract.KindDispatchSyncRun: {
			Kind: syncdispatchcontract.KindDispatchSyncRun, Delivery: syncdispatchcontract.DeliveryAtLeastOnce,
			Route: syncdispatchcontract.RouteRiver, RollbackRoute: syncdispatchcontract.RouteCelery,
		},
		syncdispatchcontract.KindPostSync: {
			Kind: syncdispatchcontract.KindPostSync, Delivery: syncdispatchcontract.DeliveryAtMostOnceMarkBefore,
			Route: syncdispatchcontract.RouteRiver, RollbackRoute: syncdispatchcontract.RouteCelery,
		},
	}
	empty, err := NewCapabilities(nil)
	if err != nil {
		t.Fatal(err)
	}
	begins := 0
	begin := func(context.Context) (pgx.Tx, error) {
		begins++
		return nil, errors.New("must not begin")
	}
	controller, err := newController(begin, registry, empty, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Resume(context.Background(), syncdispatchcontract.KindDispatchSyncRun, syncdispatchcontract.RouteRiver, time.Second); !errors.Is(err, ErrCapabilityMissing) {
		t.Fatalf("missing capability error=%v", err)
	}
	postCapabilities, err := NewCapabilities([]Capability{{
		Kind: syncdispatchcontract.KindPostSync, Transport: syncdispatchcontract.RouteRiver,
	}})
	if err != nil {
		t.Fatal(err)
	}
	controller, err = newController(begin, registry, postCapabilities, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Resume(context.Background(), syncdispatchcontract.KindPostSync, syncdispatchcontract.RouteRiver, time.Second); !errors.Is(err, ErrQuiescenceMissing) {
		t.Fatalf("missing post_sync quiescer error=%v", err)
	}
	if begins != 0 {
		t.Fatalf("unsafe resume opened %d transactions", begins)
	}
}

func TestPostSyncQuiescenceTimeoutIsStrictlyBounded(t *testing.T) {
	t.Parallel()
	registry := controlRegistry{
		syncdispatchcontract.KindPostSync: {
			Kind: syncdispatchcontract.KindPostSync, Delivery: syncdispatchcontract.DeliveryAtMostOnceMarkBefore,
			Route: syncdispatchcontract.RouteRiver, RollbackRoute: syncdispatchcontract.RouteCelery,
		},
	}
	capabilities, err := NewCapabilities([]Capability{{
		Kind: syncdispatchcontract.KindPostSync, Transport: syncdispatchcontract.RouteRiver,
		Quiescer: quiescerFunc(func(context.Context, QuiescenceRequest) error { return nil }),
	}})
	if err != nil {
		t.Fatal(err)
	}
	controller, err := newController(
		func(context.Context) (pgx.Tx, error) { return nil, errors.New("must not begin") },
		registry, capabilities, time.Now,
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, timeout := range []time.Duration{0, maximumQuiescenceTimeout + time.Nanosecond} {
		if _, err := controller.Resume(context.Background(), syncdispatchcontract.KindPostSync, syncdispatchcontract.RouteRiver, timeout); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("timeout %s error=%v", timeout, err)
		}
	}
}

type quiescerFunc func(context.Context, QuiescenceRequest) error

func (function quiescerFunc) Quiesce(ctx context.Context, request QuiescenceRequest) error {
	return function(ctx, request)
}
