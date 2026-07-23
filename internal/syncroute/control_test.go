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

func TestResumeFailsBeforeTransactionWithoutMatchingRiverCapability(t *testing.T) {
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
	if begins != 0 {
		t.Fatalf("unsafe resume opened %d transactions", begins)
	}
}

func TestResumeCapabilityAllowsSameTransportPostSyncAndGuardsCutover(t *testing.T) {
	t.Parallel()
	empty, err := NewCapabilities(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, required, err := resumeCapability(
		empty, syncdispatchcontract.KindPostSync,
		syncdispatchcontract.RouteCelery, syncdispatchcontract.RouteCelery,
	); err != nil || required {
		t.Fatalf("same-transport post_sync capability = required:%t err:%v", required, err)
	}
	riverWithoutBarrier, err := NewCapabilities([]Capability{{
		Kind: syncdispatchcontract.KindPostSync, Transport: syncdispatchcontract.RouteRiver,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := resumeCapability(
		riverWithoutBarrier, syncdispatchcontract.KindPostSync,
		syncdispatchcontract.RouteCelery, syncdispatchcontract.RouteRiver,
	); !errors.Is(err, ErrQuiescenceMissing) {
		t.Fatalf("Celery-to-River post_sync error=%v", err)
	}
	riverWithBarrier, err := NewCapabilities([]Capability{{
		Kind: syncdispatchcontract.KindPostSync, Transport: syncdispatchcontract.RouteRiver,
		Quiescer: quiescerFunc(func(context.Context, QuiescenceRequest) error { return nil }),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, required, err := resumeCapability(
		riverWithBarrier, syncdispatchcontract.KindPostSync,
		syncdispatchcontract.RouteCelery, syncdispatchcontract.RouteRiver,
	); err != nil || !required {
		t.Fatalf("registered cutover capability = required:%t err:%v", required, err)
	}
}

func TestPostSyncQuiescenceTimeoutIsStrictlyBounded(t *testing.T) {
	t.Parallel()
	for _, timeout := range []time.Duration{0, maximumQuiescenceTimeout + time.Nanosecond} {
		if validQuiescenceTimeout(timeout) {
			t.Fatalf("timeout %s was accepted", timeout)
		}
	}
	if !validQuiescenceTimeout(maximumQuiescenceTimeout) {
		t.Fatal("maximum timeout was rejected")
	}
}

type quiescerFunc func(context.Context, QuiescenceRequest) error

func (function quiescerFunc) Quiesce(ctx context.Context, request QuiescenceRequest) error {
	return function(ctx, request)
}
