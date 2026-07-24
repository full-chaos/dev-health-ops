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
			Kind: syncdispatchcontract.KindPostSync, Delivery: syncdispatchcontract.DeliveryAtLeastOnce,
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

func TestResumeCapabilityTreatsPostSyncAsOrdinaryClaimFencedRoute(t *testing.T) {
	t.Parallel()
	empty, err := NewCapabilities(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := resumeCapability(
		empty, syncdispatchcontract.KindPostSync,
		syncdispatchcontract.RouteCelery, syncdispatchcontract.RouteCelery,
	); err != nil {
		t.Fatalf("same-transport post_sync capability error=%v", err)
	}
	riverCapability, err := NewCapabilities([]Capability{{
		Kind: syncdispatchcontract.KindPostSync, Transport: syncdispatchcontract.RouteRiver,
	}})
	if err != nil {
		t.Fatal(err)
	}
	err = resumeCapability(
		riverCapability, syncdispatchcontract.KindPostSync,
		syncdispatchcontract.RouteCelery, syncdispatchcontract.RouteRiver,
	)
	if err != nil {
		t.Fatalf("post_sync cutover capability error=%v", err)
	}
}
