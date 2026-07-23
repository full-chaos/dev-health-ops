package joboutbox

import (
	"context"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

type fakeRouteResolver struct {
	deferred []string
	route    string
	err      error
}

func (resolver fakeRouteResolver) DeferredKinds(context.Context) ([]string, error) {
	return append([]string(nil), resolver.deferred...), resolver.err
}

func (resolver fakeRouteResolver) Resolve(context.Context, string) (string, error) {
	return resolver.route, resolver.err
}

type enumerableRegistry struct {
	descriptors []jobruntime.Descriptor
	byKind      map[string]jobruntime.Descriptor
}

type descriptorOnlyRegistry struct{}

func (descriptorOnlyRegistry) Descriptor(string) (jobruntime.Descriptor, bool) {
	return jobruntime.Descriptor{}, false
}

func (registry enumerableRegistry) Descriptor(kind string) (jobruntime.Descriptor, bool) {
	descriptor, ok := registry.byKind[kind]
	return descriptor, ok
}

func (registry enumerableRegistry) Descriptors() []jobruntime.Descriptor {
	return append([]jobruntime.Descriptor(nil), registry.descriptors...)
}

func TestDeferredRelayKindsSeparatesKnownCeleryFromExecutableRoutes(t *testing.T) {
	descriptors := []jobruntime.Descriptor{
		{Kind: "job.river", Route: "river"},
		{Kind: "job.celery", Route: "celery"},
		{Kind: "job.shadow", Route: "shadow"},
		{Kind: "job.canary", Route: "river_canary"},
	}
	registry := enumerableRegistry{descriptors: descriptors, byKind: map[string]jobruntime.Descriptor{}}
	for _, descriptor := range descriptors {
		registry.byKind[descriptor.Kind] = descriptor
	}

	deferred, err := deferredRelayKinds(registry)
	if err != nil {
		t.Fatal(err)
	}
	if len(deferred) != 1 || deferred[0] != "job.celery" {
		t.Fatalf("deferredRelayKinds() = %#v", deferred)
	}
}

func TestDeferredRelayKindsAllowsAllKnownKindsToRemainOnCelery(t *testing.T) {
	descriptors := []jobruntime.Descriptor{
		{Kind: "job.alpha", Route: "celery"},
		{Kind: "job.beta", Route: "celery"},
	}
	registry := enumerableRegistry{descriptors: descriptors, byKind: map[string]jobruntime.Descriptor{}}
	for _, descriptor := range descriptors {
		registry.byKind[descriptor.Kind] = descriptor
	}

	deferred, err := deferredRelayKinds(registry)
	if err != nil {
		t.Fatal(err)
	}
	if len(deferred) != 2 || deferred[0] != "job.alpha" || deferred[1] != "job.beta" {
		t.Fatalf("deferredRelayKinds() = %#v", deferred)
	}
}

func TestDeferredRelayKindsFailClosedOnIncompleteEnumeration(t *testing.T) {
	tests := []enumerableRegistry{
		{},
		{
			descriptors: []jobruntime.Descriptor{{Kind: "job", Route: "river"}},
			byKind:      map[string]jobruntime.Descriptor{},
		},
		{
			descriptors: []jobruntime.Descriptor{{Kind: "job", Route: "unexpected"}},
			byKind:      map[string]jobruntime.Descriptor{"job": {Kind: "job", Route: "unexpected"}},
		},
	}
	for index, registry := range tests {
		if _, err := deferredRelayKinds(registry); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("case %d error = %v", index, err)
		}
	}
}

func TestNewRelayRequiresEnumerableInserterPolicy(t *testing.T) {
	inserter := &RiverInserter{registry: descriptorOnlyRegistry{}}
	if _, err := NewRelay(&Repository{}, inserter, DefaultRelayConfig()); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("NewRelay() error = %v", err)
	}
}

func TestNewRelayWithRoutesRequiresDurableResolver(t *testing.T) {
	descriptors := []jobruntime.Descriptor{{Kind: "job.alpha", Route: "celery"}}
	registry := enumerableRegistry{
		descriptors: descriptors,
		byKind: map[string]jobruntime.Descriptor{
			"job.alpha": descriptors[0],
		},
	}
	inserter := &RiverInserter{client: nil, registry: registry}
	if _, err := NewRelayWithRoutes(&Repository{}, inserter, nil, DefaultRelayConfig()); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("nil resolver error=%v", err)
	}
	relay, err := NewRelayWithRoutes(
		&Repository{}, inserter,
		fakeRouteResolver{deferred: []string{"job.alpha"}},
		DefaultRelayConfig(),
	)
	if err != nil || relay.routes == nil {
		t.Fatalf("dynamic relay=%#v err=%v", relay, err)
	}
}

func TestClassifyRecordOutcomePropagatesPersistenceFailure(t *testing.T) {
	result := StepResult{}
	recorded, err := classifyRecordOutcome(ErrUnavailable, &result)
	if recorded || !errors.Is(err, ErrUnavailable) || result.LeaseLost != 0 {
		t.Fatalf("classifyRecordOutcome() = recorded %v, error %v, result %#v", recorded, err, result)
	}
}

func TestClassifyRecordOutcomeToleratesOnlyLostLease(t *testing.T) {
	result := StepResult{}
	recorded, err := classifyRecordOutcome(ErrLeaseLost, &result)
	if recorded || err != nil || result.LeaseLost != 1 {
		t.Fatalf("classifyRecordOutcome() = recorded %v, error %v, result %#v", recorded, err, result)
	}
}
