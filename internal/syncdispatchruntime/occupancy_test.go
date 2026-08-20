package syncdispatchruntime

import (
	"reflect"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
)

// TestRiverQueueOccupancyCoversEveryPublishableKind is the CHAOS-3938 class
// guard at this plane's boundary: every kind this package can put in a River
// queue must be declared as occupying that queue, so a reader of river_job can
// always resolve it. Convert is the only door into a published job -- the
// publisher calls nothing else -- so binding the occupancy to the exact set
// Convert accepts leaves no publishable kind undeclared.
func TestRiverQueueOccupancyCoversEveryPublishableKind(t *testing.T) {
	t.Parallel()
	reference := DomainReference{
		OrganizationID: "0f9d3a1c-2b4e-4d6a-8c1f-5e7b9a0d3c42",
		SyncRunID:      "1a2b3c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d",
	}
	occupancy := RiverQueueOccupancy()
	declared := make(map[string]QueueOccupancy, len(occupancy))
	for _, occupant := range occupancy {
		if _, duplicate := declared[occupant.Kind]; duplicate {
			t.Fatalf("kind %q declared twice", occupant.Kind)
		}
		declared[occupant.Kind] = occupant
	}

	for _, kind := range syncdispatchcontract.Kinds() {
		args, err := Convert(Claim{
			OutboxID:        "2b3c4d5e-6f7a-4b8c-9d0e-1f2a3b4c5d6e",
			Kind:            kind,
			RouteGeneration: 1,
			DeliveryAttempt: 1,
		}, reference)
		if err != nil {
			t.Fatalf("Convert rejected frozen kind %q: %v", kind, err)
		}
		occupant, ok := declared[args.Kind()]
		if !ok {
			t.Fatalf("publishable kind %q occupies a River queue but is not declared "+
				"in RiverQueueOccupancy; the startup contract-version check cannot resolve it", args.Kind())
		}
		if occupant.Queue != syncdispatchcontract.RiverQueue {
			t.Fatalf("kind %q declared for queue %q, want %q",
				args.Kind(), occupant.Queue, syncdispatchcontract.RiverQueue)
		}
		if !reflect.DeepEqual(occupant.SupportedVersions, []int{ContractVersionV1}) {
			t.Fatalf("kind %q declares versions %v, want %v",
				args.Kind(), occupant.SupportedVersions, []int{ContractVersionV1})
		}
		delete(declared, args.Kind())
	}
	if len(declared) != 0 {
		t.Fatalf("RiverQueueOccupancy declares kinds nothing can publish: %v", declared)
	}
}

// TestPublisherOptionsAcceptOnlyTheDeclaredRiverQueue closes the other half of
// the class. A well-formed queue name that is not the declared one used to be
// accepted, so composition could publish into a queue the readiness reader had
// no way to enumerate -- which is exactly what happened when the reconciler
// passed a bare "sync" literal that nothing tied to this plane.
func TestPublisherOptionsAcceptOnlyTheDeclaredRiverQueue(t *testing.T) {
	t.Parallel()
	if !(PublisherOptions{Queue: syncdispatchcontract.RiverQueue, MaxAttempts: 5}).valid() {
		t.Fatal("declared River queue was rejected")
	}
	for _, queue := range []string{"sync_provider", "sync2", "metrics", ""} {
		if (PublisherOptions{Queue: queue, MaxAttempts: 5}).valid() {
			t.Fatalf("queue %q was accepted; only %q is resolvable by the readiness check",
				queue, syncdispatchcontract.RiverQueue)
		}
	}
}
