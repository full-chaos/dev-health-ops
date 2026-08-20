package syncdispatchruntime

import (
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
)

// QueueOccupancy is one (River queue, kind) pair this plane may leave rows for
// in river.river_job, together with the complete contract-version window its
// consumers accept.
//
// It exists because river.river_job is shared by two route planes that are
// deliberately NOT one registry: the bounded jobs registry
// (contracts/jobs/v1/registry.json, projected through jobruntime.Descriptor)
// and the sync-dispatch transport routes
// (contracts/sync-dispatch/v1/transport-routes.json, this package). Anything
// that READS the table -- as opposed to executing a kind -- has to resolve
// both, and until CHAOS-3938 the startup contract-version check resolved only
// the first, so every pending dispatch_sync_run row read as an unsupported
// contract version and refused readiness.
type QueueOccupancy struct {
	Queue             string
	Kind              string
	SupportedVersions []int
}

// RiverQueueOccupancy returns every queue/kind/version triple this plane can
// put in a River queue.
//
// It is derived from syncdispatchcontract.Kinds() and the single publisher
// queue, not hand-listed, so a new frozen kind cannot arrive here unresolved:
// the same edit that makes a kind publishable makes it resolvable. That
// derivation is the guard -- registering one missing kind by hand would have
// left the next one to trip the identical wire.
//
// This is not a capability, route-activation, or executable-readiness claim:
// it says only "a row of this shape in this queue is legitimate", which is
// exactly the question a reader of river_job needs answered. It deliberately
// stays independent of whether the durable route currently points at River,
// because rows published before a rollback to Celery are still legitimately
// sitting in the queue afterwards.
func RiverQueueOccupancy() []QueueOccupancy {
	kinds := syncdispatchcontract.Kinds()
	occupancy := make([]QueueOccupancy, 0, len(kinds))
	for _, kind := range kinds {
		occupancy = append(occupancy, QueueOccupancy{
			Queue:             syncdispatchcontract.RiverQueue,
			Kind:              kind,
			SupportedVersions: []int{ContractVersionV1},
		})
	}
	return occupancy
}
