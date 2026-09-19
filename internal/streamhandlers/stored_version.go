package streamhandlers

import (
	"context"

	"github.com/full-chaos/dev-health-ops/internal/storedversion"
)

// Stable log event names for the stream-handler writers' contract tables.
// "carried" is the normal path for a column the payload does not state;
// "refused" means a payload stated null for a terminal column that holds a
// value, which points at a stale or racing upstream read.
const (
	storedVersionCarriedEvent = "streamhandlers.stored_version.carried_forward"
	storedVersionRefusedEvent = "streamhandlers.stored_version.null_over_value_refused"
)

// In practice the read-then-insert window is shared only by writers of one
// key running at once: external ingest (one consumer) against provider sync
// on shared github/gitlab keys, and internal ingest replicas against each
// other or, for work items, against external ingest and provider sync.
func applyContract(
	ctx context.Context, conn productClickHouse, contract storedversion.Contract,
	orgID, insert string, rows []storedversion.Row,
) error {
	outcomes, err := contract.Apply(ctx, conn, orgID, insert, rows)
	if err != nil {
		return err
	}
	contract.Log(ctx, orgID, storedVersionCarriedEvent, storedVersionRefusedEvent, outcomes)
	return nil
}
