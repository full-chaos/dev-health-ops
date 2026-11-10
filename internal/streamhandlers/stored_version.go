package streamhandlers

import (
	"context"
	"fmt"

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
// on shared keys, and for work items against other external writers.
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

func externalWriter(kind, system string) (storedversion.Spec, error) {
	contract, ok := externalContract(kind, system)
	if !ok {
		return storedversion.Spec{}, fmt.Errorf("%s has no contract", kind)
	}
	query, err := externalInsertQuery(kind)
	if err != nil {
		return storedversion.Spec{}, err
	}
	extended, _, err := withContractColumns(query, contract)
	if err != nil {
		return storedversion.Spec{}, err
	}
	return storedversion.Spec{
		Name: "external " + kind + " " + system, Contract: contract, Insert: extended,
		Carry: func(payload map[string]any) map[string]bool { return externalCarry(contract, payload) },
	}, nil
}

// StoredVersionSpecs lists every stream-handler writer of each in-scope
// table, keyed by table, for the stored-version invariant enumeration.
func StoredVersionSpecs() (map[string][]storedversion.Spec, error) {
	var firstErr error
	external := func(kind, system string) storedversion.Spec {
		spec, err := externalWriter(kind, system)
		if err != nil && firstErr == nil {
			firstErr = err
		}
		return spec
	}
	specs := map[string][]storedversion.Spec{
		"git_pull_requests": {
			external("pull_request.v1", "github"),
		},
		"git_pull_request_reviews": {
			external("review.v1", "github"),
		},
		"git_commits": {
			external("commit.v1", "github"),
		},
		"work_items": {
			external("work_item.v1", "jira"), external("work_item.v1", "github"),
			external("work_item.v1", "gitlab"), external("work_item.v1", "linear"),
		},
		"repos":      {external("repository.v1", "github")},
		"identities": {external("identity.v1", "github")},
	}
	return specs, firstErr
}
