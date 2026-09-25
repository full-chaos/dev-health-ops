package integrationsadmin

import (
	"net/http"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/synchandoff"
)

// syncTriggerBody is SyncTriggerRequest. A nil list is Python's None.
type syncTriggerBody struct {
	sourceIDs   []string
	datasetKeys []string
	fullResync  bool
	// sourceIDsSet is whether the body named source_ids (null is None).
	sourceIDsSet bool
	// datasetKeysSet is the same for dataset_keys.
	datasetKeysSet bool
}

func parseSyncTrigger(body pybody.Body) (syncTriggerBody, pybody.Errors) {
	var problems pybody.Errors
	object, ok := problems.Object(body)
	if !ok {
		return syncTriggerBody{}, problems
	}
	var out syncTriggerBody
	out.sourceIDsSet = present(object, "source_ids")
	out.datasetKeysSet = present(object, "dataset_keys")
	out.sourceIDs, _ = problems.OptionalStringList(object, "source_ids")
	out.datasetKeys, _ = problems.OptionalStringList(object, "dataset_keys")
	out.fullResync, _ = problems.DefaultedBool(object, "full_resync")
	return out, problems
}

// present reports whether the body carried the list (a JSON null is None).
func present(object *pyjson.Object, name string) bool {
	raw, ok := object.Get(name)
	return ok && raw != nil
}

// syncTrigger is POST /integrations/{integration_id}/sync.
func (h handlers) syncTrigger(w http.ResponseWriter, r *http.Request) {
	body, _ := policy.BodyFrom(r.Context())
	request, problems := parseSyncTrigger(body)
	if len(problems) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems), nil)
		return
	}
	orgID := orgOf(r)
	var trigger synchandoff.Trigger
	err := h.inTx(r.Context(), func(tx pgx.Tx) error {
		current, id, err := requireIntegration(r.Context(), tx, orgID, r.PathValue("integration_id"))
		if err != nil {
			return err
		}
		scope, err := handoffTarget(r.Context(), tx, orgID, id, current.IsActive)
		if err != nil {
			return err
		}
		input := synchandoff.MintInput{Mode: "incremental", TriggeredBy: "manual", SourceIDs: scope.sources, DatasetKeys: scope.datasets}
		if request.fullResync {
			input.Mode = "full_resync"
		}
		if request.sourceIDsSet {
			input.SourceIDs = append([]string{}, request.sourceIDs...)
		}
		if request.datasetKeysSet {
			input.DatasetKeys = append([]string{}, request.datasetKeys...)
		}
		trigger, err = synchandoff.Mint(r.Context(), tx, scope.config, input, h.Now())
		return err
	})
	if err != nil {
		h.fail(w, r, "hand off the sync", err)
		return
	}
	h.handoffResponse(w, r, r.PathValue("integration_id"), trigger)
}
