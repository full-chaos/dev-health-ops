package integrationsadmin

import (
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// syncTriggerBody is SyncTriggerRequest. A nil list is Python's None.
type syncTriggerBody struct {
	scope      triggerScope
	fullResync bool
}

func parseSyncTrigger(body pybody.Body) (syncTriggerBody, pybody.Errors) {
	var problems pybody.Errors
	object, ok := problems.Object(body)
	if !ok {
		return syncTriggerBody{}, problems
	}
	var out syncTriggerBody
	out.scope.sourceIDsSet = present(object, "source_ids")
	out.scope.datasetKeysSet = present(object, "dataset_keys")
	out.scope.sourceIDs, _ = problems.OptionalStringList(object, "source_ids")
	out.scope.datasetKeys, _ = problems.OptionalStringList(object, "dataset_keys")
	out.fullResync, _ = problems.DefaultedBool(object, "full_resync")
	return out, problems
}

// present reports whether the body carried the field (a JSON null is None).
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
	mode := "incremental"
	if request.fullResync {
		mode = "full_resync"
	}
	h.handOff(w, r, request.scope, mode, "manual", nil, nil)
}
