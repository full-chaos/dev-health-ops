package authapi

import (
	"errors"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/org"
)

// PatternOrgState is the organization state read route, mounted only when the embedded endpoint-profile document holds
// its row: without the row it answers 404 and the service starts.
const PatternOrgState = "/internal/v1/organizations/state"

const opOrgState = "organization_state"

type stateAnswer struct {
	OrganizationID string `json:"organization_id"`
	Status         string `json:"status"`
	Revision       int64  `json:"revision"`
}

// withStateRoute mounts the state route when the profile document holds a row listing every class the route admits,
// else logs one line naming the route.
func (h provisioningHandlers) withStateRoute(routes []httpapi.Route, rows []byte) []httpapi.Route {
	if rowsAcceptFor(rows, []string{PatternOrgState}, admittedClasses(PatternOrgState, h.Workload != nil)) != nil {
		h.Logger.Info("auth provisioning route not mounted: no endpoint-profile row", "route", PatternOrgState)
		return routes
	}
	return append(routes, httpapi.Route{Method: http.MethodPost, Pattern: PatternOrgState, Handler: http.HandlerFunc(h.orgState)})
}

// orgState answers an organization's status and aggregate revision, read in one statement, for go-api's admin PATCH to
// send as if_revision. It takes the session issuer service credential or the go-api workload token, admitted as the
// other provisioning routes are. Read-only: no actor, no audit row.
func (h provisioningHandlers) orgState(w http.ResponseWriter, r *http.Request) {
	body, _, ok := h.admit(w, r, opOrgState, "organization_id")
	if !ok {
		return
	}
	orgID, err := body.uuid("organization_id")
	if err != nil {
		h.rec.refuse(w, r, opOrgState, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	ctx := r.Context()
	found, err := h.Orgs.Get(ctx, orgID)
	if err != nil {
		answer := unavailable(reasonStoreUnavailable)
		if errors.Is(err, org.ErrNotFound) {
			answer = refusal{http.StatusNotFound, reasonOrganizationNotFound}
		}
		h.rec.refuse(w, r, opOrgState, answer, failure(answer, err))
		return
	}
	h.rec.record(ctx, opOrgState, "success", reasonAccepted, nil)
	writeJSON(w, http.StatusOK, stateAnswer{OrganizationID: orgID.String(), Status: string(found.Status), Revision: found.Revision})
}
