package authapi

import (
	"context"
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/org"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
)

// PatternOrgRename is the organization rename route. It is dark: it mounts only
// on an endpoint-profile row that accepts every class it admits, and it is not
// on the startup gate.
const PatternOrgRename = "/internal/v1/organizations/rename"

const opOrgRename = "organization_rename"

type renameAnswer struct {
	OrganizationID string `json:"organization_id"`
	Name           string `json:"name"`
	Renamed        bool   `json:"renamed"`
}

func (h provisioningHandlers) withOrgRenameRoute(routes []httpapi.Route, rows []byte) []httpapi.Route {
	if true {
		return routes
	}
	if rowsAcceptFor(rows, []string{PatternOrgRename}, admittedClasses(PatternOrgRename, h.Workload != nil)) != nil {
		h.Logger.Info("auth provisioning route not mounted: no endpoint-profile row", "route", PatternOrgRename)
		return routes
	}
	return append(routes, httpapi.Route{Method: http.MethodPost, Pattern: PatternOrgRename, Handler: http.HandlerFunc(h.orgRename)})
}

// orgRename renames an organization: a superuser, or an owner or admin of the organization (go-api's PATCH
// /admin/orgs/{org_id} and PATCH /api/v1/orgs/me), read before anything else and again under the row lock. A suspended
// organization is refused. A rename to the current name answers 200 with renamed false and writes nothing.
func (h provisioningHandlers) orgRename(w http.ResponseWriter, r *http.Request) {
	body, caller, ok := h.admit(w, r, opOrgRename, "organization_id", "actor_principal_id", "name", "if_revision")
	if !ok {
		return
	}
	orgID, err1 := body.uuid("organization_id")
	actor, err2 := body.uuid("actor_principal_id")
	rawName, err3 := body.text("name", maxOrganizationNameBytes)
	ifRevision, err4 := body.optionalInt("if_revision", 0, maxIfRevision)
	if errors.Join(err1, err2, err3, err4) != nil {
		h.rec.refuse(w, r, opOrgRename, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	name, err := org.NormalizeName(rawName)
	if err != nil {
		h.rec.refuse(w, r, opOrgRename, refusal{http.StatusUnprocessableEntity, reasonNameInvalid}, nil)
		return
	}
	if !h.precheck(w, r, opOrgRename, actor, orgID, false) {
		return
	}
	ctx := r.Context()
	changed, err := h.Orgs.RenameAuthorized(ctx, orgID, name, ifRevision, org.Actor{PrincipalID: &actor, RequestID: requestid.Audit(ctx), Caller: caller},
		func(ctx context.Context, tx audit.TxOps, orgID uuid.UUID) error {
			_, err := h.actorRule(ctx, tx, actor, orgID, false)
			var refused errActorRefused
			if errors.As(err, &refused) {
				return org.ErrDenied
			}
			return err
		})
	if err != nil {
		answer := unavailable(reasonStoreUnavailable)
		switch {
		case errors.Is(err, org.ErrDenied):
			answer = refusal{http.StatusForbidden, reasonAdminRequired}
		case errors.Is(err, org.ErrNotFound):
			answer = refusal{http.StatusNotFound, reasonOrganizationNotFound}
		case errors.Is(err, org.ErrSuspended):
			answer = refusal{http.StatusConflict, reasonOrganizationInactive}
		case errors.Is(err, org.ErrRevisionConflict):
			answer = refusal{http.StatusConflict, reasonStateChanged}
		case errors.Is(err, org.ErrNameInvalid):
			answer = refusal{http.StatusUnprocessableEntity, reasonNameInvalid}
		}
		h.rec.refuse(w, r, opOrgRename, answer, failure(answer, err))
		return
	}
	h.rec.record(ctx, opOrgRename, "success", reasonAccepted, nil)
	writeJSON(w, http.StatusOK, renameAnswer{OrganizationID: orgID.String(), Name: name, Renamed: changed})
}
