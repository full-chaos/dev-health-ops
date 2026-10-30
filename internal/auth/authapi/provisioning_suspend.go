package authapi

import (
	"context"
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/org"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
)

// PatternOrgSuspend is the organization suspend route. It is mounted only when
// the embedded endpoint-profile document holds its row (darkPatterns): without
// the row it answers 404, the other provisioning routes are unaffected, and the
// service starts.
const PatternOrgSuspend = "/internal/v1/organizations/suspend"

const opOrgSuspend = "organization_suspend"

// darkPatterns are the routes mounted only when the endpoint-profile document
// holds their row. A missing row of provisioningPatterns refuses the whole
// switch; a missing row here leaves only that route unmounted.
var darkPatterns = []string{PatternOrgSuspend}

// OrganizationLifecycle ends and begins an organization's suspension, authorizing the actor under the row lock
// (*org.Store).
type OrganizationLifecycle interface {
	OrganizationReactivator
	SuspendAuthorized(ctx context.Context, id uuid.UUID, ifRevision *int64, a org.Actor, authz org.Authorize) (changed bool, err error)
	// Get reads the organization's status and revision in one statement.
	Get(ctx context.Context, id uuid.UUID) (*org.Organization, error)
}

// maxIfRevision bounds the optional if_revision of the suspend and reactivate routes.
const maxIfRevision = 1<<63 - 1

type suspendAnswer struct {
	OrganizationID string `json:"organization_id"`
	Status         string `json:"status"`
	Suspended      bool   `json:"suspended"`
}

// orgSuspend suspends an organization: a superuser actor only (go-api's PATCH /admin/orgs/{org_id} is
// policy.Superuser), read through the users seam before anything is read for the request and again inside the verb's
// transaction after the row lock. An organization that is already suspended answers 200 with suspended false and
// writes nothing. A fault of the store, an actor with no auth.principals row included, is the fail-closed 503.
func (h provisioningHandlers) orgSuspend(w http.ResponseWriter, r *http.Request) {
	body, caller, ok := h.admit(w, r, opOrgSuspend, "organization_id", "actor_principal_id", "if_revision")
	if !ok {
		return
	}
	orgID, err1 := body.uuid("organization_id")
	actor, err2 := body.uuid("actor_principal_id")
	ifRevision, err3 := body.optionalInt("if_revision", 0, maxIfRevision)
	if errors.Join(err1, err2, err3) != nil {
		h.rec.refuse(w, r, opOrgSuspend, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	if !h.precheck(w, r, opOrgSuspend, actor, orgID, true) {
		return
	}
	ctx := r.Context()
	changed, err := h.Orgs.SuspendAuthorized(ctx, orgID, ifRevision, org.Actor{PrincipalID: &actor, RequestID: requestid.Audit(ctx), Caller: caller},
		func(ctx context.Context, tx audit.TxOps, orgID uuid.UUID) error {
			_, err := h.actorRule(ctx, tx, actor, orgID, true)
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
			answer = refusal{http.StatusForbidden, reasonSuperuserRequired}
		case errors.Is(err, org.ErrNotFound):
			answer = refusal{http.StatusNotFound, reasonOrganizationNotFound}
		case errors.Is(err, org.ErrRevisionConflict):
			answer = refusal{http.StatusConflict, reasonStateChanged}
		}
		h.rec.refuse(w, r, opOrgSuspend, answer, failure(answer, err))
		return
	}
	h.rec.record(ctx, opOrgSuspend, "success", reasonAccepted, nil)
	writeJSON(w, http.StatusOK, suspendAnswer{OrganizationID: orgID.String(), Status: "suspended", Suspended: changed})
}
