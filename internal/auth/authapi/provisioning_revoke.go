package authapi

import (
	"context"
	"errors"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/invite"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
)

// PatternInviteRevoke is the individual invite revoke route. It is handled
// here but not mounted yet: ProvisioningRoutes serves it once its
// endpoint-profile row exists.
const PatternInviteRevoke = "/internal/v1/invites/revoke"

const (
	opInviteRevoke         = "invite_revoke"
	reasonInviteNotFound   = "invite_not_found"
	reasonInviteNotPending = "invite_not_pending"
)

// InviteRevoker is the invite aggregate's revoke (*invite.Store).
type InviteRevoker interface {
	Revoke(ctx context.Context, in invite.RevokeInput) error
}

// revokeRoute is the invite revoke route on revoker. Nothing mounts it yet.
func (h provisioningHandlers) revokeRoute(revoker InviteRevoker) httpapi.Route {
	return httpapi.Route{Method: http.MethodPost, Pattern: PatternInviteRevoke, Handler: http.HandlerFunc(h.inviteRevoke(revoker))}
}

type revokeAnswer struct {
	InviteID string `json:"invite_id"`
	Status   string `json:"status"`
}

// inviteRevoke ends one pending invite. The actor rule runs on the pool
// before the invite is looked up, so a refused actor learns nothing about it,
// and again inside the revoke after its locks (the actor's and the inviter's
// rows, then the organization row). A repeat is a 200 no-op.
func (h provisioningHandlers) inviteRevoke(revoker InviteRevoker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, caller, ok := h.admit(w, r, opInviteRevoke, "organization_id", "invite_id", "actor_principal_id")
		if !ok {
			return
		}
		orgID, err1 := body.uuid("organization_id")
		inviteID, err2 := body.uuid("invite_id")
		actor, err3 := body.uuid("actor_principal_id")
		if errors.Join(err1, err2, err3) != nil {
			h.rec.refuse(w, r, opInviteRevoke, refusal{http.StatusBadRequest, reasonMalformed}, nil)
			return
		}
		if !h.precheck(w, r, opInviteRevoke, actor, orgID, false) {
			return
		}
		err := revoker.Revoke(r.Context(), invite.RevokeInput{OrgID: orgID, InviteID: inviteID,
			Actor:     invite.Actor{PrincipalID: &actor, RequestID: requestid.Audit(r.Context()), Caller: caller},
			Authorize: h.inviteAuthorize(actor)})
		if err != nil {
			answer := inviteRefusal(err)
			h.rec.refuse(w, r, opInviteRevoke, answer, failure(answer, err))
			return
		}
		h.rec.record(r.Context(), opInviteRevoke, "success", reasonAccepted, nil)
		writeJSON(w, http.StatusOK, revokeAnswer{InviteID: inviteID.String(), Status: string(invite.StatusRevoked)})
	}
}
