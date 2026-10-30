package admin

import (
	"context"
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/orgreactivate"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
)

// orgStatusInAuth is the first step of a PATCH that carries is_active when
// AUTH_PROXY_ORG_PROVISIONING is on: a suspension in the auth schema begins and
// ends only by auth-service's suspend and reactivate routes, so the call runs
// before go-api writes its own row (true calls reactivate, false calls suspend).
// With no client for the direction (the switch is off) it does nothing and reads
// nothing. Otherwise it reads auth's state first and sends the revision it read
// as if_revision, so a change that lands in between is a 409 state_changed and
// nothing is written: one re-read for the answer only, never a retry. It answers
// and returns false when the PATCH must not go on: the organization is missing
// (404, as today), the read failed (500), the state moved (409), or auth-service
// refused or is down (503, nothing written: fail closed). The call is made
// whatever status the read saw: a change of state between the read and the write
// must not let the write set is_active with no auth call, and for an organization
// already in that state auth answers a no-op, still guarded by the revision.
func (h *handlers) orgStatusInAuth(w http.ResponseWriter, r *http.Request, orgID uuid.UUID, active bool) bool {
	var call func(ctx context.Context, orgID, actor uuid.UUID, ifRevision *int64) error
	verb := "suspend"
	if active {
		verb = "reactivate"
		if h.orgReactivator != nil {
			call = h.orgReactivator.Reactivate
		}
	} else if h.orgSuspender != nil {
		call = h.orgSuspender.Suspend
	}
	if call == nil {
		return true
	}
	ctx := r.Context()
	if h.orgState == nil {
		h.logger.ErrorContext(ctx, "admin: no auth state reader wired for the organization lifecycle client", "verb", verb)
		policy.WriteInternal(w)
		return false
	}
	existing, err := h.store.orgByID(ctx, orgID)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: read organization before the auth call failed", "error", err, "verb", verb)
		policy.WriteInternal(w)
		return false
	}
	if existing == nil {
		policy.WriteDetail(w, http.StatusNotFound, "Organization not found", nil)
		return false
	}
	user := policy.UserFrom(ctx)
	if user == nil {
		policy.WriteInternal(w)
		return false
	}
	state, err := h.orgState.State(ctx, orgID)
	if err != nil {
		h.logger.WarnContext(ctx, "admin: auth-service state read failed; nothing was written",
			"verb", verb, "refused", errors.Is(err, orgreactivate.ErrRefused))
		policy.WriteDetail(w, http.StatusServiceUnavailable, policy.ErrorDetail(sessionissue.UnavailableText), nil)
		return false
	}
	if err := call(ctx, orgID, user.ID, &state.Revision); err != nil {
		if errors.Is(err, orgreactivate.ErrStateChanged) {
			h.writeStateChanged(w, r, orgID, verb)
			return false
		}
		h.logger.WarnContext(ctx, "admin: auth-service did not change the organization; nothing was written",
			"verb", verb, "refused", errors.Is(err, orgreactivate.ErrRefused))
		policy.WriteDetail(w, http.StatusServiceUnavailable, policy.ErrorDetail(sessionissue.UnavailableText), nil)
		return false
	}
	return true
}

// writeStateChanged answers 409: the organization changed in auth-service after the state the request read. It
// re-reads once, for the answer's text only; it never writes and never calls auth-service again to change anything.
func (h *handlers) writeStateChanged(w http.ResponseWriter, r *http.Request, orgID uuid.UUID, verb string) {
	ctx := r.Context()
	message := "Organization state changed; retry the request"
	if now, err := h.orgState.State(ctx, orgID); err == nil {
		message += " (current status: " + now.Status + ")"
	}
	h.logger.InfoContext(ctx, "admin: organization state changed since it was read; nothing was written", "verb", verb)
	policy.WriteDetail(w, http.StatusConflict, message, nil)
}
