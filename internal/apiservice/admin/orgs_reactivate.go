package admin

import (
	"context"
	"errors"
	"net/http"
	"time"

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
// It also answers the state the accepted call set (nil when no call was made).
func (h *handlers) orgStatusInAuth(w http.ResponseWriter, r *http.Request, orgID uuid.UUID, active bool) (accepted *bool, ok bool) {
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
		return nil, true
	}
	ctx := r.Context()
	if h.orgState == nil {
		h.logger.ErrorContext(ctx, "admin: no auth state reader wired for the organization lifecycle client", "verb", verb)
		policy.WriteInternal(w)
		return nil, false
	}
	existing, err := h.store.orgByID(ctx, orgID)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: read organization before the auth call failed", "error", err, "verb", verb)
		policy.WriteInternal(w)
		return nil, false
	}
	if existing == nil {
		policy.WriteDetail(w, http.StatusNotFound, "Organization not found", nil)
		return nil, false
	}
	user := policy.UserFrom(ctx)
	if user == nil {
		policy.WriteInternal(w)
		return nil, false
	}
	state, err := h.orgState.State(ctx, orgID)
	if err != nil {
		h.logger.WarnContext(ctx, "admin: auth-service state read failed; nothing was written",
			"verb", verb, "refused", errors.Is(err, orgreactivate.ErrRefused))
		policy.WriteDetail(w, http.StatusServiceUnavailable, policy.ErrorDetail(sessionissue.UnavailableText), nil)
		return nil, false
	}
	if err := call(ctx, orgID, user.ID, &state.Revision); err != nil {
		if errors.Is(err, orgreactivate.ErrStateChanged) {
			h.writeStateChanged(w, r, orgID, verb)
			return nil, false
		}
		h.logger.WarnContext(ctx, "admin: auth-service did not change the organization; nothing was written",
			"verb", verb, "refused", errors.Is(err, orgreactivate.ErrRefused))
		policy.WriteDetail(w, http.StatusServiceUnavailable, policy.ErrorDetail(sessionissue.UnavailableText), nil)
		return nil, false
	}
	return &active, true
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

// orgAuthReReadTimeout bounds the one auth state read made while the organization row is locked; the request's own
// context does not bound it.
const orgAuthReReadTimeout = 3 * time.Second

// orgAuthStateFunc is the store's hook that answers the is_active to write while the row is locked.
type orgAuthStateFunc = func(context.Context) (bool, error)

// authStateHook is the store's read of auth's state under the row lock: the PATCH stores what auth holds, not the
// PATCH value. When the read fails, the state the accepted call set is stored and the failure is logged by verb and
// reason class only.
func (h *handlers) authStateHook(orgID uuid.UUID, accepted bool) orgAuthStateFunc {
	return func(ctx context.Context) (bool, error) {
		readCtx, cancel := context.WithTimeout(ctx, orgAuthReReadTimeout)
		defer cancel()
		state, err := h.orgState.State(readCtx, orgID)
		if err != nil {
			verb := "suspend"
			if accepted {
				verb = "reactivate"
			}
			reason := "error"
			switch {
			case errors.Is(err, context.DeadlineExceeded):
				reason = "timeout"
			case errors.Is(err, orgreactivate.ErrRefused):
				reason = "refused"
			case errors.Is(err, orgreactivate.ErrUnavailable):
				reason = "unavailable"
			}
			h.logger.WarnContext(ctx, "admin: auth state re-read failed after an accepted change", "verb", verb, "reason", reason)
			return accepted, nil
		}
		return state.Status == "active", nil
	}
}
