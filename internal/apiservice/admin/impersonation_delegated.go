package admin

import (
	"log/slog"
	"net/http"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
)

// impersonateStart is the start route's handler: go-api's own start, or, with a
// Delegator (AUTH_PROXY_IMPERSONATION), the start through auth-service.
func (h *handlers) impersonateStart() http.Handler {
	if h.delegator == nil {
		return http.HandlerFunc(h.startImpersonation)
	}
	return http.HandlerFunc(h.startDelegatedImpersonation)
}

// impersonateStop is the stop route's handler, chosen as impersonateStart is.
func (h *handlers) impersonateStop() http.Handler {
	if h.delegator == nil {
		return http.HandlerFunc(h.stopImpersonation)
	}
	return http.HandlerFunc(h.stopDelegatedImpersonation)
}

func (h *handlers) writeDelegatedUnavailable(w http.ResponseWriter) {
	policy.WriteDetail(w, http.StatusServiceUnavailable, policy.ErrorDetail(sessionissue.UnavailableText), nil)
}

// startDelegatedImpersonation is startImpersonation with auth-service the writer
// of the session: it checks the target as startImpersonation does, then asks
// auth-service to start a delegated session of the caller's own session.
// auth-service writes go-api's impersonation row and the start audit row, so
// this handler writes neither. It answers startImpersonation's members and the
// delegated tokens. Any failure to start one is a 503 that changes nothing here:
// there is no fallback to a go-api session.
func (h *handlers) startDelegatedImpersonation(w http.ResponseWriter, r *http.Request) {
	user := policy.UserFrom(r.Context())
	body := bodyFromContext(r.Context())

	var errs pybody.Errors
	object, ok := errs.Object(body)
	var targetUserIDRaw string
	if ok {
		targetUserIDRaw, _ = errs.RequiredString(object, "target_user_id", 0, 0)
	}
	if len(errs) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return
	}
	targetUserID, parseErr := uuid.Parse(targetUserIDRaw)
	if parseErr != nil {
		policy.WriteDetail(w, http.StatusBadRequest, "Invalid target_user_id", nil)
		return
	}
	adminUserID, parseErr := uuid.Parse(user.UserID)
	if parseErr != nil {
		policy.WriteDetail(w, http.StatusBadRequest, "Invalid user_id", nil)
		return
	}
	if targetUserID == adminUserID {
		policy.WriteDetail(w, http.StatusBadRequest, "Cannot impersonate yourself", nil)
		return
	}

	ctx := r.Context()
	target, storeErr := h.store.userByID(ctx, targetUserID)
	if storeErr != nil {
		h.logger.ErrorContext(ctx, "admin impersonate: target lookup failed", slog.Any("error", storeErr))
		policy.WriteInternal(w)
		return
	}
	if target == nil {
		policy.WriteDetail(w, http.StatusNotFound, "Target user not found", nil)
		return
	}
	if !target.IsActive {
		policy.WriteDetail(w, http.StatusBadRequest, "Target user is not active", nil)
		return
	}
	if target.IsSuperuser {
		policy.WriteDetail(w, http.StatusForbidden, "Cannot impersonate a superuser", nil)
		return
	}
	membership, storeErr := h.store.firstMembership(ctx, targetUserID)
	if storeErr != nil {
		h.logger.ErrorContext(ctx, "admin impersonate: membership lookup failed", slog.Any("error", storeErr))
		policy.WriteInternal(w)
		return
	}
	if membership == nil {
		policy.WriteDetail(w, http.StatusNotFound, "Target user has no organization membership", nil)
		return
	}
	if user.SessionID == nil {
		policy.WriteDetail(w, http.StatusConflict, "Impersonation needs an authentication-service session; sign in again", nil)
		return
	}

	started, err := h.delegator.StartDelegated(ctx, r, sessionissue.DelegatedRequest{
		BaseSessionID: *user.SessionID, TargetPrincipalID: targetUserID, OrganizationID: membership.OrgID,
		Reason: sessionissue.ReasonAdminImpersonation,
	})
	if err != nil {
		h.logger.ErrorContext(ctx, "admin impersonate: delegated session start failed", slog.Any("error", err))
		h.writeDelegatedUnavailable(w)
		return
	}

	targetEmail := target.Email
	h.cache.setActiveSession(ctx, user.UserID, &cachedImpersonationSession{
		ID:           started.SessionID.String(),
		AdminUserID:  adminUserID.String(),
		TargetUserID: targetUserID.String(),
		TargetOrgID:  membership.OrgID.String(),
		TargetRole:   membership.Role,
		TargetEmail:  &targetEmail,
		ExpiresAt:    started.ExpiresAt,
	})

	response := pyjson.NewObject()
	response.Set("status", "active")
	targetObject := pyjson.NewObject()
	targetObject.Set("id", target.ID.String())
	targetObject.Set("email", target.Email)
	targetObject.Set("org_id", membership.OrgID.String())
	targetObject.Set("role", membership.Role)
	response.Set("target_user", targetObject)
	response.Set("expires_at", pyTimeString(started.ExpiresAt))
	response.Set("access_token", started.Access)
	response.Set("token_type", "bearer")
	response.Set("expires_in", started.ExpiresIn)
	if started.SessionAccess != "" {
		response.Set("session_access_token", started.SessionAccess)
	}
	policy.WriteModel(w, http.StatusOK, response, nil)
}

// stopDelegatedImpersonation ends the caller's open delegated session at
// auth-service, which ends go-api's row and writes the stop audit row. With no
// open session it answers as stopImpersonation does.
func (h *handlers) stopDelegatedImpersonation(w http.ResponseWriter, r *http.Request) {
	user := policy.UserFrom(r.Context())
	adminUserID, parseErr := uuid.Parse(user.UserID)
	if parseErr != nil {
		policy.WriteDetail(w, http.StatusBadRequest, "Invalid user_id", nil)
		return
	}
	ctx := r.Context()
	active, err := h.store.activeImpersonationSession(ctx, adminUserID.String())
	if err != nil {
		h.logger.ErrorContext(ctx, "admin impersonate stop: lookup failed", slog.Any("error", err))
		policy.WriteInternal(w)
		return
	}
	if active == nil {
		policy.WriteDetail(w, http.StatusBadRequest, "No active impersonation session", nil)
		return
	}
	if user.SessionID == nil {
		policy.WriteDetail(w, http.StatusConflict, "Impersonation needs an authentication-service session; sign in again", nil)
		return
	}
	sessionID, err := uuid.Parse(active.ID)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin impersonate stop: session id unreadable", slog.Any("error", err))
		policy.WriteInternal(w)
		return
	}
	if err := h.delegator.EndDelegated(ctx, r, sessionID); err != nil {
		h.logger.ErrorContext(ctx, "admin impersonate stop: delegated session end failed", slog.Any("error", err))
		h.writeDelegatedUnavailable(w)
		return
	}
	h.cache.setActiveSession(ctx, user.UserID, nil)

	response := pyjson.NewObject()
	response.Set("status", "stopped")
	policy.WriteModel(w, http.StatusOK, response, nil)
}
