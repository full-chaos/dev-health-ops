// Package acrcredentials is go-api's session route that revokes an MCP-bound acr credential by id for a signed-in
// user who lost the token (CHAOS-12959). go-api authorizes the human; acr decides whether the credential admits that
// actor: an organization admin, or the user who approved the credential. A platform admin (admin.orgs) is the
// organization admin of any organization, with or without a membership row there (product rule 1).
//
// The route is a Go-only route (no FastAPI ancestor), so its answers are written with policy.WriteDetail, never
// WriteModel. Routes is not mounted by apiservice until the route's endpoint-profile row lands (D6617).
package acrcredentials

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/acrrevoke"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

// RevokePattern is the route's pattern.
const RevokePattern = "/api/v1/acr/credentials/{credential_id}/revoke"

const (
	msgNotFound      = "Credential not found"
	msgConflict      = "Credential is already revoked"
	msgUnavailable   = "Credential service unavailable"
	msgNotMember     = "Organization access denied"
	msgImpersonation = "Not allowed in an impersonation session"
)

// Members is the live membership read; policy.PGStore has it. found is false when the user holds no membership row
// of an active organization.
type Members interface {
	Membership(ctx context.Context, userID, orgID uuid.UUID) (role string, found bool, err error)
}

// Revoker is acr's on-behalf revoke; *acrrevoke.Client has it.
type Revoker interface {
	Revoke(ctx context.Context, orgID, credentialID, actorType, actorID string) error
}

// Deps builds the routes.
type Deps struct {
	Guard   *policy.Guard
	Members Members
	Revoker Revoker
	Logger  *slog.Logger
}

// Routes returns the area's one route.
func Routes(deps Deps) []httpapi.Route {
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}
	h := handlers{members: deps.Members, revoker: deps.Revoker, logger: logger}
	return []httpapi.Route{
		{Method: http.MethodPost, Pattern: RevokePattern, EmptyBody: true,
			Handler: deps.Guard.Wrap(policy.Authenticated, deps.Guard.RefuseImpersonation(http.HandlerFunc(h.revoke)))},
	}
}

type handlers struct {
	members Members
	revoker Revoker
	logger  *slog.Logger
}

var noStore = http.Header{"Cache-Control": {"no-store"}}

func (h handlers) answer(w http.ResponseWriter, r *http.Request, status int, detail, outcome, actorType string) {
	h.logger.InfoContext(r.Context(), "acr credential revoke answered", slog.String("outcome", outcome),
		slog.String("actor_type", actorType), slog.Int("status", status))
	if status == http.StatusNoContent {
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(status)
		return
	}
	policy.WriteDetail(w, status, detail, noStore)
}

// revoke runs behind policy.Authenticated and RefuseImpersonation. The organization is the token's org claim, as on
// the admin routes; the actor kind is the admin for a holder of admin.orgs (no membership read, as
// ensureOrgAdminAccess), else the caller's live membership row there, never the role claim.
func (h handlers) revoke(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	user := policy.UserFrom(ctx)
	switch {
	case !policy.ImpersonationDecided(ctx):
		// Only the api listener's Scope decides impersonation; a request it did not decide is never served.
		h.answer(w, r, http.StatusServiceUnavailable, msgUnavailable, "undecided", "")
		return
	case user.ImpersonatedBy != nil:
		// A delegated session's bearer is refused even if a row later admits delegated bearers to the guard.
		h.answer(w, r, http.StatusForbidden, msgImpersonation, "delegated", "")
		return
	}
	credentialID := r.PathValue("credential_id")
	if !acrrevoke.ValidCredentialID(credentialID) {
		h.answer(w, r, http.StatusNotFound, msgNotFound, "malformed_id", "")
		return
	}
	org, ok := policy.ParsePyUUID(user.OrgID)
	if !ok {
		h.answer(w, r, http.StatusForbidden, msgNotMember, "no_org", "")
		return
	}
	if h.members == nil || h.revoker == nil {
		h.answer(w, r, http.StatusServiceUnavailable, msgUnavailable, "not_configured", "")
		return
	}
	actorType, ok := h.actor(w, r, user, org)
	if !ok {
		return
	}
	err := h.revoker.Revoke(ctx, org.String(), credentialID, actorType, user.ID.String())
	switch {
	case err == nil:
		h.answer(w, r, http.StatusNoContent, "", "revoked", actorType)
	case errors.Is(err, acrrevoke.ErrNotFound):
		h.answer(w, r, http.StatusNotFound, msgNotFound, "not_found", actorType)
	case errors.Is(err, acrrevoke.ErrConflict):
		h.answer(w, r, http.StatusConflict, msgConflict, "conflict", actorType)
	default:
		h.logger.WarnContext(ctx, "acr credential revoke failed", slog.String("error", err.Error()))
		h.answer(w, r, http.StatusServiceUnavailable, msgUnavailable, "acr_failed", actorType)
	}
}

// actor is the acr actor type of user in org, or false once it has answered.
func (h handlers) actor(w http.ResponseWriter, r *http.Request, user *policy.User, org uuid.UUID) (string, bool) {
	held, err := policy.HoldsPlatform(r.Context(), user, rule.ActionAdminOrgs)
	if err != nil {
		policy.WritePlatformDecisionRefusal(w, err)
		return "", false
	}
	if held {
		return acrrevoke.ActorAdmin, true
	}
	role, found, err := h.members.Membership(r.Context(), user.ID, org)
	switch {
	case err != nil:
		h.answer(w, r, http.StatusServiceUnavailable, msgUnavailable, "membership_read_failed", "")
		return "", false
	case !found:
		h.answer(w, r, http.StatusForbidden, msgNotMember, "not_member", "")
		return "", false
	case role == "owner" || role == "admin":
		return acrrevoke.ActorAdmin, true
	}
	return acrrevoke.ActorUser, true
}
