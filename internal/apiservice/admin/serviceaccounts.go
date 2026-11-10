package admin

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// maxServiceAccountNameLength is auth-service's bound on a service account
// name (maxServiceAccountNameBytes); auth-service checks its pattern.
const maxServiceAccountNameLength = 63

// The service account lifecycle routes keep no state of their own: each asks
// auth-service, which runs its actor rule and writes the account, the audit
// row and the outbox event in one commit, and answers its wire body; go-api
// passes that body through. They mount only with the switch on
// (AUTH_PROXY_SERVICE_ACCOUNT_KEYS): off, the paths are not routed and answer
// main's 404, byte for byte. The platform gate answers before a body is read.
func (h *handlers) serviceAccountRoutes() []httpapi.Route {
	if h.serviceAccounts == nil {
		return nil
	}
	return []httpapi.Route{
		{Method: http.MethodPost, Pattern: usersPrefix + "/service-accounts",
			Handler: h.guard.WrapPlatform(policy.ActionAdminUsers, h.guard.BodyAfter(http.HandlerFunc(h.createServiceAccount)))},
		{Method: http.MethodPost, Pattern: usersPrefix + "/service-accounts/{service_account_id}/disable",
			Handler: h.guard.WrapPlatform(policy.ActionAdminUsers, http.HandlerFunc(h.disableServiceAccount))},
		{Method: http.MethodPost, Pattern: usersPrefix + "/service-accounts/{service_account_id}/enable",
			Handler: h.guard.WrapPlatform(policy.ActionAdminUsers, http.HandlerFunc(h.enableServiceAccount))},
		{Method: http.MethodPost, Pattern: usersPrefix + "/service-accounts/{service_account_id}/owner",
			Handler: h.guard.WrapPlatform(policy.ActionAdminUsers, h.guard.BodyAfter(http.HandlerFunc(h.setServiceAccountOwner)))},
	}
}

// serviceAccountAnswers are the go-api answers of the lifecycle routes' auth
// refusals; the actor rule's own refusals are actorAnswers. A legacy users
// row (auth_provider "service") is no service account: auth-service answers
// service_account_not_found for its id.
var serviceAccountAnswers = withDeactivated(map[string]provisionAnswer{
	"service_account_not_found": {http.StatusNotFound, "Service account not found"},
	"organization_not_found":    {http.StatusNotFound, "Organization not found"},
	"organization_suspended":    {http.StatusConflict, "Organization is suspended"},
	"name_invalid":              {http.StatusBadRequest, "Invalid service account name"},
	"name_taken":                {http.StatusConflict, "Service account name is taken"},
	"owner_invalid":             {http.StatusConflict, "Owner must be an active user"},
	"owner_not_member":          {http.StatusConflict, "Owner is not a member of the organization"},
	"already_in_state":          {http.StatusConflict, "Service account is already in that state"},
	"state_changed":             {http.StatusConflict, "Service account changed; try again"},
	"users_not_owned":           {http.StatusConflict, "Service account is not in that organization"},
	"service_account_disabled":  {http.StatusConflict, "Service account is disabled"},
	"malformed":                 {http.StatusBadRequest, "Invalid service account request"},
})

// writeServiceAccountAnswer writes auth-service's answer, or the go-api answer
// of its refusal.
func (h *handlers) writeServiceAccountAnswer(ctx context.Context, w http.ResponseWriter, route string, answer json.RawMessage, err error) {
	if err != nil {
		h.writeProvisionRefusal(ctx, w, route, err, serviceAccountAnswers)
		return
	}
	value, decodeErr := pyjson.DecodeString(string(answer))
	if decodeErr != nil {
		h.writeProvisionUnavailable(w)
		return
	}
	policy.WriteJSON(w, http.StatusOK, value, nil)
}

func (h *handlers) createServiceAccount(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var org, owner uuid.UUID
	var name string
	if !keyBody(w, ctx, func(errs *pybody.Errors, object *pyjson.Object) {
		org, _ = errs.RequiredUUID(object, "organization_id")
		owner, _ = errs.RequiredUUID(object, "owner_principal_id")
		name, _ = errs.RequiredString(object, "name", 1, maxServiceAccountNameLength)
	}) {
		return
	}
	answer, err := h.serviceAccounts.CreateAccount(ctx, org, owner, policy.UserFrom(ctx).ID, name)
	h.writeServiceAccountAnswer(ctx, w, "create_service_account", answer, err)
}

func (h *handlers) disableServiceAccount(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	account, ok := serviceAccountID(w, r)
	if !ok {
		return
	}
	answer, err := h.serviceAccounts.DisableAccount(ctx, account, policy.UserFrom(ctx).ID)
	h.writeServiceAccountAnswer(ctx, w, "disable_service_account", answer, err)
}

func (h *handlers) enableServiceAccount(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	account, ok := serviceAccountID(w, r)
	if !ok {
		return
	}
	answer, err := h.serviceAccounts.EnableAccount(ctx, account, policy.UserFrom(ctx).ID)
	h.writeServiceAccountAnswer(ctx, w, "enable_service_account", answer, err)
}

func (h *handlers) setServiceAccountOwner(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var owner uuid.UUID
	if !keyBody(w, ctx, func(errs *pybody.Errors, object *pyjson.Object) {
		owner, _ = errs.RequiredUUID(object, "owner_principal_id")
	}) {
		return
	}
	account, ok := serviceAccountID(w, r)
	if !ok {
		return
	}
	answer, err := h.serviceAccounts.SetAccountOwner(ctx, account, owner, policy.UserFrom(ctx).ID)
	h.writeServiceAccountAnswer(ctx, w, "set_service_account_owner", answer, err)
}
