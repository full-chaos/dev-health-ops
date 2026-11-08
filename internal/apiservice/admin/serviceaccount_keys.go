package admin

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"strings"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// EnvServiceAccountKeys is the go-api switch of the service account key
// routes. Off, the routes stay mounted and answer 503 config_missing.
const EnvServiceAccountKeys = "AUTH_PROXY_SERVICE_ACCOUNT_KEYS"

// The service account key routes keep no state of their own: each asks
// auth-service, which runs its actor rule, writes the key, the audit row and
// the outbox event in one commit, and answers its wire body. go-api passes
// that body through; an issue or rotate receipt carries the key's secret, so
// the answer is never logged and never cached (no-store).
func (h *handlers) serviceAccountKeyRoutes() []httpapi.Route {
	return []httpapi.Route{
		{Method: http.MethodGet, Pattern: usersPrefix + "/service-accounts/{service_account_id}/keys",
			Handler: h.guard.WrapPlatform(policy.ActionAdminUsers, http.HandlerFunc(h.listServiceAccountKeys))},
		{Method: http.MethodPost, Pattern: usersPrefix + "/service-accounts/{service_account_id}/keys",
			Handler: h.bodyFirstPlatform(policy.ActionAdminUsers, http.HandlerFunc(h.issueServiceAccountKey))},
		{Method: http.MethodPost, Pattern: usersPrefix + "/service-accounts/{service_account_id}/keys/rotate",
			Handler: h.bodyFirstPlatform(policy.ActionAdminUsers, http.HandlerFunc(h.rotateServiceAccountKey))},
		{Method: http.MethodPost, Pattern: usersPrefix + "/service-accounts/keys/{credential_id}/revoke",
			Handler: h.bodyFirstPlatform(policy.ActionAdminUsers, http.HandlerFunc(h.revokeServiceAccountKey))},
	}
}

const (
	invalidServiceAccountIDDetail = "Invalid service account id"
	invalidCredentialIDDetail     = "Invalid credential id"
	maxRevokeReasonLength         = 32
)

// serviceAccountKeyAnswers are the go-api answers of the key routes' auth
// refusals. A legacy users row (auth_provider "service") is no service
// account: auth-service answers service_account_not_found for its id.
var serviceAccountKeyAnswers = map[string]provisionAnswer{
	"service_account_not_found": {http.StatusNotFound, "Service account not found"},
	"key_not_found":             {http.StatusNotFound, "Key not found"},
	"organization_not_found":    {http.StatusNotFound, "Organization not found"},
	"service_account_disabled":  {http.StatusConflict, "Service account is disabled"},
	"key_live":                  {http.StatusConflict, "Service account already has a live key"},
	"key_rotating":              {http.StatusConflict, "Service account key is already rotating"},
	"key_none":                  {http.StatusConflict, "Service account has no live key"},
	"expiry_invalid":            {http.StatusBadRequest, "Invalid key expiry"},
	"overlap_invalid":           {http.StatusBadRequest, "Invalid key overlap"},
	"reason_invalid":            {http.StatusBadRequest, "Invalid revoke reason"},
	"malformed":                 {http.StatusBadRequest, "Invalid key request"},
}

// serviceAccountKeysReady answers 503 config_missing when the switch is off.
func (h *handlers) serviceAccountKeysReady(w http.ResponseWriter) bool {
	if h.serviceAccountKeys == nil {
		policy.WriteConfigMissing(w, EnvServiceAccountKeys)
		return false
	}
	return true
}

func serviceAccountID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, ok := pathUUID(r, "service_account_id")
	if !ok {
		writeInvalidID(w, invalidServiceAccountIDDetail)
	}
	return id, ok
}

// writeServiceAccountKeyAnswer writes auth-service's answer, or the go-api
// answer of its refusal.
func (h *handlers) writeServiceAccountKeyAnswer(ctx context.Context, w http.ResponseWriter, route string, answer json.RawMessage, err error) {
	if err != nil {
		h.writeProvisionRefusal(ctx, w, route, err, serviceAccountKeyAnswers)
		return
	}
	value, decodeErr := pyjson.DecodeString(string(answer))
	if decodeErr != nil {
		h.writeProvisionUnavailable(w)
		return
	}
	policy.WriteJSON(w, http.StatusOK, value, nil)
}

func (h *handlers) listServiceAccountKeys(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if !h.serviceAccountKeysReady(w) {
		return
	}
	account, ok := serviceAccountID(w, r)
	if !ok {
		return
	}
	answer, err := h.serviceAccountKeys.ListKeys(ctx, account, policy.UserFrom(ctx).ID)
	h.writeServiceAccountKeyAnswer(ctx, w, "list_service_account_keys", answer, err)
}

// keyBody reads the request object; a body that fails validation is answered
// 422 here.
func keyBody(w http.ResponseWriter, ctx context.Context, read func(*pybody.Errors, *pyjson.Object)) bool {
	var errs pybody.Errors
	if object, ok := errs.Object(bodyFromContext(ctx)); ok {
		read(&errs, object)
	}
	if len(errs) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return false
	}
	return true
}

func optionalSeconds(errs *pybody.Errors, object *pyjson.Object, name string, minValue int64) *int64 {
	value, present := errs.OptionalBoundedInt(object, name, minValue, math.MaxInt32)
	if !present {
		return nil
	}
	return &value
}

func (h *handlers) issueServiceAccountKey(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var expires *int64
	if !keyBody(w, ctx, func(errs *pybody.Errors, object *pyjson.Object) {
		expires = optionalSeconds(errs, object, "expires_in_seconds", 1)
	}) || !h.serviceAccountKeysReady(w) {
		return
	}
	account, ok := serviceAccountID(w, r)
	if !ok {
		return
	}
	answer, err := h.serviceAccountKeys.IssueKey(ctx, account, policy.UserFrom(ctx).ID, expires)
	h.writeServiceAccountKeyAnswer(ctx, w, "issue_service_account_key", answer, err)
}

func (h *handlers) rotateServiceAccountKey(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var expires, overlap *int64
	if !keyBody(w, ctx, func(errs *pybody.Errors, object *pyjson.Object) {
		if _, present := object.Get("overlap_seconds"); !present {
			*errs = append(*errs, pybody.Error{Type: "missing", Loc: []pyjson.Value{"body", "overlap_seconds"}, Msg: "Field required", Input: object})
		} else if overlap = optionalSeconds(errs, object, "overlap_seconds", 0); overlap == nil && len(*errs) == 0 {
			*errs = append(*errs, pybody.Error{Type: "int_type", Loc: []pyjson.Value{"body", "overlap_seconds"}, Msg: "Input should be a valid integer", Input: nil})
		}
		expires = optionalSeconds(errs, object, "expires_in_seconds", 1)
	}) || !h.serviceAccountKeysReady(w) {
		return
	}
	account, ok := serviceAccountID(w, r)
	if !ok {
		return
	}
	answer, err := h.serviceAccountKeys.RotateKey(ctx, account, policy.UserFrom(ctx).ID, *overlap, expires)
	h.writeServiceAccountKeyAnswer(ctx, w, "rotate_service_account_key", answer, err)
}

func (h *handlers) revokeServiceAccountKey(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var reason string
	if !keyBody(w, ctx, func(errs *pybody.Errors, object *pyjson.Object) {
		reason, _ = errs.RequiredString(object, "reason", 1, maxRevokeReasonLength)
	}) || !h.serviceAccountKeysReady(w) {
		return
	}
	credential := r.PathValue("credential_id")
	bare, found := strings.CutPrefix(credential, "crd_")
	if id, err := uuid.Parse(bare); !found || err != nil || id.String() != bare {
		writeInvalidID(w, invalidCredentialIDDetail)
		return
	}
	answer, err := h.serviceAccountKeys.RevokeKey(ctx, credential, policy.UserFrom(ctx).ID, reason)
	h.writeServiceAccountKeyAnswer(ctx, w, "revoke_service_account_key", answer, err)
}
