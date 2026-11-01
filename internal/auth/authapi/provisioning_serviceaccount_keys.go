package authapi

import (
	"net/http"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/serviceaccount"
)

// The service account key routes. Each is mounted only when the endpoint-profile document holds a row accepting every
// class it admits (withServiceAccountKeyRoutes); without the row it answers 404 and the service starts. They are not on
// the startup gate (provisioningPatterns) until their rows ship.
const PatternServiceAccountKeyIssue = "/internal/v1/service-accounts/keys/issue"
const PatternServiceAccountKeyRotate = "/internal/v1/service-accounts/keys/rotate"
const PatternServiceAccountKeyRevoke = "/internal/v1/service-accounts/keys/revoke"
const PatternServiceAccountKeyList = "/internal/v1/service-accounts/keys/list"

const (
	opServiceAccountKeyIssue  = "service_account_key_issue"
	opServiceAccountKeyRotate = "service_account_key_rotate"
	opServiceAccountKeyRevoke = "service_account_key_revoke"
	opServiceAccountKeyList   = "service_account_key_list"
)

// Refusal codes of the key routes.
const (
	reasonServiceAccountDisabled = "service_account_disabled"
	reasonKeyLive                = "key_live"
	reasonKeyRotating            = "key_rotating"
	reasonKeyNone                = "key_none"
	reasonKeyNotFound            = "key_not_found"
	reasonExpiryInvalid          = "expiry_invalid"
	reasonOverlapInvalid         = "overlap_invalid"
	reasonRevokeReasonInvalid    = "reason_invalid"
)

// withServiceAccountKeyRoutes mounts each key route whose profile row lists every class it admits, and logs one line
// naming each route it leaves out.
func (h provisioningHandlers) withServiceAccountKeyRoutes(routes []httpapi.Route, rows []byte) []httpapi.Route {
	for _, r := range []httpapi.Route{
		{Method: http.MethodPost, Pattern: PatternServiceAccountKeyIssue, Handler: http.HandlerFunc(h.serviceAccountKeyIssue)},
		{Method: http.MethodPost, Pattern: PatternServiceAccountKeyRotate, Handler: http.HandlerFunc(h.serviceAccountKeyRotate)},
		{Method: http.MethodPost, Pattern: PatternServiceAccountKeyRevoke, Handler: http.HandlerFunc(h.serviceAccountKeyRevoke)},
		{Method: http.MethodPost, Pattern: PatternServiceAccountKeyList, Handler: http.HandlerFunc(h.serviceAccountKeyList)},
	} {
		if rowsAcceptFor(rows, []string{r.Pattern}, admittedClasses(r.Pattern, h.Workload != nil)) != nil {
			h.Logger.Info("auth provisioning route not mounted: no endpoint-profile row", "route", r.Pattern)
			continue
		}
		routes = append(routes, r)
	}
	return routes
}

func keyBody(serviceaccount.Key, time.Time) contracts.CredentialBody {
	return contracts.CredentialBody{}
}

func (h provisioningHandlers) serviceAccountKeyIssue(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}

func (h provisioningHandlers) serviceAccountKeyRotate(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}

func (h provisioningHandlers) serviceAccountKeyRevoke(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}

func (h provisioningHandlers) serviceAccountKeyList(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}
