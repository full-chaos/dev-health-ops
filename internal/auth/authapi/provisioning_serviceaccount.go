package authapi

import (
	"context"
	"net/http"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/serviceaccount"
)

// The organization service account routes. Each is mounted only when the endpoint-profile document holds a row accepting
// every class it admits (withServiceAccountRoutes); without the row it answers 404 and the service starts. They are not on
// the startup gate (provisioningPatterns) until their rows ship.
const PatternServiceAccountCreate = "/internal/v1/service-accounts/create"
const PatternServiceAccountDisable = "/internal/v1/service-accounts/disable"
const PatternServiceAccountEnable = "/internal/v1/service-accounts/enable"
const PatternServiceAccountOwner = "/internal/v1/service-accounts/owner"

const (
	opServiceAccountCreate  = "service_account_create"
	opServiceAccountDisable = "service_account_disable"
	opServiceAccountEnable  = "service_account_enable"
	opServiceAccountOwner   = "service_account_owner"
)

// Refusal codes of the service account routes.
const (
	reasonServiceAccountNotFound = "service_account_not_found"
	reasonNameTaken              = "name_taken"
	reasonNameInvalid            = "name_invalid"
	reasonOwnerInvalid           = "owner_invalid"
	reasonOwnerNotMember         = "owner_not_member"
	reasonAlreadyInState         = "already_in_state"
)

// maxServiceAccountNameBytes bounds the name member: the longest name the store's pattern admits.
const maxServiceAccountNameBytes = 63

// ServiceAccounts is the service account aggregate (*serviceaccount.Store).
type ServiceAccounts interface {
	Create(ctx context.Context, in serviceaccount.CreateInput, a serviceaccount.Actor) (uuid.UUID, error)
	Disable(ctx context.Context, id uuid.UUID, a serviceaccount.Actor) error
	Enable(ctx context.Context, id uuid.UUID, a serviceaccount.Actor) error
	SetOwner(ctx context.Context, id, newOwner uuid.UUID, a serviceaccount.Actor) error
	Lookup(ctx context.Context, id uuid.UUID) (*serviceaccount.Info, error)
}

func (h provisioningHandlers) serviceAccountCreate(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)
}

func (h provisioningHandlers) serviceAccountSwitch(op string, enable bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not implemented", http.StatusNotImplemented)
	}
}

func (h provisioningHandlers) serviceAccountOwner(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)
}

// withServiceAccountRoutes mounts each service account route whose profile row lists every class it admits, and logs one
// line naming each route it leaves out.
func (h provisioningHandlers) withServiceAccountRoutes(routes []httpapi.Route, rows []byte) []httpapi.Route {
	for _, r := range []httpapi.Route{
		{Method: http.MethodPost, Pattern: PatternServiceAccountCreate, Handler: http.HandlerFunc(h.serviceAccountCreate)},
		{Method: http.MethodPost, Pattern: PatternServiceAccountDisable, Handler: h.serviceAccountSwitch(opServiceAccountDisable, false)},
		{Method: http.MethodPost, Pattern: PatternServiceAccountEnable, Handler: h.serviceAccountSwitch(opServiceAccountEnable, true)},
		{Method: http.MethodPost, Pattern: PatternServiceAccountOwner, Handler: http.HandlerFunc(h.serviceAccountOwner)},
	} {
		if rowsAcceptFor(rows, []string{r.Pattern}, admittedClasses(r.Pattern, h.Workload != nil)) != nil {
			h.Logger.Info("auth provisioning route not mounted: no endpoint-profile row", "route", r.Pattern)
			continue
		}
		routes = append(routes, r)
	}
	return routes
}
