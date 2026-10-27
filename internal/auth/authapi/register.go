package authapi

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/registerorg"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/local"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

const (
	PatternRegister      = "/api/v1/auth/register"
	opRegister           = "register"
	textRegisterAccepted = "Check your email to continue"
)

// registerIPLimit is go-api's auth_register budget (3 per hour per client
// address); the key is the end client address, never the proxy's.
var registerIPLimit = httpapi.Limit{ID: "auth_svc_register_ip", Count: 3, Window: time.Hour}

// registerFlow is local.Flows' sign-up; the credential flows interface stays
// as it is.
type registerFlow interface {
	Register(ctx context.Context, req local.RegisterRequest) (local.FlowOutcome, error)
}

type registerInputKey struct{}

type registerInput struct {
	email, password   string
	fullName, orgName *string
}

// registerGetenv reads AUTH_AUTO_CREATE_ORG_ON_REGISTER on every request, as
// go-api does; a test replaces it.
var registerGetenv = secrets.ProcessGetenv

// registerRoute is POST /api/v1/auth/register. Beside the user it inserts the
// organization and owner membership go-api's register inserts, unless
// AUTH_AUTO_CREATE_ORG_ON_REGISTER turns that off; it inserts new rows only
// and updates none.
func (h credentialHandlers) registerRoute(flows CredentialFlows) (httpapi.Route, error) {
	register, ok := flows.(registerFlow)
	if !ok {
		return httpapi.Route{}, errors.New("authapi: the credential flows cannot register")
	}
	handler := httpapi.ValidateThenLimitDependency(h.validateRegister, h.limits.limiter(registerIPLimit), clientKey, goAPIRefusal)(h.register(register))
	return httpapi.Route{Method: http.MethodPost, Pattern: PatternRegister, Handler: handler}, nil
}

// validateRegister is RegisterRequest: EmailStr, a password of 8 to 128
// characters, and an optional full name and organization name (read, unused).
func (h credentialHandlers) validateRegister(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
	body, ok := h.compat.readCompatBody(w, r, opRegister, nil)
	if !ok {
		return nil, false
	}
	var errs pybody.Errors
	var input registerInput
	if object, ok := errs.Object(body); ok {
		input.email, _ = errs.RequiredEmailStr(object, "email")
		input.password, _ = errs.RequiredString(object, "password", 8, 128)
		if value, present := errs.OptionalUTF8String(object, "full_name"); present {
			input.fullName = &value
		}
		if value, present := errs.OptionalUTF8String(object, "org_name"); present {
			input.orgName = &value
		}
	}
	if len(errs) > 0 {
		h.compat.rec.record(r.Context(), opRegister, "denied", reasonMalformed, nil)
		writeValidation(w, errs)
		return nil, false
	}
	return r.WithContext(context.WithValue(r.Context(), registerInputKey{}, input)), true
}

func (h credentialHandlers) register(flow registerFlow) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		input, _ := r.Context().Value(registerInputKey{}).(registerInput)
		request := local.RegisterRequest{Email: input.email, Password: input.password, FullName: input.fullName}
		if registerorg.Enabled(registerGetenv(registerorg.EnvName)) {
			name := registerorg.Name(input.orgName)
			request.OrganizationName = &name
		}
		outcome, err := flow.Register(r.Context(), request)
		if err != nil {
			h.compat.fail(w, r, opRegister, err)
			return
		}
		if outcome.Kind == local.FlowPolicy {
			h.compat.rec.record(r.Context(), opRegister, "denied", reasonPasswordPolicy, nil)
			policy.WriteDetail(w, http.StatusUnprocessableEntity, violationsDetail(textPasswordValidation, outcome.Violations), nil)
			return
		}
		h.compat.rec.record(r.Context(), opRegister, "success", reasonAccepted, nil)
		out := pyjson.NewObject()
		out.Set("message", textRegisterAccepted)
		policy.WriteModel(w, http.StatusAccepted, out, http.Header{"Cache-Control": {"no-store"}, "Pragma": {"no-cache"}})
	}
}
