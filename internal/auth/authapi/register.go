package authapi

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/local"
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
	email, password string
	fullName        *string
}

// registerRoute is POST /api/v1/auth/register. It writes users only: no
// organization or membership (the aggregate owner keeps them), so the new user
// reaches an organization through /api/v1/auth/onboard.
func (h credentialHandlers) registerRoute(flows CredentialFlows) (httpapi.Route, error) {
	register, ok := flows.(registerFlow)
	if !ok {
		return httpapi.Route{}, errors.New("authapi: the credential flows cannot register")
	}
	handler := httpapi.ValidateThenLimit(h.validateRegister, h.limits.limiter(registerIPLimit), clientKey, goAPIRefusal)(h.register(register))
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
		if value, present := errs.OptionalString(object, "full_name", 0, 0); present {
			input.fullName = &value
		}
		_, _ = errs.OptionalString(object, "org_name", 0, 0)
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
		outcome, err := flow.Register(r.Context(), local.RegisterRequest{Email: input.email, Password: input.password, FullName: input.fullName})
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
