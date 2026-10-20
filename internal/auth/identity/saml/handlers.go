package saml

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

const maxBodyBytes = 1 << 20

// SessionIssuer starts the session for a completed sign-in and writes the
// response (the session domain owns it).
type SessionIssuer interface {
	IssueFederatedSession(w http.ResponseWriter, r *http.Request, signIn SignIn)
}

// Handlers are the SAML routes. The router mounts them at
// GET /saml/{provider_id}/metadata, POST /saml/{provider_id}/initiate and
// POST /saml/{provider_id}/acs.
type Handlers struct {
	Service  *Service
	Sessions SessionIssuer
	Logger   *slog.Logger
}

func (h Handlers) logger() *slog.Logger {
	if h.Logger != nil {
		return h.Logger
	}
	return slog.Default()
}

// Metadata is GET /saml/{provider_id}/metadata.
func (h Handlers) Metadata(w http.ResponseWriter, r *http.Request) {
	providerID, ok := h.providerID(w, r)
	if !ok {
		return
	}
	md, err := h.Service.Metadata(r.Context(), providerID)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"metadata_xml": md.XML, "entity_id": md.EntityID, "acs_url": md.ACSURL})
}

// Initiate is POST /saml/{provider_id}/initiate with {"relay_state": ...}.
func (h Handlers) Initiate(w http.ResponseWriter, r *http.Request) {
	providerID, ok := h.providerID(w, r)
	if !ok {
		return
	}
	var body struct {
		RelayState *string `json:"relay_state"`
	}
	if !readJSON(w, r, &body) {
		return
	}
	relayState := ""
	if body.RelayState != nil {
		relayState = *body.RelayState
	}
	init, err := h.Service.Initiate(r.Context(), providerID, relayState)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name: init.CookieName, Value: init.CookieValue, Path: init.CookiePath,
		MaxAge: int(init.MaxAge.Seconds()), HttpOnly: true, Secure: true, SameSite: http.SameSiteLaxMode,
	})
	writeJSON(w, http.StatusOK, map[string]string{"redirect_url": init.RedirectURL})
}

// ACS is POST /saml/{provider_id}/acs with {"SAMLResponse": ..., "RelayState": ...}
// (snake_case names are accepted too).
func (h Handlers) ACS(w http.ResponseWriter, r *http.Request) {
	providerID, ok := h.providerID(w, r)
	if !ok {
		return
	}
	var body struct {
		SAMLResponse  *string `json:"SAMLResponse"`
		SAMLResponse2 *string `json:"saml_response"`
	}
	if !readJSON(w, r, &body) {
		return
	}
	posted := body.SAMLResponse
	if posted == nil {
		posted = body.SAMLResponse2
	}
	if posted == nil {
		writeDetail(w, http.StatusUnprocessableEntity, "SAMLResponse is required")
		return
	}
	cookie := func(name string) string {
		c, err := r.Cookie(name)
		if err != nil {
			return ""
		}
		http.SetCookie(w, &http.Cookie{Name: name, Value: "", Path: ACSPath(Provider{ID: providerID}),
			MaxAge: -1, HttpOnly: true, Secure: true, SameSite: http.SameSiteLaxMode})
		return c.Value
	}
	signIn, err := h.Service.Consume(r.Context(), providerID, *posted, cookie)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	h.Sessions.IssueFederatedSession(w, r, signIn)
}

func (h Handlers) providerID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.PathValue("provider_id"))
	if err != nil {
		writeDetail(w, http.StatusUnprocessableEntity, "provider_id must be a UUID")
		return uuid.Nil, false
	}
	return id, true
}

// writeError maps a domain error to the go-api status and detail. Every
// verification failure has the same detail.
func (h Handlers) writeError(w http.ResponseWriter, r *http.Request, err error) {
	var conflict *EmailConflictError
	switch {
	case errors.Is(err, ErrProviderNotFound):
		writeDetail(w, http.StatusNotFound, "SSO provider not found")
	case errors.Is(err, ErrNotEntitled):
		writeJSON(w, http.StatusPaymentRequired, notLicensedBody())
	case errors.Is(err, ErrNotSAML):
		writeDetail(w, http.StatusBadRequest, "Provider is not SAML")
	case errors.Is(err, ErrProviderInactive):
		writeDetail(w, http.StatusBadRequest, "SSO provider is not active")
	case errors.As(err, &conflict):
		method := conflict.Method
		if method == "" {
			method = "your existing method"
		}
		writeJSON(w, http.StatusConflict, map[string]any{"detail": map[string]string{
			"message":           "An account with this email already exists. Please sign in with " + method + ".",
			"existing_provider": conflict.Method,
		}})
	case errors.Is(err, ErrSignupNotAvailable):
		writeDetail(w, http.StatusForbidden, "Sign-up through SSO is not available")
	case errors.Is(err, ErrAutoProvisionDisabled), errors.Is(err, ErrInvalidEmailAddress):
		writeDetail(w, http.StatusBadRequest, "SAML user provisioning failed")
	case errors.Is(err, ErrEmailMissing):
		writeDetail(w, http.StatusBadRequest, "Email address is missing from SAML response")
	case errors.Is(err, ErrEmailMalformed):
		writeDetail(w, http.StatusBadRequest, "Email address from SAML response is malformed")
	case errors.Is(err, ErrDomainNotAllowed):
		writeDetail(w, http.StatusForbidden, "Email domain is not allowed for this provider")
	case errors.Is(err, ErrAccountDisabled):
		writeDetail(w, http.StatusForbidden, "Account is disabled")
	case errors.Is(err, ErrUnsolicited), errors.Is(err, ErrFlowNotFound), errors.Is(err, ErrFlowConsumed), errors.Is(err, ErrAssertionReplayed):
		writeDetail(w, http.StatusBadRequest, "SAML authentication failed")
	default:
		if _, ok := IsRefused(err); ok {
			writeDetail(w, http.StatusBadRequest, "SAML authentication failed")
			return
		}
		h.logger().ErrorContext(r.Context(), "auth saml: request failed", "route", r.Pattern,
			"request_id", httpapi.LoggableRequestID(r.Context()), logging.ErrorAttr(err))
		writeDetail(w, http.StatusInternalServerError, "Internal Server Error")
	}
}

func readJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeDetail(w, http.StatusRequestEntityTooLarge, "Request body too large")
		return false
	}
	if len(data) == 0 {
		data = []byte("{}")
	}
	if err := json.Unmarshal(data, v); err != nil {
		writeDetail(w, http.StatusUnprocessableEntity, "Request body must be a JSON object")
		return false
	}
	return true
}

func writeDetail(w http.ResponseWriter, status int, detail string) {
	writeJSON(w, status, map[string]string{"detail": detail})
}

// writeJSON serializes the body first: a body that cannot be serialized is
// logged and answered 500, never written half.
func writeJSON(w http.ResponseWriter, status int, v any) {
	payload, err := json.Marshal(v)
	if err != nil {
		slog.Default().Error("auth saml: response body could not be serialized", slog.Int("status", status), logging.ErrorAttr(err))
		payload, status = []byte(`{"detail":"Internal Server Error"}`), http.StatusInternalServerError
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
	w.WriteHeader(status)
	_, _ = io.Copy(w, bytes.NewReader(payload))
}
