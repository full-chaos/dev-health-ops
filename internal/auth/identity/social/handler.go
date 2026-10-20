package social

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
)

// maxRequestBody bounds an exchange or link request.
const maxRequestBody = 64 << 10

// SessionWriter issues a session for a signed-in principal and writes the
// login response. The session domain mounts it (amr "fed").
type SessionWriter interface {
	WriteSession(w http.ResponseWriter, r *http.Request, principalID uuid.UUID, created bool)
}

// Authenticator returns the signed-in session behind a request, if any.
type Authenticator interface {
	SignedIn(r *http.Request) (SignedIn, bool)
}

// Handlers serve the social routes. Errors answer in go-api's body shape
// ({"detail": ...}) so the compatibility proxy forwards them unchanged.
type Handlers struct {
	Service  *Service
	Sessions SessionWriter
	Auth     Authenticator
}

type tokenRequest struct {
	Provider            string                 `json:"provider"`
	ProviderAccessToken contracts.SecretString `json:"provider_access_token"`
}

func readTokenRequest(w http.ResponseWriter, r *http.Request) (Provider, contracts.SecretString, bool) {
	var body tokenRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBody))
	if err := decodeOne(dec, &body); err != nil || body.ProviderAccessToken.Reveal() == "" {
		writeDetail(w, http.StatusUnprocessableEntity, "provider and provider_access_token are required")
		return "", contracts.SecretString{}, false
	}
	provider, ok := ParseProvider(body.Provider)
	if !ok {
		writeDetail(w, http.StatusUnprocessableEntity, "provider must be one of github, gitlab, google")
		return "", contracts.SecretString{}, false
	}
	return provider, body.ProviderAccessToken, true
}

// Exchange is POST social exchange: {provider, provider_access_token}.
func (h Handlers) Exchange(w http.ResponseWriter, r *http.Request) {
	provider, token, ok := readTokenRequest(w, r)
	if !ok {
		return
	}
	result, err := h.Service.Exchange(r.Context(), provider, token, requestid.Audit(r.Context()))
	if err != nil {
		writeError(w, provider, err)
		return
	}
	h.Sessions.WriteSession(w, r, result.PrincipalID, result.Created)
}

// Link is POST link: {provider, provider_access_token}, for a signed-in caller.
func (h Handlers) Link(w http.ResponseWriter, r *http.Request) {
	who, ok := h.Auth.SignedIn(r)
	if !ok {
		writeDetail(w, http.StatusUnauthorized, "Not authenticated")
		return
	}
	provider, token, ok := readTokenRequest(w, r)
	if !ok {
		return
	}
	who.RequestID = requestid.Audit(r.Context())
	linked, err := h.Service.Link(r.Context(), who, provider, token)
	if err != nil {
		writeError(w, provider, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"id": linked.ID.String(), "provider": string(linked.Provider)})
}

// Unlink is DELETE of one identity, path value "id", for a signed-in caller.
func (h Handlers) Unlink(w http.ResponseWriter, r *http.Request) {
	who, ok := h.Auth.SignedIn(r)
	if !ok {
		writeDetail(w, http.StatusUnauthorized, "Not authenticated")
		return
	}
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeDetail(w, http.StatusNotFound, "Identity not found")
		return
	}
	who.RequestID = requestid.Audit(r.Context())
	if err := h.Service.Unlink(r.Context(), who, id); err != nil {
		writeError(w, "", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func writeError(w http.ResponseWriter, provider Provider, err error) {
	var conflict *ConflictError
	switch {
	case errors.As(err, &conflict):
		method := conflict.ExistingMethod
		if method == "" {
			method = "your existing method"
		}
		writeJSON(w, http.StatusConflict, map[string]any{"detail": map[string]string{
			"message":           "An account with this email already exists. Please sign in with " + method + ".",
			"existing_provider": conflict.ExistingMethod,
		}})
	case errors.Is(err, ErrProviderNotConfigured):
		writeDetail(w, http.StatusServiceUnavailable, "Social login not configured for "+string(provider))
	case errors.Is(err, ErrProviderRefused):
		writeDetail(w, http.StatusUnauthorized, "Invalid or expired provider token")
	case errors.Is(err, ErrEmailUnverified):
		writeDetail(w, http.StatusForbidden, "The provider has not verified this email address")
	case errors.Is(err, ErrPrincipalDisabled):
		writeDetail(w, http.StatusForbidden, "Account is disabled")
	case errors.Is(err, identity.ErrSignupNotAvailable):
		writeDetail(w, http.StatusForbidden, "Sign-up with "+string(provider)+" is not available")
	case errors.Is(err, ErrReauthenticationRequired):
		writeDetail(w, http.StatusForbidden, "Sign in again to change linked accounts")
	case errors.Is(err, identity.ErrIdentityConflict):
		writeDetail(w, http.StatusConflict, "This "+string(provider)+" account is linked to another user")
	case errors.Is(err, ErrProviderLinked):
		writeDetail(w, http.StatusConflict, "A "+string(provider)+" account is already linked")
	case errors.Is(err, identity.ErrNotFound):
		writeDetail(w, http.StatusNotFound, "Identity not found")
	case errors.Is(err, identity.ErrLastSignInMethod):
		writeDetail(w, http.StatusConflict, "Cannot remove the last sign-in method")
	default:
		writeDetail(w, http.StatusInternalServerError, "Internal server error")
	}
}

func writeDetail(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"detail": message})
}

// writeJSON serializes the body first, then writes it with its length. The
// bodies here are fixed strings and ids, so a marshal failure is a bug, and it
// answers a fixed 500.
func writeJSON(w http.ResponseWriter, status int, body any) {
	payload, err := json.Marshal(body)
	if err != nil {
		status, payload = http.StatusInternalServerError, []byte(`{"detail":"Internal server error"}`)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
	w.WriteHeader(status)
	_, _ = io.Copy(w, bytes.NewReader(payload))
}
