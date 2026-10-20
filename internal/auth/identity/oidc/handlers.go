package oidc

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
)

const maxBodyBytes = 64 << 10

// fieldError is one 422 item, in the go-api (pydantic) detail shape.
type fieldError struct {
	Type string   `json:"type"`
	Loc  []string `json:"loc"`
	Msg  string   `json:"msg"`
}

func bodyError(kind, field, msg string) fieldError {
	loc := []string{"body"}
	if field != "" {
		loc = append(loc, field)
	}
	return fieldError{Type: kind, Loc: loc, Msg: msg}
}

// decodeBody reads one JSON object with no duplicate members.
func decodeBody(data []byte) (map[string]any, []fieldError) {
	if len(data) > maxBodyBytes {
		return nil, []fieldError{bodyError("value_error", "", "Body too large")}
	}
	if err := checkJSONObject(data); err != nil {
		return nil, []fieldError{bodyError("model_attributes_type", "", "Input should be a valid JSON object")}
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var object map[string]any
	if err := dec.Decode(&object); err != nil {
		return nil, []fieldError{bodyError("model_attributes_type", "", "Input should be a valid JSON object")}
	}
	return object, nil
}

func requiredString(object map[string]any, field string, errs *[]fieldError) string {
	value, present := object[field]
	if !present {
		*errs = append(*errs, bodyError("missing", field, "Field required"))
		return ""
	}
	text, ok := value.(string)
	if !ok {
		*errs = append(*errs, bodyError("string_type", field, "Input should be a valid string"))
	}
	return text
}

func optionalString(object map[string]any, field string, errs *[]fieldError) string {
	value, present := object[field]
	if !present || value == nil {
		return ""
	}
	text, ok := value.(string)
	if !ok {
		*errs = append(*errs, bodyError("string_type", field, "Input should be a valid string"))
	}
	return text
}

func optionalBool(object map[string]any, field string, errs *[]fieldError) {
	if value, present := object[field]; present {
		if _, ok := value.(bool); !ok {
			*errs = append(*errs, bodyError("bool_type", field, "Input should be a valid boolean"))
		}
	}
}

// authorizeBody is OIDCAuthRequest: redirect_uri (optional) and use_pkce
// (optional, type-checked; PKCE is always used).
type authorizeBody struct{ RedirectURI string }

func parseAuthorizeBody(data []byte) (authorizeBody, []fieldError) {
	object, errs := decodeBody(data)
	if errs != nil {
		return authorizeBody{}, errs
	}
	body := authorizeBody{RedirectURI: optionalString(object, "redirect_uri", &errs)}
	optionalBool(object, "use_pkce", &errs)
	return body, errs
}

// callbackBody is OIDCCallbackRequest: code and state (required) and
// code_verifier (optional, type-checked, never used: the verifier comes only
// from the sealed state).
type callbackBody struct{ Code, State string }

func parseCallbackBody(data []byte) (callbackBody, []fieldError) {
	object, errs := decodeBody(data)
	if errs != nil {
		return callbackBody{}, errs
	}
	body := callbackBody{Code: requiredString(object, "code", &errs), State: requiredString(object, "state", &errs)}
	optionalString(object, "code_verifier", &errs)
	return body, errs
}

func readBody(w http.ResponseWriter, r *http.Request) ([]byte, *Error) {
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		return nil, deny(http.StatusRequestEntityTooLarge, ReasonMalformed, "Request body too large")
	}
	return data, nil
}

// reject records a refusal of route once and writes its response.
func (s *Service) reject(w http.ResponseWriter, r *http.Request, route string, providerID uuid.UUID, failure *Error) {
	s.refused(r.Context(), route, providerID, failure)
	s.writeError(w, failure)
}

// writeJSON serializes first and writes the bytes with their length, the
// policy.writePayload pattern; a body that cannot be serialized is a bare 500.
func writeJSON(w http.ResponseWriter, status int, body any) {
	payload, err := json.Marshal(body)
	if err != nil {
		slog.Default().Error("auth oidc: response body could not be serialized", slog.Int("status", status))
		http.Error(w, `{"detail":"Internal Server Error"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
	w.WriteHeader(status)
	_, _ = io.Copy(w, bytes.NewReader(payload))
}

func (s *Service) writeError(w http.ResponseWriter, err error) {
	var failure *Error
	if !errors.As(err, &failure) {
		failure = internal(ReasonDependency, err)
	}
	writeJSON(w, failure.Status, map[string]any{"detail": failure.Detail})
}

func providerIDFrom(r *http.Request) (uuid.UUID, *Error) {
	id, err := uuid.Parse(r.PathValue("provider_id"))
	if err != nil {
		return uuid.UUID{}, deny(http.StatusUnprocessableEntity, ReasonMalformed, []fieldError{{
			Type: "uuid_parsing", Loc: []string{"path", "provider_id"}, Msg: "Input should be a valid UUID"}})
	}
	return id, nil
}

// AuthorizeHandler serves POST /api/v1/auth/oidc/{provider_id}/authorize.
func (s *Service) AuthorizeHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		providerID, failure := providerIDFrom(r)
		if failure != nil {
			s.reject(w, r, routeAuthorize, providerID, failure)
			return
		}
		if _, failure := s.admit(r.Context(), providerID); failure != nil {
			s.reject(w, r, routeAuthorize, providerID, failure)
			return
		}
		data, failure := readBody(w, r)
		if failure != nil {
			s.reject(w, r, routeAuthorize, providerID, failure)
			return
		}
		body, errs := parseAuthorizeBody(data)
		if len(errs) > 0 {
			s.reject(w, r, routeAuthorize, providerID, deny(http.StatusUnprocessableEntity, ReasonMalformed, errs))
			return
		}
		result, err := s.Authorize(r.Context(), providerID, body.RedirectURI)
		if err != nil {
			s.writeError(w, err)
			return
		}
		http.SetCookie(w, result.Cookie)
		writeJSON(w, http.StatusOK, map[string]any{"authorization_url": result.AuthorizationURL, "state": result.State})
	})
}

// CallbackHandler serves POST /api/v1/auth/oidc/{provider_id}/callback.
func (s *Service) CallbackHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		providerID, failure := providerIDFrom(r)
		if failure != nil {
			s.reject(w, r, routeCallback, providerID, failure)
			return
		}
		if _, failure := s.admit(r.Context(), providerID); failure != nil {
			s.reject(w, r, routeCallback, providerID, failure)
			return
		}
		data, failure := readBody(w, r)
		if failure != nil {
			s.reject(w, r, routeCallback, providerID, failure)
			return
		}
		body, errs := parseCallbackBody(data)
		if len(errs) > 0 {
			s.reject(w, r, routeCallback, providerID, deny(http.StatusUnprocessableEntity, ReasonMalformed, errs))
			return
		}
		login, clear, err := s.Callback(r.Context(), providerID, CallbackInput{
			Code: body.Code, State: body.State, RequestID: requestid.Audit(r.Context()),
			Cookie: func(name string) (string, bool) {
				c, err := r.Cookie(name)
				if err != nil {
					return "", false
				}
				return c.Value, true
			},
		})
		if clear != nil {
			http.SetCookie(w, clear)
		}
		if err != nil {
			s.writeError(w, err)
			return
		}
		orgID := ""
		if login.OrgID != nil {
			orgID = login.OrgID.String()
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"access_token": login.Tokens.AccessToken, "refresh_token": login.Tokens.RefreshToken,
			"token_type": "bearer", "expires_in": login.Tokens.ExpiresIn,
			"user_id": login.PrincipalID.String(), "email": login.Email, "org_id": orgID, "role": login.Role,
		})
	})
}
