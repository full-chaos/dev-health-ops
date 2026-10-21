package oidc

import (
	"net/http"

	"github.com/google/uuid"
)

// OAuthAuthorizeHandler serves POST /api/v1/auth/oauth/{provider_id}/authorize.
// The body (redirect_uri) is read for its shape only, as in go-api.
func (s *Service) OAuthAuthorizeHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		providerID, failure := providerIDFrom(r)
		if failure != nil {
			s.reject(w, r, routeOAuthAuthorize, providerID, failure)
			return
		}
		if _, failure := s.admit(r.Context(), providerID, oauthSpec); failure != nil {
			s.reject(w, r, routeOAuthAuthorize, providerID, failure)
			return
		}
		data, failure := readBody(w, r)
		if failure != nil {
			s.reject(w, r, routeOAuthAuthorize, providerID, failure)
			return
		}
		if _, errs := parseAuthorizeBody(data); len(errs) > 0 {
			s.reject(w, r, routeOAuthAuthorize, providerID, deny(http.StatusUnprocessableEntity, ReasonMalformed, errs))
			return
		}
		result, err := s.AuthorizeOAuth(r.Context(), providerID)
		writeAuthorize(s, w, result, err)
	})
}

// OAuthAuthorizeByTypeHandler serves GET
// /api/v1/auth/oauth/{provider_type}/authorize?org_id=...: the type is checked,
// then the org_id query, then the entitlement gate, then the row lookup.
func (s *Service) OAuthAuthorizeByTypeHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		providerType := r.PathValue("provider_type")
		if _, ok := oauthProtocolForType(providerType); !ok {
			s.reject(w, r, routeOAuthAuthorize, uuid.Nil, deny(http.StatusBadRequest, ReasonMalformed, "Invalid OAuth provider type"))
			return
		}
		values := r.URL.Query()["org_id"]
		if len(values) == 0 {
			s.reject(w, r, routeOAuthAuthorize, uuid.Nil, deny(http.StatusUnprocessableEntity, ReasonMalformed,
				[]fieldError{{Type: "missing", Loc: []string{"query", "org_id"}, Msg: "Field required"}}))
			return
		}
		orgID, err := uuid.Parse(values[len(values)-1])
		if err != nil {
			s.reject(w, r, routeOAuthAuthorize, uuid.Nil, deny(http.StatusUnprocessableEntity, ReasonMalformed,
				[]fieldError{{Type: "uuid_parsing", Loc: []string{"query", "org_id"}, Msg: "Input should be a valid UUID"}}))
			return
		}
		result, err := s.AuthorizeOAuthByType(r.Context(), providerType, orgID)
		writeAuthorize(s, w, result, err)
	})
}

// OAuthCallbackHandler serves POST /api/v1/auth/oauth/{provider_id}/callback.
func (s *Service) OAuthCallbackHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		providerID, failure := providerIDFrom(r)
		if failure != nil {
			s.reject(w, r, routeOAuthCallback, providerID, failure)
			return
		}
		if _, failure := s.admit(r.Context(), providerID, oauthSpec); failure != nil {
			s.reject(w, r, routeOAuthCallback, providerID, failure)
			return
		}
		data, failure := readBody(w, r)
		if failure != nil {
			s.reject(w, r, routeOAuthCallback, providerID, failure)
			return
		}
		body, errs := parseCallbackBody(data)
		if len(errs) > 0 {
			s.reject(w, r, routeOAuthCallback, providerID, deny(http.StatusUnprocessableEntity, ReasonMalformed, errs))
			return
		}
		login, clear, err := s.CallbackOAuth(r.Context(), providerID, callbackInput(r, body))
		s.writeLogin(w, login, clear, err)
	})
}

func writeAuthorize(s *Service, w http.ResponseWriter, result AuthorizeResult, err error) {
	if err != nil {
		s.writeError(w, err)
		return
	}
	http.SetCookie(w, result.Cookie)
	writeJSON(w, http.StatusOK, map[string]any{"authorization_url": result.AuthorizationURL, "state": result.State})
}
