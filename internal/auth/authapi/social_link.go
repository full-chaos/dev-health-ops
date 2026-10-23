package authapi

import (
	"errors"
	"log/slog"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/social"
)

// Social link route patterns. The endpoint-profile rows and the OpenAPI
// document name these exact strings.
const (
	PatternSocialLink   = "/v1/identities/social"
	PatternSocialUnlink = "/v1/identities/social/{id}"
)

const (
	opSocialLink   = "social_link"
	opSocialUnlink = "social_unlink"
)

// SocialLinkDeps are the link routes' collaborators.
type SocialLinkDeps struct {
	Sessions Sessions
	Users    Users
	Issuer   *Issuer
	Logger   *slog.Logger
	Meter    metric.Meter
	// Social serves the routes; nil leaves them unmounted.
	Social social.Domain
}

// SocialLinkRoutes returns the bearer-only link and unlink routes, or none
// when no social domain is configured.
func SocialLinkRoutes(deps SocialLinkDeps) ([]httpapi.Route, error) {
	if deps.Social == nil {
		return nil, nil
	}
	if deps.Sessions == nil || deps.Users == nil || deps.Issuer == nil {
		return nil, errors.New("authapi: social link routes need sessions, users and an issuer")
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	meter := deps.Meter
	if meter == nil {
		meter = otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/authapi")
	}
	rec, err := newRecorder(deps.Logger, meter)
	if err != nil {
		return nil, err
	}
	h := socialLinkHandlers{handlers: handlers{Deps: Deps{Sessions: deps.Sessions, Users: deps.Users, Issuer: deps.Issuer, Logger: deps.Logger, Meter: meter}, rec: rec}, social: deps.Social}
	return []httpapi.Route{
		{Method: http.MethodPost, Pattern: PatternSocialLink, Handler: http.HandlerFunc(h.link)},
		{Method: http.MethodDelete, Pattern: PatternSocialUnlink, Handler: http.HandlerFunc(h.unlink)},
	}, nil
}

type socialLinkHandlers struct {
	handlers
	social social.Domain
}

// signedInBearer is the social domain's view of an authenticated request: the
// bearer's session principal and the session's sign-in time.
type signedInBearer struct{ who social.SignedIn }

func (s signedInBearer) SignedIn(*http.Request) (social.SignedIn, bool) { return s.who, true }

// serve authenticates the bearer (cookies are never read) and hands the
// request to the social handlers with that session's principal.
func (h socialLinkHandlers) serve(w http.ResponseWriter, r *http.Request, operation string, run func(social.Handlers)) {
	bearer, record, ok := h.authenticate(w, r, operation)
	if !ok {
		return
	}
	who := social.SignedIn{PrincipalID: bearer.PrincipalID, AuthTime: record.AuthTime}
	run(social.Handlers{Service: h.social, Auth: signedInBearer{who: who}})
}

func (h socialLinkHandlers) link(w http.ResponseWriter, r *http.Request) {
	h.serve(w, r, opSocialLink, func(s social.Handlers) { s.Link(w, r) })
}

func (h socialLinkHandlers) unlink(w http.ResponseWriter, r *http.Request) {
	h.serve(w, r, opSocialUnlink, func(s social.Handlers) { s.Unlink(w, r) })
}
