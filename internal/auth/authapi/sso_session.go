package authapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/oidc"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
)

const (
	opSSOSession = "sso_session"
	ssoProvider  = "sso"
	samlProvider = "saml"
)

// SSOSessions starts the session of a completed federated sign-in: the same
// session row, audit row and access claims as a social sign-in.
type SSOSessions struct{ h compatHandlers }

// NewSSOSessions binds the session domain of deps.
func NewSSOSessions(deps CompatDeps) (SSOSessions, error) {
	if deps.Sessions == nil || deps.Users == nil || deps.Issuer == nil || deps.Projector == nil || deps.AppendAudit == nil {
		return SSOSessions{}, errors.New("authapi: sso sessions need sessions, users, an issuer, a projector and an audit appender")
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
		return SSOSessions{}, err
	}
	return SSOSessions{h: compatHandlers{CompatDeps: deps, rec: rec}}, nil
}

type ssoIssued struct {
	access, refresh string
	email, role     string
	org             *uuid.UUID
}

func (s SSOSessions) issue(ctx context.Context, principalID uuid.UUID, org *uuid.UUID, role *string, amr []string, provider string) (ssoIssued, error) {
	var membership *Membership
	if org != nil {
		membership = &Membership{OrgID: *org, Role: role}
	}
	user, found, err := s.h.Users.State(ctx, principalID)
	if err != nil {
		return ssoIssued{}, err
	}
	if !found {
		return ssoIssued{}, errors.New("authapi: a federated sign-in's user is not in public.users")
	}
	issued, err := s.h.startSession(ctx, principalID, membership, nil, amr, provider)
	if err != nil {
		return ssoIssued{}, err
	}
	text := roleText(role, org != nil)
	access, err := s.h.Issuer.Access(Grant{PrincipalID: principalID, OrganizationID: org, Role: text, SessionID: issued.Session.ID, User: user})
	if err != nil {
		return ssoIssued{}, err
	}
	return ssoIssued{access: access, refresh: issued.Credential.Reveal(), email: user.Email, role: text, org: org}, nil
}

// OIDC is the oidc.SessionIssuer.
func (s SSOSessions) OIDC() oidc.SessionIssuer { return oidcSessions{s} }

type oidcSessions struct{ SSOSessions }

func (s oidcSessions) Issue(ctx context.Context, req oidc.SessionRequest) (oidc.Tokens, error) {
	role := req.Role
	out, err := s.issue(ctx, req.PrincipalID, req.OrgID, &role, req.AMR, ssoProvider)
	if err != nil {
		return oidc.Tokens{}, err
	}
	return oidc.Tokens{AccessToken: out.access, RefreshToken: out.refresh, ExpiresIn: int64(AccessExpiresIn)}, nil
}

// SAML is the saml.SessionIssuer.
func (s SSOSessions) SAML() saml.SessionIssuer { return samlSessions{s} }

type samlSessions struct{ SSOSessions }

func (s samlSessions) IssueFederatedSession(w http.ResponseWriter, r *http.Request, signIn saml.SignIn) {
	ctx := r.Context()
	var role *string
	if signIn.OrganizationID != nil {
		found := false
		var err error
		if role, found, err = s.h.Users.Role(ctx, signIn.PrincipalID, *signIn.OrganizationID); err != nil {
			s.h.fail(w, r, opSSOSession, err)
			return
		}
		if !found {
			s.h.fail(w, r, opSSOSession, errors.New("authapi: a federated sign-in's membership is gone"))
			return
		}
	}
	out, err := s.issue(ctx, signIn.PrincipalID, signIn.OrganizationID, role, signIn.AMR, samlProvider)
	if err != nil {
		s.h.fail(w, r, opSSOSession, err)
		return
	}
	body := pyjson.NewObject()
	body.Set("access_token", out.access)
	body.Set("refresh_token", out.refresh)
	body.Set("token_type", "bearer")
	body.Set("expires_in", pyjson.IntOf(int64(AccessExpiresIn)))
	body.Set("user_id", signIn.PrincipalID.String())
	body.Set("email", out.email)
	orgText := ""
	if out.org != nil {
		orgText = out.org.String()
	}
	body.Set("org_id", orgText)
	body.Set("role", out.role)
	s.h.rec.record(ctx, opSSOSession, "success", reasonAccepted, nil)
	writeCompatModel(w, body)
}

// TransportRefusal renders a federated route's transport refusals, auditing a
// refused body the way the other sign-in routes do.
func (s SSOSessions) TransportRefusal(provider string) httpapi.ErrorWriter {
	return s.h.signInTransportRefusal(provider)
}
