package authapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/oidc"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/ssoaudit"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

const (
	opSSOSession = "sso_session"
	ssoProvider  = "oidc"
	samlProvider = "saml"
)

// SSOSessions starts the session of a completed federated sign-in: the same
// session row, audit row and access claims as a social sign-in.
type SSOSessions struct {
	h compatHandlers
	// record writes go-api's success record of a SAML sign-in.
	record ssoaudit.SignInRecord
}

// NewSSOSessions binds the session domain of deps.
func NewSSOSessions(deps CompatDeps, record ssoaudit.SignInRecord) (SSOSessions, error) {
	if deps.Sessions == nil || deps.Users == nil || deps.Issuer == nil || deps.Projector == nil || deps.AppendAudit == nil || record == nil {
		return SSOSessions{}, errors.New("authapi: sso sessions need sessions, users, an issuer, a projector, an audit appender and a sign-in record")
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
	return SSOSessions{h: compatHandlers{CompatDeps: deps, rec: rec}, record: record}, nil
}

type ssoIssued struct {
	tokens      Tokens
	refresh     string
	email, role string
	org         *uuid.UUID
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
	// The routes check before they consume the flow; this repeat, inside the
	// memo window, runs no second check.
	if err := s.h.Issuer.Preflight(ctx); err != nil {
		return ssoIssued{}, fmt.Errorf("%w: %w", errSignerUnavailable, err)
	}
	issued, err := s.h.startSession(ctx, principalID, membership, amr, provider)
	if err != nil {
		return ssoIssued{}, err
	}
	text := roleText(role, org != nil)
	tokens, err := s.h.Issuer.Tokens(ctx, Grant{PrincipalID: principalID, OrganizationID: org, Role: text, SessionID: issued.Session.ID, User: user})
	if err != nil {
		return ssoIssued{}, err
	}
	return ssoIssued{tokens: tokens, refresh: issued.Credential.Reveal(), email: user.Email, role: text, org: org}, nil
}

// LoginEvent is the audit row of a sign-in whose session a join's commit writes.
func (s SSOSessions) LoginEvent(ctx context.Context, principalID uuid.UUID, org *uuid.UUID) audit.AuditEvent {
	return loginSucceeded(ctx, principalID, org, ssoProvider)
}

// MintOIDC turns a session the join's commit stored into the caller's tokens. It
// writes nothing.
func (s SSOSessions) MintOIDC(ctx context.Context, issued session.Issued, req oidc.SessionRequest) (oidc.Tokens, error) {
	user, found, err := s.h.Users.State(ctx, req.PrincipalID)
	if err != nil {
		return oidc.Tokens{}, err
	}
	if !found {
		return oidc.Tokens{}, errors.New("authapi: a federated sign-in's user is not in public.users")
	}
	role := req.Role
	tokens, err := s.h.Issuer.Tokens(ctx, Grant{PrincipalID: req.PrincipalID, OrganizationID: req.OrgID, Role: roleText(&role, req.OrgID != nil),
		SessionID: issued.Session.ID, User: user})
	if err != nil {
		return oidc.Tokens{}, err
	}
	return oidc.Tokens{AccessToken: tokens.Access, RefreshToken: issued.Credential.Reveal(), ExpiresIn: int64(s.h.Issuer.ExpiresIn()),
		SessionAccessToken: tokens.Session}, nil
}

// OIDC is the oidc.SessionIssuer.
func (s SSOSessions) OIDC() oidc.SessionIssuer { return oidcSessions{s} }

type oidcSessions struct{ SSOSessions }

func (s oidcSessions) Issue(ctx context.Context, req oidc.SessionRequest) (oidc.Tokens, error) {
	role := req.Role
	out, err := s.issue(ctx, req.PrincipalID, req.OrgID, &role, req.AMR, ssoProvider)
	if errors.Is(err, errSignerUnavailable) {
		return oidc.Tokens{}, fmt.Errorf("%w: %w", oidc.ErrSessionUnavailable, err)
	}
	if err != nil {
		return oidc.Tokens{}, err
	}
	return oidc.Tokens{AccessToken: out.tokens.Access, RefreshToken: out.refresh, ExpiresIn: int64(s.h.Issuer.ExpiresIn()),
		SessionAccessToken: out.tokens.Session}, nil
}

func (s oidcSessions) Preflight(ctx context.Context) error {
	if err := s.h.Issuer.Preflight(ctx); err != nil {
		return fmt.Errorf("%w: %w", oidc.ErrSessionUnavailable, err)
	}
	return nil
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
	if errors.Is(err, errSignerUnavailable) {
		s.h.unavailableCompat(w, r, opSSOSession, err)
		return
	}
	if err != nil {
		s.h.fail(w, r, opSSOSession, err)
		return
	}
	// Before the body: a record that cannot be written withholds the tokens.
	if err := s.recordSignIn(ctx, signIn); err != nil {
		s.h.fail(w, r, opSSOSession, err)
		return
	}
	body := pyjson.NewObject()
	body.Set("access_token", out.tokens.Access)
	body.Set("refresh_token", out.refresh)
	body.Set("token_type", "bearer")
	body.Set("expires_in", pyjson.IntOf(int64(s.h.Issuer.ExpiresIn())))
	body.Set("user_id", signIn.PrincipalID.String())
	body.Set("email", out.email)
	orgText := ""
	if out.org != nil {
		orgText = out.org.String()
	}
	body.Set("org_id", orgText)
	body.Set("role", out.role)
	setSessionAccessToken(body, out.tokens)
	s.h.rec.record(ctx, opSSOSession, "success", reasonAccepted, nil)
	writeCompatModel(w, body)
}

func (s samlSessions) recordSignIn(ctx context.Context, signIn saml.SignIn) error {
	if signIn.OrganizationID == nil {
		return errors.New("authapi: a SAML sign-in has no provider organization to record")
	}
	return s.record.Succeeded(ctx, ssoaudit.Success{OrgID: *signIn.OrganizationID, ProviderID: signIn.ProviderID,
		PrincipalID: signIn.PrincipalID, Protocol: samlProvider})
}

func (s samlSessions) Preflight(ctx context.Context) error { return s.h.Issuer.Preflight(ctx) }

// TransportRefusal renders a federated route's transport refusals, auditing a
// refused body the way the other sign-in routes do.
func (s SSOSessions) TransportRefusal(provider string) httpapi.ErrorWriter {
	return s.h.signInTransportRefusal(provider)
}
