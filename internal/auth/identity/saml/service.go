package saml

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// AMRFederated is the session amr value for a sign-in at an external IdP.
const AMRFederated = "fed"

// Domain errors. The handlers map them to statuses.
var (
	ErrNotSAML               = errors.New("saml: provider is not SAML")
	ErrProviderInactive      = errors.New("saml: provider is not active")
	ErrNotEntitled           = errors.New("saml: organization is not entitled to SSO")
	ErrUnsolicited           = errors.New("saml: unsolicited response refused")
	ErrEmailMissing          = errors.New("saml: assertion has no email")
	ErrEmailMalformed        = errors.New("saml: asserted email is malformed")
	ErrDomainNotAllowed      = errors.New("saml: email domain is not allowed for this provider")
	ErrAutoProvisionDisabled = errors.New("saml: user not found and auto-provisioning disabled")
	ErrAccountDisabled       = errors.New("saml: account is disabled")
)

// EmailConflictError: no identity matched, and the asserted email belongs to
// an existing account. Nothing was written. The user signs in with Method and
// links the identity explicitly.
type EmailConflictError struct {
	Method string
}

func (e *EmailConflictError) Error() string { return "saml: email belongs to an existing account" }

// Service is the SAML SP domain.
type Service struct {
	Providers    ProviderReader
	Entitlements Entitlements
	Flows        FlowStore
	Uses         AssertionUses
	Store        Store
	Memberships  Memberships
	JIT          MembershipJIT
	BaseURL      func() string
	Now          func() time.Time
	Logger       *slog.Logger
}

func (s *Service) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *Service) logger() *slog.Logger {
	if s.Logger != nil {
		return s.Logger
	}
	return slog.Default()
}

// provider loads a SAML provider and applies the entitlement gate. active
// additionally requires status "active".
func (s *Service) provider(ctx context.Context, id uuid.UUID, active bool) (Provider, error) {
	p, err := s.Providers.Provider(ctx, id)
	if err != nil {
		return Provider{}, err
	}
	allowed, err := s.Entitlements.OrgHasSSO(ctx, p.OrgID)
	if err != nil {
		s.logger().ErrorContext(ctx, "auth saml: entitlement check failed; denying", "provider_id", id, logging.ErrorAttr(err))
	}
	if !allowed {
		return Provider{}, ErrNotEntitled
	}
	if p.Protocol != "saml" {
		return Provider{}, ErrNotSAML
	}
	if active && p.Status != "active" {
		return Provider{}, ErrProviderInactive
	}
	return p, nil
}

// Metadata is the SP metadata of one provider.
type Metadata struct {
	XML      string
	EntityID string
	ACSURL   string
}

// Metadata returns the SP metadata document.
func (s *Service) Metadata(ctx context.Context, providerID uuid.UUID) (Metadata, error) {
	p, err := s.provider(ctx, providerID, false)
	if err != nil {
		return Metadata{}, err
	}
	base := s.BaseURL()
	return Metadata{
		XML:      SPMetadataXML(p.SPEntityID(base), p.ACSURL(base), p.Config.NameIDFormat),
		EntityID: p.SPEntityID(base),
		ACSURL:   p.ACSURL(base),
	}, nil
}

// Initiation is an SP-initiated sign-in: the IdP redirect and the browser
// binding cookie to set.
type Initiation struct {
	RedirectURL string
	CookieName  string
	CookieValue string
	CookiePath  string
	MaxAge      time.Duration
}

// Initiate stores a new request and builds the AuthnRequest redirect.
func (s *Service) Initiate(ctx context.Context, providerID uuid.UUID, relayState string) (Initiation, error) {
	p, err := s.provider(ctx, providerID, true)
	if err != nil {
		return Initiation{}, err
	}
	base := s.BaseURL()
	requestID, err := newRequestID()
	if err != nil {
		return Initiation{}, err
	}
	nonce, err := newNonce()
	if err != nil {
		return Initiation{}, err
	}
	now := s.now()
	if err := s.Flows.SaveFlow(ctx, Flow{
		ProviderID: p.ID, RequestID: requestID, BindingHash: bindingHash(nonce), ExpiresAt: now.Add(FlowTTL),
	}); err != nil {
		return Initiation{}, fmt.Errorf("saml: store request: %w", err)
	}
	destination := p.Config.SSOURL
	if destination == "" {
		destination = "None"
	}
	redirect, err := redirectURL(destination,
		authnRequestXML(requestID, now, destination, p.ACSURL(base), p.SPEntityID(base), p.Config.NameIDFormat), relayState)
	if err != nil {
		return Initiation{}, err
	}
	s.logger().InfoContext(ctx, "auth saml: sign-in request stored", "provider_id", p.ID)
	return Initiation{
		RedirectURL: redirect, CookieName: BindingCookieName(requestID), CookieValue: nonce,
		CookiePath: ACSPath(p), MaxAge: FlowTTL,
	}, nil
}

// SignIn is a completed SAML sign-in, for the session issuer.
type SignIn struct {
	PrincipalID    uuid.UUID
	OrganizationID *uuid.UUID
	ProviderID     uuid.UUID
	AMR            []string
	AuthTime       time.Time
	Created        bool
}

// Consume verifies a posted SAMLResponse and resolves the account. cookie
// returns a request cookie value by name ("" when absent).
func (s *Service) Consume(ctx context.Context, providerID uuid.UUID, posted string, cookie func(string) string) (SignIn, error) {
	p, err := s.provider(ctx, providerID, true)
	if err != nil {
		return SignIn{}, err
	}
	raw, err := DecodeResponse(posted)
	if err != nil {
		return SignIn{}, s.refuse(ctx, p, err)
	}
	if p.Config.Certificate == "" {
		return SignIn{}, s.refuse(ctx, p, refused(ReasonSignatureInvalid, "provider has no certificate"))
	}
	cert, err := ParseCertificate(p.Config.Certificate)
	if err != nil {
		return SignIn{}, s.refuse(ctx, p, refused(ReasonSignatureInvalid, "provider certificate does not parse"))
	}
	base := s.BaseURL()
	now := s.now()
	v, err := Verify(raw, SP{EntityID: p.SPEntityID(base), ACSURL: p.ACSURL(base), IdPEntityID: p.Config.EntityID, IdPCert: cert}, now)
	if err != nil {
		return SignIn{}, s.refuse(ctx, p, err)
	}

	if err := s.matchRequest(ctx, p, v, cookie, now); err != nil {
		return SignIn{}, s.refuse(ctx, p, err)
	}
	if err := s.Uses.UseAssertion(ctx, p.ID, v.Issuer, v.AssertionID, v.NotOnOrAfter); err != nil {
		return SignIn{}, s.refuse(ctx, p, err)
	}

	email := s.email(p, v)
	if email == "" {
		return SignIn{}, s.refuse(ctx, p, ErrEmailMissing)
	}
	if len(p.AllowedDomains) > 0 && !strings.Contains(email, "@") {
		return SignIn{}, s.refuse(ctx, p, ErrEmailMalformed)
	}
	if !p.DomainAllowed(email) {
		return SignIn{}, s.refuse(ctx, p, ErrDomainNotAllowed)
	}

	subject, err := Subject(v.NameID)
	if err != nil {
		return SignIn{}, s.refuse(ctx, p, refused(ReasonNameID, "NameID cannot key an identity"))
	}
	id := Identity{Issuer: v.Issuer, Provider: p.ID.String(), Subject: subject}
	legacy := &LegacyMatch{AuthProvider: "saml", Subject: v.NameID.Value, MembershipOrg: p.OrgID}
	principal, found, err := s.Store.IdentityBySubject(ctx, id, legacy)
	if err != nil {
		return SignIn{}, err
	}
	created := false
	if found {
		if principal.Disabled {
			return SignIn{}, s.refuse(ctx, p, ErrAccountDisabled)
		}
	} else {
		principal.ID, err = s.provision(ctx, p, id, email, s.displayName(p, v))
		if err != nil {
			return SignIn{}, s.refuse(ctx, p, err)
		}
		created = true
	}

	out := SignIn{PrincipalID: principal.ID, ProviderID: p.ID, AMR: []string{AMRFederated}, AuthTime: now, Created: created}
	member, err := s.Memberships.Member(ctx, principal.ID, p.OrgID)
	if err != nil {
		return SignIn{}, err
	}
	if !member && s.JIT != nil {
		if err := s.JIT.JoinOnSignIn(ctx, principal.ID, p); err != nil {
			return SignIn{}, err
		}
		if member, err = s.Memberships.Member(ctx, principal.ID, p.OrgID); err != nil {
			return SignIn{}, err
		}
	}
	if member {
		org := p.OrgID
		out.OrganizationID = &org
	}
	s.logger().InfoContext(ctx, "auth saml: sign-in accepted", "provider_id", p.ID, "created", created, "member", member)
	return out, nil
}

// matchRequest binds a solicited response to its stored request and browser.
// A response that answers no stored request is accepted only for a provider
// with allow_idp_initiated = true; every other check has already applied. A
// response to a request that was already used is refused for every provider.
func (s *Service) matchRequest(ctx context.Context, p Provider, v Verified, cookie func(string) string, now time.Time) error {
	unsolicitedAllowed := p.AllowIdPInitiated
	if v.InResponseTo == "" {
		if !unsolicitedAllowed {
			return ErrUnsolicited
		}
		return nil
	}
	nonce := ""
	if cookie != nil {
		nonce = cookie(BindingCookieName(v.InResponseTo))
	}
	err := ErrFlowNotFound
	if nonce != "" {
		err = s.Flows.ConsumeFlow(ctx, p.ID, v.InResponseTo, bindingHash(nonce), now)
	}
	if errors.Is(err, ErrFlowNotFound) && unsolicitedAllowed {
		return nil
	}
	return err
}

func (s *Service) provision(ctx context.Context, p Provider, id Identity, email, name string) (uuid.UUID, error) {
	normalized, err := s.Store.NormalizeEmail(email)
	if err != nil {
		return uuid.Nil, ErrInvalidEmailAddress
	}
	owner, taken, err := s.Store.PrincipalByEmail(ctx, normalized)
	if err != nil {
		return uuid.Nil, err
	}
	if taken {
		return uuid.Nil, &EmailConflictError{Method: owner.Method}
	}
	if !p.AutoProvision {
		return uuid.Nil, ErrAutoProvisionDisabled
	}
	principalID, err := s.Store.ProvisionFederated(ctx, NewFederatedUser{
		Email: normalized, DisplayName: name, Identity: id, AuditRequestID: auditRequestID(ctx),
	})
	switch {
	case errors.Is(err, ErrEmailTaken):
		conflict := &EmailConflictError{}
		if owner, ok, lookupErr := s.Store.PrincipalByEmail(ctx, normalized); lookupErr == nil && ok {
			conflict.Method = owner.Method
		}
		return uuid.Nil, conflict
	case err != nil:
		return uuid.Nil, err
	}
	return principalID, nil
}

func (s *Service) email(p Provider, v Verified) string {
	if src, ok := p.Config.AttributeMapping["email"]; ok && v.Attributes[src] != "" {
		return v.Attributes[src]
	}
	if e := v.Attributes["email"]; e != "" {
		return e
	}
	if strings.Contains(v.NameID.Value, "@") {
		return v.NameID.Value
	}
	return ""
}

func (s *Service) displayName(p Provider, v Verified) string {
	for _, target := range []string{"full_name", "name"} {
		if src, ok := p.Config.AttributeMapping[target]; ok && v.Attributes[src] != "" {
			return v.Attributes[src]
		}
	}
	return ""
}

// refuse logs a refusal with its auth-outcome reason code and the failed
// check, and returns err unchanged. No NameID, email or assertion content is
// logged.
func (s *Service) refuse(ctx context.Context, p Provider, err error) error {
	check, detail := "", ""
	var r *RefusedError
	if errors.As(err, &r) {
		check, detail = r.Reason, r.Detail
	}
	s.logger().WarnContext(ctx, "auth saml: sign-in refused", "provider_id", p.ID, "org_id", p.OrgID,
		"reason_code", OutcomeReason(err), "check", check, "detail", detail)
	return err
}

// OutcomeReason is the auth-outcome.v1 reason_code for a refusal.
func OutcomeReason(err error) string {
	var conflict *EmailConflictError
	switch {
	case err == nil:
		return "accepted"
	case errors.As(err, &conflict), errors.Is(err, ErrEmailTaken):
		return "email_collision"
	case errors.Is(err, ErrAssertionReplayed), errors.Is(err, ErrFlowConsumed):
		return "replayed"
	case errors.Is(err, ErrUnsolicited), errors.Is(err, ErrFlowNotFound):
		return "nonce_mismatch"
	case errors.Is(err, ErrSignupNotAvailable):
		return "signup_unavailable"
	case errors.Is(err, ErrAutoProvisionDisabled):
		return "provisioning_disabled"
	case errors.Is(err, ErrDomainNotAllowed):
		return "domain_not_allowed"
	case errors.Is(err, ErrNotEntitled):
		return "not_entitled"
	case errors.Is(err, ErrAccountDisabled):
		return "principal_deactivated"
	case errors.Is(err, ErrEmailMissing), errors.Is(err, ErrEmailMalformed), errors.Is(err, ErrInvalidEmailAddress):
		return "malformed"
	}
	var r *RefusedError
	if !errors.As(err, &r) {
		return "dependency_unavailable"
	}
	switch r.Reason {
	case ReasonSignatureMissing, ReasonSignaturePlacement, ReasonSignatureAlgorithm, ReasonSignatureReference, ReasonSignatureInvalid:
		return "bad_signature"
	case ReasonIssuer:
		return "issuer"
	case ReasonAudience, ReasonRecipient, ReasonDestination:
		return "audience"
	case ReasonExpired, ReasonIssueInstant:
		return "expired"
	case ReasonNotYetValid:
		return "not_yet_valid"
	case ReasonInResponseTo:
		return "nonce_mismatch"
	case ReasonStatus:
		return "invalid_credentials"
	}
	return "malformed"
}
