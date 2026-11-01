package saml

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/ssoaudit"
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
	// ErrUnusableEmail: the asserted email fails the shared provider email text rule.
	ErrUnusableEmail = identity.ErrUnusableProviderEmail
	// ErrSessionUnavailable: tokens cannot be minted now; ACS answers 503.
	ErrSessionUnavailable = errors.New("saml: the session cannot be issued now")
)

// UnavailableText is go-api's 503 detail when it cannot issue a session.
const UnavailableText = "Authentication service unavailable"

// EmailConflictError: no identity matched, and the asserted email belongs to
// an existing account. No account, identity or session was written; the request and assertion are consumed. The user signs in with Method and
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
	// ProviderFailure records a provider failure (an assertion without an
	// email: go-api's recordSSOFailure) before the refusal answers; nil
	// records none.
	ProviderFailure ssoaudit.ProviderFailure
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

// Resolve loads the provider, then its organization's entitlement gate, then
// the protocol and (with active) status checks. Every route runs it before it
// reads a request body.
func (s *Service) Resolve(ctx context.Context, id uuid.UUID, active bool) (Provider, error) {
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
	p, err := s.Resolve(ctx, providerID, false)
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

// Initiate resolves the provider and starts a sign-in.
func (s *Service) Initiate(ctx context.Context, providerID uuid.UUID, relayState string) (Initiation, error) {
	p, err := s.Resolve(ctx, providerID, true)
	if err != nil {
		return Initiation{}, err
	}
	return s.InitiateFor(ctx, p, relayState)
}

// InitiateFor stores a new request for a resolved provider and builds the
// AuthnRequest redirect.
func (s *Service) InitiateFor(ctx context.Context, p Provider, relayState string) (Initiation, error) {
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
		CookiePath: ACSPath(p, base), MaxAge: FlowTTL,
	}, nil
}

// SignIn is a completed SAML sign-in, for the session issuer.
type SignIn struct {
	PrincipalID    uuid.UUID
	OrganizationID *uuid.UUID
	ProviderID     uuid.UUID
	AMR            []string
	AuthTime       time.Time
}

// Consume resolves the provider and completes a sign-in.
func (s *Service) Consume(ctx context.Context, providerID uuid.UUID, posted string, cookie func(string) string) (SignIn, error) {
	p, err := s.Resolve(ctx, providerID, true)
	if err != nil {
		return SignIn{}, err
	}
	if err := s.BoundToRoute(ctx, p, providerID); err != nil {
		return SignIn{}, err
	}
	return s.ConsumeFor(ctx, p, posted, cookie)
}

// BoundToRoute refuses a response unless the provider row is the one the
// receiving route names and the path of its ACS URL ends with
// /saml/{that row id}/acs, so a response addressed to one row never completes
// a sign-in through another row's route. It reads no state.
func (s *Service) BoundToRoute(ctx context.Context, p Provider, routeID uuid.UUID) error {
	if p.ID != routeID {
		return s.refuse(ctx, p, refused(ReasonAudience, "the route names another provider row"))
	}
	if !strings.HasSuffix(ACSPath(p, s.BaseURL()), acsPathSuffix(p.ID)) {
		return s.refuse(ctx, p, refused(ReasonAudience, "the ACS URL path names another provider row"))
	}
	return nil
}

// ConsumeFor verifies a posted SAMLResponse for a resolved provider and
// resolves the account. cookie returns a request cookie value by name (""
// when absent). Before the lookup, the assertion's email must be usable,
// present and allowed by the provider's domain list, on a key hit or a miss;
// on a hit the account then signs in from its stored state.
// Every sign-in ends in the provider's organization: an account that is not
// a member after the membership seam ran is ErrNotOrgMember, and a miss
// creates an account only through the membership seam (see provisionNew).
func (s *Service) ConsumeFor(ctx context.Context, p Provider, posted string, cookie func(string) string) (SignIn, error) {
	raw, err := DecodeResponse(posted)
	if err != nil {
		return SignIn{}, s.refuse(ctx, p, err)
	}
	if p.Config.Certificate == "" {
		if err := s.recordProviderFailure(ctx, p, "", msgNoCertificate); err != nil {
			return SignIn{}, err
		}
		return SignIn{}, s.refuse(ctx, p, refused(ReasonSignatureInvalid, "provider has no certificate"))
	}
	cert, err := ParseCertificate(p.Config.Certificate)
	if err != nil {
		if err := s.recordProviderFailure(ctx, p, "", msgCertificateParse); err != nil {
			return SignIn{}, err
		}
		return SignIn{}, s.refuse(ctx, p, refused(ReasonSignatureInvalid, "provider certificate does not parse"))
	}
	base := s.BaseURL()
	now := s.now()
	v, err := Verify(raw, SP{EntityID: p.SPEntityID(base), ACSURL: p.ACSURL(base), IdPEntityID: p.Config.EntityID, IdPCert: cert}, now)
	if err != nil {
		var r *RefusedError
		if errors.As(err, &r) && r.NoConfirmation {
			if err := s.recordProviderFailure(ctx, p, "", msgNoConfirmation); err != nil {
				return SignIn{}, err
			}
		}
		return SignIn{}, s.refuse(ctx, p, err)
	}

	if err := s.matchRequest(ctx, p, v, cookie, now); err != nil {
		return SignIn{}, s.refuse(ctx, p, err)
	}
	if err := s.Uses.UseAssertion(ctx, p.ID, v.Issuer, v.AssertionID, v.NotOnOrAfter); err != nil {
		return SignIn{}, s.refuse(ctx, p, err)
	}

	subject, err := Subject(v.NameID)
	if err != nil {
		return SignIn{}, s.refuse(ctx, p, refused(ReasonNameID, "NameID cannot key an identity"))
	}
	id := Identity{Issuer: v.Issuer, Provider: p.ID.String(), Subject: subject}
	email := s.email(p, v)
	if identity.RefuseProviderEmail(email) != nil {
		return SignIn{}, s.refuse(ctx, p, ErrUnusableEmail)
	}
	// The address and the provider's domain list decide every sign-in, linked
	// or not, before the lookup (go-api saml.go).
	if email == "" {
		if err := s.recordProviderFailure(ctx, p, "", msgEmailMissing); err != nil {
			return SignIn{}, err
		}
		return SignIn{}, s.refuse(ctx, p, ErrEmailMissing)
	}
	if len(p.AllowedDomains) > 0 && !strings.Contains(email, "@") {
		return SignIn{}, s.refuse(ctx, p, ErrEmailMalformed)
	}
	if !p.DomainAllowed(email) {
		return SignIn{}, s.refuse(ctx, p, ErrDomainNotAllowed)
	}
	legacy := &LegacyMatch{AuthProvider: "saml", Subject: v.NameID.Value, MembershipOrg: p.OrgID}
	principal, found, err := s.Store.IdentityBySubject(ctx, id, legacy)
	if errors.Is(err, ErrNotOrgMember) || errors.Is(err, ErrIdentityAmbiguous) {
		return SignIn{}, s.refuse(ctx, p, err)
	}
	if err != nil {
		return SignIn{}, err
	}
	if !found {
		principalID, err := s.provisionNew(ctx, p, id, v.NameID.Value, email, s.fullName(p, v))
		if err != nil {
			return SignIn{}, err
		}
		org := p.OrgID
		s.logger().InfoContext(ctx, "auth saml: sign-in accepted", "provider_id", p.ID, "org_id", org)
		return SignIn{PrincipalID: principalID, OrganizationID: &org, ProviderID: p.ID, AMR: []string{AMRFederated}, AuthTime: now}, nil
	}
	if principal.Disabled || principal.ServiceProvider {
		return SignIn{}, s.refuse(ctx, p, ErrAccountDisabled)
	}
	if err := s.requireMember(ctx, p, principal.ID); err != nil {
		return SignIn{}, err
	}

	org := p.OrgID
	s.logger().InfoContext(ctx, "auth saml: sign-in accepted", "provider_id", p.ID, "org_id", org)
	return SignIn{PrincipalID: principal.ID, OrganizationID: &org, ProviderID: p.ID, AMR: []string{AMRFederated}, AuthTime: now}, nil
}

// requireMember refuses an existing account that is not a member of the
// provider's organization after the membership seam (when set) tried to join
// it.
func (s *Service) requireMember(ctx context.Context, p Provider, principalID uuid.UUID) error {
	member, err := s.Memberships.Member(ctx, principalID, p.OrgID)
	if err != nil {
		return err
	}
	if !member && s.JIT != nil && p.AutoProvision {
		if err := s.JIT.JoinOnSignIn(ctx, principalID, p); err != nil {
			return err
		}
		if member, err = s.Memberships.Member(ctx, principalID, p.OrgID); err != nil {
			return err
		}
	}
	if !member {
		return s.refuse(ctx, p, ErrNotOrgMember)
	}
	return nil
}

// matchRequest binds a response to its stored request and browser. A response
// that names a request (InResponseTo) must match an open request of this
// provider whose binding cookie this browser holds; a response that names none
// is accepted only for a provider with allow_idp_initiated = true. Every other
// check has already applied.
func (s *Service) matchRequest(ctx context.Context, p Provider, v Verified, cookie func(string) string, now time.Time) error {
	if v.InResponseTo == "" {
		if !p.AllowIdPInitiated {
			return ErrUnsolicited
		}
		return nil
	}
	nonce := ""
	if cookie != nil {
		nonce = cookie(BindingCookieName(v.InResponseTo))
	}
	if nonce == "" {
		return ErrFlowNotFound
	}
	return s.Flows.ConsumeFlow(ctx, p.ID, v.InResponseTo, bindingHash(nonce), now)
}

// provisionNew handles a sign-in whose key matches no account. An asserted
// email another account holds is an EmailConflictError naming its sign-in
// method; a provider without auto-provisioning is ErrAutoProvisionDisabled; a
// service without the membership seam is ErrNotOrgMember. All of these write
// nothing. Otherwise the seam creates the account, its identity and its
// membership in one transaction; the session is issued after it returns.
func (s *Service) provisionNew(ctx context.Context, p Provider, id Identity, nameID, email, fullName string) (uuid.UUID, error) {
	normalized, err := s.Store.NormalizeEmail(email)
	if err != nil {
		return uuid.Nil, s.refuse(ctx, p, ErrInvalidEmailAddress)
	}
	owner, taken, err := s.Store.PrincipalByEmail(ctx, normalized)
	if err != nil {
		return uuid.Nil, s.refuse(ctx, p, err)
	}
	if taken {
		return uuid.Nil, s.refuse(ctx, p, &EmailConflictError{Method: owner.Method})
	}
	if !p.AutoProvision {
		if err := s.recordProviderFailure(ctx, p, stageProvisioning, msgAutoProvisionOff); err != nil {
			return uuid.Nil, err
		}
		return uuid.Nil, s.refuse(ctx, p, ErrAutoProvisionDisabled)
	}
	if s.JIT == nil {
		return uuid.Nil, s.refuse(ctx, p, ErrNotOrgMember)
	}
	var legacyName *string
	if fullName != "" {
		legacyName = &fullName
	}
	principalID, err := s.JIT.ProvisionOnSignIn(ctx,
		NewAccount{Identity: id, Email: normalized, FullName: fullName, LegacySubject: nameID, LegacyFullName: legacyName}, p)
	switch {
	case err == nil:
		return principalID, nil
	case errors.Is(err, ErrEmailTaken):
		return uuid.Nil, s.refuse(ctx, p, &EmailConflictError{})
	}
	return uuid.Nil, s.refuse(ctx, p, err)
}

func (s *Service) fullName(p Provider, v Verified) string {
	if src, ok := p.Config.AttributeMapping["name"]; ok && v.Attributes[src] != "" {
		return v.Attributes[src]
	}
	return v.Attributes["name"]
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

// refuse logs a refusal with its auth-outcome reason code and the failed
// check, and returns err unchanged. No NameID, email or assertion content is
// logged.
// go-api's SAML provider failure texts (internal/api/sso saml.go, provision.go), the record's error_message. A record
// carries one of these fixed texts, never an error's text or a value of the assertion.
const (
	msgEmailMissing     = "SAML assertion missing email attribute"
	msgNoCertificate    = "SAML certificate is required for validation"
	msgCertificateParse = "SAML signature validation failed"
	msgNoConfirmation   = "SAML subject confirmation missing"
	msgAutoProvisionOff = "User not found and auto-provisioning disabled"

	stageProvisioning = "provisioning"
)

// recordProviderFailure records a provider failure as go-api's recordSSOFailure does (protocol saml, the stage if
// any). A write error is returned as is: the 500 go-api answers.
func (s *Service) recordProviderFailure(ctx context.Context, p Provider, stage, message string) error {
	if s.ProviderFailure == nil {
		return nil
	}
	return s.ProviderFailure.Failed(ctx, ssoaudit.Row{OrgID: p.OrgID, ProviderID: p.ID, Protocol: "saml", Stage: stage, ErrorMessage: message})
}

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
	case errors.Is(err, ErrNotOrgMember):
		return "membership_required"
	case errors.Is(err, ErrIdentityAmbiguous):
		return "identity_conflict"
	case errors.Is(err, ErrEmailMissing), errors.Is(err, ErrEmailMalformed), errors.Is(err, ErrInvalidEmailAddress),
		errors.Is(err, ErrUnusableEmail):
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
