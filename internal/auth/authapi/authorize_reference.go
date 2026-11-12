package authapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/full-chaos/dev-health-go/authverify"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// principalReferencePattern is authorization_request.v1's principal_reference
// pattern.
var principalReferencePattern = regexp.MustCompile(`^[A-Za-z0-9._~-]{16,1000}$`)

// accessVerifier is satisfied by *authverify.Verifier.
type accessVerifier interface {
	Verify(token string, now time.Time) (*authverify.AccessClaims, error)
}

// PrincipalReferences resolves an authorize request's principal_reference: the
// end user's EdDSA access token (access-token.v1, typ at+jwt) for the platform
// API audience, never an HS256 token, a workload token or a session-route
// token. A nil PrincipalReferences refuses every reference.
type PrincipalReferences struct {
	verifier accessVerifier
	now      func() time.Time
	// webApprover is the web approver reference kind; nil is off.
	webApprover *WebApproverReferences
}

// PrincipalReferencesConfig builds a PrincipalReferences.
type PrincipalReferencesConfig struct {
	// IssuerURL is the iss every token carries (AUTH_TOKEN_ISSUER_URL).
	IssuerURL string
	// Keys is the signing key set the JWKS publishes.
	Keys KeySetSource
	Now  func() time.Time
	// WebApprover turns on the web approver reference kind (CHAOS-11207); nil
	// refuses it.
	WebApprover *WebApproverReferences
}

// ReferenceKind is the kind of principal a verified reference names.
type ReferenceKind uint8

const (
	// ReferenceUser is an end user's access token: its session must be live.
	ReferenceUser ReferenceKind = iota
	// ReferenceServiceAccount is a service-account token: it has no session;
	// the key row it was exchanged for must be live.
	ReferenceServiceAccount
	// ReferenceWebApprover is a web approver reference (CHAOS-11207): web
	// signed it for one approval and acr-api forwards it. Its session must be
	// live; it answers only acr.credential.issue on its own organization.
	ReferenceWebApprover
)

// ReferencedPrincipal is the principal a verified reference names. It is
// identity only: its session or key, user and memberships are current state
// the caller still has to read.
type ReferencedPrincipal struct {
	ID        uuid.UUID
	Kind      ReferenceKind
	SessionID string
	// CredentialID and OrgID are set for a service account only: the key row
	// the token was exchanged for and the organization the token names.
	CredentialID uuid.UUID
	OrgID        string
	TokenID      string
}

// NewPrincipalReferences validates cfg.
func NewPrincipalReferences(cfg PrincipalReferencesConfig) (*PrincipalReferences, error) {
	if cfg.Keys == nil {
		return nil, errors.New("authapi: the principal reference resolver needs a key set")
	}
	verifier, err := authverify.NewVerifier(authverify.VerifierConfig{
		Issuer: cfg.IssuerURL, Audience: contracts.PlatformAPIAudience, Keys: publishedKeys{source: cfg.Keys},
	})
	if err != nil {
		return nil, fmt.Errorf("authapi: the principal reference verifier: %w", err)
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &PrincipalReferences{verifier: verifier, now: now, webApprover: cfg.WebApprover}, nil
}

// ResolveFor resolves reference for the authenticated caller. A web approver
// reference (typ web-approver+jwt) resolves only for the configured approver
// caller, and that caller may present nothing else; every other caller gets
// Resolve's answer.
func (p *PrincipalReferences) ResolveFor(reference, caller string) (ReferencedPrincipal, string, error) {
	if p == nil || p.webApprover == nil {
		return p.Resolve(reference)
	}
	if !principalReferencePattern.MatchString(reference) {
		return ReferencedPrincipal{}, "malformed", nil
	}
	header, _, ok := compactSegments(reference)
	if !ok {
		return ReferencedPrincipal{}, "contract", nil
	}
	var parsed struct {
		Typ string `json:"typ"`
	}
	approver := json.Unmarshal(header, &parsed) == nil && parsed.Typ == webApproverReferenceType
	switch {
	case approver && caller != p.webApprover.Caller():
		return ReferencedPrincipal{}, "approver_caller", nil
	case approver:
		return p.webApprover.resolve(reference)
	case caller == p.webApprover.Caller():
		return ReferencedPrincipal{}, "approver_caller_other_class", nil
	}
	return p.Resolve(reference)
}

// Resolve verifies reference. Another token class or a wrong audience is
// refused from the unverified header and payload before the key set is read.
// err is set only when the key set could not be read. The reason goes to the
// log, never to the caller.
func (p *PrincipalReferences) Resolve(reference string) (ReferencedPrincipal, string, error) {
	if p == nil || p.verifier == nil {
		return ReferencedPrincipal{}, "not_configured", nil
	}
	if !principalReferencePattern.MatchString(reference) {
		return ReferencedPrincipal{}, "malformed", nil
	}
	header, payload, ok := compactSegments(reference)
	if !ok {
		return ReferencedPrincipal{}, "contract", nil
	}
	var parsed struct {
		Alg string `json:"alg"`
		Typ string `json:"typ"`
	}
	if err := json.Unmarshal(header, &parsed); err != nil || parsed.Alg != "EdDSA" || parsed.Typ != accessTokenTyp {
		return ReferencedPrincipal{}, "credential_class", nil
	}
	aud, _, ok := peekAudienceAndSubject(payload)
	if !ok {
		return ReferencedPrincipal{}, "payload_not_strict", nil
	}
	if aud != contracts.PlatformAPIAudience {
		return ReferencedPrincipal{}, "audience", nil
	}
	claims, err := p.verifier.Verify(reference, p.now())
	if errors.Is(err, authverify.ErrKeySourceUnavailable) {
		return ReferencedPrincipal{}, "unavailable", err
	}
	if err != nil {
		return ReferencedPrincipal{}, string(authverify.ReasonOf(err)), nil
	}
	scope, _ := contracts.AccessTokenScope(contracts.PlatformAPIAudience)
	if claims.Audience != contracts.PlatformAPIAudience || claims.Scope != scope {
		return ReferencedPrincipal{}, "audience", nil
	}
	principal, ok := parseWirePrincipal(claims.Subject)
	if !ok {
		return ReferencedPrincipal{}, "subject", nil
	}
	return ReferencedPrincipal{ID: principal, SessionID: claims.SessionID, TokenID: claims.TokenID}, "", nil
}
