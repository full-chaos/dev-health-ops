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
}

// PrincipalReferencesConfig builds a PrincipalReferences.
type PrincipalReferencesConfig struct {
	// IssuerURL is the iss every token carries (AUTH_TOKEN_ISSUER_URL).
	IssuerURL string
	// Keys is the signing key set the JWKS publishes.
	Keys KeySetSource
	Now  func() time.Time
}

// ReferencedPrincipal is the principal a verified reference names. It is
// identity only: its session, user and memberships are current state the
// caller still has to read.
type ReferencedPrincipal struct {
	ID        uuid.UUID
	SessionID string
	TokenID   string
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
	return &PrincipalReferences{verifier: verifier, now: now}, nil
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
