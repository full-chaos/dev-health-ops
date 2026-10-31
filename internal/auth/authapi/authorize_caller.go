package authapi

import (
	"errors"
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-go/authverify"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// authorizeCallerAudience is the one audience a policy-enforcement point's
// workload token carries to the authorize routes.
const authorizeCallerAudience = "auth-service"

// AuthorizeCallers authenticates the policy-enforcement point that calls the
// authorize routes: a workload token (JOSE typ workload+jwt), audience
// auth-service, whose subject is one of the configured caller workloads. A
// nil AuthorizeCallers refuses every caller.
type AuthorizeCallers struct {
	verifier workloadVerifier
	subjects map[string]struct{}
	now      func() time.Time
}

// AuthorizeCallersConfig builds an AuthorizeCallers.
type AuthorizeCallersConfig struct {
	// IssuerURL is the iss every token carries (AUTH_TOKEN_ISSUER_URL).
	IssuerURL string
	// Keys is the signing key set the JWKS publishes.
	Keys KeySetSource
	// Subjects are the caller workload principals, in their session.v1 wire
	// form. At least one.
	Subjects []string
	Now      func() time.Time
}

// AuthorizeCaller is an authenticated policy-enforcement point.
type AuthorizeCaller struct {
	Subject   string
	Principal uuid.UUID
	TokenID   string
}

// NewAuthorizeCallers validates cfg. Its errors never carry a subject.
func NewAuthorizeCallers(cfg AuthorizeCallersConfig) (*AuthorizeCallers, error) {
	if cfg.Keys == nil {
		return nil, errors.New("authapi: the authorize caller gate needs a key set")
	}
	if len(cfg.Subjects) == 0 {
		return nil, errors.New("authapi: the authorize caller gate needs at least one caller subject")
	}
	subjects := make(map[string]struct{}, len(cfg.Subjects))
	for i, subject := range cfg.Subjects {
		id, ok := parseWirePrincipal(subject)
		if !ok || session.WirePrincipalID(id) != subject {
			return nil, fmt.Errorf("authapi: authorize caller subject %d is not a principal id", i)
		}
		if _, dup := subjects[subject]; dup {
			return nil, fmt.Errorf("authapi: authorize caller subject %d is repeated", i)
		}
		subjects[subject] = struct{}{}
	}
	verifier, err := authverify.NewWorkloadVerifier(authverify.WorkloadVerifierConfig{
		Issuer: cfg.IssuerURL, Audience: authorizeCallerAudience, Keys: publishedKeys{source: cfg.Keys},
	})
	if err != nil {
		return nil, fmt.Errorf("authapi: the authorize caller verifier: %w", err)
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &AuthorizeCallers{verifier: verifier, subjects: subjects, now: now}, nil
}

// Authenticate decides the caller's bearer. Every other credential class is
// refused from the bearer's text; a wrong audience or an unlisted subject is
// refused from the unverified payload before the key set is read; then the
// signature, issuer, audience and times are verified and the subject is
// checked again on the verified claims. err is set only when the key set could
// not be read. The reason goes to the log, never to the caller.
func (a *AuthorizeCallers) Authenticate(bearer string) (AuthorizeCaller, string, error) {
	if a == nil || a.verifier == nil || len(a.subjects) == 0 {
		return AuthorizeCaller{}, "not_configured", nil
	}
	switch class, reason := classifyIssueBearer(bearer); class {
	case issueClassWorkload:
	case issueClassCompat:
		return AuthorizeCaller{}, "other_credential_class", nil
	default:
		return AuthorizeCaller{}, reason, nil
	}
	_, payload, ok := compactSegments(bearer)
	if !ok {
		return AuthorizeCaller{}, "contract", nil
	}
	aud, sub, ok := peekAudienceAndSubject(payload)
	if !ok {
		return AuthorizeCaller{}, "payload_not_strict", nil
	}
	if aud != authorizeCallerAudience {
		return AuthorizeCaller{}, "audience", nil
	}
	if _, listed := a.subjects[sub]; !listed {
		return AuthorizeCaller{}, "subject", nil
	}
	claims, err := a.verifier.Verify(bearer, a.now())
	if errors.Is(err, authverify.ErrKeySourceUnavailable) {
		return AuthorizeCaller{}, "unavailable", err
	}
	if err != nil {
		return AuthorizeCaller{}, string(authverify.ReasonOf(err)), nil
	}
	_, listed := a.subjects[claims.Subject]
	principal, ok := parseWirePrincipal(claims.Subject)
	if claims.Audience != authorizeCallerAudience || claims.Subject != sub || !listed || !ok {
		return AuthorizeCaller{}, "subject", nil
	}
	return AuthorizeCaller{Subject: claims.Subject, Principal: principal, TokenID: claims.TokenID}, "", nil
}
