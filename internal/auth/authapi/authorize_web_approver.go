package authapi

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-go/authverify"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/auth/workload"
)

// The web approver reference (CHAOS-11207): web signs it with its ACR web
// assertion key for a device or consent approval; acr-api checks it against
// the request's web assertion and forwards only it here as the
// principal_reference. Architecture: web docs/auth-acr-device-approval.md;
// diagram docs/reference/auth/diagrams/web-approver-reference.mmd.
const (
	webApproverReferenceType = "web-approver+jwt"
	// webApproverAction is the one action the reference answers for.
	webApproverAction = "acr.credential.issue"
	// webApproverLifetime and webApproverClockSkew equal acr's web assertion
	// bounds (acr internal/auth/web_assertion.go maxWebAssertionLifetime and
	// webAssertionClockSkew; a cell pins them).
	webApproverLifetime  = 30 * time.Second
	webApproverClockSkew = 5 * time.Second
)

// WebApproverReferencesConfig builds the web approver reference kind. Every
// field is required; the kind is off without it.
type WebApproverReferencesConfig struct {
	// Issuer is the web assertion issuer (AUTH_AUTHORIZE_WEB_APPROVER_ISSUER).
	Issuer string
	// JWKSFile is the public JWKS acr verifies web assertions with
	// (AUTH_AUTHORIZE_WEB_APPROVER_JWKS_FILE). It is read on every reference.
	JWKSFile string
	// Caller is the one workload principal (prn_) that may present the kind
	// (AUTH_AUTHORIZE_WEB_APPROVER_CALLER): acr-api.
	Caller string
	Now    func() time.Time
}

// WebApproverReferences verifies web approver references.
type WebApproverReferences struct {
	issuer string
	keys   *authverify.Ed25519JWKSVerifier
	caller string
	now    func() time.Time
}

// NewWebApproverReferences validates cfg. Its errors never carry a value.
func NewWebApproverReferences(cfg WebApproverReferencesConfig) (*WebApproverReferences, error) {
	if strings.TrimSpace(cfg.Issuer) == "" || strings.TrimSpace(cfg.JWKSFile) == "" {
		return nil, errors.New("authapi: the web approver reference needs an issuer and a JWKS file")
	}
	if id, ok := parseWirePrincipal(cfg.Caller); !ok || session.WirePrincipalID(id) != cfg.Caller {
		return nil, errors.New("authapi: the web approver reference caller is not a principal id")
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &WebApproverReferences{issuer: cfg.Issuer, keys: authverify.NewEd25519JWKSVerifier(cfg.JWKSFile), caller: cfg.Caller, now: now}, nil
}

// Caller is the workload principal that may present the kind.
func (w *WebApproverReferences) Caller() string { return w.caller }

type webApproverClaims struct {
	Action         string      `json:"action"`
	Audience       string      `json:"aud"`
	ExpiresAt      json.Number `json:"exp"`
	IssuedAt       json.Number `json:"iat"`
	Impersonating  *bool       `json:"impersonating"`
	Issuer         string      `json:"iss"`
	JWTID          string      `json:"jti"`
	NotBefore      json.Number `json:"nbf"`
	OrganizationID string      `json:"org_id"`
	Request        string      `json:"req"`
	SessionID      string      `json:"sid"`
	Subject        string      `json:"sub"`
}

// resolve verifies reference. err is set only when the JWKS file could not be
// read; the reason goes to the log, never to the caller.
func (w *WebApproverReferences) resolve(reference string) (ReferencedPrincipal, string, error) {
	parts := strings.Split(reference, ".")
	if len(parts) != 3 {
		return ReferencedPrincipal{}, "contract", nil
	}
	var header struct {
		Alg string `json:"alg"`
		Typ string `json:"typ"`
		Kid string `json:"kid"`
	}
	if !strictSegment(parts[0], &header) || header.Alg != "EdDSA" || header.Typ != webApproverReferenceType || header.Kid == "" || len(header.Kid) > 256 {
		return ReferencedPrincipal{}, "credential_class", nil
	}
	set, err := w.keys.KeySet()
	if err != nil {
		if errors.Is(err, authverify.ErrInvalidJWKS) {
			return ReferencedPrincipal{}, "unavailable", fmt.Errorf("authapi: the web approver JWKS is invalid: %w", err)
		}
		return ReferencedPrincipal{}, "unavailable", fmt.Errorf("authapi: the web approver JWKS could not be read: %w", authverify.ErrKeySourceUnavailable)
	}
	key, found := set[header.Kid]
	if !found {
		return ReferencedPrincipal{}, "unknown_kid", nil
	}
	now := w.now().UTC()
	if key.RetireAfter != nil && (!now.Before(key.RetireAfter.UTC()) || key.RetireAfter.UTC().After(now.Add(workload.AssertionOverlapCeiling))) {
		return ReferencedPrincipal{}, "key_retired", nil
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || !ed25519.Verify(key.Public, []byte(parts[0]+"."+parts[1]), signature) {
		return ReferencedPrincipal{}, "signature", nil
	}
	var claims webApproverClaims
	if !strictSegment(parts[1], &claims) {
		return ReferencedPrincipal{}, "payload_not_strict", nil
	}
	return w.principal(claims, now)
}

func (w *WebApproverReferences) principal(claims webApproverClaims, now time.Time) (ReferencedPrincipal, string, error) {
	issuedAt, issuedErr := claims.IssuedAt.Int64()
	notBefore, notBeforeErr := claims.NotBefore.Int64()
	expiresAt, expiresErr := claims.ExpiresAt.Int64()
	switch {
	case claims.Issuer != w.issuer:
		return ReferencedPrincipal{}, "issuer", nil
	case claims.Audience != authorizeCallerAudience:
		return ReferencedPrincipal{}, "audience", nil
	case claims.Action != webApproverAction:
		return ReferencedPrincipal{}, "action", nil
	case issuedErr != nil || notBeforeErr != nil || expiresErr != nil || expiresAt < issuedAt ||
		expiresAt-issuedAt > int64(webApproverLifetime/time.Second):
		return ReferencedPrincipal{}, "lifetime", nil
	case issuedAt > now.Add(webApproverClockSkew).Unix() || notBefore > now.Add(webApproverClockSkew).Unix():
		return ReferencedPrincipal{}, "not_yet_valid", nil
	case expiresAt < now.Add(-webApproverClockSkew).Unix():
		return ReferencedPrincipal{}, "expired", nil
	case claims.Impersonating == nil || *claims.Impersonating:
		return ReferencedPrincipal{}, "impersonating", nil
	case claims.JWTID == "" || len(claims.JWTID) > 256 || len(claims.Request) != 43:
		return ReferencedPrincipal{}, "contract", nil
	}
	principal, err := uuid.Parse(claims.Subject)
	if err != nil || principal.String() != claims.Subject || principal == uuid.Nil {
		return ReferencedPrincipal{}, "subject", nil
	}
	org, err := uuid.Parse(claims.OrganizationID)
	if err != nil || org.String() != claims.OrganizationID {
		return ReferencedPrincipal{}, "organization", nil
	}
	if _, err := session.ParseWireSessionID(claims.SessionID); err != nil {
		return ReferencedPrincipal{}, "session", nil
	}
	return ReferencedPrincipal{ID: principal, Kind: ReferenceWebApprover, SessionID: claims.SessionID, OrgID: claims.OrganizationID,
		TokenID: claims.JWTID}, "", nil
}

// strictSegment decodes one base64url JSON segment into v: no member v lacks,
// numbers kept as text, nothing after the object.
func strictSegment(segment string, v any) bool {
	raw, err := base64.RawURLEncoding.Strict().DecodeString(segment)
	if err != nil {
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(v); err != nil || decoder.More() {
		return false
	}
	return true
}

// webApproverBinds reports whether one question may be decided for a web
// approver reference: only acr.credential.issue on the reference's own
// organization, keyed by the organization id, on the single authorize route.
func webApproverBinds(op string, q AuthorizationQuestion, p ReferencedPrincipal) bool {
	return op == opAuthorize && q.Action == webApproverAction && q.ResourceType == "organization" &&
		q.ResourceKey == p.OrgID && q.OrganizationID == p.OrgID
}
