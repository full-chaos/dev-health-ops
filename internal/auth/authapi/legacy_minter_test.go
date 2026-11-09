package authapi

import (
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// legacyMinter mints go-api's HS256 access token for a test that needs a bearer of the retired class. No
// production code mints one.
type legacyMinter struct {
	secret, issuer, audience string
	now                      func() time.Time
}

func newLegacyMinter(secret, issuer, audience string) (*legacyMinter, error) {
	if _, err := edgetoken.New(secret, issuer, audience); err != nil {
		return nil, err
	}
	return &legacyMinter{secret: secret, issuer: issuer, audience: audience, now: time.Now}, nil
}

func (m *legacyMinter) Access(grant Grant) (string, error) {
	now := m.now()
	expires, err := grantExpiry(grant, now, contracts.AccessTokenLifetimeCeiling)
	if err != nil {
		return "", err
	}
	org := ""
	if grant.OrganizationID != nil {
		org = grant.OrganizationID.String()
	}
	claims := jwt.MapClaims{
		"sub": grant.PrincipalID.String(), "email": grant.User.Email, "org_id": org, "role": grant.Role,
		"is_superuser": grant.User.IsSuperuser, "type": "access", "iss": m.issuer, "aud": m.audience,
		"exp": expires.Unix(), "iat": now.Unix(), "jti": uuid.NewString(), "tv": grant.User.TokenVersion,
		"sid": session.WireSessionID(grant.SessionID),
	}
	if grant.Actor != nil {
		claims["impersonating_user_id"] = grant.Actor.String()
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(m.secret))
	if err != nil {
		return "", fmt.Errorf("%w: %w", errIssue, err)
	}
	return token, nil
}

// newTestIssuer is an EdDSA issuer over a file signer; its verifier reads go-api's HS256 token under testSecret.
func newTestIssuer(t *testing.T) *Issuer {
	t.Helper()
	signer, _, _ := fileSigner(t)
	return newEdDSAIssuer(t, signer, testAudience)
}

// legacyAccess is go-api's HS256 access token for grant under testSecret, the bearer the session routes still verify.
func legacyAccess(t *testing.T, grant Grant) string {
	t.Helper()
	minter, err := newLegacyMinter(testSecret, testIssuer, testAudience)
	if err != nil {
		t.Fatal(err)
	}
	token, err := minter.Access(grant)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

// eddsaBearer reads the principal and session an EdDSA access token names, as the Verify of the retired class did.
func eddsaBearer(t *testing.T, compact string) (Bearer, string, error) {
	t.Helper()
	claims := claimsOf(t, compact)
	sessionID, err := session.ParseWireSessionID(claims.Sid)
	if err != nil {
		return Bearer{}, "", err
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(claims.Sub, "prn_"))
	if err != nil {
		return Bearer{}, "", err
	}
	principal, err := uuid.FromBytes(raw)
	if err != nil {
		return Bearer{}, "", err
	}
	return Bearer{PrincipalID: principal, SessionID: sessionID}, "", nil
}
