package authruntime

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

// legacyAccess is go-api's HS256 access token for grant, a bearer of the retired class. No production code mints one.
func legacyAccess(t *testing.T, secret, issuer, audience string, grant authapi.Grant) string {
	t.Helper()
	now := time.Now()
	claims := jwt.MapClaims{
		"sub": grant.PrincipalID.String(), "email": "", "org_id": "", "role": grant.Role, "is_superuser": false,
		"type": "access", "iss": issuer, "aud": audience, "exp": now.Add(edgetoken.AccessLifetime).Unix(), "iat": now.Unix(),
		"jti": uuid.NewString(), "tv": grant.User.TokenVersion, "sid": session.WireSessionID(grant.SessionID),
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(secret))
	if err != nil {
		t.Fatal(err)
	}
	return token
}

// legacyRefresh is go-api's HS256 refresh token for grant: the token logout still revokes (CHAOS-13125).
func legacyRefresh(t *testing.T, secret, issuer, audience string, grant authapi.Grant) string {
	t.Helper()
	signer, err := edgetoken.NewSigner(secret, issuer, audience)
	if err != nil {
		t.Fatal(err)
	}
	token, err := signer.Refresh(edgetoken.RefreshClaims{UserID: grant.PrincipalID.String(), FamilyID: uuid.NewString()}, time.Now(), uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	return token
}

// eddsaIssuer is the production issuer type over a file signer, for tests that mint a real token.
func eddsaIssuer(t *testing.T) *authapi.Issuer {
	t.Helper()
	dir := t.TempDir()
	writeSigningKey(t, dir, "k1.pem")
	signer := token.NewFileSigner(token.FileConfig{KeyPath: filepath.Join(dir, "k1.pem"), KeyID: "k1"})
	issuer, err := authapi.NewEdDSAIssuer(testAccessSecret, "dev-health-ops", "dev-health-api", signer, "https://auth.example.test")
	if err != nil {
		t.Fatal(err)
	}
	return issuer
}
