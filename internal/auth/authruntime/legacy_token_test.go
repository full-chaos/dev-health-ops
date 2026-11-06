package authruntime

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

// legacyAccess is go-api's HS256 access token for grant, a bearer of the retired class. No production code mints one.
func legacyAccess(t *testing.T, secret, issuer, audience string, grant authapi.Grant) string {
	t.Helper()
	signer, err := edgetoken.NewSigner(secret, issuer, audience)
	if err != nil {
		t.Fatal(err)
	}
	sid := session.WireSessionID(grant.SessionID)
	now := time.Now()
	token, err := signer.AccessUntil(edgetoken.AccessClaims{
		UserID: grant.PrincipalID.String(), Role: grant.Role, TokenVersion: grant.User.TokenVersion, SessionID: &sid,
	}, now, now.Add(edgetoken.AccessLifetime), uuid.NewString())
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
