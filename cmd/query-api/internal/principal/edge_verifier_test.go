package principal

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// edgeTestSecret is a fixture key built at runtime, never a literal --
// a short, obviously-fake token repeated past NewEdgeVerifier's
// 32-character floor. Never a real value, never read from any
// environment or file.
var edgeTestSecret = strings.Repeat("not-a-real-secret-", 3)

const (
	edgeTestIssuer   = "dev-health-ops"
	edgeTestAudience = "dev-health-api"
)

// mustEdgeVerifier builds an EdgeVerifier and fails the test immediately
// on an unexpected construction error.
func mustEdgeVerifier(t *testing.T, secret, issuer, audience string) *EdgeVerifier {
	t.Helper()
	v, err := NewEdgeVerifier(secret, issuer, audience)
	if err != nil {
		t.Fatalf("NewEdgeVerifier: unexpected error: %v", err)
	}
	return v
}

// signEdgeToken signs claims with secret using HS256, mirroring
// AuthService.create_access_token's own jwt.encode call shape.
func signEdgeToken(t *testing.T, secret string, claims jwt.MapClaims) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString([]byte(secret))
	if err != nil {
		t.Fatalf("sign edge token: %v", err)
	}
	return signed
}

// validEdgeClaims returns a claim set matching every field
// AuthService.create_access_token (services/auth.py) sets on a real
// access token, for an org-scoped user.
func validEdgeClaims(orgID string) jwt.MapClaims {
	now := time.Now()
	return jwt.MapClaims{
		"sub":          "11111111-1111-1111-1111-111111111111",
		"email":        "user@example.com",
		"org_id":       orgID,
		"role":         "member",
		"is_superuser": false,
		"type":         "access",
		"iss":          edgeTestIssuer,
		"aud":          edgeTestAudience,
		"exp":          now.Add(time.Hour).Unix(),
		"iat":          now.Unix(),
		"jti":          "11111111-2222-3333-4444-555555555555",
		"tv":           0,
	}
}

func TestEdgeVerifier_AcceptsValidAccessToken(t *testing.T) {
	v := mustEdgeVerifier(t, edgeTestSecret, edgeTestIssuer, edgeTestAudience)
	token := signEdgeToken(t, edgeTestSecret, validEdgeClaims("org-42"))

	claims, err := v.Verify(context.Background(), token)
	if err != nil {
		t.Fatalf("Verify: unexpected error: %v", err)
	}
	if claims.OrgID != "org-42" {
		t.Fatalf("OrgID = %q, want %q", claims.OrgID, "org-42")
	}
}

func TestEdgeVerifier_MissingOrgIDDefaultsEmpty(t *testing.T) {
	v := mustEdgeVerifier(t, edgeTestSecret, edgeTestIssuer, edgeTestAudience)
	claims := validEdgeClaims("")
	delete(claims, "org_id")
	token := signEdgeToken(t, edgeTestSecret, claims)

	got, err := v.Verify(context.Background(), token)
	if err != nil {
		t.Fatalf("Verify: unexpected error: %v", err)
	}
	if got.OrgID != "" {
		t.Fatalf("OrgID = %q, want empty", got.OrgID)
	}
}

func TestEdgeVerifier_RejectsWrongSecret(t *testing.T) {
	v := mustEdgeVerifier(t, edgeTestSecret, edgeTestIssuer, edgeTestAudience)
	token := signEdgeToken(t, "a-completely-different-fixture-secret!!", validEdgeClaims("org-1"))

	if _, err := v.Verify(context.Background(), token); err == nil {
		t.Fatal("Verify: want error for a token signed with the wrong secret")
	}
}

func TestEdgeVerifier_RejectsAlgConfusion(t *testing.T) {
	v := mustEdgeVerifier(t, edgeTestSecret, edgeTestIssuer, edgeTestAudience)
	// "none" alg, unsigned -- classic alg-confusion probe.
	unsafeToken := jwt.NewWithClaims(jwt.SigningMethodNone, validEdgeClaims("org-1"))
	token, err := unsafeToken.SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatalf("sign none-alg token: %v", err)
	}
	if _, err := v.Verify(context.Background(), token); err == nil {
		t.Fatal("Verify: want error for a none-alg token")
	}
}

func TestEdgeVerifier_RejectsExpiredToken(t *testing.T) {
	v := mustEdgeVerifier(t, edgeTestSecret, edgeTestIssuer, edgeTestAudience)
	claims := validEdgeClaims("org-1")
	claims["exp"] = time.Now().Add(-time.Hour).Unix()
	token := signEdgeToken(t, edgeTestSecret, claims)

	if _, err := v.Verify(context.Background(), token); err == nil {
		t.Fatal("Verify: want error for an expired token")
	}
}

func TestEdgeVerifier_RejectsMissingExp(t *testing.T) {
	v := mustEdgeVerifier(t, edgeTestSecret, edgeTestIssuer, edgeTestAudience)
	claims := validEdgeClaims("org-1")
	delete(claims, "exp")
	token := signEdgeToken(t, edgeTestSecret, claims)

	if _, err := v.Verify(context.Background(), token); err == nil {
		t.Fatal("Verify: want error for a token with no exp claim")
	}
}

func TestEdgeVerifier_RejectsMissingSub(t *testing.T) {
	v := mustEdgeVerifier(t, edgeTestSecret, edgeTestIssuer, edgeTestAudience)
	claims := validEdgeClaims("org-1")
	delete(claims, "sub")
	token := signEdgeToken(t, edgeTestSecret, claims)

	if _, err := v.Verify(context.Background(), token); err == nil {
		t.Fatal("Verify: want error for a token with no sub claim")
	}
}

func TestEdgeVerifier_RejectsMissingType(t *testing.T) {
	v := mustEdgeVerifier(t, edgeTestSecret, edgeTestIssuer, edgeTestAudience)
	claims := validEdgeClaims("org-1")
	delete(claims, "type")
	token := signEdgeToken(t, edgeTestSecret, claims)

	if _, err := v.Verify(context.Background(), token); err == nil {
		t.Fatal("Verify: want error for a token with no type claim")
	}
}

func TestEdgeVerifier_RejectsRefreshTokenType(t *testing.T) {
	v := mustEdgeVerifier(t, edgeTestSecret, edgeTestIssuer, edgeTestAudience)
	claims := validEdgeClaims("org-1")
	claims["type"] = "refresh"
	token := signEdgeToken(t, edgeTestSecret, claims)

	if _, err := v.Verify(context.Background(), token); err == nil {
		t.Fatal("Verify: want error for a valid refresh token presented as an access token")
	}
}

func TestEdgeVerifier_AcceptsMissingAudienceAndIssuer(t *testing.T) {
	// validate_token only checks aud/iss when the token itself carries
	// the claim at all -- absent means unchecked, not rejected.
	v := mustEdgeVerifier(t, edgeTestSecret, edgeTestIssuer, edgeTestAudience)
	claims := validEdgeClaims("org-1")
	delete(claims, "aud")
	delete(claims, "iss")
	token := signEdgeToken(t, edgeTestSecret, claims)

	if _, err := v.Verify(context.Background(), token); err != nil {
		t.Fatalf("Verify: unexpected error for a token with no aud/iss claims: %v", err)
	}
}

func TestEdgeVerifier_RejectsWrongAudienceWhenPresent(t *testing.T) {
	v := mustEdgeVerifier(t, edgeTestSecret, edgeTestIssuer, edgeTestAudience)
	claims := validEdgeClaims("org-1")
	claims["aud"] = "someone-else"
	token := signEdgeToken(t, edgeTestSecret, claims)

	if _, err := v.Verify(context.Background(), token); err == nil {
		t.Fatal("Verify: want error for a token whose aud claim does not match")
	}
}

func TestEdgeVerifier_RejectsWrongIssuerWhenPresent(t *testing.T) {
	v := mustEdgeVerifier(t, edgeTestSecret, edgeTestIssuer, edgeTestAudience)
	claims := validEdgeClaims("org-1")
	claims["iss"] = "someone-else"
	token := signEdgeToken(t, edgeTestSecret, claims)

	if _, err := v.Verify(context.Background(), token); err == nil {
		t.Fatal("Verify: want error for a token whose iss claim does not match")
	}
}

func TestEdgeVerifier_RejectsMalformedToken(t *testing.T) {
	v := mustEdgeVerifier(t, edgeTestSecret, edgeTestIssuer, edgeTestAudience)

	if _, err := v.Verify(context.Background(), "not-a-jwt-at-all"); err == nil {
		t.Fatal("Verify: want error for a malformed token")
	}
}

func TestNewEdgeVerifier_RejectsShortSecret(t *testing.T) {
	if _, err := NewEdgeVerifier("too-short", edgeTestIssuer, edgeTestAudience); err == nil {
		t.Fatal("NewEdgeVerifier: want error for a secret under 32 characters")
	}
}

func TestNewEdgeVerifier_RejectsEmptyIssuer(t *testing.T) {
	if _, err := NewEdgeVerifier(edgeTestSecret, "", edgeTestAudience); err == nil {
		t.Fatal("NewEdgeVerifier: want error for an empty issuer")
	}
}

func TestNewEdgeVerifier_RejectsEmptyAudience(t *testing.T) {
	if _, err := NewEdgeVerifier(edgeTestSecret, edgeTestIssuer, ""); err == nil {
		t.Fatal("NewEdgeVerifier: want error for an empty audience")
	}
}
