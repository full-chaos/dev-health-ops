package livee2e_test

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
	"github.com/full-chaos/dev-health-ops/internal/cigates/livee2e"
	"github.com/full-chaos/dev-health-ops/internal/signingkey"
)

const (
	tokenUser    = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	tokenOrg     = "11111111-2222-4333-8444-555555555555"
	tokenSession = "cccccccc-1111-4222-8333-444444444444"
	tokenIssuer  = "https://localhost:8443"
)

func writeIssuerKey(t *testing.T) string {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "issuer.key")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o400); err != nil {
		t.Fatal(err)
	}
	return path
}

func tokenEnv(keyFile string) func(string) string {
	env := map[string]string{
		"E2E_USER_ID":         tokenUser,
		"E2E_ORG_ID":          tokenOrg,
		"E2E_SESSION_ID":      tokenSession,
		"E2E_ISSUER_URL":      tokenIssuer,
		"E2E_ISSUER_KEY_FILE": keyFile,
	}
	return func(name string) string { return env[name] }
}

func decodeSegment(t *testing.T, segment string) map[string]any {
	t.Helper()
	raw, err := base64.RawURLEncoding.DecodeString(segment)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestMintTokenIsEdDSA(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := livee2e.Run([]string{"mint-token"}, tokenEnv(writeIssuerKey(t)), &stdout, &stderr)
	if code != 0 {
		t.Fatalf("mint-token exited %d: %s", code, stderr.String())
	}
	parts := strings.Split(strings.TrimSpace(stdout.String()), ".")
	if len(parts) != 3 {
		t.Fatalf("mint-token printed %d segments, want 3", len(parts))
	}
	header := decodeSegment(t, parts[0])
	if header["alg"] != "EdDSA" || header["typ"] != "at+jwt" || header["kid"] == nil || header["kid"] == "" {
		t.Fatalf("header = %v, want alg EdDSA, typ at+jwt and a kid", header)
	}
	claims := decodeSegment(t, parts[1])
	if claims["iss"] != tokenIssuer || claims["aud"] != "dev-health-api" || claims["scope"] != "api:access" {
		t.Fatalf("claims name the wrong issuer, audience or scope")
	}
}

func TestNoHS256SecretVerb(t *testing.T) {
	var stdout, stderr bytes.Buffer
	getenv := func(name string) string {
		if name == "ENC_KEY_INPUT" {
			return "dev-key-not-for-prod"
		}
		return ""
	}
	if code := livee2e.Run([]string{"jwt-secret"}, getenv, &stdout, &stderr); code != 2 {
		t.Fatalf("jwt-secret exited %d, want 2 (no such verb)", code)
	}
	if strings.Contains(stderr.String(), "jwt-secret") {
		t.Fatalf("usage still names jwt-secret")
	}
}

func mintedToken(t *testing.T, keyFile string, env map[string]string) string {
	t.Helper()
	base := map[string]string{
		"E2E_USER_ID": tokenUser, "E2E_ORG_ID": tokenOrg, "E2E_SESSION_ID": tokenSession,
		"E2E_ISSUER_URL": tokenIssuer, "E2E_ISSUER_KEY_FILE": keyFile,
	}
	for name, value := range env {
		base[name] = value
	}
	var stdout, stderr bytes.Buffer
	if code := livee2e.Run([]string{"mint-token"}, func(name string) string { return base[name] }, &stdout, &stderr); code != 0 {
		t.Fatalf("mint-token exited %d: %s", code, stderr.String())
	}
	return strings.TrimSpace(stdout.String())
}

func publicKeyOf(t *testing.T, keyFile string) ed25519.PublicKey {
	t.Helper()
	pemBytes, err := os.ReadFile(keyFile)
	if err != nil {
		t.Fatal(err)
	}
	key, err := signingkey.LoadPrivateKey(pemBytes)
	if err != nil {
		t.Fatal(err)
	}
	return key.Public().(ed25519.PublicKey)
}

func TestMintedTokenVerifiesAsAnAccessTokenV1ForTheKeyAndIssuer(t *testing.T) {
	keyFile := writeIssuerKey(t)
	token := mintedToken(t, keyFile, nil)
	verifier, err := authverify.NewVerifier(authverify.VerifierConfig{
		Issuer: tokenIssuer, Audience: "dev-health-api", Keys: authverify.StaticKeys{livee2e.DefaultIssuerKeyID: publicKeyOf(t, keyFile)},
	})
	if err != nil {
		t.Fatal(err)
	}
	claims, err := verifier.Verify(token, time.Now())
	if err != nil {
		t.Fatalf("token does not verify: %v", err)
	}
	user := uuid.MustParse(tokenUser)
	if claims.Subject != "prn_"+base64.RawURLEncoding.EncodeToString(user[:]) {
		t.Fatalf("sub does not name the user the caller gave")
	}
	if claims.OrgID == nil || *claims.OrgID != tokenOrg || claims.Scope != "api:access" {
		t.Fatalf("claims = %+v", claims)
	}
	if id, err := sessionid.Parse(claims.SessionID); err != nil || id != uuid.MustParse(tokenSession) {
		t.Fatalf("sid does not name the session the caller gave")
	}
	other := authverify.StaticKeys{livee2e.DefaultIssuerKeyID: publicKeyOf(t, writeIssuerKey(t))}
	otherVerifier, _ := authverify.NewVerifier(authverify.VerifierConfig{Issuer: tokenIssuer, Audience: "dev-health-api", Keys: other})
	if _, err := otherVerifier.Verify(token, time.Now()); authverify.ReasonOf(err) != authverify.ReasonSignature {
		t.Fatalf("another key must refuse the token by signature, got %v", err)
	}
}

func TestMintTokenNeedsItsInputs(t *testing.T) {
	keyFile := writeIssuerKey(t)
	for name, env := range map[string]map[string]string{
		"no session":   {"E2E_SESSION_ID": ""},
		"bad session":  {"E2E_SESSION_ID": "not-a-uuid"},
		"no issuer":    {"E2E_ISSUER_URL": ""},
		"no key file":  {"E2E_ISSUER_KEY_FILE": ""},
		"missing file": {"E2E_ISSUER_KEY_FILE": filepath.Join(t.TempDir(), "absent")},
		"bad user":     {"E2E_USER_ID": "user-7"},
	} {
		base := map[string]string{
			"E2E_USER_ID": tokenUser, "E2E_ORG_ID": tokenOrg, "E2E_SESSION_ID": tokenSession,
			"E2E_ISSUER_URL": tokenIssuer, "E2E_ISSUER_KEY_FILE": keyFile,
		}
		for k, v := range env {
			base[k] = v
		}
		var stdout, stderr bytes.Buffer
		if code := livee2e.Run([]string{"mint-token"}, func(n string) string { return base[n] }, &stdout, &stderr); code != 1 || stdout.Len() != 0 {
			t.Errorf("%s: exit %d, stdout %d bytes, want exit 1 and no token", name, code, stdout.Len())
		}
	}
}

func TestMintTokenRefusesAKeyFileOthersCanRead(t *testing.T) {
	keyFile := writeIssuerKey(t)
	if err := os.Chmod(keyFile, 0o644); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	getenv := tokenEnv(keyFile)
	if code := livee2e.Run([]string{"mint-token"}, getenv, &stdout, &stderr); code != 1 || stdout.Len() != 0 {
		t.Fatalf("exit %d, want exit 1 and no token for a key file with group or other bits", code)
	}
}
