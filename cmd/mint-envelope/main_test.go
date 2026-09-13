package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"os"
	"strings"
	"testing"

	"github.com/golang-jwt/jwt/v5"

	"github.com/full-chaos/dev-health-ops/internal/envelopemint"
)

// verifyForTest is a minimal, LOCAL structural check -- this package
// cannot import cmd/query-api/internal/principal (it lives under a
// directory scoped to cmd/query-api by Go's own internal-package
// visibility rule). The real byte-compatibility proof against that
// verifier lives in cmd/query-api/internal/principal's own test suite,
// which imports this package instead (the import direction Go allows)
// and signs with it.
func verifyForTest(token string, pub ed25519.PublicKey) (*envelopemint.Claims, error) {
	claims := &envelopemint.Claims{}
	_, err := jwt.ParseWithClaims(token, claims, func(*jwt.Token) (any, error) {
		return pub, nil
	}, jwt.WithValidMethods([]string{"EdDSA"}), jwt.WithExpirationRequired())
	return claims, err
}

func generateKeyPEM(t *testing.T) (string, ed25519.PublicKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal pkcs8: %v", err)
	}
	block := &pem.Block{Type: "PRIVATE KEY", Bytes: der}
	return string(pem.EncodeToMemory(block)), pub
}

func TestRunRequiresOrg(t *testing.T) {
	t.Setenv(envelopemint.PrivateKeyEnvVar, "")
	if err := run([]string{}, &bytes.Buffer{}); err == nil {
		t.Fatal("expected -org to be required")
	}
}

func TestRunRequiresAKey(t *testing.T) {
	t.Setenv(envelopemint.PrivateKeyEnvVar, "")
	if err := run([]string{"-org", "70d529e0"}, &bytes.Buffer{}); err == nil {
		t.Fatal("expected a missing key to be refused")
	}
}

func TestRunFromEnvMintsAnEnvelope(t *testing.T) {
	pemText, pub := generateKeyPEM(t)
	t.Setenv(envelopemint.PrivateKeyEnvVar, pemText)

	var stdout bytes.Buffer
	if err := run([]string{"-org", "70d529e0"}, &stdout); err != nil {
		t.Fatalf("run: %v", err)
	}
	envelope := strings.TrimSpace(stdout.String())
	if strings.Count(envelope, ".") != 2 {
		t.Fatalf("stdout = %q, want a three-segment JWT", envelope)
	}

	claims, err := verifyForTest(envelope, pub)
	if err != nil {
		t.Fatalf("the minted envelope did not verify against its own public key: %v", err)
	}
	if claims.OrgID != "70d529e0" {
		t.Errorf("OrgID = %q, want 70d529e0", claims.OrgID)
	}
	if !claims.IsSuperuser || !claims.IsSuperuserVerified {
		t.Errorf("IsSuperuser/IsSuperuserVerified = %v/%v, want true/true", claims.IsSuperuser, claims.IsSuperuserVerified)
	}
}

func TestRunFromKeyFile(t *testing.T) {
	pemText, pub := generateKeyPEM(t)
	t.Setenv(envelopemint.PrivateKeyEnvVar, "") // must NOT fall back to env
	keyPath := t.TempDir() + "/key.pem"
	if err := os.WriteFile(keyPath, []byte(pemText), 0o600); err != nil {
		t.Fatalf("write key file: %v", err)
	}

	var stdout bytes.Buffer
	if err := run([]string{"-org", "70d529e0", "-key-file", keyPath}, &stdout); err != nil {
		t.Fatalf("run: %v", err)
	}
	envelope := strings.TrimSpace(stdout.String())
	if _, err := verifyForTest(envelope, pub); err != nil {
		t.Fatalf("the minted envelope did not verify: %v", err)
	}
}

func TestRunNeverPrintsTheKeyOnError(t *testing.T) {
	t.Setenv(envelopemint.PrivateKeyEnvVar, "not-a-valid-pem-at-all")
	var stdout bytes.Buffer
	err := run([]string{"-org", "70d529e0"}, &stdout)
	if err == nil {
		t.Fatal("expected an error for a malformed key")
	}
	if strings.Contains(err.Error(), "not-a-valid-pem-at-all") {
		t.Fatalf("error must never echo the key material, got: %v", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout must be empty on failure, got %q", stdout.String())
	}
}
