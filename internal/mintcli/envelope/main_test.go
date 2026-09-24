package envelope

import (
	"bytes"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/golang-jwt/jwt/v5"

	"github.com/full-chaos/dev-health-ops/internal/envelopemint"
)

// verifyForTest is a minimal, LOCAL structural check -- this package
// does not import internal/queryapi/principal. The real
// byte-compatibility proof against that verifier lives in
// internal/queryapi/principal's own test suite, which imports this
// package instead and signs with it.
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
	if err := run([]string{}, &bytes.Buffer{}, io.Discard); err == nil {
		t.Fatal("expected -org to be required")
	}
}

func TestRunRequiresAKey(t *testing.T) {
	t.Setenv(envelopemint.PrivateKeyEnvVar, "")
	if err := run([]string{"-org", "70d529e0"}, &bytes.Buffer{}, io.Discard); err == nil {
		t.Fatal("expected a missing key to be refused")
	}
}

func TestRunFromEnvMintsAnEnvelope(t *testing.T) {
	pemText, pub := generateKeyPEM(t)
	t.Setenv(envelopemint.PrivateKeyEnvVar, pemText)

	var stdout bytes.Buffer
	if err := run([]string{"-org", "70d529e0"}, &stdout, io.Discard); err != nil {
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
	if err := run([]string{"-org", "70d529e0", "-key-file", keyPath}, &stdout, io.Discard); err != nil {
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
	err := run([]string{"-org", "70d529e0"}, &stdout, io.Discard)
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

// TestMint_MalformedFlagNeverReachesRealStderr pins the exact contract a
// codex round found broken: under the OLD subprocess model, a helper's
// stderr was simply never wired to the parent process, so a flag-parse
// diagnostic could never leak. Minting in process removes that free
// isolation -- Mint must uphold the SAME silence deliberately, by routing
// the flag set's own error output to io.Discard, not by relying on a
// process boundary that no longer exists. This test drives Mint (the
// exact in-process entry point internal/goapiproof.MintViaAllowlistedHelper
// calls) through a REAL os.Stderr swap, so a regression back to the
// zero-value flag.FlagSet (which defaults its output to os.Stderr) is
// caught by a real capture, not by reading the source.
func TestMint_MalformedFlagNeverReachesRealStderr(t *testing.T) {
	old := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stderr = w
	defer func() { os.Stderr = old }()

	done := make(chan []byte, 1)
	go func() {
		data, _ := io.ReadAll(r)
		done <- data
	}()

	_, mintErr := Mint([]string{"-not-a-real-flag"})

	_ = w.Close()
	captured := <-done
	os.Stderr = old

	if mintErr == nil {
		t.Fatal("Mint([]string{\"-not-a-real-flag\"}) = nil error, want a refusal")
	}
	if len(captured) != 0 {
		t.Fatalf("Mint wrote %d byte(s) to the real stderr, want none: %q", len(captured), captured)
	}
}
