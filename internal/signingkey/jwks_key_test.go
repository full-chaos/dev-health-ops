package signingkey

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"os"
	"testing"
)

// The JWKS public key written with a key pair verifies a signature made with the private
// half, and does not verify a signature made with another key under the same key id.
func TestGeneratedJWKSKeyVerifiesOnlyItsOwnPrivateKey(t *testing.T) {
	paths, err := GenerateKeyFiles(t.TempDir(), DefaultKeyID)
	if err != nil {
		t.Fatalf("GenerateKeyFiles: %v", err)
	}
	pemBytes, err := os.ReadFile(paths.Private)
	if err != nil {
		t.Fatal(err)
	}
	priv, err := LoadPrivateKey(pemBytes)
	if err != nil {
		t.Fatalf("LoadPrivateKey: %v", err)
	}
	raw, err := os.ReadFile(paths.JWKS)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Keys []struct {
			Kid string `json:"kid"`
			X   string `json:"x"`
		} `json:"keys"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil || len(doc.Keys) != 1 || doc.Keys[0].Kid != DefaultKeyID {
		t.Fatalf("JWKS = %s, err %v", raw, err)
	}
	pubBytes, err := base64.RawURLEncoding.DecodeString(doc.Keys[0].X)
	if err != nil || len(pubBytes) != ed25519.PublicKeySize {
		t.Fatalf("JWKS x: %v", err)
	}
	pub := ed25519.PublicKey(pubBytes)
	msg := []byte("signing input")
	if !ed25519.Verify(pub, msg, ed25519.Sign(priv, msg)) {
		t.Fatal("the JWKS key does not verify a signature of its own private key")
	}
	_, other, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	if ed25519.Verify(pub, msg, ed25519.Sign(other, msg)) {
		t.Fatal("the JWKS key verified a signature made with another key")
	}
}
