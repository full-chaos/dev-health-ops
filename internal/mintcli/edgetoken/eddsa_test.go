package edgetoken

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeEdDSAKey(t *testing.T) string {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "edge.key")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o400); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRunMintsAnEdDSATokenWithAKeyFileAndNoHS256Secret(t *testing.T) {
	t.Setenv("JWT_SECRET_KEY", "")
	t.Setenv("EDGE_TOKEN_SIGNING_KEY_FILE", writeEdDSAKey(t))
	t.Setenv("EDGE_TOKEN_KID", "ci-e2e-1")
	t.Setenv("EDGE_TOKEN_SESSION_ID", "cccccccc-1111-4222-8333-444444444444")
	t.Setenv("AUTH_ACCESS_TOKEN_ISSUER", "https://localhost:8443")
	var opened, closed bool
	var out bytes.Buffer
	if err := run(context.Background(), []string{"-org", testOrg}, &out, io.Discard, opener(&fakeDB{active: true}, &opened, &closed)); err != nil {
		t.Fatalf("run: %v", err)
	}
	parts := strings.Split(strings.TrimSpace(out.String()), ".")
	if len(parts) != 3 {
		t.Fatalf("printed %d segments, want 3", len(parts))
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		t.Fatal(err)
	}
	var header map[string]any
	if err := json.Unmarshal(raw, &header); err != nil {
		t.Fatal(err)
	}
	if header["alg"] != "EdDSA" || header["typ"] != "at+jwt" || header["kid"] != "ci-e2e-1" {
		t.Fatalf("header = %v, want alg EdDSA, typ at+jwt, kid ci-e2e-1", header)
	}
}

func TestRunEdDSARefusesWithoutASessionAfterNamingThePrincipal(t *testing.T) {
	t.Setenv("JWT_SECRET_KEY", "")
	t.Setenv("EDGE_TOKEN_SIGNING_KEY_FILE", writeEdDSAKey(t))
	t.Setenv("EDGE_TOKEN_KID", "ci-e2e-1")
	t.Setenv("EDGE_TOKEN_SESSION_ID", "")
	t.Setenv("AUTH_ACCESS_TOKEN_ISSUER", "https://localhost:8443")
	var opened, closed bool
	var out bytes.Buffer
	err := run(context.Background(), []string{"-org", testOrg}, &out, io.Discard, opener(&fakeDB{active: true}, &opened, &closed))
	if err == nil || !strings.Contains(err.Error(), "EDGE_TOKEN_SESSION_ID") || out.Len() != 0 {
		t.Fatalf("err = %v, stdout %d bytes; want a refusal naming EDGE_TOKEN_SESSION_ID and no token", err, out.Len())
	}
	err = run(context.Background(), []string{"-org", testOrg}, &out, io.Discard, opener(&fakeDB{active: false}, &opened, &closed))
	if err == nil || !strings.Contains(err.Error(), "not active") {
		t.Fatalf("an inactive principal must be named before the session: %v", err)
	}
}

func TestRunEdDSAKeyFileModeNeverReadsTheHS256Secret(t *testing.T) {
	t.Setenv("JWT_SECRET_KEY", testKey)
	t.Setenv("EDGE_TOKEN_SIGNING_KEY_FILE", filepath.Join(t.TempDir(), "absent"))
	var opened, closed bool
	err := run(context.Background(), []string{"-org", testOrg}, &bytes.Buffer{}, io.Discard, opener(&fakeDB{active: true}, &opened, &closed))
	if err == nil || opened {
		t.Fatalf("a missing key file must refuse before the database opens, even with the HS256 secret set: err=%v opened=%v", err, opened)
	}
}
