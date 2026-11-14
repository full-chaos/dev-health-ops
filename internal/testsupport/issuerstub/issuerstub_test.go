package issuerstub_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/cigates/livee2e"
	"github.com/full-chaos/dev-health-ops/internal/signingkey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/issuerstub"
)

func TestKeyIDMatchesTheMinter(t *testing.T) {
	if issuerstub.DefaultKeyID != livee2e.DefaultIssuerKeyID {
		t.Fatalf("stub kid %q, minter kid %q", issuerstub.DefaultKeyID, livee2e.DefaultIssuerKeyID)
	}
}

func TestPrepareWritesCustodyModesAndRefusesToOverwrite(t *testing.T) {
	dir := t.TempDir()
	files, err := issuerstub.Prepare(dir, "")
	if err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]os.FileMode{
		files.PrivateKey: 0o400, files.ServerKey: 0o400, files.JWKS: 0o444, files.CA: 0o444, files.ServerCert: 0o444,
	} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != want {
			t.Errorf("%s mode %o, want %o", path, info.Mode().Perm(), want)
		}
	}
	if _, err := signingkey.LoadPrivateKeyFromFile(files.PrivateKey); err != nil {
		t.Fatalf("minter cannot load the signing key: %v", err)
	}
	if _, err := issuerstub.Prepare(dir, ""); err == nil {
		t.Fatal("a second Prepare in the same directory must refuse")
	}
}

func TestServedIssuerIsTrustedByTheJWKSClientThroughItsCAOnly(t *testing.T) {
	files, err := issuerstub.Prepare(t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = issuerstub.Serve(files) }()
	var issuer string
	for i := 0; i < 200 && issuer == ""; i++ {
		if raw, err := os.ReadFile(files.IssuerURL); err == nil {
			issuer = strings.TrimSpace(string(raw))
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !strings.HasPrefix(issuer, "https://localhost:") {
		t.Fatalf("issuer url %q", issuer)
	}
	caPEM, err := os.ReadFile(files.CA)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		t.Fatal("ca.pem holds no certificate")
	}
	trusting := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}}
	keys, err := authverify.NewJWKSClient(authverify.JWKSClientConfig{Issuer: issuer, HTTPClient: trusting})
	if err != nil {
		t.Fatal(err)
	}
	if key, ok := keys.PublicKey(issuerstub.DefaultKeyID); !ok || len(key) == 0 {
		t.Fatal("the JWKS client did not resolve the stub's key through discovery and the JWKS")
	}
	untrusting, err := authverify.NewJWKSClient(authverify.JWKSClientConfig{Issuer: issuer, HTTPClient: &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := untrusting.Refresh(context.Background()); !errors.Is(err, authverify.ErrJWKSUnavailable) {
		t.Fatalf("a client without the CA must not reach the stub, got %v", err)
	}
	resp, err := trusting.Get(issuer + "/anything-else")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("an unknown path answered %d, want 404", resp.StatusCode)
	}
}
