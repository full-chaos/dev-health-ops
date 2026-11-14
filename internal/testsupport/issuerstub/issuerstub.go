// Package issuerstub is the CI-only access-token issuer of the live e2e: an HTTPS listener on localhost that
// serves the discovery document and the JWKS of one throwaway Ed25519 key. go-api and query-api verify the e2e
// token against it through AUTH_ACCESS_TOKEN_ISSUER, with the stub's CA in SSL_CERT_FILE. No product image holds
// it, the keys are generated at every start, and the prod binaries carry no test key source.
package issuerstub

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/signingkey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/idpstub"
)

// DefaultKeyID is the kid of the stub's key when none is given.
const DefaultKeyID = "ci-e2e-1"

const (
	discoveryPath = "/.well-known/openid-configuration"
	jwksPath      = "/.well-known/jwks.json"
	certLifetime  = 24 * time.Hour
	host          = "localhost"
)

// Files are the paths Prepare wrote under its directory.
type Files struct {
	// PrivateKey is the PKCS8 PEM of the signing key (0400); only the e2e minter reads it.
	PrivateKey string
	// JWKS is the public key set the stub serves (0444).
	JWKS string
	// CA is the certificate authority PEM (0444) go-api and query-api trust through SSL_CERT_FILE.
	CA string
	// ServerCert and ServerKey are the listener's certificate and key (0400).
	ServerCert, ServerKey string
	// IssuerURL is written by Serve once the listener is up (0444).
	IssuerURL string
}

// Prepare writes a new signing key, its JWKS, a CA and a localhost server certificate into dir. It refuses to
// overwrite an existing key set and returns no key material.
func Prepare(dir, keyID string) (Files, error) {
	if keyID == "" {
		keyID = DefaultKeyID
	}
	paths, err := signingkey.GenerateKeyFiles(dir, keyID)
	if err != nil {
		return Files{}, err
	}
	ca, err := idpstub.NewCA(certLifetime)
	if err != nil {
		return Files{}, err
	}
	certPEM, keyPEM, err := ca.IssueServer([]string{host}, certLifetime)
	if err != nil {
		return Files{}, err
	}
	files := Files{
		PrivateKey: paths.Private, JWKS: paths.JWKS,
		CA:         filepath.Join(dir, "ca.pem"),
		ServerCert: filepath.Join(dir, "server.pem"),
		ServerKey:  filepath.Join(dir, "server.key"),
		IssuerURL:  filepath.Join(dir, "issuer-url"),
	}
	for path, entry := range map[string]struct {
		data []byte
		mode os.FileMode
	}{
		files.CA:         {ca.CertPEM, 0o444},
		files.ServerCert: {certPEM, 0o444},
		files.ServerKey:  {keyPEM, 0o400},
	} {
		if err := writeNew(path, entry.data, entry.mode); err != nil {
			return Files{}, err
		}
	}
	return files, nil
}

func writeNew(path string, data []byte, mode os.FileMode) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// Handler serves the discovery document of issuer and the JWKS, and nothing else.
func Handler(issuer string, jwks []byte) http.Handler {
	discovery, _ := json.Marshal(map[string]any{
		"issuer":                                issuer,
		"jwks_uri":                              issuer + jwksPath,
		"id_token_signing_alg_values_supported": []string{"EdDSA"},
	})
	mux := http.NewServeMux()
	serve := func(body []byte) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(body)
		}
	}
	mux.HandleFunc("GET "+discoveryPath, serve(discovery))
	mux.HandleFunc("GET "+jwksPath, serve(jwks))
	return mux
}

// Serve listens on 127.0.0.1 (port 0: any free port) with the files Prepare wrote, records the issuer URL in
// files.IssuerURL, and serves until the listener fails. The issuer is https://localhost:<port>.
func Serve(files Files) error {
	jwks, err := os.ReadFile(files.JWKS)
	if err != nil {
		return err
	}
	cert, err := tls.LoadX509KeyPair(files.ServerCert, files.ServerKey)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		return errors.New("issuerstub: listener has no TCP address")
	}
	issuer := fmt.Sprintf("https://%s:%d", host, addr.Port)
	if err := writeNew(files.IssuerURL, []byte(issuer+"\n"), 0o444); err != nil {
		return err
	}
	server := &http.Server{
		Handler:           Handler(issuer, jwks),
		TLSConfig:         &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12},
		ReadHeaderTimeout: 5 * time.Second,
	}
	return server.ServeTLS(listener, "", "")
}
