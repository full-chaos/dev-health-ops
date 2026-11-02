// Package workloadclient builds a workloadtoken.Client for tests of go-api's auth-service clients: it exchanges
// against its own local server, which answers a claims-valid workload token (the client checks claims, not the
// signature).
package workloadclient

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/workloadtoken"
)

// Issuer is the issuer the minted tokens carry.
const Issuer = "https://auth.example.test"

// Token is a workload-token.v1 access token for auth-service, valid now for 300 s.
func Token(t testing.TB) string {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	enc := base64.RawURLEncoding
	now := time.Now().Unix()
	payload := fmt.Sprintf(`{"iss":%q,"aud":"auth-service","sub":"prn_AAAAAAAAAAAAAAAAAAAAAA","principal_type":"workload","jti":"%024d","iat":%d,"nbf":%d,"exp":%d}`,
		Issuer, 7, now, now, now+300)
	input := enc.EncodeToString([]byte(`{"alg":"EdDSA","typ":"workload+jwt","kid":"test-key"}`)) + "." + enc.EncodeToString([]byte(payload))
	return input + "." + enc.EncodeToString(ed25519.Sign(key, []byte(input)))
}

// New returns a client over a bootstrap credential file that exchanges at a local server, and the token that server
// issues (the Bearer every call of the client carries).
func New(t testing.TB) (*workloadtoken.Client, string) {
	t.Helper()
	token := Token(t)
	exchange := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": token, "issued_token_type": workloadtoken.TokenTypeJWT, "token_type": "Bearer", "expires_in": 300})
	}))
	t.Cleanup(exchange.Close)
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "bootstrap")
	if err := os.WriteFile(path, []byte(workloadtoken.BootstrapPrefix+base64.RawURLEncoding.EncodeToString(random)), 0o600); err != nil {
		t.Fatal(err)
	}
	client, err := workloadtoken.New(workloadtoken.Config{ExchangeURL: exchange.URL, AllowPlainHTTP: true, Issuer: Issuer,
		Subject: workloadtoken.BootstrapCredentialFile(path), Transport: &http.Transport{}})
	if err != nil {
		t.Fatal(err)
	}
	return client, token
}
