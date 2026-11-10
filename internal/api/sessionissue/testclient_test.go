package sessionissue

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/workloadtoken"
)

const testWorkloadIssuer = "https://auth.example.test"

// testToken is a workload-token.v1 access token for auth-service, valid now for 300 s, built at run time (the
// workload client checks its claims, not its signature).
func testToken(t *testing.T) string {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	jti := make([]byte, 16)
	if _, err := rand.Read(jti); err != nil {
		t.Fatal(err)
	}
	enc := base64.RawURLEncoding
	now := time.Now().Unix()
	payload := fmt.Sprintf(`{"iss":%q,"aud":%q,"sub":"prn_AAAAAAAAAAAAAAAAAAAAAA","principal_type":"workload","jti":%q,"iat":%d,"nbf":%d,"exp":%d}`,
		testWorkloadIssuer, WorkloadAudience, enc.EncodeToString(jti), now, now, now+300)
	input := enc.EncodeToString([]byte(`{"alg":"EdDSA","typ":"workload+jwt","kid":"test-key"}`)) + "." + enc.EncodeToString([]byte(payload))
	return input + "." + enc.EncodeToString(ed25519.Sign(key, []byte(input)))
}

// newTestClient is NewWorkload for base over a workload client whose exchange (a server of its own) always answers
// token: every call of the client presents exactly that Bearer.
func newTestClient(t *testing.T, base *url.URL, token string, timeout time.Duration) (*Client, error) {
	t.Helper()
	exchange := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		body, _ := json.Marshal(map[string]any{"access_token": token, "issued_token_type": workloadtoken.TokenTypeJWT, "token_type": "Bearer", "expires_in": 300})
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
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
	workload, err := workloadtoken.New(workloadtoken.Config{ExchangeURL: exchange.URL, AllowPlainHTTP: true, Issuer: testWorkloadIssuer,
		Subject: workloadtoken.BootstrapCredentialFile(path), Transport: &http.Transport{}})
	if err != nil {
		t.Fatal(err)
	}
	return NewWorkload(base, workload, timeout)
}
