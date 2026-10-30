package authprovision_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/workloadtoken"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/orgreactivate"
)

const workloadIssuer = "https://auth.example.test"

// mintWorkloadToken is a workload-token.v1 access token for auth-service,
// valid now for 300 s (the client checks its claims, not its signature).
func mintWorkloadToken(t *testing.T) string {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	enc := base64.RawURLEncoding
	now := time.Now().Unix()
	payload := fmt.Sprintf(`{"iss":%q,"aud":"auth-service","sub":"prn_AAAAAAAAAAAAAAAAAAAAAA","principal_type":"workload","jti":"%024d","iat":%d,"nbf":%d,"exp":%d}`,
		workloadIssuer, 7, now, now, now+300)
	input := enc.EncodeToString([]byte(`{"alg":"EdDSA","typ":"workload+jwt","kid":"test-key"}`)) + "." + enc.EncodeToString([]byte(payload))
	return input + "." + enc.EncodeToString(ed25519.Sign(key, []byte(input)))
}

func issuedAnswer(token string) func() (int, string) {
	return func() (int, string) {
		body, _ := json.Marshal(map[string]any{"access_token": token, "issued_token_type": workloadtoken.TokenTypeJWT, "token_type": "Bearer", "expires_in": 300})
		return http.StatusOK, string(body)
	}
}

// newWorkloadClient is a workloadtoken.Client over a bootstrap credential file
// that exchanges at g.
func newWorkloadClient(t *testing.T, g *goldenServer) *workloadtoken.Client {
	t.Helper()
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "bootstrap")
	if err := os.WriteFile(path, []byte(workloadtoken.BootstrapPrefix+base64.RawURLEncoding.EncodeToString(random)), 0o600); err != nil {
		t.Fatal(err)
	}
	client, err := workloadtoken.New(workloadtoken.Config{ExchangeURL: g.URL.String(), AllowPlainHTTP: true, Issuer: workloadIssuer,
		Subject: workloadtoken.BootstrapCredentialFile(path), Transport: &http.Transport{}})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func newWorkloadClients(t *testing.T, g *goldenServer) (authprovision.Provisioner, lifecycle) {
	t.Helper()
	shared := newWorkloadClient(t, g)
	prov, err := authprovision.NewWorkload(g.URL, shared, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	life, err := orgreactivate.NewWorkload(g.URL, shared, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return prov, life
}

// With the workload credential every verb carries exactly one Bearer, the
// workload token, and no byte of the session credential; one exchange serves
// the calls of the token's lifetime, across both clients.
func TestWorkloadCredentialCarriesOnlyTheWorkloadToken(t *testing.T) {
	g := newGoldenServer(t)
	token := mintWorkloadToken(t)
	g.exchange = issuedAnswer(token)
	prov, life := newWorkloadClients(t, g)
	for round := 0; round < 2; round++ {
		goldenVerbs(t, g, prov, life)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.provisioning) != 18 {
		t.Fatalf("%d provisioning requests, want 18", len(g.provisioning))
	}
	for _, request := range g.provisioning {
		if len(request.Auth) != 1 || request.Auth[0] != "Bearer "+token {
			t.Errorf("%s: Authorization %q, want exactly the workload token", request.Path, request.Auth)
		}
		if strings.Contains(request.Dump, "svc_authsess_") {
			t.Errorf("%s: the request holds svc_authsess_ bytes", request.Path)
		}
	}
	if g.exchanges != 1 {
		t.Errorf("%d exchanges for 18 calls, want 1", g.exchanges)
	}
}

// A failed exchange fails the call as unavailable and sends nothing to the
// provisioning routes: there is no session credential to fall back to.
func TestWorkloadExchangeFailureSendsNoRequest(t *testing.T) {
	g := newGoldenServer(t)
	g.exchange = func() (int, string) { return http.StatusServiceUnavailable, `{"error":"temporarily_unavailable"}` }
	prov, life := newWorkloadClients(t, g)
	for _, call := range goldenCalls(prov, life) {
		err := call.do()
		if !errors.Is(err, authprovision.ErrUnavailable) && !errors.Is(err, orgreactivate.ErrUnavailable) {
			t.Errorf("%s: error %v, want unavailable", call.verb, err)
		}
		if err != nil && strings.Contains(err.Error(), "Bearer") {
			t.Errorf("%s: the error carries a credential: %v", call.verb, err)
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.provisioning) != 0 {
		t.Fatalf("%d provisioning requests after a failed exchange, want 0", len(g.provisioning))
	}
}

func TestNewWorkloadRefusesNoClientAndNoBase(t *testing.T) {
	g := newGoldenServer(t)
	if _, err := authprovision.NewWorkload(g.URL, nil, time.Second); err == nil {
		t.Error("authprovision.NewWorkload accepted no workload client")
	}
	if _, err := authprovision.NewWorkload(nil, newWorkloadClient(t, g), time.Second); err == nil {
		t.Error("authprovision.NewWorkload accepted no URL")
	}
	if _, err := orgreactivate.NewWorkload(g.URL, nil, time.Second); err == nil {
		t.Error("orgreactivate.NewWorkload accepted no workload client")
	}
	if _, err := orgreactivate.NewWorkload(nil, newWorkloadClient(t, g), time.Second); err == nil {
		t.Error("orgreactivate.NewWorkload accepted no URL")
	}
}
