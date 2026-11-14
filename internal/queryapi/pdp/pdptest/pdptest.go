// Package pdptest is a fake auth-service for query-api PDP tests: the workload exchange and POST /v1/authorize.
package pdptest

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/workloadtoken"
)

// Issuer is the workload issuer the fake mints for.
const Issuer = "https://auth.example.test"

// Request is one /v1/authorize call as the fake saw it.
type Request struct {
	Authorization      string
	PrincipalReference string
	Action             string
	ResourceType       string
	ResourceKey        string
	OrganizationID     string
	ClientService      string
}

// Server answers the exchange with WorkloadToken and every authorize call with the current answer.
type Server struct {
	URL           string
	WorkloadToken string

	mu        sync.Mutex
	allowed   bool
	reason    string
	status    int
	oblig     map[string]any
	obligSet  bool
	requests  []Request
	exchanges int
}

// New starts a fake that allows with allowed_by_role.
func New(t *testing.T) *Server {
	t.Helper()
	s := &Server{WorkloadToken: workloadToken(t), allowed: true, reason: "allowed_by_role", status: http.StatusOK}
	srv := httptest.NewServer(http.HandlerFunc(s.serve))
	t.Cleanup(srv.Close)
	s.URL = srv.URL
	return s
}

// Allow makes every next decision an allow with reason.
func (s *Server) Allow(reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.allowed, s.reason = true, reason
}

// Deny makes every next decision a deny with reason.
func (s *Server) Deny(reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.allowed, s.reason = false, reason
}

// Obligations makes every next allow carry exactly obligations (a nil map carries none). Until it is called an allow
// carries what auth-service gives an organization-wide allow: the asked organization as tenant_scope and
// repository_scope organization.
func (s *Server) Obligations(obligations map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.oblig, s.obligSet = obligations, true
}

// Fail makes every next authorize call answer status with no decision.
func (s *Server) Fail(status int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = status
}

// Requests are the authorize calls so far.
func (s *Server) Requests() []Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Request(nil), s.requests...)
}

// Exchanges is the number of workload exchanges so far.
func (s *Server) Exchanges() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.exchanges
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	s.mu.Lock()
	defer s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if r.URL.Path == workloadtoken.ExchangePath {
		s.exchanges++
		body, _ := json.Marshal(map[string]any{"access_token": s.WorkloadToken, "issued_token_type": workloadtoken.TokenTypeJWT, "token_type": "Bearer", "expires_in": 300})
		_, _ = w.Write(body)
		return
	}
	var body struct {
		PrincipalReference string `json:"principal_reference"`
		Action             string `json:"action"`
		Resource           struct {
			Type           string `json:"type"`
			OrganizationID string `json:"organization_id"`
			Key            string `json:"key"`
		} `json:"resource"`
		Context struct {
			ClientService string `json:"client_service"`
		} `json:"context"`
	}
	_ = json.Unmarshal(raw, &body)
	s.requests = append(s.requests, Request{Authorization: r.Header.Get("Authorization"), PrincipalReference: body.PrincipalReference,
		Action: body.Action, ResourceType: body.Resource.Type, ResourceKey: body.Resource.Key, OrganizationID: body.Resource.OrganizationID,
		ClientService: body.Context.ClientService})
	if s.status != http.StatusOK {
		w.WriteHeader(s.status)
		return
	}
	var org any
	if body.Resource.OrganizationID != "" {
		org = body.Resource.OrganizationID
	}
	decision := map[string]any{
		"schema_version": "authorization_decision.v1", "decision_id": "dec_" + strings.Repeat("A", 26), "allowed": s.allowed,
		"reason_code": s.reason, "principal_id": "prn_AAAAAAAAAAAAAAAAAAAAAA", "organization_id": org, "policy_revision": 1,
		"expires_at": time.Now().Add(time.Hour).UTC().Format("2006-01-02T15:04:05Z"),
	}
	if s.allowed {
		switch {
		case !s.obligSet:
			decision["obligations"] = map[string]any{"tenant_scope": body.Resource.OrganizationID, "repository_scope": "organization"}
		case s.oblig != nil:
			decision["obligations"] = s.oblig
		}
	}
	_ = json.NewEncoder(w).Encode(decision)
}

// BootstrapFile is a workload bootstrap credential file in a temporary directory.
func BootstrapFile(t *testing.T) string {
	t.Helper()
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "bootstrap")
	if err := os.WriteFile(path, []byte(workloadtoken.BootstrapPrefix+base64.RawURLEncoding.EncodeToString(random)), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func workloadToken(t *testing.T) string {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	enc := base64.RawURLEncoding
	now := time.Now().Unix()
	payload := fmt.Sprintf(`{"iss":%q,"aud":"auth-service","sub":"prn_QQQQQQQQQQQQQQQQQQQQQQ","principal_type":"workload","jti":"%024d","iat":%d,"nbf":%d,"exp":%d}`,
		Issuer, 7, now, now, now+300)
	input := enc.EncodeToString([]byte(`{"alg":"EdDSA","typ":"workload+jwt","kid":"test-key"}`)) + "." + enc.EncodeToString([]byte(payload))
	return input + "." + enc.EncodeToString(ed25519.Sign(key, []byte(input)))
}
