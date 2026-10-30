package sessionissue_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
)

const goldenPath = "testdata/outgoing_issue.golden.json"

const goldenToken = "svc_authsess_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

var (
	goldenPrincipal   = uuid.MustParse("22222222-2222-4222-8222-222222222222")
	goldenOrg         = uuid.MustParse("11111111-1111-4111-8111-111111111111")
	goldenPredecessor = uuid.MustParse("55555555-5555-4555-8555-555555555555")
)

// goldenRequest is one outgoing issue request as the server saw it, with the bearer replaced by a placeholder that
// keeps its credential class prefix.
type goldenRequest struct {
	Case    string              `json:"case"`
	Method  string              `json:"method"`
	Path    string              `json:"path"`
	Headers map[string][]string `json:"headers"`
	Body    string              `json:"body"`
}

func bearerPlaceholder(value string) string {
	if strings.HasPrefix(value, "Bearer svc_authsess_") {
		return "Bearer svc_authsess_<PLACEHOLDER>"
	}
	return "Bearer <NOT-svc_authsess_>"
}

// issueServer is an auth-service issue route that records every request it receives, and serves the workload token
// exchange when exchange is set.
type issueServer struct {
	mu        sync.Mutex
	URL       *url.URL
	last      goldenRequest
	requests  []rawRequest
	exchanges int
	exchange  func() (int, string)
}

type rawRequest struct {
	Auth []string
	Dump string
}

func newIssueServer(t *testing.T) *issueServer {
	t.Helper()
	s := &issueServer{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		defer s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != sessionissue.Path {
			s.exchanges++
			if s.exchange == nil {
				http.NotFound(w, r)
				return
			}
			status, body := s.exchange()
			w.WriteHeader(status)
			_, _ = io.WriteString(w, body)
			return
		}
		headers := map[string][]string{}
		var dump strings.Builder
		for name, values := range r.Header {
			headers[name] = append([]string(nil), values...)
			sort.Strings(headers[name])
			dump.WriteString(name + ": " + strings.Join(values, ",") + "\n")
			if name == "Authorization" {
				for i, value := range values {
					headers[name][i] = bearerPlaceholder(value)
				}
			}
		}
		dump.WriteString(string(raw))
		s.last = goldenRequest{Method: r.Method, Path: r.URL.Path, Headers: headers, Body: string(raw)}
		s.requests = append(s.requests, rawRequest{Auth: append([]string(nil), r.Header.Values("Authorization")...), Dump: dump.String()})
		_, _ = io.WriteString(w, `{"access_token":"a.b.c","refresh_token":"dhr1_x","token_type":"bearer","expires_in":3600}`)
	}))
	t.Cleanup(server.Close)
	base, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	s.URL = base
	return s
}

func endUserRequest() *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", nil)
	r.RemoteAddr = "198.51.100.23:4711"
	r.Header.Set("User-Agent", "Mozilla/5.0 (golden)")
	return r
}

type issueCase struct {
	name string
	req  sessionissue.Request
}

// issueCases are the fixed issue requests: no organization, an organization, an organization with a predecessor.
func issueCases() []issueCase {
	org, predecessor := goldenOrg, goldenPredecessor
	return []issueCase{
		{"no_org", sessionissue.Request{PrincipalID: goldenPrincipal, AMR: []string{"pwd"}}},
		{"org", sessionissue.Request{PrincipalID: goldenPrincipal, OrganizationID: &org, AMR: []string{"pwd", "otp"}}},
		{"org_predecessor", sessionissue.Request{PrincipalID: goldenPrincipal, OrganizationID: &org, AMR: []string{"pwd"}, PredecessorSessionID: &predecessor}},
	}
}

func runIssueCases(t *testing.T, s *issueServer, issuer sessionissue.Issuer) []goldenRequest {
	t.Helper()
	var out []goldenRequest
	for _, c := range issueCases() {
		if _, err := issuer.Issue(context.Background(), endUserRequest(), c.req); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		s.mu.Lock()
		got := s.last
		s.mu.Unlock()
		got.Case = c.name
		out = append(out, got)
	}
	return out
}

func renderGolden(requests []goldenRequest) []byte {
	raw, err := json.MarshalIndent(requests, "", "  ")
	if err != nil {
		panic(err)
	}
	return append(raw, '\n')
}

// The outgoing issue request of every case, with the session credential, is pinned by testdata/outgoing_issue.golden.json.
func TestOutgoingIssueRequestsMatchTheGolden(t *testing.T) {
	s := newIssueServer(t)
	client, err := sessionissue.New(s.URL, goldenToken, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	got := renderGolden(runIssueCases(t, s, client))
	if os.Getenv("SESSIONISSUE_WRITE_GOLDEN") == "1" {
		if err := os.WriteFile(goldenPath, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("outgoing issue requests differ from %s\n--- got\n%s\n--- want\n%s", goldenPath, got, want)
	}
}
