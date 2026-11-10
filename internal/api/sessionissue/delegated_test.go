package sessionissue

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
)

type delegatedServer struct {
	mu     sync.Mutex
	token  string
	paths  []string
	bodies []string
	auth   []string
	agents []string
	fwd    []string
	status int
	answer string
}

func newDelegatedClient(t *testing.T, s *delegatedServer) *Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		defer s.mu.Unlock()
		s.paths = append(s.paths, r.Method+" "+r.URL.Path)
		s.bodies = append(s.bodies, string(raw))
		s.auth = append(s.auth, r.Header.Get("Authorization"))
		s.agents = append(s.agents, r.Header.Get("User-Agent"))
		s.fwd = append(s.fwd, r.Header.Get("X-Forwarded-For"))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(s.status)
		_, _ = io.WriteString(w, s.answer)
	}))
	t.Cleanup(server.Close)
	base, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	s.token = testToken(t)
	client, err := newTestClient(t, base, s.token, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func delegatedAnswerText(session uuid.UUID, expires string, extra string) string {
	return `{"session":{"session_id":"` + sessionid.Wire(session) + `","expires_at":"` + expires + `"},"access_token":"a.b.c",` + extra +
		`"token_type":"bearer","expires_in":900}`
}

func delegatedTestRequest() *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/api/v1/admin/impersonate", nil)
	r.RemoteAddr = "198.51.100.23:4711"
	r.Header.Set("User-Agent", "admin-ui")
	return r
}

func TestStartDelegatedSendsTheBodyAndCredentialAndReadsTheSession(t *testing.T) {
	session := uuid.New()
	s := &delegatedServer{status: http.StatusCreated, answer: delegatedAnswerText(session, "2026-11-04T10:15:00.123456Z", `"session_access_token":"s.t.u",`)}
	client := newDelegatedClient(t, s)
	base, target, org := uuid.New(), uuid.New(), uuid.New()
	got, err := client.StartDelegated(context.Background(), delegatedTestRequest(), DelegatedRequest{
		BaseSessionID: base, TargetPrincipalID: target, OrganizationID: org, Reason: ReasonAdminImpersonation,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.SessionID != session || got.Access != "a.b.c" || got.SessionAccess != "s.t.u" || got.ExpiresIn != 900 ||
		!got.ExpiresAt.Equal(time.Date(2026, 11, 4, 10, 15, 0, 123456000, time.UTC)) {
		t.Fatalf("session = %+v", got)
	}
	if len(s.paths) != 1 || s.paths[0] != "POST /internal/v1/sessions/delegated" {
		t.Fatalf("requests = %v", s.paths)
	}
	var body map[string]string
	if err := json.Unmarshal([]byte(s.bodies[0]), &body); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"base_session_id": base.String(), "target_principal_id": target.String(), "organization_id": org.String(), "reason": "admin_impersonation"}
	if len(body) != len(want) {
		t.Fatalf("body = %v", body)
	}
	for k, v := range want {
		if body[k] != v {
			t.Errorf("body[%s] = %q, want %q", k, body[k], v)
		}
	}
	if s.auth[0] != "Bearer "+s.token || s.agents[0] != "admin-ui" || s.fwd[0] != "198.51.100.23" {
		t.Errorf("headers = %q %q %q", s.auth[0], s.agents[0], s.fwd[0])
	}
}

func TestStartDelegatedFailsClosedOnEveryOtherAnswer(t *testing.T) {
	good := delegatedAnswerText(uuid.New(), "2026-11-04T10:15:00Z", "")
	for name, tc := range map[string]struct {
		status int
		answer string
	}{
		"server_error":     {http.StatusInternalServerError, `{}`},
		"not_entitled":     {http.StatusForbidden, `{"schema_version":"error.v1","status":403,"reason_code":"not_entitled"}`},
		"session_revoked":  {http.StatusUnauthorized, `{"schema_version":"error.v1","status":401,"reason_code":"session_revoked"}`},
		"ok_not_created":   {http.StatusOK, good},
		"not_json":         {http.StatusCreated, `nope`},
		"no_session":       {http.StatusCreated, `{"access_token":"a.b.c","token_type":"bearer","expires_in":900}`},
		"no_token":         {http.StatusCreated, strings.Replace(good, `"access_token":"a.b.c",`, "", 1)},
		"not_bearer":       {http.StatusCreated, strings.Replace(good, `"bearer"`, `"mac"`, 1)},
		"expires_in_high":  {http.StatusCreated, strings.Replace(good, `"expires_in":900`, `"expires_in":3601`, 1)},
		"expires_in_zero":  {http.StatusCreated, strings.Replace(good, `"expires_in":900`, `"expires_in":0`, 1)},
		"no_expires_in":    {http.StatusCreated, strings.Replace(good, `,"expires_in":900`, "", 1)},
		"bad_session_id":   {http.StatusCreated, strings.Replace(good, `"ses_`, `"xxx_`, 1)},
		"bad_expires_at":   {http.StatusCreated, strings.Replace(good, `2026-11-04T10:15:00Z`, `soon`, 1)},
		"answer_too_large": {http.StatusCreated, good + strings.Repeat(" ", maxAnswerBytes)},
	} {
		t.Run(name, func(t *testing.T) {
			s := &delegatedServer{status: tc.status, answer: tc.answer}
			_, err := newDelegatedClient(t, s).StartDelegated(context.Background(), delegatedTestRequest(), DelegatedRequest{})
			if !errors.Is(err, ErrUnavailable) {
				t.Fatalf("err = %v, want ErrUnavailable", err)
			}
		})
	}
}

func TestStartDelegatedUnreachableIsUnavailable(t *testing.T) {
	s := &delegatedServer{status: http.StatusCreated}
	client := newDelegatedClient(t, s)
	client.endpoint = "http://127.0.0.1:1" + Path
	if _, err := client.StartDelegated(context.Background(), delegatedTestRequest(), DelegatedRequest{}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v", err)
	}
}

func TestEndDelegatedSendsTheSessionAndTreatsAGoneSessionAsEnded(t *testing.T) {
	id := uuid.New()
	for name, tc := range map[string]struct {
		status  int
		answer  string
		wantErr bool
	}{
		"ended":     {http.StatusNoContent, "", false},
		"not_found": {http.StatusNotFound, `{"schema_version":"error.v1","status":404,"reason_code":"session_not_found"}`, false},
		"forbidden": {http.StatusForbidden, `{"schema_version":"error.v1","status":403,"reason_code":"not_entitled"}`, true},
		"other_404": {http.StatusNotFound, `{"schema_version":"error.v1","status":404,"reason_code":"other"}`, true},
		"unavail":   {http.StatusServiceUnavailable, `{}`, true},
	} {
		t.Run(name, func(t *testing.T) {
			s := &delegatedServer{status: tc.status, answer: tc.answer}
			err := newDelegatedClient(t, s).EndDelegated(context.Background(), delegatedTestRequest(), id)
			if (err != nil) != tc.wantErr || (err != nil && !errors.Is(err, ErrUnavailable)) {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if s.paths[0] != "POST /internal/v1/sessions/delegated/end" || s.bodies[0] != `{"session_id":"`+id.String()+`"}` {
				t.Fatalf("request = %v %v", s.paths, s.bodies)
			}
		})
	}
}

func TestDelegatedCallsNeverFollowARedirect(t *testing.T) {
	var hits int
	other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits++ }))
	defer other.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	base, _ := url.Parse(server.URL)
	client, err := newTestClient(t, base, testToken(t), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.StartDelegated(context.Background(), delegatedTestRequest(), DelegatedRequest{}); !errors.Is(err, ErrUnavailable) || hits != 0 {
		t.Fatalf("err = %v, hits = %d", err, hits)
	}
}
