//go:build integration

package apiservice

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// membersStub is the provider both planes reach for the team member routes
// (Jira and GitLab take their host from the stored credential's config, so
// both planes can be pointed at it). It records every request as
// "METHOD path?sorted-query" so the two planes' provider traffic can be
// compared after each has served the whole request list. Unknown resources
// answer 404, never 5xx: the Go client retries a 5xx, Python's does not, and
// that difference is not what this oracle measures.
type membersStub struct {
	server *httptest.Server
	mu     sync.Mutex
	seen   []string
}

func newMembersStub(t *testing.T) *membersStub {
	t.Helper()
	stub := &membersStub{}
	stub.server = httptest.NewServer(http.HandlerFunc(stub.handle))
	t.Cleanup(stub.server.Close)
	return stub
}

// take returns the recorded signatures (sorted) and clears them.
func (s *membersStub) take() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]string(nil), s.seen...)
	s.seen = nil
	sort.Strings(out)
	return out
}

func canonicalQuery(values url.Values) string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		for _, value := range values[key] {
			parts = append(parts, url.QueryEscape(key)+"="+url.QueryEscape(value))
		}
	}
	return strings.Join(parts, "&")
}

func (s *membersStub) handle(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	signature := r.Method + " " + r.URL.EscapedPath()
	if encoded := canonicalQuery(query); encoded != "" {
		signature += "?" + encoded
	}
	s.mu.Lock()
	s.seen = append(s.seen, signature)
	s.mu.Unlock()

	write := func(status int, body string, headers map[string]string) {
		w.Header().Set("Content-Type", "application/json")
		for key, value := range headers {
			w.Header().Set(key, value)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}
	notFound := func() { write(http.StatusNotFound, `{"message":"venue stub: not found"}`, nil) }
	path := r.URL.Path

	switch {
	// --- Jira: a project's lead ---
	case strings.HasPrefix(path, "/rest/api/3/project/"):
		switch strings.TrimPrefix(path, "/rest/api/3/project/") {
		case "design":
			write(200, `{"key":"design","lead":{"accountId":"acc-1","displayName":"Acc One","emailAddress":"acc1@example.com"}}`, nil)
		case "qa":
			write(200, `{"key":"qa","lead":{"displayName":"Alice Inferred"}}`, nil)
		case "leademail":
			write(200, `{"lead":{"emailAddress":"only@example.com"}}`, nil)
		case "leadless":
			write(200, `{"key":"leadless"}`, nil)
		case "dr-team":
			write(200, `{"lead":{"accountId":"new-acc","displayName":"Brand New","emailAddress":"alice-new@example.com"}}`, nil)
		default:
			notFound()
		}
	// --- Jira: issue search for inference ---
	case path == "/rest/api/3/search/jql":
		s.jiraSearch(w, query, write, notFound)
	// --- GitLab: a group and its members ---
	case path == "/api/v4/groups/design":
		write(200, `{"id":42,"full_path":"design","name":"Design"}`, nil)
	case path == "/api/v4/groups/dr-team":
		write(200, `{"id":43,"full_path":"dr-team","name":"DR"}`, nil)
	case path == "/api/v4/groups/42/members":
		if query.Get("page") == "2" {
			write(200, `[{"id":3,"username":"third","name":"Third Person","state":"active","access_level":50,"email":"third@example.com"},{"id":4,"username":null,"name":"No Username","access_level":10}]`, nil)
			return
		}
		next := fmt.Sprintf(`<%s/api/v4/groups/42/members?page=2&per_page=100>; rel="next"`, "http://"+r.Host)
		write(200, `[{"id":1,"username":"acc-1","name":"Acc One","state":"active","access_level":30},{"id":2,"username":"","name":"Empty Username","access_level":20},{"id":5,"username":"mixed","name":null,"access_level":0}]`, map[string]string{"Link": next})
	case path == "/api/v4/groups/43/members":
		write(200, `[]`, nil)
	default:
		notFound()
	}
}

func (s *membersStub) jiraSearch(w http.ResponseWriter, query url.Values, write func(int, string, map[string]string), notFound func()) {
	jql := query.Get("jql")
	actor := func(id, name, email string) string {
		if email == "" {
			return fmt.Sprintf(`{"accountId":%q,"displayName":%q}`, id, name)
		}
		return fmt.Sprintf(`{"accountId":%q,"displayName":%q,"emailAddress":%q}`, id, name, email)
	}
	issue := func(updated, assignee, reporter, creator string) string {
		return fmt.Sprintf(`{"key":"X-1","fields":{"updated":%q,"assignee":%s,"reporter":%s,"creator":%s}}`, updated, assignee, reporter, creator)
	}
	switch {
	case strings.Contains(strings.ToLower(jql), "project = 'design'"):
		if query.Get("nextPageToken") == "tok2" {
			write(200, `{"issues":[`+
				issue("2026-09-03T10:00:00.500+0000", actor("acc-1", "Acc One", "acc1@example.com"), actor("acc-9", "", ""), "null")+","+
				issue("2026-09-04T10:00:00.000+05:30", actor("acc-9", "Nine", "nine@example.com"), "null", actor("acc-1", "", ""))+
				`],"isLast":true}`, nil)
			return
		}
		write(200, `{"issues":[`+
			issue("2026-09-01T10:00:00.000+0000", actor("acc-1", "Acc One", "acc1@example.com"), actor("acc-2", "Two", ""), actor("acc-3", "Three", "three@example.com"))+","+
			issue("2026-09-02T10:00:00.000+0000", actor("acc-1", "", ""), actor("acc-2", "", "two@example.com"), actor("acc-3", "", ""))+","+
			issue("not a date", actor("acc-1", "", ""), actor("acc-4", "Four", ""), "null")+","+
			issue("2026-09-02T12:00:00Z", actor("acc-1", "", ""), "null", "null")+","+
			`{"key":"X-5","fields":null},{"key":"X-6","fields":{"assignee":"nope","reporter":{"displayName":"no id"}}}`+
			`],"nextPageToken":"tok2","isLast":false}`, nil)
	case strings.Contains(strings.ToLower(jql), "project = 'qa'"):
		if query.Get("startAt") != "0" {
			write(200, `{"issues":[]}`, nil)
			return
		}
		write(200, `{"issues":[`+
			issue("2026-09-05T08:00:00.000+0000", actor("acc-3", "", ""), actor("acc-3", "", ""), actor("acc-3", "", ""))+","+
			issue("2026-09-06T08:00:00.000+0000", actor("acc-3", "", ""), actor("acc-3", "", ""), actor("acc-3", "", ""))+","+
			// A fractional-second UTC offset: CPython parses it and pydantic
			// prints only its hours and minutes.
			issue("2026-09-07T10:00:00+05:30:15.5", actor("acc-3", "", ""), "null", "null")+","+
			// Later wall clock, earlier instant: activity is ordered by the
			// moment, not the clock reading.
			issue("2026-09-06T08:00:00.000+0000", actor("acc-8", "", ""), "null", "null")+","+
			issue("2026-09-06T10:00:00+14:00", actor("acc-8", "", ""), "null", "null")+
			`]}`, nil)
	case strings.Contains(strings.ToLower(jql), "project = 'naive'"):
		write(200, `{"issues":[`+
			issue("2026-09-05T08:00:00", actor("acc-7", "Seven", ""), "null", "null")+","+
			issue("2026-09-06T08:00:00+0000", actor("acc-7", "", ""), "null", "null")+
			`],"isLast":true}`, nil)
	case strings.Contains(strings.ToLower(jql), "project = 'badactor'"):
		write(200, `{"issues":[{"key":"B-1","fields":{"assignee":{"accountId":["acc-odd"],"displayName":"Odd"}}}],"isLast":true}`, nil)
	case strings.Contains(strings.ToLower(jql), "project = 'objactor'"):
		write(200, `{"issues":[{"key":"B-2","fields":{"assignee":{"accountId":{"k":1}}}},{"key":"B-3","fields":{"assignee":{"accountId":[]}}}],"isLast":true}`, nil)
	case strings.Contains(strings.ToLower(jql), "project = 'numactor'"):
		write(200, `{"issues":[{"key":"B-4","fields":{"assignee":{"accountId":12,"displayName":"Num"},"reporter":{"accountId":"12"},"creator":{"accountId":true}}}],"isLast":true}`, nil)
	case strings.Contains(strings.ToLower(jql), "project = 'empty'"):
		write(200, `{"issues":[]}`, nil)
	default:
		notFound()
	}
}

// memberCredential is one integration_credentials row the member routes
// resolve; secrets are encrypted by the Python plane's own encrypt_value.
type memberCredential struct {
	provider, name string
	secrets        map[string]any
	config         map[string]any
}

func memberCredentials(stubURL string) []memberCredential {
	return []memberCredential{
		{"jira", "jira-ok", map[string]any{"email": "ops@example.com", "api_token": "tok"}, map[string]any{"url": stubURL}},
		{"jira", "jira-no-url", map[string]any{"email": "ops@example.com", "api_token": "tok"}, map[string]any{}},
		{"jira", "jira-no-email", map[string]any{"api_token": "tok"}, map[string]any{"url": stubURL}},
		{"gitlab", "gl-ok", map[string]any{"token": "gltok"}, map[string]any{"url": stubURL}},
		{"gitlab", "gl-no-token", map[string]any{}, map[string]any{"url": stubURL}},
		{"github", "gh-no-org", map[string]any{"token": "ghtok"}, map[string]any{}},
		{"github", "gh-app", map[string]any{"app_id": "1", "private_key": "x", "installation_id": "2"}, map[string]any{"org": "acme"}},
	}
}

// seedMemberCredentials writes the rows for org, every ciphertext produced
// by the Python plane's encrypt_value.
func seedMemberCredentials(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue, org uuid.UUID, stubURL string) {
	t.Helper()
	credentials := memberCredentials(stubURL)
	calls := make([]venueoracle.PythonCall, len(credentials))
	for index, credential := range credentials {
		encoded, err := json.Marshal(credential.secrets)
		if err != nil {
			t.Fatal(err)
		}
		calls[index] = venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:encrypt_value", Args: []any{string(encoded)}}
	}
	results := venue.CallPython(t, calls...)
	for index, credential := range credentials {
		var ciphertext string
		if err := json.Unmarshal(results[index], &ciphertext); err != nil {
			t.Fatalf("decode ciphertext: %v", err)
		}
		config, err := json.Marshal(credential.config)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := admin.Exec(ctx, `INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
			VALUES ($1, $2, $3, $4, true, $5, $6::jsonb, now(), now())`,
			uuid.New(), org, credential.provider, credential.name, ciphertext, string(config)); err != nil {
			t.Fatalf("seed credential %s: %v", credential.name, err)
		}
	}
}
