package acrlisting

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

const (
	exchangePath = "/api/v1/oauth/token"
	listPath     = "/internal/v1/credentials/lifecycle"
	subjectJWT   = "subject-jwt-one"
	accessOne    = "access-one"
)

type listCall struct {
	method, query, auth string
}

// fakeACR is the acr token endpoint and the credential listing of one organization.
type fakeACR struct {
	t   *testing.T
	srv *httptest.Server

	mu          sync.Mutex
	rows        []Row
	exchanges   []url.Values
	exchangeCT  []string
	listCalls   []listCall
	methods     []string
	access      string
	expiresIn   int
	exchangeRC  int
	exchangeRaw string
	listRC      int
	retryAfter  string
	listRaw     func(after string, limit int) (int, string)
}

func newFake(t *testing.T, rows ...Row) *fakeACR {
	t.Helper()
	f := &fakeACR{t: t, access: accessOne, expiresIn: 3600}
	f.rows = append(f.rows, rows...)
	sort.Slice(f.rows, func(i, j int) bool { return f.rows[i].ID < f.rows[j].ID })
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeACR) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.methods = append(f.methods, r.Method+" "+r.URL.Path)
	switch r.URL.Path {
	case exchangePath:
		_ = r.ParseForm()
		f.exchanges = append(f.exchanges, r.PostForm)
		f.exchangeCT = append(f.exchangeCT, r.Header.Get("Content-Type"))
		if f.exchangeRC != 0 {
			w.WriteHeader(f.exchangeRC)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if f.exchangeRaw != "" {
			_, _ = w.Write([]byte(f.exchangeRaw))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"schema_version": "token_exchange_response.v1", "access_token": f.access,
			"issued_token_type": "urn:ietf:params:oauth:token-type:access_token", "token_type": "Bearer", "expires_in": f.expiresIn,
		})
	case listPath:
		f.listCalls = append(f.listCalls, listCall{r.Method, r.URL.RawQuery, r.Header.Get("Authorization")})
		after := r.URL.Query().Get("after")
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if f.retryAfter != "" {
			w.Header().Set("Retry-After", f.retryAfter)
		}
		if f.listRC != 0 {
			w.WriteHeader(f.listRC)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if f.listRaw != nil {
			rc, body := f.listRaw(after, limit)
			w.WriteHeader(rc)
			_, _ = w.Write([]byte(body))
			return
		}
		items := []map[string]any{}
		var next any
		for _, row := range f.rows {
			if after != "" && row.ID <= after {
				continue
			}
			if len(items) == limit {
				next = items[len(items)-1]["credential_id"]
				break
			}
			items = append(items, map[string]any{"credential_id": row.ID, "revision": row.Revision, "status": row.Status})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"schema_version": "credential_lifecycle_list.v1", "items": items, "next_after": next})
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (f *fakeACR) snapshotCalls() []listCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]listCall(nil), f.listCalls...)
}

func subjectFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "subject")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func newClient(t *testing.T, f *fakeACR, mod func(*Config)) *Client {
	t.Helper()
	base, _ := url.Parse(f.srv.URL)
	tok, _ := url.Parse(f.srv.URL + exchangePath)
	cfg := Config{BaseURL: base, TokenEndpoint: tok, SubjectTokenFile: subjectFile(t, subjectJWT+"\n"), Timeout: 5 * time.Second}
	if mod != nil {
		mod(&cfg)
	}
	c, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func row(id string, rev int64, status string) Row { return Row{ID: id, Revision: rev, Status: status} }
