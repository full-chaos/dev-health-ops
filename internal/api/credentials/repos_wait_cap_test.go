package credentials

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

type rateLimitedTransport struct {
	headers http.Header
	status  int
	calls   int
}

func (r *rateLimitedTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	r.calls++
	return &http.Response{StatusCode: r.status, Header: r.headers.Clone(), Body: io.NopCloser(strings.NewReader(`{"message":"planted provider text"}`)), Request: request}, nil
}

func rateLimitedRepos(t *testing.T, provider string, transport *rateLimitedTransport) (*httptest.ResponseRecorder, string) {
	t.Helper()
	logs := &bytes.Buffer{}
	h := newHandlers(Deps{HTTPClient: &http.Client{Transport: transport}, Logger: slog.New(slog.NewTextHandler(logs, nil)),
		HostLookup: func(context.Context, string) ([]netip.Addr, error) {
			return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
		}})
	decrypted := pyjson.NewObject()
	decrypted.Set("token", "tok")
	decrypted.Set("url", "https://gitlab.example.com")
	decrypted.Set("base_url", "https://ghe.example.com")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	request := httptest.NewRequest(http.MethodGet, "/credentials/x/repos", nil).WithContext(ctx)
	recorder := httptest.NewRecorder()
	credential := &repoCredential{provider: provider, decrypted: decrypted}
	if provider == "gitlab" {
		h.listGitLabRepos(recorder, request, credential, "grp", "", nil)
	} else {
		h.listGitHubRepos(recorder, request, credential, "acme", "", nil)
	}
	return recorder, logs.String()
}

func TestARateLimitWaitPastTheCeilingAnswers429AtOnceOnTheRepoBrowse(t *testing.T) {
	for _, provider := range []string{"gitlab", "github"} {
		transport := &rateLimitedTransport{status: 429, headers: http.Header{"Retry-After": {"86400"}}}
		recorder, _ := rateLimitedRepos(t, provider, transport)
		if recorder.Code != http.StatusTooManyRequests {
			t.Fatalf("%s: status %d, WANT 429: %s", provider, recorder.Code, recorder.Body.String())
		}
		if want := `{"detail":"The provider rate limit was reached. Try again later."}`; recorder.Body.String() != want {
			t.Fatalf("%s: body %q, want %q", provider, recorder.Body.String(), want)
		}
		if transport.calls != 1 {
			t.Fatalf("%s: %d requests, WANT 1", provider, transport.calls)
		}
	}
}

func TestARefusedWaitOnARetryable503KeepsTheMappingOfItsClassOnTheRepoBrowse(t *testing.T) {
	for _, row := range []struct {
		provider string
		status   int
	}{{"github", http.StatusBadGateway}, {"gitlab", http.StatusInternalServerError}} {
		transport := &rateLimitedTransport{status: 503, headers: http.Header{"Retry-After": {"60"}}}
		recorder, logs := rateLimitedRepos(t, row.provider, transport)
		if recorder.Code != row.status {
			t.Fatalf("%s: status %d, want %d: %s", row.provider, recorder.Code, row.status, recorder.Body.String())
		}
		if transport.calls != 1 {
			t.Fatalf("%s: %d requests, want 1", row.provider, transport.calls)
		}
		if !strings.Contains(logs, "503") || strings.Contains(logs, "deadline exceeded") {
			t.Fatalf("%s: the failure is not the refused 503 (a slept wait ends as a context error): %s", row.provider, logs)
		}
	}
}
