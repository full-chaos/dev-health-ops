package credentials

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// hostRecorder answers every request in memory and records the host each one went to.
type hostRecorder struct{ hosts []string }

func (h *hostRecorder) RoundTrip(request *http.Request) (*http.Response, error) {
	h.hosts = append(h.hosts, request.URL.Host)
	body, status := `[]`, http.StatusOK
	if strings.HasSuffix(request.URL.Path, "/access_tokens") {
		body, status = `{"token":"inst_ok","expires_at":"2099-01-01T00:00:00Z"}`, http.StatusCreated
	}
	return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

// An App credential (the secret holds app_id, private_key and installation_id) lists its repositories against the
// secret's base or the default GitHub API, never a base the row's config names; renaming the row or changing its
// config auth_mode does not change that.
func TestGitHubAppRepositoryListingTakesNoBaseFromConfig(t *testing.T) {
	const configBase = "https://config-base.example.test"
	for name, config := range map[string]string{
		"config base":              `{"base_url":"` + configBase + `"}`,
		"config base and new mode": `{"base_url":"` + configBase + `","auth_mode":"token","org":"acme"}`,
	} {
		t.Run(name, func(t *testing.T) {
			transport := &hostRecorder{}
			h := newHandlers(Deps{HTTPClient: &http.Client{Transport: transport}, Logger: slog.New(slog.DiscardHandler),
				HostLookup: func(context.Context, string) ([]netip.Addr, error) {
					return []netip.Addr{netip.MustParseAddr("93.184.216.34")}, nil
				}})
			decrypted := pyjson.NewObject()
			decrypted.Set("app_id", "1")
			decrypted.Set("private_key", testPrivateKey(t))
			decrypted.Set("installation_id", "2")
			credential := &repoCredential{provider: "github", decrypted: decrypted, config: decodeObject(t, config)}
			recorder := httptest.NewRecorder()
			h.listGitHubRepos(recorder, httptest.NewRequest(http.MethodGet, "/credentials/x/repos", nil), credential, "acme", "", nil)
			if len(transport.hosts) == 0 {
				t.Fatalf("no provider request was sent (status %d): the cell measures nothing", recorder.Code)
			}
			for _, host := range transport.hosts {
				if host == "config-base.example.test" {
					t.Errorf("a request went to the config base %s; want none (hosts %v)", host, transport.hosts)
					break
				}
			}
		})
	}
	// Control: a token credential keeps its config base, as today.
	transport := &hostRecorder{}
	h := newHandlers(Deps{HTTPClient: &http.Client{Transport: transport}, Logger: slog.New(slog.DiscardHandler),
		HostLookup: func(context.Context, string) ([]netip.Addr, error) {
			return []netip.Addr{netip.MustParseAddr("93.184.216.34")}, nil
		}})
	decrypted := pyjson.NewObject()
	decrypted.Set("token", "tok")
	h.listGitHubRepos(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/credentials/x/repos", nil),
		&repoCredential{provider: "github", decrypted: decrypted, config: decodeObject(t, `{"base_url":"`+configBase+`"}`)}, "acme", "", nil)
	if len(transport.hosts) == 0 || transport.hosts[0] != "config-base.example.test" {
		t.Errorf("control: a token credential sent its requests to %v; want the config base first", transport.hosts)
	}
}
