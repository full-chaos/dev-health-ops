//go:build unix

package credentials

import (
	"context"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strconv"
	"strings"
	"testing"
)

// authHosts answers every request with an empty list and records where it went and what it carried.
type authHosts struct{ requests []string }

func (a *authHosts) RoundTrip(request *http.Request) (*http.Response, error) {
	a.requests = append(a.requests, request.URL.Host+" "+request.Header.Get("Authorization"))
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}},
		Body: io.NopCloser(strings.NewReader(`[]`)), Request: request}, nil
}

// A stored row written before the refusal, holding a valid token and BOTH key-path spellings (a path first, a falsy
// value second), through the repository listing: no key file is opened, and every provider request goes to the
// default GitHub host with the row's own token.
func TestRepositoryListingLegacyBothSpellingsRow(t *testing.T) {
	counter := newOpenCounter(t, testPrivateKey(t))
	path := strconv.Quote(counter.path)
	for name, fields := range map[string]string{
		"private_key_path=path,privateKeyPath=false": `"private_key_path":` + path + `,"privateKeyPath":false`,
		"private_key_path=path,privateKeyPath=blank": `"private_key_path":` + path + `,"privateKeyPath":""`,
		"privateKeyPath=path,private_key_path=false": `"privateKeyPath":` + path + `,"private_key_path":false`,
		"privateKeyPath=path,private_key_path=zero":  `"privateKeyPath":` + path + `,"private_key_path":0`,
	} {
		t.Run(name, func(t *testing.T) {
			transport := &authHosts{}
			h := newHandlers(Deps{HTTPClient: &http.Client{Transport: transport}, Logger: slog.New(slog.DiscardHandler),
				HostLookup: func(context.Context, string) ([]netip.Addr, error) {
					return []netip.Addr{netip.MustParseAddr("93.184.216.34")}, nil
				}})
			decrypted := decodeObject(t, `{"token":"ghp_synthetic_row_token",`+fields+`}`)
			opensBefore := counter.opens.Load()
			recorder := httptest.NewRecorder()
			h.listGitHubRepos(recorder, httptest.NewRequest(http.MethodGet, "/credentials/x/repos", nil),
				&repoCredential{provider: "github", decrypted: decrypted}, "acme", "", big.NewInt(100))
			if opened := counter.opens.Load() - opensBefore; opened != 0 {
				t.Errorf("%d key-file open(s); want 0", opened)
			}
			for _, request := range transport.requests {
				if request != "api.github.com token ghp_synthetic_row_token" {
					t.Errorf("a provider request went to %q; want the default host with the row's own token", request)
				}
			}
			t.Logf("status %d, %d provider request(s)", recorder.Code, len(transport.requests))
		})
	}
}
