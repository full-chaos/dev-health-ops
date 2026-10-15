package credentials

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

type browseTransport struct {
	routes   map[string]string
	statuses map[string]int
	seen     []string
}

func (b *browseTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	b.seen = append(b.seen, request.URL.Path)
	body, status := `{"message":"Not Found"}`, http.StatusNotFound
	if code, found := b.statuses[request.URL.Path]; found {
		body, status = `{"message":"denied"}`, code
	} else if strings.HasSuffix(request.URL.Path, "/access_tokens") {
		body, status = `{"token":"inst_ok","expires_at":"2099-01-01T00:00:00Z"}`, http.StatusCreated
	} else if routed, found := b.routes[request.URL.Path]; found {
		body, status = routed, http.StatusOK
	}
	return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func browseItem(fullName string) string {
	return `{"id":1,"name":"` + fullName[strings.Index(fullName, "/")+1:] + `","full_name":"` + fullName + `","description":null,"html_url":"https://ghe.example.com/` + fullName + `"}`
}

func browse(t *testing.T, transport *browseTransport, stored map[string]pyjson.Value, owner string) (map[string]any, string) {
	t.Helper()
	recorder, logs := browseRaw(t, transport, stored, owner, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status %d: %s", recorder.Code, recorder.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	return body, logs
}

func browseRaw(t *testing.T, transport *browseTransport, stored map[string]pyjson.Value, owner string, maxRepos *big.Int) (*httptest.ResponseRecorder, string) {
	t.Helper()
	logs := &bytes.Buffer{}
	h := newHandlers(Deps{HTTPClient: &http.Client{Transport: transport}, Logger: slog.New(slog.NewTextHandler(logs, nil)),
		HostLookup: func(context.Context, string) ([]netip.Addr, error) {
			return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
		}})
	decrypted := pyjson.NewObject()
	decrypted.Set("base_url", "https://ghe.example.com")
	for key, value := range stored {
		decrypted.Set(key, value)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/credentials/x/repos", nil)
	h.listGitHubRepos(recorder, request, &repoCredential{provider: "github", decrypted: decrypted}, owner, "", maxRepos)
	return recorder, logs.String()
}

func TestBrowseOfAPersonalOwnerListsTheirRepositoriesAndSaysTheOwnerIsAUser(t *testing.T) {
	transport := &browseTransport{routes: map[string]string{
		"/user/repos":           "[" + browseItem("token-owner/mine") + "]",
		"/users/chrisgeo/repos": "[" + browseItem("chrisgeo/dotfiles") + "]",
	}}

	body, _ := browse(t, transport, map[string]pyjson.Value{"token": "tok"}, "chrisgeo")

	repos, _ := body["repos"].([]any)
	if len(repos) != 1 || repos[0].(map[string]any)["full_name"] != "chrisgeo/dotfiles" || body["total"] != float64(1) {
		t.Fatalf("repos %v total %v, want chrisgeo/dotfiles", body["repos"], body["total"])
	}
	if body["owner_type"] != "user" || body["installation_scoped"] != false || body["diagnostic"] != nil {
		t.Fatalf("owner_type %v installation_scoped %v diagnostic %v, want user, false, null", body["owner_type"], body["installation_scoped"], body["diagnostic"])
	}
}

func TestBrowseOfAnUnknownOwnerReportsOwnerNotFoundInsteadOfASilentEmptyList(t *testing.T) {
	transport := &browseTransport{routes: map[string]string{"/user/repos": "[]"}}

	body, logs := browse(t, transport, map[string]pyjson.Value{"token": "tok"}, "ghost")

	if repos, _ := body["repos"].([]any); len(repos) != 0 || body["diagnostic"] != "owner_not_found" || body["owner_type"] != nil {
		t.Fatalf("body %v, want no repos, diagnostic owner_not_found, owner_type null", body)
	}
	if strings.Contains(logs, "ghost") {
		t.Fatalf("the log names the owner: %s", logs)
	}
}

func TestBrowseOfAnAppInstallationFlagsTheInstallationScopeAndNeverCallsUserRepos(t *testing.T) {
	transport := &browseTransport{routes: map[string]string{"/orgs/acme/repos": "[" + browseItem("acme/api") + "]"}}
	stored := map[string]pyjson.Value{"app_id": "1", "private_key": testPrivateKey(t), "installation_id": "111"}

	body, _ := browse(t, transport, stored, "acme")

	if body["installation_scoped"] != true || body["owner_type"] != "org" {
		t.Fatalf("body %v, want installation_scoped true, owner_type org", body)
	}
	for _, path := range transport.seen {
		if path == "/user/repos" {
			t.Fatalf("an installation credential called /user/repos: %v", transport.seen)
		}
	}
}

func TestBrowseNeverReturnsTheOwnerLoginInAnErrorText(t *testing.T) {
	transport := &browseTransport{statuses: map[string]int{
		"/orgs/private-login/repos":  http.StatusForbidden,
		"/users/private-login/repos": http.StatusForbidden,
	}, routes: map[string]string{"/user/repos": "[]"}}

	recorder, logs := browseRaw(t, transport, map[string]pyjson.Value{"token": "tok"}, "private-login", nil)

	if recorder.Code != http.StatusUnauthorized || strings.Contains(recorder.Body.String(), "private-login") || strings.Contains(logs, "private-login") {
		t.Fatalf("status %d body %s log %s, want a 401 whose text and log carry no login", recorder.Code, recorder.Body.String(), logs)
	}
}

func TestBrowseTreatsAMissingTokenListingAsACredentialFaultNotAMissingOwner(t *testing.T) {
	transport := &browseTransport{} // /user/repos answers 404

	recorder, _ := browseRaw(t, transport, map[string]pyjson.Value{"token": "tok"}, "acme", nil)

	if recorder.Code != http.StatusBadGateway || strings.Contains(recorder.Body.String(), "owner_not_found") {
		t.Fatalf("status %d body %s, want 502 and no owner_not_found diagnostic", recorder.Code, recorder.Body.String())
	}
}

func TestBrowseWithANegativeMaxReposKeepsOneRepositoryAndDoesNotPanic(t *testing.T) {
	transport := &browseTransport{routes: map[string]string{
		"/user/repos":      "[]",
		"/orgs/acme/repos": "[" + browseItem("acme/api") + "," + browseItem("acme/web") + "]",
	}}

	recorder, _ := browseRaw(t, transport, map[string]pyjson.Value{"token": "tok"}, "acme", big.NewInt(-1))

	var body map[string]any
	if recorder.Code != http.StatusOK || json.Unmarshal(recorder.Body.Bytes(), &body) != nil || body["total"] != float64(1) {
		t.Fatalf("status %d body %s, want 200 with one repository", recorder.Code, recorder.Body.String())
	}
}

func TestBrowseOfAnOrganizationReportsOrg(t *testing.T) {
	transport := &browseTransport{routes: map[string]string{"/user/repos": "[]", "/orgs/acme/repos": "[" + browseItem("acme/api") + "]"}}

	body, _ := browse(t, transport, map[string]pyjson.Value{"token": "tok"}, "acme")

	if body["owner_type"] != "org" || body["installation_scoped"] != false {
		t.Fatalf("body %v", body)
	}
}
