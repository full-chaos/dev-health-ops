package credentials

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

type browseTransport struct {
	routes map[string]string
	seen   []string
}

func (b *browseTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	b.seen = append(b.seen, request.URL.Path)
	body, status := `{"message":"Not Found"}`, http.StatusNotFound
	if routed, found := b.routes[request.URL.Path]; found {
		body, status = routed, http.StatusOK
	}
	return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func browseItem(fullName string) string {
	return `{"id":1,"name":"` + fullName[strings.Index(fullName, "/")+1:] + `","full_name":"` + fullName + `","description":null,"html_url":"https://ghe.example.com/` + fullName + `"}`
}

func browse(t *testing.T, transport *browseTransport, stored map[string]pyjson.Value, owner string) (map[string]any, string) {
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
	h.listGitHubRepos(recorder, request, &repoCredential{provider: "github", decrypted: decrypted}, owner, "", nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status %d: %s", recorder.Code, recorder.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	return body, logs.String()
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

func TestBrowseOfAnOrganizationReportsOrgAndTheResponseFlagsInstallationScope(t *testing.T) {
	transport := &browseTransport{routes: map[string]string{"/user/repos": "[]", "/orgs/acme/repos": "[" + browseItem("acme/api") + "]"}}
	// An App credential needs a minted installation token, so the scope flag is asserted at the response builder and the
	// no-/user/repos rule at the client (githubcode list_namespace_test.go).
	body, _ := browse(t, transport, map[string]pyjson.Value{"token": "tok"}, "acme")
	if body["owner_type"] != "org" || body["installation_scoped"] != false {
		t.Fatalf("body %v", body)
	}
	scoped := githubRepoListing(nil, githubScope{installation: true, ownerType: "org"})
	if value, _ := scoped.Get("installation_scoped"); value != true {
		t.Fatalf("installation_scoped %v, want true", value)
	}
}
