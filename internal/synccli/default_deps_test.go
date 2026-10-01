package synccli

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"atlassian/atlassian"
	"atlassian/atlassian/graph"

	"github.com/full-chaos/dev-health-ops/internal/atlassianteams"
	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// CHAOS-7132 follow-up: `dho sync teams --provider jira` failed on bigboy with "provider credential is
// invalid" because defaultDeps() left doer nil: resolveJiraStoredSettings hands it straight to
// providerfoundation.NewJiraClient, which refuses a nil doer. Every test injected a doer, so nothing ever ran
// the production wiring. The class is a dependency that production leaves nil and only tests populate.
//
// This guard walks every field of deps: defaultDeps() (what the verb really runs with) must leave none nil.
// A new field with no production default fails here instead of in an operator's terminal.
func TestDefaultDepsLeavesNoDependencyNil(t *testing.T) {
	value := reflect.ValueOf(defaultDeps())
	for index := 0; index < value.NumField(); index++ {
		field := value.Type().Field(index)
		switch field.Type.Kind() {
		case reflect.Func, reflect.Interface, reflect.Pointer, reflect.Map, reflect.Slice, reflect.Chan:
			if value.Field(index).IsNil() {
				t.Errorf("defaultDeps() leaves deps.%s (%s) nil: production runs the verb with no %s, and only tests populate it",
					field.Name, field.Type, field.Name)
			}
		}
	}
}

// The same guard for the in-process sync executor's deps: defaultInlineDeps() is what production hands it.
func TestDefaultInlineDepsLeavesNoDependencyNil(t *testing.T) {
	value := reflect.ValueOf(defaultInlineDeps())
	for index := 0; index < value.NumField(); index++ {
		field := value.Type().Field(index)
		if field.Type.Kind() == reflect.Func && value.Field(index).IsNil() {
			t.Errorf("defaultInlineDeps() leaves InlineDeps.%s nil", field.Name)
		}
	}
}

// The doer production runs with follows redirects (the unauthenticated tenant lookup needs it) but never
// replays a credential to another host.
func TestProductionDoerDropsCredentialsOnAHostChange(t *testing.T) {
	var gotAuthorization, gotPrivateToken string
	var hits int
	target := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		hits++
		gotAuthorization, gotPrivateToken = r.Header.Get("Authorization"), r.Header.Get("Private-Token")
	}))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, http.StatusFound) }))
	defer origin.Close()
	request, _ := http.NewRequest(http.MethodGet, origin.URL, nil)
	request.Header.Set("Authorization", "Bearer not-a-real-credential")
	request.Header.Set("Private-Token", "not-a-real-credential")
	response, err := productionDoer().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if hits != 1 {
		t.Fatalf("the redirect was not followed (hits=%d)", hits)
	}
	if gotAuthorization != "" || gotPrivateToken != "" {
		t.Fatalf("a credential header reached the other host: Authorization=%q Private-Token=%q", gotAuthorization, gotPrivateToken)
	}
}

// r1 P1: the Atlassian gateway clients (teams and organization resolver) carried the stored Authorization
// header and followed redirects. Behaviour, not text: the clients defaultDeps() builds must not follow one.
func TestDefaultDepsGatewayClientsRefuseRedirects(t *testing.T) {
	d := defaultDeps()
	for name, client := range map[string]any{
		"teams client":          d.newClient("https://tenant.example.test/gateway/api", atlassian.BasicAPITokenAuth{Email: "e@example.test", Token: "t"}),
		"organization resolver": d.newOrganizationResolver("https://tenant.example.test/gateway/api", atlassian.BasicAPITokenAuth{Email: "e@example.test", Token: "t"}),
	} {
		graphClient, ok := client.(*graph.Client)
		if !ok || graphClient.HTTPClient == nil || graphClient.HTTPClient.CheckRedirect == nil {
			t.Errorf("%s: no redirect policy on the production HTTP client", name)
			continue
		}
		if err := graphClient.HTTPClient.CheckRedirect(nil, nil); err != http.ErrUseLastResponse {
			t.Errorf("%s: CheckRedirect = %v, want http.ErrUseLastResponse (never follow)", name, err)
		}
	}
}

// r1 P1: a refused catalog client names its cause (the typed reason), never a value.
func TestCatalogClientRefusalNamesItsCause(t *testing.T) {
	for _, provider := range []string{"github", "gitlab", "linear"} {
		var stderr strings.Builder
		env := cli.Env{Stderr: &stderr, Lookup: func(key string) (string, bool) {
			switch key {
			case "GITHUB_URL", "GITLAB_URL", "LINEAR_URL":
				return "not a url", true
			}
			return "", false
		}}
		_, _, _, code := buildCatalogCollector(env, defaultDeps(), catalogRequest{provider: provider}, "owner", "tok-not-real", nil)
		if code == 0 {
			t.Errorf("%s: a base URL that is not absolute built a client", provider)
			continue
		}
		if !strings.Contains(stderr.String(), "base_url_invalid") {
			t.Errorf("%s: the refusal does not name its cause: %q", provider, stderr.String())
		}
		if strings.Contains(stderr.String(), "tok-not-real") || strings.Contains(stderr.String(), "not a url") {
			t.Errorf("%s: the refusal carries a value: %q", provider, stderr.String())
		}
	}
}

// r2 P1: the tenant lookup is unauthenticated and a tenant may answer with a redirect to its canonical host:
// the production doer must still follow it (a refused redirect silently produced the hostname fallback id).
func TestProductionDoerLetsTheTenantLookupFollowItsRedirect(t *testing.T) {
	canonical := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"cloudId":"actual-cloud-id"}`))
	}))
	defer canonical.Close()
	tenant := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, canonical.URL+r.URL.Path, http.StatusFound)
	}))
	defer tenant.Close()
	base, err := url.Parse(tenant.URL)
	if err != nil {
		t.Fatal(err)
	}
	id, err := atlassianteams.ResolveCloudID(context.Background(), productionDoer(), base)
	if err != nil || id != "actual-cloud-id" {
		t.Fatalf("cloud id %q err %v, want the canonical host's answer", id, err)
	}
}
