package synccli

import (
	"net/http"
	"reflect"
	"strings"
	"testing"

	"atlassian/atlassian"
	"atlassian/atlassian/graph"

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

// The doer production runs with never follows a redirect: a stored credential must not be replayed to
// another host (the worker's own doer has the same CheckRedirect).
func TestProductionDoerDoesNotFollowRedirects(t *testing.T) {
	doer := productionDoer()
	if doer.CheckRedirect == nil || doer.CheckRedirect(nil, nil) == nil {
		t.Fatal("the production doer follows redirects")
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
