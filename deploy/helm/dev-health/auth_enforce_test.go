package devhealth_test

import (
	"encoding/json"
	"os"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const enforceEnvName = "GO_API_AUTHZ_ENFORCE"

func enforceArgs(extra ...string) []string {
	return goAPIWithAuth(append(append([]string{"--set", "authService.phase=2"}, phase3Secrets...), extra...)...)
}

func goAPIEnforceEnv(t *testing.T, docs []map[string]any, name string) (string, bool) {
	t.Helper()
	return containerEnvValue(t, authDoc(docs, "Deployment", "a-dev-health-go-api"), name)
}

// The switch is dark: the default render, with go-api and the auth proxy on, carries no trace of it.
func TestEnforceListIsAbsentByDefault(t *testing.T) {
	docs, out, err := renderAuth(t, enforceArgs()...)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	if value, set := goAPIEnforceEnv(t, docs, enforceEnvName); set {
		t.Fatalf("%s=%q in the default render; want it unset", enforceEnvName, value)
	}
	if strings.Contains(out, "AUTHZ_ENFORCE") {
		t.Fatal("the default render names AUTHZ_ENFORCE")
	}
}

func TestEnforceListRendersOnGoAPIAsOneCommaList(t *testing.T) {
	docs, out, err := renderAuth(t, enforceArgs("--set-json", `authService.enforce.goApi=["billing","teams"]`)...)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	if value, set := goAPIEnforceEnv(t, docs, enforceEnvName); !set || value != "billing,teams" {
		t.Fatalf("%s=%q set=%v; want billing,teams", enforceEnvName, value, set)
	}
	for _, name := range []string{"QUERY_API_AUTHZ_ENFORCE", "QUERY_API_HEADERS_AUTHZ_ENFORCE", "QUERY_API_PDP_MODE"} {
		if strings.Contains(out, name) {
			t.Fatalf("a go-api list render names %s: the query-api keys are unset", name)
		}
	}
}

var queryAPIEnforceNames = []string{"QUERY_API_PDP_MODE", "QUERY_API_AUTHZ_ENFORCE", "QUERY_API_HEADERS_AUTHZ_ENFORCE"}

func queryAPIEnv(t *testing.T, extra ...string) map[string]string {
	t.Helper()
	docs, out, err := renderAuth(t, phaseArgs(2, extra...)...)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	doc := authDoc(docs, "Deployment", "a-dev-health-query-api")
	got := map[string]string{}
	for _, name := range queryAPIEnforceNames {
		if v, set := containerEnvValue(t, doc, name); set {
			got[name] = v
		}
	}
	return got
}

func TestQueryAPIEnforceUnsetRendersNothing(t *testing.T) {
	if got := queryAPIEnv(t); len(got) != 0 {
		t.Fatalf("unset keys render %v; want none", got)
	}
	if got := queryAPIEnv(t, "--set-json", `authService.enforce.queryApi=[]`, "--set-json", `authService.enforce.queryApiHeaders=[]`, "--set", "authService.enforce.queryApiPdpMode="); len(got) != 0 {
		t.Fatalf("empty keys render %v; want none", got)
	}
}

func TestQueryAPIEnforceCells(t *testing.T) {
	for name, tc := range map[string]struct {
		extra []string
		want  map[string]string
	}{
		"shadow": {[]string{"--set", "authService.enforce.queryApiPdpMode=shadow"},
			map[string]string{"QUERY_API_PDP_MODE": "shadow"}},
		"off": {[]string{"--set", "authService.enforce.queryApiPdpMode=off"},
			map[string]string{"QUERY_API_PDP_MODE": "off"}},
		"enforce plus list": {[]string{"--set", "authService.enforce.queryApiPdpMode=enforce", "--set-json", `authService.enforce.queryApi=["analytics","work_items"]`},
			map[string]string{"QUERY_API_PDP_MODE": "enforce", "QUERY_API_AUTHZ_ENFORCE": "analytics,work_items"}},
		"headers list": {[]string{"--set", "authService.enforce.queryApiPdpMode=shadow", "--set-json", `authService.enforce.queryApiHeaders=["git"]`},
			map[string]string{"QUERY_API_PDP_MODE": "shadow", "QUERY_API_HEADERS_AUTHZ_ENFORCE": "git"}},
	} {
		t.Run(name, func(t *testing.T) {
			got := queryAPIEnv(t, tc.extra...)
			if len(got) != len(tc.want) {
				t.Fatalf("env %v; want %v", got, tc.want)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Fatalf("env %v; want %v", got, tc.want)
				}
			}
		})
	}
}

func TestQueryAPIEnforceRefusals(t *testing.T) {
	mode := func(m string) []string { return []string{"--set", "authService.enforce.queryApiPdpMode=" + m} }
	for name, tc := range map[string]struct {
		extra []string
		want  string
	}{
		"unknown namespace":         {append(mode("shadow"), "--set-json", `authService.enforce.queryApi=["nope"]`), "enforce"},
		"go-api-only namespace":     {append(mode("shadow"), "--set-json", `authService.enforce.queryApi=["billing"]`), "enforce"},
		"unknown headers namespace": {append(mode("shadow"), "--set-json", `authService.enforce.queryApiHeaders=["nope"]`), "enforce"},
		"bad mode":                  {mode("on"), "queryApiPdpMode"},
		"list without mode":         {[]string{"--set-json", `authService.enforce.queryApi=["git"]`}, "authService.enforce.queryApi needs"},
		"list with mode off":        {append(mode("off"), "--set-json", `authService.enforce.queryApi=["git"]`), "authService.enforce.queryApi needs"},
		"headers without mode":      {[]string{"--set-json", `authService.enforce.queryApiHeaders=["git"]`}, "authService.enforce.queryApiHeaders needs"},
		"extraEnv mode":             {[]string{"--set-json", `queryApi.extraEnv=[{"name":"QUERY_API_DATABASE_ROLE","value":"devhealth_query"},{"name":"QUERY_API_PDP_MODE","value":"shadow"}]`}, "queryApi.extraEnv must not set QUERY_API_PDP_MODE"},
		"extraEnv list":             {[]string{"--set-json", `queryApi.extraEnv=[{"name":"QUERY_API_DATABASE_ROLE","value":"devhealth_query"},{"name":"QUERY_API_AUTHZ_ENFORCE","value":"git"}]`}, "queryApi.extraEnv must not set QUERY_API_AUTHZ_ENFORCE"},
		"extraEnv headers":          {[]string{"--set-json", `queryApi.extraEnv=[{"name":"QUERY_API_DATABASE_ROLE","value":"devhealth_query"},{"name":"QUERY_API_HEADERS_AUTHZ_ENFORCE","value":"git"}]`}, "queryApi.extraEnv must not set QUERY_API_HEADERS_AUTHZ_ENFORCE"},
	} {
		for _, enabled := range []string{"true", "false"} {
			t.Run(name+"/queryApi.enabled="+enabled, func(t *testing.T) {
				_, out, err := renderAuth(t, phaseArgs(2, append(append([]string{}, tc.extra...), "--set", "queryApi.enabled="+enabled)...)...)
				if err == nil || !strings.Contains(out, tc.want) {
					t.Fatalf("rendered: err=%v out=%.300s; want a refusal containing %q", err, out, tc.want)
				}
			})
		}
	}
}

func TestEnforceListRefusals(t *testing.T) {
	for name, tc := range map[string]struct {
		args []string
		want string
	}{
		"unknown namespace": {enforceArgs("--set-json", `authService.enforce.goApi=["nope"]`), "enforce"},
		"star":              {enforceArgs("--set-json", `authService.enforce.goApi=["*"]`), "enforce"},
		"no proxy (phase 0)": {goAPIWithAuth("--set-json", `authService.enforce.goApi=["billing"]`),
			"authService.enforce.goApi needs go-api to reach auth-service"},
		"extraEnv": {enforceArgs("--set-json", `goApi.extraEnv=[{"name":"GO_API_AUTHZ_ENFORCE","value":"billing"}]`),
			"goApi.extraEnv must not set GO_API_AUTHZ_ENFORCE"},
	} {
		t.Run(name, func(t *testing.T) {
			_, out, err := renderAuth(t, tc.args...)
			if err == nil || !strings.Contains(out, tc.want) {
				t.Fatalf("rendered: err=%v out=%.300s; want a refusal containing %q", err, out, tc.want)
			}
		})
	}
}

// The chart's namespace enum is the catalog's namespace set: a namespace added to the catalog without the chart fails here.
func TestEnforceChartEnumIsTheCatalogNamespaces(t *testing.T) {
	raw, err := os.ReadFile("values.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var schema map[string]any
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	items := at(schema, "properties", "authService", "properties", "enforce", "properties", "goApi", "items", "enum")
	list, _ := items.([]any)
	var got []string
	for _, v := range list {
		got = append(got, v.(string))
	}
	catalog, err := os.ReadFile("../../../contracts/auth/v1/vocabulary/actions.v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Actions []struct {
			Action string `json:"action"`
		} `json:"actions"`
	}
	if err := json.Unmarshal(catalog, &doc); err != nil {
		t.Fatal(err)
	}
	var want []string
	for _, a := range doc.Actions {
		if ns, _, _ := strings.Cut(a.Action, "."); !slices.Contains(want, ns) {
			want = append(want, ns)
		}
	}
	slices.Sort(want)
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Fatalf("schema enum %v, want the catalog namespaces %v", got, want)
	}
}

func TestEnforceDefaultValueIsAnEmptyList(t *testing.T) {
	raw, err := os.ReadFile("values.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var values map[string]any
	if err := yaml.Unmarshal(raw, &values); err != nil {
		t.Fatal(err)
	}
	list, ok := at(values, "authService", "enforce", "goApi").([]any)
	if !ok || len(list) != 0 {
		t.Fatalf("authService.enforce.goApi default = %v, want an empty list", at(values, "authService", "enforce", "goApi"))
	}
}
