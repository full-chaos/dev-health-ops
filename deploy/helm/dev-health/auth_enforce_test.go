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
	if strings.Contains(out, "QUERY_API_AUTHZ_ENFORCE") {
		t.Fatal("the render names QUERY_API_AUTHZ_ENFORCE: query-api has no cell wiring yet")
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
