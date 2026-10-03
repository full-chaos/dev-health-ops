package devhealth_test

import (
	"os/exec"
	"strings"
	"testing"
)

// Every render refusal that covers a host (the Python allow-list rules, the go-api default rules, the forbidden services
// and annotations) is a security or correctness control on the values; the table below pins that each one fires and says
// what it should.

// hostEntry builds a one-host ingress.hosts value.
func hostEntry(rest string) string {
	return `[{"host":"h",` + rest + `}]`
}

func renderIngress(args ...string) (string, error) {
	full := append([]string{"template", "b", ".", "--set", "ingress.enabled=true"}, args...)
	out, err := exec.Command("helm", full...).CombinedOutput()
	return string(out), err
}

// TestEveryHostRefusalFires renders each refusal family once and requires the render to fail with the message this table
// expects.
func TestEveryHostRefusalFires(t *testing.T) {
	const goDefault = `"paths":[{"path":"/","pathType":"Prefix","service":"go-api"}]`
	goOn := []string{"--set", "goApi.enabled=true", "--set", "queryApi.enabled=true"}
	cases := []struct {
		name string
		host string
		args []string
		want string
	}{
		{"allow-list type", `"pythonAllowList":"yes",` + goDefault, goOn, "pythonAllowList must be true or a list"},
		{"go default without the opt-in", goDefault, goOn, "without pythonAllowList"},
		{"go-api path that is not literal", `"pythonAllowList":true,"paths":[{"path":"/x","pathType":"ImplementationSpecific","service":"go-api"}]`, goOn, "only literal Prefix/Exact"},
		{"go-api disabled", `"pythonAllowList":true,` + goDefault, []string{"--set", "goApi.enabled=false"}, "goApi.enabled is false"},
		{"regex annotation with a go-api route", `"pythonAllowList":true,` + goDefault,
			append([]string{"--set-string", `ingress.annotations.nginx\.ingress\.kubernetes\.io/use-regex=true`}, goOn...), "on an Ingress that routes to go-api"},
		{"go-api path over the internal routes", `"pythonAllowList":true,"paths":[{"path":"/api","pathType":"Prefix","service":"go-api"}]`, goOn, "covers /api/v1/internal"},
		{"service query-api-mcp", `"pythonAllowList":true,"paths":[{"path":"/","pathType":"Prefix","service":"go-api"},{"path":"/m","pathType":"Prefix","service":"query-api-mcp"}]`, goOn, "routes to query-api-mcp"},
		{"service go-api-internal", `"pythonAllowList":true,"paths":[{"path":"/","pathType":"Prefix","service":"go-api"},{"path":"/i","pathType":"Prefix","service":"go-api-internal"}]`, goOn, "routes to go-api-internal"},
		{"service billing-edge", `"paths":[{"path":"/","pathType":"Prefix","service":"billing-edge"}]`, goOn, `service "billing-edge" is gone`},
		{"allow-list entry without a backend (the Python api default is gone)", `"pythonAllowList":[{"path":"/metrics$","pathType":"ImplementationSpecific"}],` + goDefault,
			goOn, "has no service: the Python api (the old default backend) is gone (CHAOS-7520)"},
		{"service api", `"paths":[{"path":"/","pathType":"Prefix","service":"api"}]`, goOn, "the Python api is gone (CHAOS-7520)"},
		{"allow-list entry shape", `"pythonAllowList":[{"path":"graphql","pathType":"Prefix","service":"query-api"}],` + goDefault, goOn, "must be {path: literal /path"},
		{"allow-list dot segment", `"pythonAllowList":[{"path":"/\\.\\.$","pathType":"ImplementationSpecific"}],` + goDefault, goOn, "has a . or .. path segment"},
		{"allow-list entry for the whole host", `"pythonAllowList":[{"path":"/","pathType":"Prefix","service":"query-api"}],` + goDefault, goOn, "would route the whole host"},
		{"allow-list duplicate", `"pythonAllowList":[{"path":"/graphql","pathType":"Prefix","service":"query-api"},{"path":"/graphql","pathType":"Prefix","service":"query-api"}],` + goDefault, goOn, "duplicates another rule"},
		{"Exact entry on an anchored host", `"pythonAllowList":[{"path":"/docs$","pathType":"ImplementationSpecific","service":"query-api"},{"path":"/graphql","pathType":"Exact","service":"query-api"}],` + goDefault, goOn, "is Exact on a host that has an anchored entry"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out, err := renderIngress(append([]string{"--set-json", "ingress.hosts=" + hostEntry(c.host)}, c.args...)...)
			if err == nil || !strings.Contains(out, c.want) {
				t.Errorf("want a render failure containing %q, got err=%v\n%s", c.want, err, out)
			}
		})
	}
}
