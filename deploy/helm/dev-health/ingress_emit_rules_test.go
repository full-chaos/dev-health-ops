package devhealth_test

import (
	"fmt"
	"os/exec"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// A host with `emitRules: false` is kept out of this chart's Ingress objects: the umbrella chart renders that host's
// rules itself, in one Ingress object per host, because ingress-nginx's admission webhook denies an Ingress that claims
// a host and path another live Ingress holds. The switch must take the host out of the OUTPUT only. Every render
// refusal that covers a host (the Python allow-list rules, the go-api default rules, the forbidden services and
// annotations) is a security or correctness control on the values, and a host that is not emitted here is still
// routed by those values elsewhere.

// emitRulesHost builds one ingress.hosts entry; key is "" (the host is emitted) or `"emitRules":false,`.
func emitRulesHost(key, rest string) string {
	return `[{"host":"h",` + key + rest + `}]`
}

func renderIngress(args ...string) (string, error) {
	full := append([]string{"template", "b", ".", "--set", "ingress.enabled=true"}, args...)
	out, err := exec.Command("helm", full...).CombinedOutput()
	return string(out), err
}

// TestEveryHostRefusalFiresWithEmitRulesOff runs each refusal family twice with the same values: once on an emitted
// host (the control: the refusal exists and says what this table expects) and once with `emitRules: false`.
func TestEveryHostRefusalFiresWithEmitRulesOff(t *testing.T) {
	const goDefault = `"paths":[{"path":"/","pathType":"Prefix","service":"go-api"}]`
	goOn := []string{"--set", "goApi.enabled=true"}
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
		{"allow-list to a disabled Python api", `"pythonAllowList":[{"path":"/metrics$","pathType":"ImplementationSpecific"}],` + goDefault,
			append([]string{"--set", "api.enabled=false"}, goOn...), "api.enabled is false"},
		{"allow-list entry shape", `"pythonAllowList":[{"path":"graphql","pathType":"Prefix"}],` + goDefault, goOn, "must be {path: literal /path"},
		{"allow-list dot segment", `"pythonAllowList":[{"path":"/\\.\\.$","pathType":"ImplementationSpecific"}],` + goDefault, goOn, "has a . or .. path segment"},
		{"allow-list entry for the whole host", `"pythonAllowList":[{"path":"/","pathType":"Prefix"}],` + goDefault, goOn, "would route the whole host"},
		{"allow-list duplicate", `"pythonAllowList":[{"path":"/graphql","pathType":"Prefix"},{"path":"/graphql","pathType":"Prefix"}],` + goDefault, goOn, "duplicates another rule"},
		{"Exact entry on an anchored host", `"pythonAllowList":[{"path":"/docs$","pathType":"ImplementationSpecific"},{"path":"/graphql","pathType":"Exact"}],` + goDefault, goOn, "is Exact on a host that has an anchored entry"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for label, key := range map[string]string{"emitted host (control)": "", "emitRules: false": `"emitRules":false,`} {
				out, err := renderIngress(append([]string{"--set-json", "ingress.hosts=" + emitRulesHost(key, c.host)}, c.args...)...)
				if err == nil || !strings.Contains(out, c.want) {
					t.Errorf("%s: want a render failure containing %q, got err=%v\n%s", label, c.want, err, out)
				}
			}
		})
	}
}

type emitRulesIngress struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec struct {
		Rules []struct {
			Host string `yaml:"host"`
			HTTP struct {
				Paths []struct {
					Path string `yaml:"path"`
				} `yaml:"paths"`
			} `yaml:"http"`
		} `yaml:"rules"`
	} `yaml:"spec"`
}

// ingressHosts returns, per Ingress object of the render, the hosts it carries.
func ingressHosts(t *testing.T, rendered string) map[string][]string {
	t.Helper()
	objects := map[string][]string{}
	for _, document := range strings.Split(rendered, "\n---") {
		var object emitRulesIngress
		if err := yaml.Unmarshal([]byte(document), &object); err != nil || object.Kind != "Ingress" {
			continue
		}
		hosts := []string{}
		for _, rule := range object.Spec.Rules {
			if len(rule.HTTP.Paths) == 0 {
				t.Fatalf("Ingress %s has a rule for %s with no path", object.Metadata.Name, rule.Host)
			}
			hosts = append(hosts, rule.Host)
		}
		objects[object.Metadata.Name] = hosts
	}
	return objects
}

// TestHostWithEmitRulesOffIsNotEmitted pins the output side: the host is in no Ingress object, the other hosts render
// as they do without it, an object that would carry only such hosts is not rendered, and the key is a strict boolean.
func TestHostWithEmitRulesOffIsNotEmitted(t *testing.T) {
	const allow = `[{"path":"/graphql$","pathType":"ImplementationSpecific"}]`
	apiHost := func(name, key string) string {
		return `{"host":"` + name + `",` + key + `"pythonAllowList":true,"paths":[{"path":"/","pathType":"Prefix","service":"go-api"}]}`
	}
	const web = `{"host":"web.test","paths":[{"path":"/","pathType":"Prefix","service":"web"}]}`
	render := func(hosts string) string {
		t.Helper()
		out, err := renderIngress("--set", "goApi.enabled=true", "--set-json", "ingress.pythonAllowList="+allow, "--set-json", "ingress.hosts=["+hosts+"]")
		if err != nil {
			t.Fatalf("render: %v\n%s", err, out)
		}
		return out
	}
	const off = `"emitRules":false,`

	all := ingressHosts(t, render(apiHost("a.test", "")+","+apiHost("b.test", "")+","+web))
	if fmt.Sprint(all) != fmt.Sprint(map[string][]string{"b-dev-health": {"web.test"}, "b-dev-health-anchored": {"a.test", "b.test"}}) {
		t.Fatalf("control: the three hosts must render in the plain and the anchored object, got %v", all)
	}

	oneOff := render(apiHost("a.test", off) + "," + apiHost("b.test", "") + "," + web)
	if got := ingressHosts(t, oneOff); fmt.Sprint(got) != fmt.Sprint(map[string][]string{"b-dev-health": {"web.test"}, "b-dev-health-anchored": {"b.test"}}) {
		t.Errorf("a.test has emitRules: false and must be in no Ingress, with the other hosts unchanged, got %v", got)
	}
	if strings.Contains(oneOff, "a.test") {
		t.Errorf("a.test must not appear anywhere in the render when its rules are not emitted:\n%s", oneOff)
	}
	if without := render(apiHost("b.test", "") + "," + web); without != oneOff {
		t.Errorf("a host with emitRules: false must leave the render exactly as if the host were not listed")
	}

	bothOff := ingressHosts(t, render(apiHost("a.test", off)+","+apiHost("b.test", off)+","+web))
	if fmt.Sprint(bothOff) != fmt.Sprint(map[string][]string{"b-dev-health": {"web.test"}}) {
		t.Errorf("with every anchored host off, the anchored object must not be rendered, got %v", bothOff)
	}
	if none := ingressHosts(t, render(apiHost("a.test", off))); len(none) != 0 {
		t.Errorf("with the only host off, this chart must render no Ingress, got %v", none)
	}

	if explicit, absent := render(apiHost("a.test", `"emitRules":true,`)+","+web), render(apiHost("a.test", "")+","+web); explicit != absent {
		t.Errorf("emitRules: true must render exactly as an absent key")
	}
	for _, value := range []string{`"no"`, `0`, `null`, `"false"`} {
		out, err := renderIngress("--set", "goApi.enabled=true", "--set-json", "ingress.hosts=["+apiHost("a.test", `"emitRules":`+value+`,`)+"]")
		if err == nil || !strings.Contains(out, "emitRules must be true or false") {
			t.Errorf("emitRules: %s must fail the render as not a boolean: err=%v\n%s", value, err, out)
		}
	}
}
