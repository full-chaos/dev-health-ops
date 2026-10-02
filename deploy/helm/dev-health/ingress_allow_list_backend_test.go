package devhealth_test

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// An allow-list entry may name its backend: `service: query-api` sends the path to the query-api Service, no key (or
// `service: api`) is the Python api. The key exists so that a path can change its backend INSIDE the Ingress object
// that holds it. ingress-nginx's admission webhook denies an Ingress that claims a host and path another live Ingress
// holds, and helm patches the objects of a release one by one: a path that changes its backend by moving to another
// object is denied in one direction and is without a rule for a moment in the other. So these tests pin that the key
// changes the backend of its own rule and NOTHING else: not the object a rule is in, not an annotation, not another
// rule, not another document.

type backendRule struct {
	Host, Path, PathType, Service string
	Port                          int
}

type backendIngress struct {
	Annotations map[string]string
	Rules       []backendRule
}

type backendDocument struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name        string            `yaml:"name"`
		Annotations map[string]string `yaml:"annotations"`
	} `yaml:"metadata"`
	Spec struct {
		Rules []struct {
			Host string `yaml:"host"`
			HTTP struct {
				Paths []struct {
					Path     string `yaml:"path"`
					PathType string `yaml:"pathType"`
					Backend  struct {
						Service struct {
							Name string `yaml:"name"`
							Port struct {
								Number int `yaml:"number"`
							} `yaml:"port"`
						} `yaml:"service"`
					} `yaml:"backend"`
				} `yaml:"paths"`
			} `yaml:"http"`
		} `yaml:"rules"`
	} `yaml:"spec"`
}

// backendIngresses returns the Ingress objects of a render by name, and the render without them.
func backendIngresses(t *testing.T, rendered string) (map[string]backendIngress, string) {
	t.Helper()
	objects := map[string]backendIngress{}
	var rest []string
	for _, document := range strings.Split(rendered, "\n---") {
		var parsed backendDocument
		if err := yaml.Unmarshal([]byte(document), &parsed); err != nil {
			t.Fatalf("a document of the render does not parse: %v", err)
		}
		if parsed.Kind != "Ingress" {
			rest = append(rest, document)
			continue
		}
		object := backendIngress{Annotations: parsed.Metadata.Annotations}
		for _, rule := range parsed.Spec.Rules {
			for _, path := range rule.HTTP.Paths {
				if path.Backend.Service.Name == "" || path.Backend.Service.Port.Number == 0 {
					t.Fatalf("Ingress %s: %s %s has no backend service or port", parsed.Metadata.Name, rule.Host, path.Path)
				}
				object.Rules = append(object.Rules, backendRule{rule.Host, path.Path, path.PathType, path.Backend.Service.Name, path.Backend.Service.Port.Number})
			}
		}
		objects[parsed.Metadata.Name] = object
	}
	return objects, strings.Join(rest, "\n---")
}

// The shape prod uses: an api host on the shared list, a web host, and a host with its own list.
func backendHosts(ownGraphql string) string {
	const goDefault = `"paths":[{"path":"/","pathType":"Prefix","service":"go-api"}]`
	return `[{"host":"api.test","pythonAllowList":true,` + goDefault + `},` +
		`{"host":"web.test","paths":[{"path":"/","pathType":"Prefix","service":"web"}]},` +
		`{"host":"in-cluster.test","pythonAllowList":[` + ownGraphql + `,{"path":"/metrics$","pathType":"ImplementationSpecific"}],` + goDefault + `}]`
}

func backendRender(t *testing.T, shared, own string, extra ...string) string {
	t.Helper()
	args := append([]string{"--set", "goApi.enabled=true", "--set", "queryApi.enabled=true",
		"--set-string", `ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-body-size=50m`,
		"--set-json", "ingress.pythonAllowList=[" + shared + "]", "--set-json", "ingress.hosts=" + backendHosts(own)}, extra...)
	out, err := renderIngress(args...)
	if err != nil {
		t.Fatalf("render: %v\n%s", err, out)
	}
	return out
}

const (
	graphqlPython = `{"path":"/graphql$","pathType":"ImplementationSpecific"}`
	graphqlQuery  = `{"path":"/graphql$","pathType":"ImplementationSpecific","service":"query-api"}`
)

// TestAllowListEntryBackendChangesOnlyItsOwnRule: with `service: query-api` on the /graphql$ entry of both lists, the
// render differs from the render without the key in the backend of exactly those two rules.
func TestAllowListEntryBackendChangesOnlyItsOwnRule(t *testing.T) {
	before, restBefore := backendIngresses(t, backendRender(t, graphqlPython, graphqlPython))
	after, restAfter := backendIngresses(t, backendRender(t, graphqlQuery, graphqlQuery))

	if restBefore != restAfter {
		t.Errorf("the key must change no document other than the Ingress objects")
	}
	names := func(objects map[string]backendIngress) []string {
		var out []string
		for name := range objects {
			out = append(out, name)
		}
		sort.Strings(out)
		return out
	}
	if want := []string{"b-dev-health", "b-dev-health-anchored"}; !reflect.DeepEqual(names(before), want) || !reflect.DeepEqual(names(after), want) {
		t.Fatalf("the Ingress objects must be the same two before and after: before %v, after %v", names(before), names(after))
	}
	type key struct{ object, host, path string }
	changed := map[key][2]backendRule{}
	for name, object := range before {
		other := after[name]
		if !reflect.DeepEqual(object.Annotations, other.Annotations) {
			t.Errorf("%s: the annotations changed: %v -> %v", name, object.Annotations, other.Annotations)
		}
		if len(object.Rules) != len(other.Rules) {
			t.Fatalf("%s: %d rules before, %d after", name, len(object.Rules), len(other.Rules))
		}
		for index, rule := range object.Rules {
			now := other.Rules[index]
			if rule.Host != now.Host || rule.Path != now.Path || rule.PathType != now.PathType {
				t.Errorf("%s rule %d: host, path or path type changed: %v -> %v", name, index, rule, now)
			}
			if rule != now {
				changed[key{name, rule.Host, rule.Path}] = [2]backendRule{rule, now}
			}
		}
	}
	python := func(host string) backendRule {
		return backendRule{host, "/graphql$", "ImplementationSpecific", "b-dev-health-api", 8000}
	}
	query := func(host string) backendRule {
		return backendRule{host, "/graphql$", "ImplementationSpecific", "b-dev-health-query-api", 8090}
	}
	want := map[key][2]backendRule{
		{"b-dev-health-anchored", "api.test", "/graphql$"}:        {python("api.test"), query("api.test")},
		{"b-dev-health-anchored", "in-cluster.test", "/graphql$"}: {python("in-cluster.test"), query("in-cluster.test")},
	}
	if !reflect.DeepEqual(changed, want) {
		t.Errorf("exactly the two /graphql$ rules of the anchored object must change their backend, from the Python api to query-api; changed:\n%v", changed)
	}
	// The rule beside it on the host's own list keeps the Python api, and the default rules keep the Go api.
	kept := map[backendRule]bool{}
	for _, rule := range after["b-dev-health-anchored"].Rules {
		kept[rule] = true
	}
	for _, rule := range []backendRule{
		{"in-cluster.test", "/metrics$", "ImplementationSpecific", "b-dev-health-api", 8000},
		{"api.test", "/", "Prefix", "b-dev-health-go-api", 8000},
		{"in-cluster.test", "/", "Prefix", "b-dev-health-go-api", 8000},
	} {
		if !kept[rule] {
			t.Errorf("the anchored object must still carry %v, got %v", rule, after["b-dev-health-anchored"].Rules)
		}
	}
}

// TestAllowListEntryBackendPerList: each list decides for its own hosts; one list with the key and the other without
// renders the two backends side by side (the chart checks one host at a time).
func TestAllowListEntryBackendPerList(t *testing.T) {
	for _, c := range []struct{ name, shared, own, apiHost, inCluster string }{
		{"shared list only", graphqlQuery, graphqlPython, "b-dev-health-query-api", "b-dev-health-api"},
		{"the host's own list only", graphqlPython, graphqlQuery, "b-dev-health-api", "b-dev-health-query-api"},
	} {
		t.Run(c.name, func(t *testing.T) {
			objects, _ := backendIngresses(t, backendRender(t, c.shared, c.own))
			got := map[string]string{}
			for _, rule := range objects["b-dev-health-anchored"].Rules {
				if rule.Path == "/graphql$" {
					got[rule.Host] = rule.Service
				}
			}
			if want := map[string]string{"api.test": c.apiHost, "in-cluster.test": c.inCluster}; !reflect.DeepEqual(got, want) {
				t.Errorf("want %v, got %v", want, got)
			}
		})
	}
}

// TestAllowListEntryServiceApiIsTheDefault: `service: api` renders exactly as an entry with no key.
func TestAllowListEntryServiceApiIsTheDefault(t *testing.T) {
	const explicit = `{"path":"/graphql$","pathType":"ImplementationSpecific","service":"api"}`
	if backendRender(t, explicit, explicit) != backendRender(t, graphqlPython, graphqlPython) {
		t.Errorf("service: api must render exactly as an entry with no service key")
	}
}

// TestAllowListEntryBackendNeedsItsService: an entry that names query-api needs query-api, and only an entry that
// routes to the Python api needs the Python api.
func TestAllowListEntryBackendNeedsItsService(t *testing.T) {
	render := func(key string, args ...string) (string, error) {
		host := `[{"host":"h",` + key + `"pythonAllowList":true,"paths":[{"path":"/","pathType":"Prefix","service":"go-api"}]}]`
		return renderIngress(append([]string{"--set", "goApi.enabled=true", "--set-json", "ingress.hosts=" + host}, args...)...)
	}
	{
		const key, label = "", "emitted host"
		out, err := render(key, "--set", "queryApi.enabled=false", "--set-json", "ingress.pythonAllowList=["+graphqlQuery+"]")
		if err == nil || !strings.Contains(out, "routes to query-api but queryApi.enabled is false") {
			t.Errorf("%s: an entry that names query-api must fail the render when queryApi is off: err=%v\n%s", label, err, out)
		}
		out, err = render(key, "--set", "queryApi.enabled=true", "--set", "api.enabled=false", "--set-json", "ingress.pythonAllowList=["+graphqlQuery+","+`{"path":"/metrics$","pathType":"ImplementationSpecific"}`+"]")
		if err == nil || !strings.Contains(out, "routes to the Python api but api.enabled is false") {
			t.Errorf("%s: an entry that routes to the Python api must still fail the render when the Python api is off: err=%v\n%s", label, err, out)
		}
	}
	out, err := render("", "--set", "queryApi.enabled=true", "--set", "api.enabled=false", "--set-json", "ingress.pythonAllowList=["+graphqlQuery+"]")
	if err != nil {
		t.Fatalf("a list whose entries all name query-api needs no Python api: %v\n%s", err, out)
	}
	objects, _ := backendIngresses(t, out)
	var got []backendRule
	for _, rule := range objects["b-dev-health-anchored"].Rules {
		if rule.Path == "/graphql$" {
			got = append(got, rule)
		}
	}
	if want := []backendRule{{"h", "/graphql$", "ImplementationSpecific", "b-dev-health-query-api", 8090}}; !reflect.DeepEqual(got, want) {
		t.Errorf("want %v, got %v", want, got)
	}
}

// TestAllowListEntryServiceIsOneOfTwo: the key is validated whenever it is present. Any other backend (the Go api
// has its own rules and refusals; the internal listeners are never public), and a value that is not a string, fail.
func TestAllowListEntryServiceIsOneOfTwo(t *testing.T) {
	for _, value := range []string{`"go-api"`, `"web"`, `"query-api-mcp"`, `"go-api-internal"`, `"Query-Api"`, `""`, `null`, `0`, `true`, `["query-api"]`} {
		{
			const key, label = "", "emitted host"
			entry := `{"path":"/graphql$","pathType":"ImplementationSpecific","service":` + value + `}`
			host := `[{"host":"h",` + key + `"pythonAllowList":true,"paths":[{"path":"/","pathType":"Prefix","service":"go-api"}]}]`
			out, err := renderIngress("--set", "goApi.enabled=true", "--set", "queryApi.enabled=true", "--set-json", "ingress.pythonAllowList=["+entry+"]", "--set-json", "ingress.hosts="+host)
			if err == nil || !strings.Contains(out, "service must be api (the Python api, the default) or query-api") {
				t.Errorf("%s, service %s: want a render failure that names the two allowed values, got err=%v\n%s", label, value, err, fmt.Sprint(out))
			}
		}
	}
}
