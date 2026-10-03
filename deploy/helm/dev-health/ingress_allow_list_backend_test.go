package devhealth_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// An allow-list entry names its backend: `service: query-api` sends the path to the query-api Service (CHAOS-7520: the
// Python api, the old default backend, is gone, so an entry with no key, or any other value, is refused). The key
// exists so that a path can change its backend INSIDE the Ingress object that holds it. ingress-nginx's admission
// webhook denies an Ingress that claims a host and path another live Ingress holds, and helm patches the objects of a
// release one by one: a path that changes its backend by moving to another object is denied in one direction and is
// without a rule for a moment in the other. So these tests pin where each entry's rule lands, and what is refused.

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
func backendHosts(ownList string) string {
	const goDefault = `"paths":[{"path":"/","pathType":"Prefix","service":"go-api"}]`
	return `[{"host":"api.test","pythonAllowList":true,` + goDefault + `},` +
		`{"host":"web.test","paths":[{"path":"/","pathType":"Prefix","service":"web"}]},` +
		`{"host":"in-cluster.test","pythonAllowList":[` + ownList + `],` + goDefault + `}]`
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
	graphqlQuery = `{"path":"/graphql$","pathType":"ImplementationSpecific","service":"query-api"}`
	otherQuery   = `{"path":"/other$","pathType":"ImplementationSpecific","service":"query-api"}`
)

// TestAllowListEntryBackendIsQueryAPI: an entry with `service: query-api` lands in the anchored Ingress object on the
// query-api Service, the host's own default rule stays on go-api, the web host stays in the plain object, and no rule
// of the render names a `-api` (Python) Service.
func TestAllowListEntryBackendIsQueryAPI(t *testing.T) {
	rendered := backendRender(t, graphqlQuery, graphqlQuery)
	objects, _ := backendIngresses(t, rendered)
	if len(objects) != 2 || objects["b-dev-health"].Rules == nil || objects["b-dev-health-anchored"].Rules == nil {
		t.Fatalf("want exactly the plain and the anchored Ingress objects, got %v", objects)
	}
	got := map[backendRule]bool{}
	for _, rule := range objects["b-dev-health-anchored"].Rules {
		got[rule] = true
	}
	for _, rule := range []backendRule{
		{"api.test", "/graphql$", "ImplementationSpecific", "b-dev-health-query-api", 8090},
		{"in-cluster.test", "/graphql$", "ImplementationSpecific", "b-dev-health-query-api", 8090},
		{"api.test", "/", "Prefix", "b-dev-health-go-api", 8000},
		{"in-cluster.test", "/", "Prefix", "b-dev-health-go-api", 8000},
	} {
		if !got[rule] {
			t.Errorf("the anchored object must carry %v, got %v", rule, objects["b-dev-health-anchored"].Rules)
		}
	}
	if plain := objects["b-dev-health"].Rules; !reflect.DeepEqual(plain, []backendRule{{"web.test", "/", "Prefix", "b-dev-health-web", 3000}}) {
		t.Errorf("the plain object must carry only the web host, got %v", plain)
	}
	for _, line := range strings.Split(rendered, "\n") {
		if strings.TrimSpace(line) == "name: b-dev-health-api" {
			t.Fatalf("a rule names the removed Python api Service")
		}
	}
}

// TestAllowListEntryBackendPerList: each list decides for its own hosts: the shared list serves api.test, the host's
// own list serves in-cluster.test, and neither leaks into the other.
func TestAllowListEntryBackendPerList(t *testing.T) {
	objects, _ := backendIngresses(t, backendRender(t, graphqlQuery, otherQuery))
	got := map[string][]string{}
	for _, rule := range objects["b-dev-health-anchored"].Rules {
		if rule.Path != "/" {
			got[rule.Host] = append(got[rule.Host], rule.Path+" -> "+rule.Service)
		}
	}
	want := map[string][]string{
		"api.test":        {"/graphql$ -> b-dev-health-query-api"},
		"in-cluster.test": {"/other$ -> b-dev-health-query-api"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("want %v, got %v", want, got)
	}
}

// TestAllowListEntryBackendNeedsItsService: an entry that names query-api needs query-api enabled.
func TestAllowListEntryBackendNeedsItsService(t *testing.T) {
	host := `[{"host":"h","pythonAllowList":true,"paths":[{"path":"/","pathType":"Prefix","service":"go-api"}]}]`
	out, err := renderIngress("--set", "goApi.enabled=true", "--set", "queryApi.enabled=false", "--set-json", "ingress.hosts="+host, "--set-json", "ingress.pythonAllowList=["+graphqlQuery+"]")
	if err == nil || !strings.Contains(out, "routes to query-api but queryApi.enabled is false") {
		t.Errorf("an entry that names query-api must fail the render when queryApi is off: err=%v\n%s", err, out)
	}
	out, err = renderIngress("--set", "goApi.enabled=true", "--set", "queryApi.enabled=true", "--set-json", "ingress.hosts="+host, "--set-json", "ingress.pythonAllowList=["+graphqlQuery+"]")
	if err != nil {
		t.Fatalf("a list whose entries all name query-api must render: %v\n%s", err, out)
	}
}

// TestAllowListEntryServiceMustBeQueryAPI: the key is validated whenever it is present, and it is required. The Python
// api (`api`, and the old no-key default) is refused, so is every other backend (the Go api has its own rules and
// refusals; the internal listeners are never public), and a value that is not a string.
func TestAllowListEntryServiceMustBeQueryAPI(t *testing.T) {
	host := `[{"host":"h","pythonAllowList":true,"paths":[{"path":"/","pathType":"Prefix","service":"go-api"}]}]`
	for _, value := range []string{`"api"`, `"go-api"`, `"web"`, `"query-api-mcp"`, `"go-api-internal"`, `"Query-Api"`, `""`, `null`, `0`, `true`, `["query-api"]`} {
		entry := `{"path":"/graphql$","pathType":"ImplementationSpecific","service":` + value + `}`
		out, err := renderIngress("--set", "goApi.enabled=true", "--set", "queryApi.enabled=true", "--set-json", "ingress.pythonAllowList=["+entry+"]", "--set-json", "ingress.hosts="+host)
		if err == nil || !strings.Contains(out, "service must be query-api (the Python api is gone, CHAOS-7520)") {
			t.Errorf("service %s: want a render failure that names query-api, got err=%v\n%s", value, err, fmt.Sprint(out))
		}
	}
	entry := `{"path":"/graphql$","pathType":"ImplementationSpecific"}`
	out, err := renderIngress("--set", "goApi.enabled=true", "--set", "queryApi.enabled=true", "--set-json", "ingress.pythonAllowList=["+entry+"]", "--set-json", "ingress.hosts="+host)
	if err == nil || !strings.Contains(out, "has no service: the Python api (the old default backend) is gone (CHAOS-7520)") {
		t.Errorf("an entry with no service key must be refused, got err=%v\n%s", err, out)
	}
}

// TestHostPathToPythonAPIIsRefused: a host path that still says `service: api` has no backend any more.
func TestHostPathToPythonAPIIsRefused(t *testing.T) {
	host := `[{"host":"h","paths":[{"path":"/api","pathType":"Prefix","service":"api"}]}]`
	out, err := renderIngress("--set-json", "ingress.hosts="+host)
	if err == nil || !strings.Contains(out, `routes to service "api": the Python api is gone (CHAOS-7520)`) {
		t.Errorf("service: api on a host path must be refused, got err=%v\n%s", err, out)
	}
}

// TestRemovedPythonAPIValuesAreIgnored: values files written before CHAOS-7520 still set api.*, metricsApi.*, image.*
// and web.backendFromRelease. The chart ignores those keys (no schema or guard refuses them): the render succeeds and
// holds no Python api object.
func TestRemovedPythonAPIValuesAreIgnored(t *testing.T) {
	out, err := renderIngress("--set", "api.enabled=true", "--set", "metricsApi.enabled=true", "--set", "api.autoscaling.enabled=true",
		"--set", "image.repository=ghcr.io/full-chaos/dev-hops-api", "--set", "web.backendFromRelease=true")
	if err != nil {
		t.Fatalf("stale api.*/metricsApi.*/image.* values must be ignored: %v\n%s", err, out)
	}
	for _, name := range []string{"b-dev-health-api", "b-dev-health-metrics-api"} {
		if strings.Contains(out, "name: "+name+"\n") {
			t.Errorf("the render holds %s although the Python api templates are deleted", name)
		}
	}
}
