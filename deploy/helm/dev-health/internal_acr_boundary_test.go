package devhealth_test

import (
	"os/exec"
	"regexp"
	"strings"
	"testing"
)

// TestInternalACRRoutesStayOffThePublicIngress pins what still holds after CHAOS-7255. The Go api serves
// GET /api/v1/internal/acr/health and GET /api/v1/internal/acr/entitlements/{org_id} with no credential check, by
// design (the network boundary is the control), but ONLY on its internal listener (port 8091): the public listener
// serves no /api/v1/internal/* route (ops #3429), which internal/apiservice TestPublicListenerNeverServesInternalRoutes
// pins at the HTTP level. So the chart's job is that no Ingress may reach the go-api pods' INTERNAL listener: no
// Ingress backend may name the go-api-internal Service or a Service port other than the public one, every Service that
// selects the go-api pods stays ClusterIP without nodePort/externalIPs, and the pods expose no hostPort. (The former
// allow-list cover of /api/v1/internal, which existed because the public listener used to serve those routes, is retired.)
func TestInternalACRRoutesStayOffThePublicIngress(t *testing.T) {
	output, err := exec.Command("helm", "template", "b", ".",
		"--set", "goApi.enabled=true", "--set", "queryApi.enabled=true",
		"--set", "ingress.enabled=true",
		// A values file that routes a path to every Service the Ingress
		// template knows, and asks it for the Go api on the internal acr
		// prefix and on the whole api.
		"--set-json", `ingress.hosts=[{"host":"h","pythonAllowList":true,"paths":[`+
			`{"path":"/","pathType":"Prefix","service":"go-api"}]}]`,
	).CombinedOutput()
	if err != nil {
		t.Fatalf("render failed: %v\n%s", err, output)
	}
	documents := strings.Split(string(output), "\n---")
	// The go-api pods' labels, and every Service whose selector matches
	// them: a Service reaches the Go api by its selector, whatever its name.
	var podLabels map[string]string
	for _, document := range documents {
		if documentKind(document) == "Deployment" && strings.Contains(document, "# Source: dev-health/templates/go-api-deployment.yaml\n") {
			if podLabels != nil {
				t.Fatalf("the render has more than one go-api Deployment")
			}
			podLabels = indentedMap(document, "\n    metadata:\n      labels:\n", "        ")
			for _, exposed := range []string{"hostPort:", "hostNetwork: true"} {
				if strings.Contains(document, exposed) {
					t.Errorf("the go-api Deployment sets %s:\n%s", exposed, document)
				}
			}
		}
	}
	if len(podLabels) == 0 {
		t.Fatalf("the render has no go-api Deployment with pod labels")
	}
	var reachGoAPI []string
	for _, document := range documents {
		if documentKind(document) != "Service" {
			continue
		}
		selector := indentedMap(document, "\n  selector:\n", "    ")
		if !selects(selector, podLabels) {
			continue
		}
		name := indentedMap(document, "\nmetadata:\n", "  ")["name"]
		reachGoAPI = append(reachGoAPI, name)
		if !strings.Contains(document, "\n  type: ClusterIP\n") {
			t.Errorf("Service %s selects the go-api pods and is not ClusterIP:\n%s", name, document)
		}
		for _, exposed := range []string{"nodePort:", "externalIPs:", "loadBalancerIP:"} {
			if strings.Contains(document, exposed) {
				t.Errorf("Service %s selects the go-api pods and sets %s:\n%s", name, exposed, document)
			}
		}
	}
	if len(reachGoAPI) == 0 {
		t.Fatalf("no Service selects the go-api pods; the render cannot show what routes to them")
	}
	ingresses := 0
	for _, document := range documents {
		kind := documentKind(document)
		// Every routing kind: Ingress, Gateway API routes, Traefik's
		// IngressRoute, OpenShift's Route.
		if !strings.Contains(kind, "Ingress") && !strings.Contains(kind, "Route") && !strings.Contains(kind, "Gateway") {
			continue
		}
		if kind == "Ingress" {
			ingresses++
		}
		if kind != "Ingress" {
			// Any other routing kind must not name a Service that reaches the Go api.
			for _, name := range reachGoAPI {
				reference := regexp.MustCompile(`(?m)^\s*(- )?(name|serviceName):\s*["']?` + regexp.QuoteMeta(name) + `["']?\s*$`)
				if reference.MatchString(document) {
					t.Errorf("a %s routes to Service %s, which reaches the Go api:\n%s", kind, name, document)
				}
			}
			continue
		}
		// No backend of any Ingress may be the internal-listener Service or a non-public port of a Service that reaches the go-api pods.
		for _, name := range reachGoAPI {
			for _, m := range regexp.MustCompile(`(?m)^\s*name: `+regexp.QuoteMeta(name)+`\n\s*port:\n\s*number: (\d+)`).FindAllStringSubmatch(document, -1) {
				if m[1] == "8091" || strings.HasSuffix(name, "go-api-internal") {
					t.Errorf("an Ingress routes to the go-api internal listener (%s:%s):\n%s", name, m[1], document)
				}
			}
		}
	}
	// A render without an Ingress would pass the routing check above.
	if ingresses != 1 {
		t.Fatalf("render has %d Ingress documents, want 1", ingresses)
	}
}

// indentedMap reads the "key: value" lines at exactly indent that follow
// header in document.
func indentedMap(document, header, indent string) map[string]string {
	out := map[string]string{}
	at := strings.Index(document, header)
	if at < 0 {
		return out
	}
	for _, line := range strings.Split(document[at+len(header):], "\n") {
		rest, ok := strings.CutPrefix(line, indent)
		if !ok || strings.HasPrefix(rest, " ") {
			break
		}
		key, value, ok := strings.Cut(rest, ": ")
		if !ok {
			break
		}
		out[key] = strings.Trim(value, `"`)
	}
	return out
}

// selects reports whether a Service selector matches pods with labels. An
// empty selector selects no pods.
func selects(selector, labels map[string]string) bool {
	if len(selector) == 0 {
		return false
	}
	for key, value := range selector {
		if labels[key] != value {
			return false
		}
	}
	return true
}

func documentKind(document string) string {
	for _, line := range strings.Split(document, "\n") {
		if kind, ok := strings.CutPrefix(line, "kind: "); ok {
			return strings.TrimSpace(kind)
		}
	}
	return ""
}

// winningBackend returns the backend Service name ingress-nginx picks for url
// among the Ingress document's paths: an Exact match, else the longest Prefix
// (whole-segment) match.
func winningBackend(document, url string) string {
	rule := regexp.MustCompile(`(?m)^\s*- path: (\S+)\n\s+pathType: (\w+)\n\s+backend:\n\s+service:\n\s+name: (\S+)`)
	best, bestLen := "", -1
	for _, m := range rule.FindAllStringSubmatch(document, -1) {
		path, kind, name := m[1], m[2], m[3]
		switch {
		case kind == "Exact" && url == path:
			return name
		case kind == "Prefix" && (path == "/" || url == path || strings.HasPrefix(url, path+"/")):
			if len(path) > bestLen {
				best, bestLen = name, len(path)
			}
		}
	}
	return best
}

// TestGoCatchAllRenderGuards pins the CHAOS-7047 render guards: a "/" to go-api
// needs the allow-list opt-in, and
// no go-api path may cover /api/v1/internal.
func TestGoCatchAllRenderGuards(t *testing.T) {
	render := func(hosts, allow string) (string, error) {
		args := []string{"template", "b", ".", "--set", "goApi.enabled=true", "--set", "queryApi.enabled=true", "--set", "ingress.enabled=true", "--set-json", "ingress.hosts=" + hosts}
		if allow != "" {
			args = append(args, "--set-json", "ingress.pythonAllowList="+allow)
		}
		out, err := exec.Command("helm", args...).CombinedOutput()
		return string(out), err
	}
	// rewrite/regex/snippet-class annotations with a go-api route.
	for _, ann := range []string{"use-regex", "rewrite-target", "app-root", "configuration-snippet", "server-snippet", "permanent-redirect", "temporal-redirect"} {
		out, err := exec.Command("helm", "template", "b", ".", "--set", "goApi.enabled=true", "--set", "queryApi.enabled=true", "--set", "ingress.enabled=true",
			"--set-string", `ingress.annotations.nginx\.ingress\.kubernetes\.io/`+ann+`=/$1`,
			"--set-json", `ingress.hosts=[{"host":"h","pythonAllowList":true,"paths":[{"path":"/","pathType":"Prefix","service":"go-api"}]}]`).CombinedOutput()
		if err == nil || !strings.Contains(string(out), "ingress.annotations "+ann+" on an Ingress that routes to go-api") {
			t.Errorf("%s with a go-api route must fail the render: err=%v\n%s", ann, err, out)
		}
	}
	// A referenced backend Service must exist.
	for name, args := range map[string][]string{
		"goApi disabled":    {"--set", "goApi.enabled=false"},
		"queryApi disabled": {"--set", "goApi.enabled=true", "--set", "queryApi.enabled=false", "--set-json", `ingress.pythonAllowList=[{"path":"/metrics$","pathType":"ImplementationSpecific","service":"query-api"}]`},
	} {
		full := append([]string{"template", "b", ".", "--set", "ingress.enabled=true", "--set-json",
			`ingress.hosts=[{"host":"h","pythonAllowList":true,"paths":[{"path":"/","pathType":"Prefix","service":"go-api"}]}]`}, args...)
		out, err := exec.Command("helm", full...).CombinedOutput()
		if err == nil || !strings.Contains(string(out), "enabled is false") {
			t.Errorf("%s: a route to a disabled Service must fail the render: err=%v\n%s", name, err, out)
		}
	}
	catchAll := `[{"host":"h","pythonAllowList":true,"paths":[{"path":"/","pathType":"Prefix","service":"go-api"}]}]`
	if out, err := render(catchAll, ""); err != nil {
		t.Fatalf("default allow-list must render: %v\n%s", err, out)
	}
	for name, c := range map[string]struct{ hosts, allow, want string }{
		"no opt-in":               {`[{"host":"h","paths":[{"path":"/","pathType":"Prefix","service":"go-api"}]}]`, "", "without pythonAllowList"},
		"regex path":              {`[{"host":"h","pythonAllowList":true,"paths":[{"path":"^/api/v1/(internal|internal/acr/.*)","pathType":"ImplementationSpecific","service":"go-api"}]}]`, "", "only literal Prefix/Exact"},
		"regex chars in prefix":   {`[{"host":"h","pythonAllowList":true,"paths":[{"path":"/api/v1/(internal)","pathType":"Prefix","service":"go-api"}]}]`, "", "only literal Prefix/Exact"},
		"implementation specific": {`[{"host":"h","pythonAllowList":true,"paths":[{"path":"/x","pathType":"ImplementationSpecific","service":"go-api"}]}]`, "", "only literal Prefix/Exact"},
		"go-api /api":             {`[{"host":"h","pythonAllowList":true,"paths":[{"path":"/api","pathType":"Prefix","service":"go-api"}]}]`, "", "covers /api/v1/internal"},
		"go-api internal":         {`[{"host":"h","pythonAllowList":true,"paths":[{"path":"/api/v1/internal/acr","pathType":"Prefix","service":"go-api"}]}]`, "", "covers /api/v1/internal"},
	} {
		out, err := render(c.hosts, c.allow)
		if err == nil || !strings.Contains(out, c.want) {
			t.Errorf("%s: want render failure containing %q, got err=%v\n%s", name, c.want, err, out)
		}
	}
}

// TestPerHostPythonAllowList pins CHAOS-7221: a host's pythonAllowList may be a list (that host's
// own allow-list, e.g. the in-cluster host keeps /metrics on Python) while `true` keeps using the
// shared ingress.pythonAllowList; every guard runs on the EFFECTIVE list.
func TestPerHostPythonAllowList(t *testing.T) {
	render := func(hosts string) (string, error) {
		out, err := exec.Command("helm", "template", "b", ".", "--set", "goApi.enabled=true", "--set", "queryApi.enabled=true", "--set", "ingress.enabled=true",
			"--set-json", "ingress.hosts="+hosts).CombinedOutput()
		return string(out), err
	}
	hostRules := func(out, host string) string {
		at := strings.Index(out, `- host: "`+host+`"`)
		if at < 0 {
			t.Fatalf("host %s not rendered:\n%s", host, out)
		}
		rest := out[at+1:]
		if next := strings.Index(rest, "\n    - host:"); next >= 0 {
			return out[at : at+1+next]
		}
		return out[at:]
	}
	own := `[{"path":"/graphql","pathType":"Prefix","service":"query-api"},{"path":"/api/v1/internal","pathType":"Prefix","service":"query-api"},{"path":"/metrics","pathType":"Exact","service":"query-api"}]`
	both := `[{"host":"shared.test","pythonAllowList":true,"paths":[{"path":"/","pathType":"Prefix","service":"go-api"}]},` +
		`{"host":"own.test","pythonAllowList":` + own + `,"paths":[{"path":"/","pathType":"Prefix","service":"go-api"}]}]`
	out, err := render(both)
	if err != nil {
		t.Fatalf("per-host list must render: %v\n%s", err, out)
	}
	shared, ownRules := hostRules(out, "shared.test"), hostRules(out, "own.test")
	if strings.Contains(shared, "path: /metrics") {
		t.Errorf("a host with pythonAllowList: true must use the shared list (no /metrics):\n%s", shared)
	}
	if !strings.Contains(ownRules, "path: /metrics") || !strings.Contains(ownRules, "path: /graphql") || !strings.Contains(ownRules, "path: /api/v1/internal") {
		t.Errorf("a host with its own list must render exactly that list:\n%s", ownRules)
	}
	// Exclusive, not merged: the own-list host renders exactly its 3 paths + "/", the shared-list host exactly the
	// shared defaults + "/" (r1 P3: presence checks alone would pass a regression that appended the shared list).
	if got := strings.Count(ownRules, "- path: "); got != 4 {
		t.Errorf("own-list host must render exactly its 3 allow-list paths plus \"/\", got %d paths:\n%s", got, ownRules)
	}
	// The shared default is EMPTY (query-api answers /graphql): the shared-list host renders "/" and nothing else.
	if got := strings.Count(shared, "- path: "); got != 1 || strings.Contains(shared, "dev-health-query-api\n") {
		t.Errorf("shared-list host must render exactly \"/\" and no query-api backend, got %d paths:\n%s", got, shared)
	}
	// Every allow-listed path, on both hosts, backs onto the query-api Service and "/" onto the Go api (r2 P3:
	// the security-relevant /api/v1/internal backend was not asserted).
	backendOf := func(rules, path string) string {
		parts := strings.SplitN(rules, "- path: "+path+"\n", 2)
		if len(parts) < 2 {
			t.Fatalf("path %s not rendered:\n%s", path, rules)
		}
		return strings.SplitN(parts[1], "- path:", 2)[0]
	}
	for _, path := range []string{"/graphql", "/api/v1/internal", "/metrics"} {
		if !strings.Contains(backendOf(ownRules, path), "name: b-dev-health-query-api\n") {
			t.Errorf("own-list host: %s must back onto the query-api Service:\n%s", path, ownRules)
		}
	}
	// A values file that lists a shared path still gets it on the Python api for a `true` host.
	listed, err := exec.Command("helm", "template", "b", ".", "--set", "goApi.enabled=true", "--set", "queryApi.enabled=true", "--set", "ingress.enabled=true",
		"--set-json", `ingress.pythonAllowList=[{"path":"/graphql$","pathType":"ImplementationSpecific","service":"query-api"}]`,
		"--set-json", `ingress.hosts=[{"host":"shared.test","pythonAllowList":true,"paths":[{"path":"/","pathType":"Prefix","service":"go-api"}]}]`).CombinedOutput()
	if err != nil {
		t.Fatalf("a listed shared path must render: %v\n%s", err, listed)
	}
	if !strings.Contains(backendOf(hostRules(string(listed), "shared.test"), "/graphql$"), "name: b-dev-health-query-api\n") {
		t.Errorf("shared-list host: a listed /graphql$ must back onto the query-api Service:\n%s", listed)
	}
	for _, rules := range []string{ownRules, shared} {
		if !strings.Contains(backendOf(rules, "/"), "name: b-dev-health-go-api\n") {
			t.Errorf(`"/" must back onto the Go api Service:\n%s`, rules)
		}
	}
	for name, c := range map[string]struct{ list, want string }{
		"empty list":              {`[]`, "without pythonAllowList"},
		"regex entry":             {`[{"path":"/api/v1/(internal)","pathType":"Prefix","service":"query-api"},{"path":"/api/v1/internal","pathType":"Prefix","service":"query-api"}]`, "literal /path"},
		"implementation specific": {`[{"path":"/x","pathType":"ImplementationSpecific","service":"query-api"},{"path":"/api/v1/internal","pathType":"Prefix","service":"query-api"}]`, "Prefix|Exact"},
		"entry not a map":         {`["/graphql"]`, "literal /path"},
		"string value":            {`"yes"`, "must be true or a list"},
		"map value":               {`{"path":"/x"}`, "must be true or a list"},
		"root entry":              {`[{"path":"/","pathType":"Prefix","service":"query-api"},{"path":"/api/v1/internal","pathType":"Prefix","service":"query-api"}]`, "whole host to the Python api"},
		"double slash root":       {`[{"path":"//","pathType":"Prefix","service":"query-api"},{"path":"/api/v1/internal","pathType":"Prefix","service":"query-api"}]`, "literal /path"},
		"double slash inside":     {`[{"path":"/api//v1","pathType":"Prefix","service":"query-api"},{"path":"/api/v1/internal","pathType":"Prefix","service":"query-api"}]`, "literal /path"},
		"dot segment":             {`[{"path":"/api/../x","pathType":"Prefix","service":"query-api"},{"path":"/api/v1/internal","pathType":"Prefix","service":"query-api"}]`, "literal /path"},
		"root entry only":         {`[{"path":"/","pathType":"Prefix","service":"query-api"}]`, "whole host to the Python api"},
		"duplicate entry":         {`[{"path":"/graphql","pathType":"Prefix","service":"query-api"},{"path":"/graphql","pathType":"Exact","service":"query-api"},{"path":"/api/v1/internal","pathType":"Prefix","service":"query-api"}]`, "duplicates another rule"},
		"entry repeats host path": {`[{"path":"/api/v1/internal","pathType":"Prefix","service":"query-api"},{"path":"/","pathType":"Exact","service":"query-api"}]`, "whole host to the Python api"},
		"null value":              {`null`, "must be true or a list"},
		"zero value":              {`0`, "must be true or a list"},
		"empty string value":      {`""`, "must be true or a list"},
	} {
		hosts := `[{"host":"h","pythonAllowList":` + c.list + `,"paths":[{"path":"/","pathType":"Prefix","service":"go-api"}]}]`
		o, err := render(hosts)
		if err == nil || !strings.Contains(o, c.want) {
			t.Errorf("%s: want render failure containing %q, got err=%v\n%s", name, c.want, err, o)
		}
	}
	// An allow-list path that repeats one of the host's own paths is refused (two rules on one path).
	dup := `[{"host":"h","pythonAllowList":[{"path":"/graphql","pathType":"Prefix","service":"query-api"},{"path":"/api/v1/internal","pathType":"Prefix","service":"query-api"}],` +
		`"paths":[{"path":"/","pathType":"Prefix","service":"go-api"},{"path":"/graphql","pathType":"Prefix","service":"web"}]}]`
	if o, err := render(dup); err == nil || !strings.Contains(o, "duplicates another rule") {
		t.Errorf("an allow-list path repeating a host path must fail the render: err=%v\n%s", err, o)
	}
}

// TestAnchoredAllowListEntries pins CHAOS-7243: an anchored entry ({path: "/docs$", pathType: ImplementationSpecific})
// renders verbatim (ingress-nginx then emits `location ~* "^/docs$"`, proven live on v1.14.5), the chart adds
// use-regex to its Ingress exactly when an anchored entry is in use, and malformed anchored entries are refused.
func TestAnchoredAllowListEntries(t *testing.T) {
	render := func(hosts string, extra ...string) (string, error) {
		args := append([]string{"template", "b", ".", "--set", "goApi.enabled=true", "--set", "queryApi.enabled=true", "--set", "ingress.enabled=true", "--set-json", "ingress.hosts=" + hosts}, extra...)
		out, err := exec.Command("helm", args...).CombinedOutput()
		return string(out), err
	}
	host := func(list string) string {
		return `[{"host":"h","pythonAllowList":` + list + `,"paths":[{"path":"/","pathType":"Prefix","service":"go-api"}]}]`
	}
	internal := `{"path":"/api/v1/internal","pathType":"Prefix","service":"query-api"}`
	ingressDoc := func(out string) string {
		for _, d := range strings.Split(out, "\n---") {
			if documentKind(d) == "Ingress" {
				return d
			}
		}
		t.Fatalf("no Ingress in render:\n%s", out)
		return ""
	}
	anchored := `[{"path":"/docs$","pathType":"ImplementationSpecific","service":"query-api"},{"path":"/docs/oauth2-redirect$","pathType":"ImplementationSpecific","service":"query-api"},` +
		`{"path":"/openapi\\.json$","pathType":"ImplementationSpecific","service":"query-api"},` + internal + `]`
	out, err := render(host(anchored))
	if err != nil {
		t.Fatalf("anchored list must render: %v\n%s", err, out)
	}
	doc := ingressDoc(out)
	for _, want := range []string{`nginx.ingress.kubernetes.io/use-regex: "true"`, "- path: /docs$\n", "- path: /docs/oauth2-redirect$\n", `- path: /openapi\.json$` + "\n"} {
		if !strings.Contains(doc, want) {
			t.Errorf("anchored render must contain %q:\n%s", want, doc)
		}
	}
	if got := strings.Count(doc, "pathType: ImplementationSpecific"); got != 3 {
		t.Errorf("want 3 ImplementationSpecific rules, got %d:\n%s", got, doc)
	}
	// The shared default list is EMPTY (query-api answers /graphql), so a `true` host renders no Python path and no
	// regex mode; a values file that lists an anchored shared entry turns regex mode on for that host.
	if o, err := render(host("true")); err != nil || strings.Contains(ingressDoc(o), "use-regex") || strings.Contains(ingressDoc(o), "/graphql") {
		t.Errorf("a `true` host with the default (empty) shared list must render no Python path and no use-regex: err=%v\n%s", err, o)
	}
	if o, err := render(host("true"), "--set-json", `ingress.pythonAllowList=[{"path":"/graphql$","pathType":"ImplementationSpecific","service":"query-api"}]`); err != nil || !strings.Contains(ingressDoc(o), `use-regex: "true"`) || !strings.Contains(ingressDoc(o), "- path: /graphql$\n") {
		t.Errorf("a `true` host with an anchored shared entry must enable use-regex: err=%v\n%s", err, o)
	}
	// No anchored entry anywhere -> no annotation (literal-only lists render as before).
	if o, err := render(host(`[{"path":"/graphql","pathType":"Exact","service":"query-api"},` + internal + `]`)); err != nil || strings.Contains(ingressDoc(o), "use-regex") {
		t.Errorf("a literal-only list must not add use-regex: err=%v\n%s", err, o)
	}
	// An operator-set use-regex on an Ingress routing to go-api is still refused.
	if o, err := render(host(anchored), "--set-string", `ingress.annotations.nginx\.ingress\.kubernetes\.io/use-regex=true`); err == nil || !strings.Contains(o, "use-regex on an Ingress that routes to go-api") {
		t.Errorf("operator-set use-regex must still be refused: err=%v\n%s", err, o)
	}
	docs := `{"path":"/docs$","pathType":"ImplementationSpecific","service":"query-api"}`
	for name, c := range map[string]struct{ list, want string }{
		"no trailing dollar":    {`[{"path":"/docs","pathType":"ImplementationSpecific","service":"query-api"},` + internal + `]`, "anchored"},
		"double dollar":         {`[{"path":"/docs$$","pathType":"ImplementationSpecific","service":"query-api"},` + internal + `]`, "anchored"},
		"dollar mid path":       {`[{"path":"/do$cs$","pathType":"ImplementationSpecific","service":"query-api"},` + internal + `]`, "anchored"},
		"unescaped dot":         {`[{"path":"/openapi.json$","pathType":"ImplementationSpecific","service":"query-api"},` + internal + `]`, "anchored"},
		"group":                 {`[{"path":"/(docs)$","pathType":"ImplementationSpecific","service":"query-api"},` + internal + `]`, "anchored"},
		"wildcard":              {`[{"path":"/docs.*$","pathType":"ImplementationSpecific","service":"query-api"},` + internal + `]`, "anchored"},
		"alternation":           {`[{"path":"/docs|x$","pathType":"ImplementationSpecific","service":"query-api"},` + internal + `]`, "anchored"},
		"caret":                 {`[{"path":"^/docs$","pathType":"ImplementationSpecific","service":"query-api"},` + internal + `]`, "anchored"},
		"lone dollar":           {`[{"path":"$","pathType":"ImplementationSpecific","service":"query-api"},` + internal + `]`, "anchored"},
		"root dollar":           {`[{"path":"/$","pathType":"ImplementationSpecific","service":"query-api"},` + internal + `]`, "anchored"},
		"double slash":          {`[{"path":"//docs$","pathType":"ImplementationSpecific","service":"query-api"},` + internal + `]`, "anchored"},
		"literal with dollar":   {`[{"path":"/docs$","pathType":"Exact","service":"query-api"},` + internal + `]`, "must be {path"},
		"duplicate of literal":  {`[` + docs + `,{"path":"/docs","pathType":"Exact","service":"query-api"},` + internal + `]`, "duplicates another rule"},
		"duplicate anchored":    {`[` + docs + `,` + docs + `,` + internal + `]`, "duplicates another rule"},
		"exact beside anchored": {`[` + docs + `,{"path":"/graphql","pathType":"Exact","service":"query-api"},` + internal + `]`, "is Exact on a host that has an anchored entry"},
	} {
		o, err := render(host(c.list))
		if err == nil || !strings.Contains(o, c.want) {
			t.Errorf("%s: want render failure containing %q, got err=%v\n%s", name, c.want, err, o)
		}
	}
	// An anchored entry whose base path repeats one of the host's own paths is refused.
	dup := `[{"host":"h","pythonAllowList":[{"path":"/graphql$","pathType":"ImplementationSpecific","service":"query-api"},` + internal + `],` +
		`"paths":[{"path":"/","pathType":"Prefix","service":"go-api"},{"path":"/graphql","pathType":"Prefix","service":"web"}]}]`
	if o, err := render(dup); err == nil || !strings.Contains(o, "duplicates another rule") {
		t.Errorf("an anchored entry repeating a host path must be refused: err=%v\n%s", err, o)
	}
	// r1 P2: an escaped dot segment ("/docs/\\.\\.$") is a . or .. segment after unescaping.
	if o, err := render(host(`[{"path":"/docs/\\.\\.$","pathType":"ImplementationSpecific","service":"query-api"},` + internal + `]`)); err == nil || !strings.Contains(o, "path segment") {
		t.Errorf("an anchored dot-dot segment must be refused: err=%v\n%s", err, o)
	}
}

// TestAnchoredHostsGetTheirOwnIngress pins CHAOS-7243 r1 P1: use-regex applies to every host of the Ingress object
// that carries it, so a host with an anchored entry lives in its own Ingress (suffix -anchored, annotated) and every
// other host stays in the unannotated main Ingress, rendered exactly as before.
func TestAnchoredHostsGetTheirOwnIngress(t *testing.T) {
	anchoredHost := `{"host":"anchored.test","pythonAllowList":[{"path":"/docs$","pathType":"ImplementationSpecific","service":"query-api"},{"path":"/api/v1/internal","pathType":"Prefix","service":"query-api"}],"paths":[{"path":"/","pathType":"Prefix","service":"go-api"}]}`
	literalHost := `{"host":"literal.test","pythonAllowList":[{"path":"/graphql","pathType":"Exact","service":"query-api"},{"path":"/api/v1/internal","pathType":"Prefix","service":"query-api"}],"paths":[{"path":"/","pathType":"Prefix","service":"go-api"}]}`
	plainHost := `{"host":"plain.test","paths":[{"path":"/","pathType":"Prefix","service":"web"}]}`
	render := func(hosts string) []string {
		out, err := exec.Command("helm", "template", "b", ".", "--set", "goApi.enabled=true", "--set", "queryApi.enabled=true", "--set", "ingress.enabled=true", "--set-json", "ingress.hosts="+hosts).CombinedOutput()
		if err != nil {
			t.Fatalf("render failed: %v\n%s", err, out)
		}
		var ingresses []string
		for _, d := range strings.Split(string(out), "\n---") {
			if documentKind(d) == "Ingress" {
				ingresses = append(ingresses, d)
			}
		}
		return ingresses
	}
	docs := render(`[` + literalHost + `,` + anchoredHost + `,` + plainHost + `]`)
	if len(docs) != 2 {
		t.Fatalf("want a main and an -anchored Ingress, got %d", len(docs))
	}
	var main, anchored string
	for _, d := range docs {
		if strings.Contains(d, "name: b-dev-health-anchored\n") {
			anchored = d
		} else {
			main = d
		}
	}
	if anchored == "" || main == "" {
		t.Fatalf("Ingress objects not found:\n%v", docs)
	}
	if !strings.Contains(anchored, `use-regex: "true"`) || !strings.Contains(anchored, "anchored.test") || strings.Contains(anchored, "literal.test") || strings.Contains(anchored, "plain.test") {
		t.Errorf("the -anchored Ingress must carry use-regex and ONLY the anchored host:\n%s", anchored)
	}
	if strings.Contains(main, "use-regex") || !strings.Contains(main, "literal.test") || !strings.Contains(main, "plain.test") || strings.Contains(main, "anchored.test") {
		t.Errorf("the main Ingress must stay unannotated and carry the other hosts:\n%s", main)
	}
	// Literal-only and mixed-without-anchored renders keep ONE Ingress, unannotated.
	if one := render(`[` + literalHost + `,` + plainHost + `]`); len(one) != 1 || strings.Contains(one[0], "use-regex") {
		t.Errorf("no anchored entry: want one unannotated Ingress, got %d", len(one))
	}
	// Only anchored hosts: a single -anchored Ingress and no empty main Ingress.
	if only := render(`[` + anchoredHost + `]`); len(only) != 1 || !strings.Contains(only[0], "b-dev-health-anchored") {
		t.Errorf("only anchored hosts: want exactly one -anchored Ingress, got %d", len(only))
	}
}

// TestAllowListNeedsNoInternalCover pins CHAOS-7255: the public Go listener serves no /api/v1/internal/* route
// (internal/apiservice TestPublicListenerNeverServesInternalRoutes), so an allow-list no longer has to carry it, while
// routing anything to the go-api INTERNAL Service stays refused.
func TestAllowListNeedsNoInternalCover(t *testing.T) {
	render := func(hosts string) (string, error) {
		out, err := exec.Command("helm", "template", "b", ".", "--set", "goApi.enabled=true", "--set", "queryApi.enabled=true", "--set", "ingress.enabled=true",
			"--set-json", "ingress.hosts="+hosts).CombinedOutput()
		return string(out), err
	}
	for name, list := range map[string]string{
		"shared default (no internal entry)": `true`,
		"own list without internal":          `[{"path":"/graphql$","pathType":"ImplementationSpecific","service":"query-api"},{"path":"/metrics$","pathType":"ImplementationSpecific","service":"query-api"}]`,
		"literal list without internal":      `[{"path":"/graphql","pathType":"Exact","service":"query-api"}]`,
		"list keeping internal":              `[{"path":"/graphql$","pathType":"ImplementationSpecific","service":"query-api"},{"path":"/api/v1/internal","pathType":"Prefix","service":"query-api"}]`,
	} {
		hosts := `[{"host":"h","pythonAllowList":` + list + `,"paths":[{"path":"/","pathType":"Prefix","service":"go-api"}]}]`
		if o, err := render(hosts); err != nil {
			t.Errorf("%s must render: %v\n%s", name, err, o)
		}
	}
	// The internal listener's Service is never routable from an Ingress.
	if o, err := render(`[{"host":"h","pythonAllowList":true,"paths":[{"path":"/","pathType":"Prefix","service":"go-api"},{"path":"/x","pathType":"Prefix","service":"go-api-internal"}]}]`); err == nil || !strings.Contains(o, "internal listener is unauthenticated") {
		t.Errorf("a route to go-api-internal must be refused: err=%v\n%s", err, o)
	}
	// A go-api path may still not cover /api/v1/internal (kept as defence in depth).
	if o, err := render(`[{"host":"h","pythonAllowList":true,"paths":[{"path":"/api/v1/internal","pathType":"Prefix","service":"go-api"}]}]`); err == nil || !strings.Contains(o, "covers /api/v1/internal") {
		t.Errorf("a go-api path covering /api/v1/internal must stay refused: err=%v\n%s", err, o)
	}
}

// TestDefaultAllowListNeedsNoPythonAPI pins CHAOS-6263: the shared allow-list is empty by default (query-api answers
// /graphql), so a host that opts in routes NOTHING to the Python api and renders with the Python api disabled; the
// moment a values file lists a path, the Python api Service is required again.
func TestDefaultAllowListNeedsNoPythonAPI(t *testing.T) {
	render := func(extra ...string) (string, error) {
		args := append([]string{"template", "b", ".", "--set", "goApi.enabled=true", "--set", "queryApi.enabled=true", "--set", "ingress.enabled=true",
			"--set-json", `ingress.hosts=[{"host":"h","pythonAllowList":true,"paths":[{"path":"/","pathType":"Prefix","service":"go-api"}]}]`}, extra...)
		out, err := exec.Command("helm", args...).CombinedOutput()
		return string(out), err
	}
	out, err := render()
	if err != nil {
		t.Fatalf("the default (empty) allow-list must render without the Python api: %v\n%s", err, out)
	}
	for _, d := range strings.Split(out, "\n---") {
		if documentKind(d) != "Ingress" {
			continue
		}
		if strings.Count(d, "- path: ") != 1 || strings.Contains(d, "/graphql") || strings.Contains(d, "dev-health-api\n") || !strings.Contains(d, "name: b-dev-health-go-api\n") {
			t.Errorf("the Ingress must carry only \"/\" -> go-api:\n%s", d)
		}
	}
	if out, err := render("--set-json", `ingress.pythonAllowList=[{"path":"/graphql$","pathType":"ImplementationSpecific"}]`); err == nil || !strings.Contains(out, "has no service: the Python api (the old default backend) is gone") {
		t.Errorf("a listed path with no backend must fail the render: err=%v\n%s", err, out)
	}
}
