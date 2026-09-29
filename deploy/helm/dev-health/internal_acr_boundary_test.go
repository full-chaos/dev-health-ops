package devhealth_test

import (
	"os/exec"
	"regexp"
	"strings"
	"testing"
)

// TestInternalACRRoutesStayOffThePublicIngress pins the control that stands
// in for a bearer check. The Go api serves GET /api/v1/internal/acr/health
// and GET /api/v1/internal/acr/entitlements/{org_id} with no credential
// check, by design: internal service-to-service calls carry none, and the
// network boundary is the control (internal/apiservice/acr's package
// comment). So no public path may resolve to the Go api for those URLs.
//
// CHAOS-7047: a host may now route "/" to go-api (the Go default backend), so
// the check resolves the winning rule for the internal acr URLs the way
// ingress-nginx does (Exact beats the longest Prefix) and requires that it is
// not a go-api Service; ingress.pythonAllowList must carry /api/v1/internal.
func TestInternalACRRoutesStayOffThePublicIngress(t *testing.T) {
	output, err := exec.Command("helm", "template", "b", ".",
		"--set", "goApi.enabled=true",
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
		for _, url := range []string{"/api/v1/internal/acr/health", "/api/v1/internal/acr/entitlements/o"} {
			backend := winningBackend(document, url)
			if backend == "" {
				t.Errorf("no rule of the Ingress matches %s:\n%s", url, document)
			}
			for _, name := range reachGoAPI {
				if backend == name {
					t.Errorf("%s resolves to Service %s, which reaches the Go api; the Go api serves /api/v1/internal/acr/* with no credential check:\n%s", url, name, document)
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
// needs the allow-list opt-in, the allow-list must cover /api/v1/internal, and
// no go-api path may cover /api/v1/internal.
func TestGoCatchAllRenderGuards(t *testing.T) {
	render := func(hosts, allow string) (string, error) {
		args := []string{"template", "b", ".", "--set", "goApi.enabled=true", "--set", "ingress.enabled=true", "--set-json", "ingress.hosts=" + hosts}
		if allow != "" {
			args = append(args, "--set-json", "ingress.pythonAllowList="+allow)
		}
		out, err := exec.Command("helm", args...).CombinedOutput()
		return string(out), err
	}
	// use-regex annotation with a go-api route.
	if out, err := exec.Command("helm", "template", "b", ".", "--set", "goApi.enabled=true", "--set", "ingress.enabled=true",
		"--set-string", `ingress.annotations.nginx\.ingress\.kubernetes\.io/use-regex=true`,
		"--set-json", `ingress.hosts=[{"host":"h","pythonAllowList":true,"paths":[{"path":"/","pathType":"Prefix","service":"go-api"}]}]`).CombinedOutput(); err == nil || !strings.Contains(string(out), "use-regex is true") {
		t.Errorf("use-regex with a go-api route must fail the render: err=%v\n%s", err, out)
	}
	catchAll := `[{"host":"h","pythonAllowList":true,"paths":[{"path":"/","pathType":"Prefix","service":"go-api"}]}]`
	if out, err := render(catchAll, ""); err != nil {
		t.Fatalf("default allow-list must render: %v\n%s", err, out)
	}
	for name, c := range map[string]struct{ hosts, allow, want string }{
		"no opt-in":                 {`[{"host":"h","paths":[{"path":"/","pathType":"Prefix","service":"go-api"}]}]`, "", "without pythonAllowList"},
		"allow-list drops internal": {catchAll, `[{"path":"/graphql","pathType":"Prefix"}]`, "must cover /api/v1/internal"},
		"string prefix only":        {catchAll, `[{"path":"/api/v1/int","pathType":"Prefix"}]`, "must cover /api/v1/internal"},
		"sibling -x":                {catchAll, `[{"path":"/api/v1/internal-x","pathType":"Prefix"}]`, "must cover /api/v1/internal"},
		"sibling s":                 {catchAll, `[{"path":"/api/v1/internals","pathType":"Prefix"}]`, "must cover /api/v1/internal"},
		"exact internal only":       {catchAll, `[{"path":"/api/v1/internal","pathType":"Exact"}]`, "must cover /api/v1/internal"},
		"regex path":                {`[{"host":"h","pythonAllowList":true,"paths":[{"path":"^/api/v1/(internal|internal/acr/.*)","pathType":"ImplementationSpecific","service":"go-api"}]}]`, "", "only literal Prefix/Exact"},
		"regex chars in prefix":     {`[{"host":"h","pythonAllowList":true,"paths":[{"path":"/api/v1/(internal)","pathType":"Prefix","service":"go-api"}]}]`, "", "only literal Prefix/Exact"},
		"implementation specific":   {`[{"host":"h","pythonAllowList":true,"paths":[{"path":"/x","pathType":"ImplementationSpecific","service":"go-api"}]}]`, "", "only literal Prefix/Exact"},
		"go-api /api":               {`[{"host":"h","pythonAllowList":true,"paths":[{"path":"/api","pathType":"Prefix","service":"go-api"}]}]`, "", "covers /api/v1/internal"},
		"go-api internal":           {`[{"host":"h","pythonAllowList":true,"paths":[{"path":"/api/v1/internal/acr","pathType":"Prefix","service":"go-api"}]}]`, "", "covers /api/v1/internal"},
	} {
		out, err := render(c.hosts, c.allow)
		if err == nil || !strings.Contains(out, c.want) {
			t.Errorf("%s: want render failure containing %q, got err=%v\n%s", name, c.want, err, out)
		}
	}
}
