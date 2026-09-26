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
// comment). So the Go api must be reachable only inside the cluster.
//
// Today the chart's Ingress has no branch that can route to the Go api, so
// this test pins that no public-routing object names any Service whose
// selector matches the go-api pods, even when a values file asks for it,
// and that no such Service and no go-api pod exposes a port outside the
// cluster network. When a go-api Ingress branch is added, this test fails;
// replace its routing check then with one that no path routed to the Go api
// covers /api/v1/internal/acr.
func TestInternalACRRoutesStayOffThePublicIngress(t *testing.T) {
	output, err := exec.Command("helm", "template", "b", ".",
		"--set", "goApi.enabled=true",
		"--set", "ingress.enabled=true",
		// A values file that routes a path to every Service the Ingress
		// template knows, and asks it for the Go api on the internal acr
		// prefix and on the whole api.
		"--set-json", `ingress.hosts=[{"host":"h","paths":[`+
			`{"path":"/api/v1/internal/acr","pathType":"Prefix","service":"go-api"},`+
			`{"path":"/api/v1","pathType":"Prefix","service":"go-api"},`+
			`{"path":"/api","pathType":"Prefix","service":"api"},`+
			`{"path":"/","pathType":"Prefix","service":"web"}]}]`,
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
		for _, name := range reachGoAPI {
			// A YAML scalar, plain or quoted, as a backend's name or
			// (older Ingress) serviceName.
			reference := regexp.MustCompile(`(?m)^\s*(- )?(name|serviceName):\s*["']?` + regexp.QuoteMeta(name) + `["']?\s*$`)
			if reference.MatchString(document) {
				t.Errorf("a %s routes to Service %s, which reaches the Go api; the Go api serves /api/v1/internal/acr/* with no credential check:\n%s", kind, name, document)
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
