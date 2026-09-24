package devhealth_test

import (
	"os/exec"
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
// this test pins that no public-routing object names the go-api Service at
// all, even when a values file asks for it, and that the go-api Service and
// pods expose no port outside the cluster network. When a go-api Ingress
// branch is added, this test fails; replace its first check then with one
// that no path routed to the Go api covers /api/v1/internal/acr.
func TestInternalACRRoutesStayOffThePublicIngress(t *testing.T) {
	output, err := exec.Command("helm", "template", "b", ".",
		"--set", "goApi.enabled=true",
		"--set", "ingress.enabled=true",
		// A values file that asks the Ingress for the Go api, on the
		// internal acr prefix and on the whole api.
		"--set-json", `ingress.hosts=[{"host":"h","paths":[`+
			`{"path":"/api/v1/internal/acr","pathType":"Prefix","service":"go-api"},`+
			`{"path":"/api","pathType":"Prefix","service":"go-api"},`+
			`{"path":"/","pathType":"Prefix","service":"web"}]}]`,
	).CombinedOutput()
	if err != nil {
		t.Fatalf("render failed: %v\n%s", err, output)
	}
	const goAPIService = "name: b-dev-health-go-api\n"
	publicKinds := []string{"Ingress", "IngressRoute", "Gateway", "HTTPRoute", "GRPCRoute", "TCPRoute", "TLSRoute"}
	var ingresses, goAPIServices, goAPIDeployments int
	for _, document := range strings.Split(string(output), "\n---") {
		kind := documentKind(document)
		for _, public := range publicKinds {
			if kind == public {
				if public == "Ingress" {
					ingresses++
				}
				if strings.Contains(document, goAPIService) {
					t.Errorf("a %s routes to the Go api, which serves /api/v1/internal/acr/* with no credential check:\n%s", kind, document)
				}
			}
		}
		if kind == "Service" && strings.Contains(document, goAPIService) {
			goAPIServices++
			if !strings.Contains(document, "\n  type: ClusterIP\n") {
				t.Errorf("the go-api Service is not ClusterIP:\n%s", document)
			}
			for _, exposed := range []string{"nodePort:", "externalIPs:", "loadBalancerIP:"} {
				if strings.Contains(document, exposed) {
					t.Errorf("the go-api Service sets %s:\n%s", exposed, document)
				}
			}
		}
		if kind == "Deployment" && strings.Contains(document, "# Source: dev-health/templates/go-api-deployment.yaml\n") {
			goAPIDeployments++
			for _, exposed := range []string{"hostPort:", "hostNetwork: true"} {
				if strings.Contains(document, exposed) {
					t.Errorf("the go-api Deployment sets %s:\n%s", exposed, document)
				}
			}
		}
	}
	// A render without the objects checked would pass every check above.
	if ingresses != 1 || goAPIServices != 1 || goAPIDeployments != 1 {
		t.Fatalf("render has %d Ingress, %d go-api Service and %d go-api Deployment documents, want 1 each", ingresses, goAPIServices, goAPIDeployments)
	}
}

func documentKind(document string) string {
	for _, line := range strings.Split(document, "\n") {
		if kind, ok := strings.CutPrefix(line, "kind: "); ok {
			return strings.TrimSpace(kind)
		}
	}
	return ""
}
