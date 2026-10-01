package devhealth_test

import (
	"os/exec"
	"strings"
	"testing"
)

// The query-api Deployment runs `dho query-api`: the chart default renders the
// dho image with the verb as the only argument, and keeps the listener port
// and probes it had before the fold.
func TestQueryAPIRunsTheDhoVerb(t *testing.T) {
	output, err := exec.Command("helm", "template", "qa-test", ".", "--set", "queryApi.enabled=true").CombinedOutput()
	if err != nil {
		t.Fatalf("render failed: %v\n%s", err, output)
	}
	deployment := deploymentOf(t, string(output), "qa-test-dev-health-query-api")
	for _, want := range []string{
		"image: ghcr.io/full-chaos/dev-health-go-dho:",
		"args: [\"query-api\"]",
		"containerPort: 8090",
		"value: \":8090\"",
	} {
		if !strings.Contains(deployment, want) {
			t.Fatalf("query-api Deployment lacks %q:\n%s", want, deployment)
		}
	}
	if strings.Contains(deployment, "command:") {
		t.Fatalf("query-api Deployment sets command:; the image entrypoint is dho:\n%s", deployment)
	}
}

// An image that cannot run `dho query-api` renders a pod that exits 2, so the
// chart refuses it: the retired query-api image by name, any other non-dho
// image by the allowlist. An operator-image digest is a dho image and renders.
func TestQueryAPIRefusesImagesThatCannotRunTheVerb(t *testing.T) {
	digest := strings.Repeat("a", 64)
	for name, testCase := range map[string]struct {
		repository string
		want       string
	}{
		"the retired image":     {"ghcr.io/full-chaos/dev-health-query-api", "retired dev-health-query-api image"},
		"the retired digest":    {"ghcr.io/full-chaos/dev-health-query-api@sha256:" + digest, "retired dev-health-query-api image"},
		"another project image": {"ghcr.io/full-chaos/dev-hops-api", "is not a dho image"},
	} {
		t.Run(name, func(t *testing.T) {
			output, err := exec.Command("helm", "template", "qa-test", ".",
				"--set", "queryApi.enabled=true", "--set", "queryApi.image.repository="+testCase.repository).CombinedOutput()
			if err == nil {
				t.Fatalf("rendered:\n%s", output)
			}
			if !strings.Contains(string(output), testCase.want) {
				t.Fatalf("render failed for another reason: %s", output)
			}
		})
	}
	output, err := exec.Command("helm", "template", "qa-test", ".", "--set", "queryApi.enabled=true",
		"--set", "queryApi.image.repository=ghcr.io/full-chaos/dev-health-go-operator@sha256:"+digest).CombinedOutput()
	if err != nil {
		t.Fatalf("an operator-image digest did not render: %v\n%s", err, output)
	}
	if !strings.Contains(deploymentOf(t, string(output), "qa-test-dev-health-query-api"), "dev-health-go-operator@sha256:"+digest) {
		t.Fatalf("the operator digest is not the query-api image:\n%s", output)
	}
}

// /graphql answers a browser with the allowed origins query-api reads from
// CORS_ALLOWED_ORIGINS: the chart value renders that variable, an empty value
// renders none (the binary's default), and one variable never gets two values.
func TestQueryAPICORSAllowedOrigins(t *testing.T) {
	render := func(extra ...string) (string, error) {
		args := append([]string{"template", "qa-test", ".", "--set", "queryApi.enabled=true"}, extra...)
		out, err := exec.Command("helm", args...).CombinedOutput()
		return string(out), err
	}
	out, err := render()
	if err != nil {
		t.Fatalf("render failed: %v\n%s", err, out)
	}
	if deployment := deploymentOf(t, out, "qa-test-dev-health-query-api"); strings.Contains(deployment, "CORS_ALLOWED_ORIGINS") {
		t.Fatalf("an empty queryApi.corsAllowedOrigins must render no CORS_ALLOWED_ORIGINS:\n%s", deployment)
	}
	out, err = render("--set-string", `queryApi.corsAllowedOrigins=https://app.example\,https://b.example`)
	if err != nil {
		t.Fatalf("render failed: %v\n%s", err, out)
	}
	deployment := deploymentOf(t, out, "qa-test-dev-health-query-api")
	if want := "- name: CORS_ALLOWED_ORIGINS\n              value: \"https://app.example,https://b.example\"\n"; !strings.Contains(deployment, want) {
		t.Fatalf("query-api Deployment lacks %q:\n%s", want, deployment)
	}
	secret := `queryApi.extraEnv=[{"name":"CORS_ALLOWED_ORIGINS","valueFrom":{"secretKeyRef":{"name":"s","key":"CORS_ALLOWED_ORIGINS"}}}]`
	if out, err = render("--set-json", secret); err != nil || strings.Count(deploymentOf(t, out, "qa-test-dev-health-query-api"), "name: CORS_ALLOWED_ORIGINS") != 1 {
		t.Fatalf("a Secret-backed CORS_ALLOWED_ORIGINS in extraEnv must render once: %v\n%s", err, out)
	}
	if out, err = render("--set-json", secret, "--set-string", "queryApi.corsAllowedOrigins=https://app.example"); err == nil || !strings.Contains(out, "two values for one variable") {
		t.Fatalf("both sources set must fail the render: err=%v\n%s", err, out)
	}
}
