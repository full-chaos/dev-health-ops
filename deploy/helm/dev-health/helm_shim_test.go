package devhealth_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// CHAOS-8310: web.env.BACKEND_URL is required, and web.enabled defaults to true, so a render that
// sets nothing about web fails. These tests render other things (images, hooks, listeners), so ONE
// shared place gives every `helm template` a placeholder BACKEND_URL: a `helm` wrapper first on PATH
// that puts `--set web.env.BACKEND_URL=<placeholder>` right after `template`. helm gives `--set` precedence over
// every `-f` values file, in either order, so a test that renders a PROFILE's own value (a `-f` file) must run
// with HELM_SHIM_OFF=1 (TestQuickstartProfileCarriesItsOwnBackendURL); only a later `--set` of the same key wins. HELM_SHIM_OFF=1 in a command's environment bypasses it: the one test that
// must see the refusal sets it (TestWebBackendURLIsRequired).
const helmShimBackendURL = "http://backend.test:8000"

func TestMain(m *testing.M) {
	os.Exit(runWithHelmShim(m))
}

func runWithHelmShim(m *testing.M) int {
	real, err := exec.LookPath("helm")
	if err != nil {
		return m.Run() // the helm tests skip themselves when helm is absent
	}
	dir, err := os.MkdirTemp("", "helm-shim-")
	if err != nil {
		panic(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	script := "#!/usr/bin/env bash\n" +
		"if [ \"${HELM_SHIM_OFF:-}\" = 1 ] || [ \"${1:-}\" != template ]; then exec \"" + real + "\" \"$@\"; fi\n" +
		"shift\nexec \"" + real + "\" template --set \"web.env.BACKEND_URL=" + helmShimBackendURL + "\" \"$@\"\n"
	if err := os.WriteFile(filepath.Join(dir, "helm"), []byte(script), 0o755); err != nil {
		panic(err)
	}
	if err := os.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH")); err != nil {
		panic(err)
	}
	return m.Run()
}

// CHAOS-8310: with nothing set, the render FAILS and the message names the key (guard observed failing
// by planting the missing value); an explicit value renders and reaches the Deployment verbatim.
func TestWebBackendURLIsRequired(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	cmd := exec.Command("helm", "template", "t", ".")
	cmd.Env = append(os.Environ(), "HELM_SHIM_OFF=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("a render with no web.env.BACKEND_URL succeeded")
	}
	if !strings.Contains(string(out), "web.env.BACKEND_URL is required whenever web.enabled=true (CHAOS-8310)") {
		t.Fatalf("the refusal does not name the key: %.400s", out)
	}
	cmd = exec.Command("helm", "template", "t", ".", "--set", "web.enabled=false")
	cmd.Env = append(os.Environ(), "HELM_SHIM_OFF=1")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("web.enabled=false must render without a BACKEND_URL: %v\n%.400s", err, out)
	}
	cmd = exec.Command("helm", "template", "t", ".", "--set", "web.env.BACKEND_URL=http://explicit.example:9000")
	cmd.Env = append(os.Environ(), "HELM_SHIM_OFF=1")
	out, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("an explicit value must render: %v\n%.400s", err, out)
	}
	if !strings.Contains(string(out), `value: "http://explicit.example:9000"`) {
		t.Fatalf("the explicit BACKEND_URL did not reach the web Deployment")
	}
}

// CHAOS-8310: the shim's --set would hide a values FILE from every other test, so the quickstart profile is
// rendered here with the shim off, exactly as its usage line says: its own BACKEND_URL reaches the Deployment.
func TestQuickstartProfileCarriesItsOwnBackendURL(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	cmd := exec.Command("helm", "template", "dev-health", ".", "-f", "values-quickstart.yaml")
	cmd.Env = append(os.Environ(), "HELM_SHIM_OFF=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("the quickstart profile must render by itself: %v\n%.400s", err, out)
	}
	if !strings.Contains(string(out), `value: "http://dev-health-api:8000"`) {
		t.Fatalf("the quickstart profile's own BACKEND_URL did not reach the web Deployment")
	}
}
