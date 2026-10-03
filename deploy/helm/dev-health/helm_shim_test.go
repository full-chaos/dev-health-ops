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
// that puts `--set web.env.BACKEND_URL=<placeholder> --set web.backendFromRelease=false` right after `template`. helm gives `--set` precedence over
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
		"shift\nexec \"" + real + "\" template --set \"web.env.BACKEND_URL=" + helmShimBackendURL + "\" --set web.backendFromRelease=false \"$@\"\n"
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
	if !strings.Contains(string(out), "web.backendFromRelease=true") {
		t.Fatalf("the refusal does not name the backendFromRelease key: %.400s", out)
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
	// The release form is release-relative: the value the chart rendered before CHAOS-8310 for each release name.
	for release, want := range map[string]string{
		"dev-health": "http://dev-health-api:8000",
		"lane-a":     "http://lane-a-dev-health-api:8000",
	} {
		cmd := exec.Command("helm", "template", release, ".", "-f", "values-quickstart.yaml")
		cmd.Env = append(os.Environ(), "HELM_SHIM_OFF=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("the quickstart profile must render by itself (release %s): %v\n%.400s", release, err, out)
		}
		if !strings.Contains(string(out), `value: "`+want+`"`) {
			t.Fatalf("release %s: the quickstart profile's BACKEND_URL is not %s", release, want)
		}
	}
}

// CHAOS-8310: the release form follows api.port, as the pre-8310 default did (a fixed 8000 would be green above).
func TestQuickstartReleaseFormFollowsAPIPort(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	cmd := exec.Command("helm", "template", "lane-a", ".", "-f", "values-quickstart.yaml", "--set", "api.port=9000")
	cmd.Env = append(os.Environ(), "HELM_SHIM_OFF=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("the quickstart profile with api.port=9000 must render: %v\n%.400s", err, out)
	}
	if !strings.Contains(string(out), `value: "http://lane-a-dev-health-api:9000"`) {
		t.Fatalf("the release form does not follow api.port")
	}
}

// CHAOS-8310: exactly one of web.env.BACKEND_URL and web.backendFromRelease; both set is refused, naming both.
func TestWebBackendURLAndBackendFromReleaseAreExclusive(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	cmd := exec.Command("helm", "template", "t", ".", "--set", "web.env.BACKEND_URL=http://x:1", "--set", "web.backendFromRelease=true")
	cmd.Env = append(os.Environ(), "HELM_SHIM_OFF=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("both keys set must be refused")
	}
	if !strings.Contains(string(out), "web.env.BACKEND_URL and web.backendFromRelease=true are both set") {
		t.Fatalf("the refusal does not name both keys: %.400s", out)
	}
	// A misspelt key is not the opt-in: the render is refused as unset.
	cmd = exec.Command("helm", "template", "t", ".", "--set", "web.backendFromRelese=true")
	cmd.Env = append(os.Environ(), "HELM_SHIM_OFF=1")
	if out, err := cmd.CombinedOutput(); err == nil || !strings.Contains(string(out), "web.env.BACKEND_URL is required") {
		t.Fatalf("a misspelt opt-in key must leave the render refused as unset: %v %.200s", err, out)
	}
}
