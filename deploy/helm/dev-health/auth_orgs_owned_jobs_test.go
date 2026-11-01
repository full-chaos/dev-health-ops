package devhealth_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// phase5Refusal is the render guard that holds phase 5 back until its lift.
const phase5Refusal = `{{- if ge (int $p) 5 }}{{- fail (printf "authService.phase %d: phase 5 (orgs owned) needs its own public-table writer leaf (D6358)`

// renderWithoutPhase5Refusal renders a copy of the chart whose phase 5 refusal line is removed, so the phase 5 render of
// the switch's consumers can be checked before the lift ships.
func renderWithoutPhase5Refusal(t *testing.T, args ...string) []map[string]any {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	dir := t.TempDir()
	for _, name := range []string{"Chart.yaml", "values.yaml", "values.schema.json"} {
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.CopyFS(filepath.Join(dir, "templates"), os.DirFS("templates")); err != nil {
		t.Fatal(err)
	}
	helpers := filepath.Join(dir, "templates", "_helpers.tpl")
	raw, err := os.ReadFile(helpers)
	if err != nil {
		t.Fatal(err)
	}
	var kept []string
	removed := 0
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, phase5Refusal) {
			removed++
			continue
		}
		kept = append(kept, line)
	}
	if removed != 1 {
		t.Fatalf("found the phase 5 refusal %d times in _helpers.tpl, want 1", removed)
	}
	if err := os.WriteFile(helpers, []byte(strings.Join(kept, "\n")), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("helm", append([]string{"template", "a", dir}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("render: %v\n%.600s", err, out)
	}
	var docs []map[string]any
	decoder := yaml.NewDecoder(strings.NewReader(string(out)))
	for {
		var doc map[string]any
		if err := decoder.Decode(&doc); err != nil {
			break
		}
		if doc != nil {
			docs = append(docs, doc)
		}
	}
	return docs
}

// At phase 5 the auth-migrate and auth-grants Jobs carry AUTH_ORGS_OWNED=true, byte-equal to auth-service's, so the
// grants pass gives the runtime role the organization write grant the service needs; at phase 4 none carries it.
func TestOrgsOwnedRendersOnTheGrantsAndMigrateJobsAtPhase5(t *testing.T) {
	targets := []struct{ kind, name string }{
		{"Deployment", "a-dev-health-auth-service"},
		{"Job", "a-dev-health-auth-migrate"},
		{"Job", "a-dev-health-auth-grants"},
	}
	for phase, want := range map[string]bool{"4": false, "5": true} {
		docs := renderWithoutPhase5Refusal(t, goAPIWithAuth(append([]string{"--set", "authService.phase=" + phase,
			"--set", "authService.usersOwnedStage=owned"}, phase3Secrets...)...)...)
		for _, target := range targets {
			doc := authDoc(docs, target.kind, target.name)
			if doc == nil {
				t.Fatalf("phase %s: no %s %s", phase, target.kind, target.name)
			}
			value, set := containerEnvValue(t, doc, "AUTH_ORGS_OWNED")
			if set != want || (want && value != "true") {
				t.Errorf("phase %s: %s AUTH_ORGS_OWNED=%q set=%v; want set=%v (true)", phase, target.name, value, set, want)
			}
		}
	}
}
