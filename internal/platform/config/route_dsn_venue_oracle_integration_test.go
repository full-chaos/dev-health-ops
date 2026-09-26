//go:build integration

package config_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// The live half of the route-activate DSN oracle (CHAOS-6902): it runs the real
// init-container script under /bin/sh with python3; see route_dsn_test.go.

// pythonRouteDSN runs the real init-container script and parses what it wrote.
func pythonRouteDSN(t *testing.T, env map[string]string) routeDSNResult {
	t.Helper()
	script, err := os.ReadFile(routeDSNScript)
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	python := pyoracle.Resolve(t, root)
	out := t.TempDir()
	text := strings.ReplaceAll(string(script), "/run/route-dsn", out)
	command := exec.Command("/bin/sh", "-ec", text)
	command.Env = []string{"PATH=" + filepath.Dir(python) + string(os.PathListSeparator) + os.Getenv("PATH")}
	for key, value := range env {
		command.Env = append(command.Env, key+"="+value)
	}
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("the route-dsn script failed: %v\n%s", err, output)
	}
	read := func(name string) routeDSNIdentity {
		raw, err := os.ReadFile(filepath.Join(out, name))
		if err != nil {
			t.Fatal(err)
		}
		return identityOf(t, string(raw))
	}
	return routeDSNResult{Domain: read("POSTGRES_URI"), Queue: read("WORKER_DATABASE_URI"), Coordinator: read("COORDINATOR_DATABASE_URI")}
}

// TestRouteDSNVenueOracleMatchesThePythonProducer runs the real script and the
// component form over the whole grid and compares the parsed identities. With
// DHO_ROUTEDSN_GOLDEN_UPDATE=1 it rewrites the frozen golden.
func TestRouteDSNVenueOracleMatchesThePythonProducer(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the live Python producer runs only with DEV_HEALTH_LIVE_PYTHON_ORACLES=1")
	}
	if _, err := exec.LookPath("/bin/sh"); err != nil {
		t.Fatal(err)
	}
	var recorded []routeDSNGoldenCase
	for _, c := range routeDSNCases() {
		python := pythonRouteDSN(t, c.Env)
		compareRouteDSN(t, c.Name, goRouteDSN(t, c.Env), python, "python")
		recorded = append(recorded, routeDSNGoldenCase{Name: c.Name, Python: python})
	}
	if os.Getenv("DHO_ROUTEDSN_GOLDEN_UPDATE") == "1" {
		raw, err := json.MarshalIndent(recorded, "", " ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(routeDSNGolden, append(raw, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if !t.Failed() {
		venueoracle.WriteProof(t)
	}
}
