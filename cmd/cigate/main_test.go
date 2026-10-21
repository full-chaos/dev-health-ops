package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// runCaptured is run(args) with os.Stdout/os.Stderr sent to files: the exit code and both streams.
func runCaptured(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	dir := t.TempDir()
	so, err := os.Create(filepath.Join(dir, "stdout"))
	if err != nil {
		t.Fatal(err)
	}
	se, err := os.Create(filepath.Join(dir, "stderr"))
	if err != nil {
		t.Fatal(err)
	}
	oldOut, oldErr := os.Stdout, os.Stderr
	os.Stdout, os.Stderr = so, se
	code := run(args)
	os.Stdout, os.Stderr = oldOut, oldErr
	_ = so.Close()
	_ = se.Close()
	out, _ := os.ReadFile(so.Name())
	errOut, _ := os.ReadFile(se.Name())
	return code, string(out), string(errOut)
}

func write(t *testing.T, dir, name, text string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

const values = `ingress:
  goApiPaths:
    - path: /api/v1/items/{item_id}
  queryApiPaths:
    - path: /api/v1/home
      pathType: Exact
ops:
  queryApi:
    extraEnv:
      - {name: GO_API_HOME_ENABLED, value: "true"}
      - {name: GO_API_EDGE_JWT_SECRET, valueFrom: {secretKeyRef: {key: JWT_SECRET_KEY}}}
  web:
    env: {BACKEND_URL: x}
`

// Each routing subcommand reaches its own routing.RunX with the arguments after the subcommand name.
func TestRoutingSubcommandsDispatch(t *testing.T) {
	dir := t.TempDir()
	v := write(t, dir, "values.yaml", values)

	code, out, errOut := runCaptured(t, "plane-split-router", v, "--format", "dynamic")
	if code != 0 || !strings.Contains(out, "go-api-paths:") || !strings.Contains(out, "^(/api/v1/items/[^/]+)$") ||
		!strings.HasPrefix(errOut, "# 1 goApiPaths, 1 query-api paths") {
		t.Errorf("plane-split-router: exit %d\nstdout:\n%s\nstderr:\n%s", code, out, errOut)
	}

	flags := filepath.Join(dir, "flags.out")
	code, out, errOut = runCaptured(t, "query-api-flags", v, "--out", flags)
	written, _ := os.ReadFile(flags)
	if code != 0 || out != "" || string(written) != "      GO_API_HOME_ENABLED: \"true\"\n      GO_API_EDGE_JWT_SECRET: ${JWT_SECRET_KEY}\n" ||
		errOut != "# 1 flags, 1 secret refs -> "+flags+"\n" {
		t.Errorf("query-api-flags: exit %d stdout %q stderr %q file %q", code, out, errOut, written)
	}

	plane := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "ROUTEPROBE" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer plane.Close()
	code, out, errOut = runCaptured(t, "route-coverage", v, "--go-api-url", plane.URL, "--query-api-url", plane.URL)
	if code != 0 || out != "ROUTE_COVERAGE plane=go-api routed=1 registered=1 missing=0 unexpected=0\nROUTE_COVERAGE plane=query-api routed=1 registered=1 missing=0 unexpected=0\n" || errOut != "" {
		t.Errorf("route-coverage: exit %d stdout %q stderr %q", code, out, errOut)
	}

	compose := write(t, dir, "compose.yml", "services:\n  web:\n    environment:\n      - BACKEND_URL=http://traefik:3000\n")
	code, out, errOut = runCaptured(t, "web-env-parity", v, compose)
	if code != 1 || out != "" || !strings.HasPrefix(errOut, "DRIFT: classified name AUTH_URL no longer exists in prod web env") {
		t.Errorf("web-env-parity: exit %d stdout %q stderr %q", code, out, errOut)
	}
}

// No subcommand and an unknown subcommand exit 2 with the usage or the error on stderr and nothing on stdout.
func TestUnknownOrMissingSubcommandExits2(t *testing.T) {
	for _, args := range [][]string{{}, {"plane-split"}, {"route_coverage"}} {
		code, out, errOut := runCaptured(t, args...)
		if code != 2 || out != "" || errOut == "" {
			t.Errorf("%v: exit %d stdout %q stderr %q", args, code, out, errOut)
		}
		if len(args) == 0 && !strings.Contains(errOut, "plane-split-router|query-api-flags|route-coverage|web-env-parity") {
			t.Errorf("the usage does not name the routing subcommands: %q", errOut)
		}
	}
}
