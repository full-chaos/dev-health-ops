package routing_test

import (
	"path/filepath"
	"strings"
	"testing"
)

// No arguments or bad arguments: exit 2, the usage or the error on stderr, nothing on stdout, for every command. The
// usage text itself is not the Python scripts' argparse/docstring text (help text is not a contract), and
// query-api-flags' no-argument usage goes to stderr where the script printed its docstring to stdout: both are ruled
// divergences.
func TestUsageAndBadArgumentsExit2OnStderrWithNothingOnStdout(t *testing.T) {
	dir := t.TempDir()
	values := writeFile(t, dir, "v.yaml", []byte("ingress: {goApiPaths: [{path: /a}], queryApiPaths: [{path: /q, pathType: Exact}]}\n"))
	compose := writeFile(t, dir, "c.yml", []byte("services: {}\n"))
	out := filepath.Join(dir, "out")
	for _, c := range []struct {
		name string
		res  result
	}{
		{"plane-split-router, no arguments", run(gen)},
		{"plane-split-router, unknown flag", run(gen, values, "--no-such-flag")},
		{"plane-split-router, bad --format", run(gen, values, "--format", "yaml")},
		{"plane-split-router, --format with no value", run(gen, values, "--format")},
		{"plane-split-router, two values files", run(gen, values, values)},
		{"plane-split-router, values file and pinned flags", run(gen, values, "--deploy-sha", "abc")},
		{"plane-split-router, pinned flags incomplete", run(gen, "--deploy-repo", dir)},
		{"query-api-flags, no arguments", run(flagsTool)},
		{"query-api-flags, no --out", run(flagsTool, values)},
		{"query-api-flags, --out with no value", run(flagsTool, values, "--out")},
		{"route-coverage, no arguments", run(coverage)},
		{"route-coverage, unknown flag", run(coverage, values, "--no-such-flag")},
		{"route-coverage, two values files", run(coverage, values, values)},
		{"route-coverage, --go-api-url with no value", run(coverage, values, "--go-api-url")},
		{"web-env-parity, no arguments", run(webEnv)},
		{"web-env-parity, one file", run(webEnv, values)},
		{"web-env-parity, three files", run(webEnv, values, compose, out)},
		{"web-env-parity, --live-names with no file", run(webEnv, values, compose, "--live-names")},
	} {
		usage := strings.HasSuffix(c.name, "no arguments")
		if c.res.code != 2 || c.res.stdout != "" || c.res.stderr == "" || (usage && !strings.Contains(c.res.stderr, "usage")) {
			t.Errorf("%s: exit %d stdout %q stderr %q, want exit 2, no stdout, the usage or the error on stderr", c.name, c.res.code, c.res.stdout, c.res.stderr)
		}
	}
}
