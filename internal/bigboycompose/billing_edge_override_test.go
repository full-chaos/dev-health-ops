// Package bigboycompose pins bigboy's compose file (ci/bigboy/compose.bigboy.yml). Test-only.
package bigboycompose_test

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
	"gopkg.in/yaml.v3"
)

// CHAOS-7055: bigboy serves the billing host from go-api's billing-edge listener and retires the Python
// `billing-edge` service. The override is in the cut's compose chain (so go-api is never recreated without the
// listener and the router labels), carries the three Stripe/license values only as name-only pass-through (no
// value is written in the file), and moves the Python service to a profile nothing enables. Nothing here reads
// a secret.

func toolsDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := moduleroot.Caller(0)
	if !ok {
		t.Fatal("resolve the package path")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "ci", "bigboy")
}

func toolsPath(t *testing.T, name string) string {
	t.Helper()
	return filepath.Join(toolsDir(t), name)
}

func read(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// environment reads a compose `environment` as a mapping or as a KEY=VALUE list (the merged file keeps both forms).
type environment map[string]string

func (e *environment) UnmarshalYAML(node *yaml.Node) error {
	out := map[string]string{}
	switch node.Kind {
	case yaml.MappingNode:
		if err := node.Decode(&out); err != nil {
			return err
		}
	case yaml.SequenceNode:
		var items []string
		if err := node.Decode(&items); err != nil {
			return err
		}
		for _, item := range items {
			key, value, _ := strings.Cut(item, "=")
			out[key] = value
		}
	}
	*e = out
	return nil
}

const composeName = "compose.bigboy.yml"

// commandArgs reads a compose command given as a list or as a shell-form string.
type commandArgs []string

func (c *commandArgs) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.ScalarNode {
		*c = strings.Fields(node.Value)
		return nil
	}
	var items []string
	if err := node.Decode(&items); err != nil {
		return err
	}
	*c = items
	return nil
}

// labelMap reads compose labels given as a mapping or as a KEY=VALUE list.
type labelMap map[string]string

func (l *labelMap) UnmarshalYAML(node *yaml.Node) error {
	return (*environment)(l).UnmarshalYAML(node)
}

type override struct {
	Services map[string]struct {
		Command     commandArgs    `yaml:"command"`
		Environment environment    `yaml:"environment"`
		Labels      labelMap       `yaml:"labels"`
		Profiles    []string       `yaml:"profiles"`
		Other       map[string]any `yaml:",inline"`
	} `yaml:"services"`
}

func load(t *testing.T) override {
	t.Helper()
	var o override
	// compose's `!override` tag is read as the value it wraps.
	if err := yaml.Unmarshal([]byte(read(t, filepath.Join(toolsDir(t), composeName))), &o); err != nil {
		t.Fatalf("parse the override: %v", err)
	}
	return o
}

func TestGoAPIGetsTheBillingListenerAndTheRouterLabels(t *testing.T) {
	goAPI, ok := load(t).Services["go-api"]
	if !ok {
		t.Fatal("the override has no go-api service")
	}
	if want := []string{"api", "--api-billing-edge-addr=:8010"}; !reflect.DeepEqual([]string(goAPI.Command), want) {
		t.Fatalf("go-api command = %v, want %v", goAPI.Command, want)
	}
	want := map[string]string{
		"traefik.enable":                                            "true",
		"traefik.scope":                                             "dev-health",
		"traefik.http.routers.billing-go.rule":                      "Host(`billing.localhost`)",
		"traefik.http.routers.billing-go.entrypoints":               "web",
		"traefik.http.routers.billing-go.service":                   "billing-go",
		"traefik.http.services.billing-go.loadbalancer.server.port": "8010",
	}
	if !reflect.DeepEqual(map[string]string(goAPI.Labels), want) {
		t.Fatalf("go-api labels = %v, want %v", goAPI.Labels, want)
	}
}

func TestTheThreeValuesAreNameOnlyPassThrough(t *testing.T) {
	env := load(t).Services["go-api"].Environment
	names := []string{"STRIPE_SECRET_KEY", "STRIPE_WEBHOOK_SECRET", "LICENSE_PRIVATE_KEY"}
	for _, name := range names {
		if got, want := env[name], "${"+name+":-}"; got != want {
			t.Fatalf("%s = %q, want the name-only pass-through %q", name, got, want)
		}
	}
}

func TestThePythonBillingEdgeServiceIsGone(t *testing.T) {
	if _, ok := load(t).Services["billing-edge"]; ok {
		t.Fatal("compose.bigboy.yml defines the Python billing-edge service again")
	}
	entries, err := os.ReadDir(toolsDir(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || entry.Name() == composeName {
			continue
		}
		if strings.Contains(read(t, filepath.Join(toolsDir(t), entry.Name())), "retired-billing-edge") && !strings.HasSuffix(entry.Name(), ".md") {
			t.Fatalf("%s enables or names the retired profile", entry.Name())
		}
	}
}

// The billing listener and the router labels are part of the one compose file the cut runs, so go-api is never
// recreated without them (CHAOS-9962: there is no chain order left to get wrong).
func TestTheCutRunsTheOneComposeFileWithTheBillingListener(t *testing.T) {
	cut := read(t, filepath.Join(toolsDir(t), "bigboy-cut.sh"))
	if !strings.Contains(cut, `-f "$HERE/compose.bigboy.yml"`) {
		t.Fatal("bigboy-cut.sh does not run compose.bigboy.yml")
	}
	if _, ok := load(t).Services["go-api"]; !ok {
		t.Fatal("compose.bigboy.yml has no go-api service")
	}
}

func TestNoSecretValueIsWrittenInTheFile(t *testing.T) {
	text := read(t, filepath.Join(toolsDir(t), composeName))
	if regexp.MustCompile(`sk_(live|test)_|whsec_|-----BEGIN`).MatchString(text) {
		t.Fatal("the override holds a secret-shaped value")
	}
}

func TestTheListenerPortIsThePortTraefikRoutesTo(t *testing.T) {
	goAPI := load(t).Services["go-api"]
	var flagPort string
	for _, argument := range goAPI.Command {
		if value, ok := strings.CutPrefix(argument, "--api-billing-edge-addr=:"); ok {
			flagPort = value
		}
	}
	routed := goAPI.Labels["traefik.http.services.billing-go.loadbalancer.server.port"]
	if flagPort == "" || flagPort != routed || flagPort != "8010" {
		t.Fatalf("billing-edge listener port %q, traefik routes to %q: both must be 8010", flagPort, routed)
	}
}
