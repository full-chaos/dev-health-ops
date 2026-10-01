// Package bigboycompose pins bigboy's compose overlays (ci/bigboy). Test-only.
package bigboycompose_test

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// CHAOS-7055: bigboy serves the billing host from go-api's billing-edge listener and retires the Python
// `billing-edge` service. The override is in the cut's compose chain (so go-api is never recreated without the
// listener and the router labels), carries the three Stripe/license values only as name-only pass-through (no
// value is written in the file), and moves the Python service to a profile nothing enables. Nothing here reads
// a secret.

func toolsDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve the package path")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "ci", "bigboy")
}

func read(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

type override struct {
	Services map[string]struct {
		Command     []string          `yaml:"command"`
		Environment map[string]string `yaml:"environment"`
		Labels      map[string]string `yaml:"labels"`
		Profiles    []string          `yaml:"profiles"`
		Other       map[string]any    `yaml:",inline"`
	} `yaml:"services"`
}

func load(t *testing.T) override {
	t.Helper()
	var o override
	// compose's `!override` tag is read as the value it wraps.
	if err := yaml.Unmarshal([]byte(read(t, filepath.Join(toolsDir(t), "compose.bigboy.billing-edge.yml"))), &o); err != nil {
		t.Fatalf("parse the override: %v", err)
	}
	return o
}

func TestGoAPIGetsTheBillingListenerAndTheRouterLabels(t *testing.T) {
	goAPI, ok := load(t).Services["go-api"]
	if !ok {
		t.Fatal("the override has no go-api service")
	}
	if want := []string{"api", "--api-billing-edge-addr=:8010"}; !reflect.DeepEqual(goAPI.Command, want) {
		t.Fatalf("go-api command = %v, want %v", goAPI.Command, want)
	}
	want := map[string]string{
		"traefik.enable":                                         "true",
		"traefik.scope":                                          "dev-health",
		"traefik.http.routers.billing.rule":                      "Host(`billing.localhost`)",
		"traefik.http.routers.billing.entrypoints":               "web",
		"traefik.http.routers.billing.service":                   "billing",
		"traefik.http.services.billing.loadbalancer.server.port": "8010",
	}
	if !reflect.DeepEqual(goAPI.Labels, want) {
		t.Fatalf("go-api labels = %v, want %v", goAPI.Labels, want)
	}
}

func TestTheThreeValuesAreNameOnlyPassThrough(t *testing.T) {
	env := load(t).Services["go-api"].Environment
	names := []string{"STRIPE_SECRET_KEY", "STRIPE_WEBHOOK_SECRET", "LICENSE_PRIVATE_KEY"}
	if len(env) != len(names) {
		t.Fatalf("go-api environment = %v, want exactly %v", env, names)
	}
	for _, name := range names {
		if got, want := env[name], "${"+name+":-}"; got != want {
			t.Fatalf("%s = %q, want the name-only pass-through %q", name, got, want)
		}
	}
}

func TestThePythonBillingEdgeServiceIsRetiredByProfile(t *testing.T) {
	edge, ok := load(t).Services["billing-edge"]
	if !ok {
		t.Fatal("the override does not retire billing-edge")
	}
	if !reflect.DeepEqual(edge.Profiles, []string{"retired-billing-edge"}) || len(edge.Command)+len(edge.Environment)+len(edge.Labels)+len(edge.Other) != 0 {
		t.Fatalf("billing-edge = %+v, want only the profile retired-billing-edge", edge)
	}
	entries, err := os.ReadDir(toolsDir(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || entry.Name() == "compose.bigboy.billing-edge.yml" {
			continue
		}
		if strings.Contains(read(t, filepath.Join(toolsDir(t), entry.Name())), "retired-billing-edge") && !strings.HasSuffix(entry.Name(), ".md") {
			t.Fatalf("%s enables or names the retired profile", entry.Name())
		}
	}
}

func TestTheOverrideIsInTheCutChainBeforeTheRouter(t *testing.T) {
	var chain string
	for _, line := range strings.Split(read(t, filepath.Join(toolsDir(t), "bigboy-cut.sh")), "\n") {
		if strings.HasPrefix(line, "export COMPOSE_FILE=") {
			chain = strings.TrimPrefix(line, "export COMPOSE_FILE=")
		}
	}
	if chain == "" {
		t.Fatal("the cut exports no COMPOSE_FILE")
	}
	files := strings.Split(strings.TrimSpace(chain), ":")
	index := func(name string) int {
		for i, file := range files {
			if file == name {
				return i
			}
		}
		return -1
	}
	billing, router := index("$HERE/compose.bigboy.billing-edge.yml"), index("$HERE/compose.bigboy.router.yml")
	if billing < 0 || router < 0 || billing >= router || router != len(files)-1 {
		t.Fatalf("billing override at %d, router at %d of %d: the override must be in the chain before the router, which stays last (%v)", billing, router, len(files), files)
	}
}

func TestNoSecretValueIsWrittenInTheFile(t *testing.T) {
	text := read(t, filepath.Join(toolsDir(t), "compose.bigboy.billing-edge.yml"))
	if regexp.MustCompile(`sk_(live|test)_|whsec_|-----BEGIN`).MatchString(text) {
		t.Fatal("the override holds a secret-shaped value")
	}
}
