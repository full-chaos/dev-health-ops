package devhealth_test

import (
	"os/exec"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// go-api reports a wide TRUSTED_PROXIES as fenced only when the chart says the request ports are fenced:
// the marker renders exactly where the fence renders, and nothing else can set it.

type envEntry struct {
	Name  string `yaml:"name"`
	Value string `yaml:"value"`
}

type deploymentDoc struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec struct {
		Template struct {
			Spec struct {
				Containers []struct {
					Name string     `yaml:"name"`
					Env  []envEntry `yaml:"env"`
				} `yaml:"containers"`
			} `yaml:"spec"`
		} `yaml:"template"`
	} `yaml:"spec"`
}

func goAPIEnv(t *testing.T, args ...string) []envEntry {
	t.Helper()
	full := append([]string{"template", "marker", ".", "--namespace", releaseNamespace, "--set", "goApi.enabled=true",
		"--set", "goWorkers.pgbouncer.postgres.networkPolicyCIDR=10.0.0.0/24"}, args...)
	out, err := exec.Command("helm", full...).CombinedOutput()
	if err != nil {
		t.Fatalf("render: %v\n%s", err, out)
	}
	for _, document := range strings.Split(string(out), "\n---") {
		var parsed deploymentDoc
		if err := yaml.Unmarshal([]byte(document), &parsed); err != nil {
			t.Fatalf("a document of the render does not parse: %v", err)
		}
		if parsed.Kind == "Deployment" && strings.HasSuffix(parsed.Metadata.Name, "-go-api") && len(parsed.Spec.Template.Spec.Containers) > 0 {
			return parsed.Spec.Template.Spec.Containers[0].Env
		}
	}
	t.Fatal("no go-api Deployment in the render")
	return nil
}

func markerValues(env []envEntry) []string {
	var values []string
	for _, entry := range env {
		if entry.Name == "TRUSTED_PROXIES_NETWORK_FENCED" {
			values = append(values, entry.Value)
		}
	}
	return values
}

func TestFenceMarkerRendersOnlyWhereTheFenceRenders(t *testing.T) {
	if got := markerValues(goAPIEnv(t)); len(got) != 0 {
		t.Errorf("with the fence off the marker renders %v, want none", got)
	}
	got := markerValues(goAPIEnv(t, "--set", "goApi.networkPolicy.enabled=true"))
	if len(got) != 1 || got[0] != "true" {
		t.Errorf("with the fence on the marker renders %v, want exactly one \"true\"", got)
	}
}

func TestFenceMarkerCannotBeSetFromExtraEnv(t *testing.T) {
	for _, fence := range []string{"false", "true"} {
		out, err := exec.Command("helm", "template", "marker", ".", "--namespace", releaseNamespace, "--set", "goApi.enabled=true",
			"--set", "goApi.networkPolicy.enabled="+fence, "--set", "goWorkers.pgbouncer.postgres.networkPolicyCIDR=10.0.0.0/24",
			"--set-json", `goApi.extraEnv=[{"name":"TRUSTED_PROXIES_NETWORK_FENCED","value":"true"}]`).CombinedOutput()
		if err == nil {
			t.Fatalf("fence %s: the render accepted the marker in extraEnv", fence)
		}
		if !strings.Contains(string(out), "TRUSTED_PROXIES_NETWORK_FENCED") {
			t.Errorf("fence %s: the refusal does not name the marker:\n%s", fence, out)
		}
	}
}
