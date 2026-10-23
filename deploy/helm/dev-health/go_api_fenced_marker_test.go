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

func TestFenceMarkerIsTrueOnlyWhereTheFenceRenders(t *testing.T) {
	if got := markerValues(goAPIEnv(t)); len(got) != 1 || got[0] != "false" {
		t.Errorf("with the fence off the marker renders %v, want exactly one \"false\" (an env entry beats the same name from envFrom)", got)
	}
	got := markerValues(goAPIEnv(t, "--set", "goApi.networkPolicy.enabled=true"))
	if len(got) != 1 || got[0] != "true" {
		t.Errorf("with the fence on the marker renders %v, want exactly one \"true\"", got)
	}
}

// The marker and the policy come from one condition: for each setting, the marker is "true" exactly when
// the request port is closed to a stranger by the rendered policy.
func TestFenceMarkerAndThePolicyShareOneCondition(t *testing.T) {
	for name, c := range map[string]struct {
		args  []string
		fence bool
	}{
		"fence off":                 {nil, false},
		"fence on":                  {[]string{"--set", "goApi.networkPolicy.enabled=true"}, true},
		"fence on, extra port":      {[]string{"--set", "goApi.networkPolicy.enabled=true", "--set-json", "goApi.networkPolicy.extraFencedPorts=[8010]"}, true},
		"fence off, extra port set": {[]string{"--set-json", "goApi.networkPolicy.extraFencedPorts=[8010]"}, false},
	} {
		t.Run(name, func(t *testing.T) {
			marker := markerValues(goAPIEnv(t, c.args...))
			full := append([]string{"template", "fence", ".", "--namespace", releaseNamespace, "--set", "goApi.enabled=true",
				"--set", "goWorkers.pgbouncer.postgres.networkPolicyCIDR=10.0.0.0/24"}, c.args...)
			out, err := exec.Command("helm", full...).CombinedOutput()
			if err != nil {
				t.Fatalf("render: %v\n%s", err, out)
			}
			var policies []netpolDoc
			for _, document := range strings.Split(string(out), "\n---") {
				var parsed netpolDoc
				if err := yaml.Unmarshal([]byte(document), &parsed); err != nil {
					t.Fatalf("parse: %v", err)
				}
				if parsed.Kind == "NetworkPolicy" {
					policies = append(policies, parsed)
				}
			}
			closedToStranger := false
			for _, port := range closedPorts(t, policies, sourceStranger) {
				if port == 8000 {
					closedToStranger = true
				}
			}
			if closedToStranger != c.fence {
				t.Fatalf("the request port is closed to a stranger = %v, want %v", closedToStranger, c.fence)
			}
			want := "false"
			if closedToStranger {
				want = "true"
			}
			if len(marker) != 1 || marker[0] != want {
				t.Errorf("marker %v with the request port closed = %v, want exactly one %q", marker, closedToStranger, want)
			}
		})
	}
}

func TestFenceMarkerCannotBeSetFromExtraEnv(t *testing.T) {
	for name, extra := range map[string][]string{
		"fence off":             nil,
		"fence on":              {"--set", "goApi.networkPolicy.enabled=true"},
		"internal listener off": {"--set", "goApi.internal.enabled=false"},
	} {
		t.Run(name, func(t *testing.T) {
			args := append([]string{"template", "marker", ".", "--namespace", releaseNamespace, "--set", "goApi.enabled=true",
				"--set", "goWorkers.pgbouncer.postgres.networkPolicyCIDR=10.0.0.0/24",
				"--set-json", `goApi.extraEnv=[{"name":"TRUSTED_PROXIES_NETWORK_FENCED","value":"true"}]`}, extra...)
			out, err := exec.Command("helm", args...).CombinedOutput()
			if err == nil {
				t.Fatal("the render accepted the marker in extraEnv")
			}
			if !strings.Contains(string(out), "TRUSTED_PROXIES_NETWORK_FENCED") {
				t.Errorf("the refusal does not name the marker:\n%s", out)
			}
		})
	}
}
