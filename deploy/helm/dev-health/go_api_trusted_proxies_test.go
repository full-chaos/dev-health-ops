package devhealth_test

import (
	"encoding/json"
	"os/exec"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/full-chaos/dev-health-ops/internal/auth/clientaddr"
)

// go-api reads TRUSTED_PROXIES (internal/api/clientip) with the auth-service parser, and the chart checks a
// goApi.extraEnv TRUSTED_PROXIES value with the same rules as authService.trustedProxies: the node pod range needs its
// node exclusions, and a value the chart renders is a value go-api parses (else go-api stops at start).

const goAPITrustedKey = "goApi.extraEnv TRUSTED_PROXIES"

func renderGoAPITrusted(t *testing.T, value string, extra ...string) (string, error) {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	env, err := json.Marshal([]map[string]string{{"name": "TRUSTED_PROXIES", "value": value}})
	if err != nil {
		t.Fatal(err)
	}
	args := append([]string{"template", "trusted", ".", "--namespace", releaseNamespace, "--set", "goApi.enabled=true",
		"--set", "goWorkers.pgbouncer.postgres.networkPolicyCIDR=10.0.0.0/24", "--set-json", "goApi.extraEnv=" + string(env)}, extra...)
	out, err := exec.Command("helm", args...).CombinedOutput()
	return string(out), err
}

func TestGoAPITrustedProxiesNeedTheNodeExclusions(t *testing.T) {
	for _, value := range []string{
		"10.42.0.0/24", "10.42.0.0/24,!10.42.0.0", "10.42.0.0/24,!10.42.0.1", "10.42.0.0/27,!10.42.0.1,!10.42.0.2",
		"10.42.0.6,10.42.0.0/25,!10.42.0.5",
	} {
		out, err := renderGoAPITrusted(t, value)
		if err == nil || !strings.Contains(out, goAPITrustedKey) || !strings.Contains(out, nodeExclusionsM) {
			t.Errorf("%q: err=%v, want the node exclusions refusal naming %s\n%.400s", value, err, goAPITrustedKey, out)
		}
	}
	for _, value := range []string{
		"10.42.0.0/24,!10.42.0.0,!10.42.0.1", " 10.42.0.0/24 , !10.42.0.1 , !10.42.0.0 ", "10.42.0.0/27,!10.42.0.0,!10.42.0.1",
		"10.42.0.6", "10.42.0.0/28", "10.42.0.0/28,!10.42.0.1", "", "2001:db8::/124", "10.42.0.6,192.0.2.1",
	} {
		out, err := renderGoAPITrusted(t, value)
		if err != nil {
			t.Errorf("%q: %v\n%.400s", value, err, out)
			continue
		}
		if !strings.Contains(out, "value: "+strings.TrimSpace(value)) && value != "" && !strings.HasPrefix(value, " ") {
			t.Errorf("%q: the value does not reach the go-api env verbatim", value)
		}
	}
}

func TestGoAPITrustedProxiesRefuseWhatGoAPIRefuses(t *testing.T) {
	for _, value := range []string{
		"bogus", "*", "10.42.0.0/24,", ",10.42.0.6", "0.0.0.0/0", "::/0", "::ffff:10.0.0.9", "::ffff:10.42.0.0/120", "10.0.0.1/8",
		"10.0.0.0/33", "fe80::1%eth0", "!10.42.0.1", "10.42.0.0/28,!10.43.0.1", "10.42.0.0/28,!10.42.0.1/32", "10.42.0.1,!10.42.0.1",
		"10.42.0.0/28,!fd00::1", "10.0.0.0/16", "fd00::/64", "fd00::/120",
	} {
		out, err := renderGoAPITrusted(t, value)
		if err == nil || !strings.Contains(out, goAPITrustedKey) {
			t.Errorf("%q: err=%v, want a refusal naming %s\n%.400s", value, err, goAPITrustedKey, out)
		}
	}
}

// Parity: every value the chart renders is a value go-api parses; the chart is stricter only on purpose (the node
// exclusions, the widths, an IPv6 exclusion it cannot place).
func TestGoAPITrustedProxyValuesRenderOnlyWhatGoAPIParses(t *testing.T) {
	var both, neither int
	for _, value := range []string{
		"10.42.0.0/24,!10.42.0.0,!10.42.0.1", "10.42.0.0/28,!10.42.0.1", "10.9.0.5,10.42.0.0/30,!10.42.0.3", "10.42.0.6",
		"10.42.0.0/24", "10.42.0.0/24,!10.42.0.0", "fd00::/124,!fd00::1", "10.42.0.0/28,!::ffff:10.42.0.1",
		"10.42.0.0/28,!10.43.0.1", "10.42.0.1,!10.42.0.1", "10.42.0.0/28,!", "10.42.0.0/28,!!10.42.0.1", "!10.42.0.1",
		"bogus", "10.42.0.0/24,", "0.0.0.0/0", "::/0", "::ffff:10.0.0.9", "10.0.0.1/8", "fe80::1%eth0", "10.0.0.0/8",
	} {
		_, parseErr := clientaddr.ParseTrust(value)
		_, renderErr := renderGoAPITrusted(t, value)
		switch {
		case renderErr == nil && parseErr != nil:
			t.Errorf("%q renders but go-api refuses it (%v): go-api would not start", value, parseErr)
		case renderErr == nil:
			both++
		case parseErr != nil:
			neither++
		}
	}
	if both < 3 || neither < 8 {
		t.Fatalf("the table does not exercise both outcomes: %d accepted by both, %d refused by both", both, neither)
	}
}

// A value the chart cannot read (valueFrom) is the deploy chart's "unreadable" rule, not a render refusal here: it
// renders, and the go-api env keeps the reference whole (its kind, name and key), with no value beside it.
func TestGoAPITrustedProxiesFromAReferenceRender(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	for _, ref := range []struct{ kind, name, key string }{
		{"configMapKeyRef", "x", "y"},
		{"secretKeyRef", "s", "k"},
	} {
		t.Run(ref.kind, func(t *testing.T) {
			extra, err := json.Marshal([]map[string]any{{"name": "TRUSTED_PROXIES",
				"valueFrom": map[string]any{ref.kind: map[string]string{"name": ref.name, "key": ref.key}}}})
			if err != nil {
				t.Fatal(err)
			}
			out, err := exec.Command("helm", "template", "trusted", ".", "--namespace", releaseNamespace, "--set", "goApi.enabled=true",
				"--set", "goWorkers.pgbouncer.postgres.networkPolicyCIDR=10.0.0.0/24", "--set-json", "goApi.extraEnv="+string(extra)).CombinedOutput()
			if err != nil {
				t.Fatalf("render: %v\n%.400s", err, out)
			}
			found := goAPITrustedEnv(t, string(out))
			if len(found) != 1 {
				t.Fatalf("the go-api env has %d TRUSTED_PROXIES entries, want exactly the one reference: %v", len(found), found)
			}
			if _, hasValue := found[0]["value"]; hasValue {
				t.Fatalf("TRUSTED_PROXIES carries a value beside the reference: %v", found[0])
			}
			from, _ := found[0]["valueFrom"].(map[string]any)
			got, _ := from[ref.kind].(map[string]any)
			if len(from) != 1 || len(got) != 2 || got["name"] != ref.name || got["key"] != ref.key {
				t.Fatalf("TRUSTED_PROXIES valueFrom = %v, want only %s {name: %s, key: %s}", found[0]["valueFrom"], ref.kind, ref.name, ref.key)
			}
		})
	}
}

// goAPITrustedEnv is every TRUSTED_PROXIES entry of the go-api container env in a render.
func goAPITrustedEnv(t *testing.T, render string) []map[string]any {
	t.Helper()
	var found []map[string]any
	for _, document := range strings.Split(render, "\n---") {
		var doc struct {
			Kind     string `yaml:"kind"`
			Metadata struct {
				Name string `yaml:"name"`
			} `yaml:"metadata"`
			Spec struct {
				Template struct {
					Spec struct {
						Containers []struct {
							Env []map[string]any `yaml:"env"`
						} `yaml:"containers"`
					} `yaml:"spec"`
				} `yaml:"template"`
			} `yaml:"spec"`
		}
		if err := yaml.Unmarshal([]byte(document), &doc); err != nil {
			t.Fatalf("a document of the render does not parse: %v", err)
		}
		if doc.Kind != "Deployment" || !strings.HasSuffix(doc.Metadata.Name, "-go-api") || len(doc.Spec.Template.Spec.Containers) == 0 {
			continue
		}
		for _, entry := range doc.Spec.Template.Spec.Containers[0].Env {
			if entry["name"] == "TRUSTED_PROXIES" {
				found = append(found, entry)
			}
		}
	}
	return found
}
