package devhealth_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const (
	orgsOwnedRemoved   = "AUTH_ORGS_OWNED is removed (D6651)"
	orgsOwnedPhase5Msg = "phase 5 (orgs owned) is cancelled (D6651)"
)

// The chart refuses AUTH_ORGS_OWNED with any value in every extraEnv list, not only auth-service's.
func TestOrgsOwnedEnvIsRefusedInEveryExtraEnv(t *testing.T) {
	for _, path := range []string{"goApi", "authService", "authService.backfill", "queryApi", "web", "migrations.hook"} {
		for _, value := range []string{"true", "false", ""} {
			t.Run(fmt.Sprintf("%s value %q", path, value), func(t *testing.T) {
				entry := fmt.Sprintf(`[{"name":"AUTH_ORGS_OWNED","value":%q}]`, value)
				_, out, err := renderAuth(t, phaseArgs(1, "--set-json", path+".extraEnv="+entry)...)
				if err == nil || !strings.Contains(out, orgsOwnedRemoved) {
					t.Fatalf("rendered: err=%v out=%.400s", err, out)
				}
			})
		}
	}
	entry := `[{"name":"AUTH_ORGS_OWNED","valueFrom":{"secretKeyRef":{"name":"s","key":"k"}}}]`
	_, out, err := renderAuth(t, phaseArgs(1, "--set-json", "goApi.extraEnv="+entry)...)
	if err == nil || !strings.Contains(out, orgsOwnedRemoved) {
		t.Fatalf("a secretKeyRef entry rendered: err=%v out=%.400s", err, out)
	}
}

// Every default goWorkers group refuses AUTH_ORGS_OWNED in its own extraEnv: groups is a list of maps, not a map.
func TestOrgsOwnedEnvIsRefusedInEveryWorkerGroup(t *testing.T) {
	raw, err := os.ReadFile("values.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var values struct {
		GoWorkers struct {
			Groups []map[string]any `yaml:"groups"`
		} `yaml:"goWorkers"`
	}
	if err := yaml.Unmarshal(raw, &values); err != nil {
		t.Fatal(err)
	}
	groups := values.GoWorkers.Groups
	if len(groups) < 9 {
		t.Fatalf("expected the default worker groups, got %d", len(groups))
	}
	entries := map[string]string{
		"true":            `[{"name":"AUTH_ORGS_OWNED","value":"true"}]`,
		"false":           `[{"name":"AUTH_ORGS_OWNED","value":"false"}]`,
		"empty":           `[{"name":"AUTH_ORGS_OWNED","value":""}]`,
		"secretKeyRef":    `[{"name":"AUTH_ORGS_OWNED","valueFrom":{"secretKeyRef":{"name":"s","key":"k"}}}]`,
		"lowercase+space": `[{"name":" auth_orgs_owned ","value":"true"}]`,
	}
	for i, group := range groups {
		for form, entry := range entries {
			t.Run(fmt.Sprintf("group %v %s", group["name"], form), func(t *testing.T) {
				var list []any
				if err := json.Unmarshal([]byte(entry), &list); err != nil {
					t.Fatal(err)
				}
				mutated := make([]map[string]any, len(groups))
				for j, g := range groups {
					mutated[j] = g
				}
				withEnv := map[string]any{}
				for k, v := range group {
					withEnv[k] = v
				}
				withEnv["extraEnv"] = list
				mutated[i] = withEnv
				encoded, err := json.Marshal(mutated)
				if err != nil {
					t.Fatal(err)
				}
				_, out, err := renderAuth(t, phaseArgs(1, "--set-json", "goWorkers.groups="+string(encoded))...)
				if err == nil || !strings.Contains(out, orgsOwnedRemoved) || !strings.Contains(out, fmt.Sprintf("goWorkers.groups[%d].extraEnv", i)) {
					t.Fatalf("rendered: err=%v out=%.400s", err, out)
				}
			})
		}
	}
}

// Phase 5 is cancelled, not waiting: the refusal says so and names the decision.
func TestOrgsOwnedPhase5IsCancelled(t *testing.T) {
	_, out, err := renderAuth(t, phaseArgs(3, "--set", "authService.phase=5")...)
	if err == nil || !strings.Contains(out, orgsOwnedPhase5Msg) || strings.Contains(out, "writer leaf") {
		t.Fatalf("phase 5: err=%v out=%.400s", err, out)
	}
	_, out, err = renderAuth(t, withoutPhase(phaseArgs(1), "--set-json", "authService.phase=null")...)
	if err == nil || !strings.Contains(out, "whole number from 0 to 4") {
		t.Fatalf("phase null: err=%v out=%.400s", err, out)
	}
}

// ssoJit=true has no supported phase until CHAOS-12678 re-homes it.
func TestSSOJITTrueIsRefusedUntil12678(t *testing.T) {
	const on = "authService.ssoJit=true"
	for name, args := range map[string][]string{
		"phase 2":       phaseArgs(2, "--set", on),
		"phase 3":       phaseArgs(3, "--set", on),
		"phase 4 goApi": phaseArgs(4, "--set", "authService.usersOwnedStage=goApi", "--set", on),
		"phase 4 owned": phaseArgs(4, "--set", on),
	} {
		_, out, err := renderAuth(t, args...)
		if err == nil || !strings.Contains(out, "CHAOS-12678") {
			t.Errorf("%s: rendered: err=%v out=%.300s", name, err, out)
		}
	}
}

// No render at a phase the chart accepts carries AUTH_ORGS_OWNED, and no template still writes the env line or reads the ladder flag.
func TestOrgsOwnedHasNoRenderedOrTemplatedConsumer(t *testing.T) {
	for phase := 0; phase <= 4; phase++ {
		_, out, err := renderAuth(t, phaseArgs(phase)...)
		if err != nil {
			t.Fatalf("phase %d: %v\n%.300s", phase, err, out)
		}
		if strings.Contains(out, "AUTH_ORGS_OWNED") {
			t.Errorf("phase %d render names AUTH_ORGS_OWNED", phase)
		}
	}
	entries, err := filepath.Glob("templates/*")
	if err != nil {
		t.Fatal(err)
	}
	consumer := regexp.MustCompile(`- name: AUTH_ORGS_OWNED|"flag" "orgsOwned"`)
	for _, path := range entries {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if consumer.Match(raw) {
			t.Errorf("%s still renders or reads the orgs-owned switch", path)
		}
	}
}
