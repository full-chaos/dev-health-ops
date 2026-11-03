package devhealth_test

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
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
