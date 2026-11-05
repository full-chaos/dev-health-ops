package devhealth_test

import (
	"strings"
	"testing"
)

// CHAOS-12914: AUTH_PROXY_IMPERSONATION, the go-api switch that starts and ends the admin impersonation session at
// auth-service (a delegated session). go-api alone only logs one ERROR line and sets
// devhealth_api_auth_proxy_impersonation_config_invalid when the switch is on without AUTH_PROXY_SESSIONS (D6465: no
// fail-start), so the chart refuses that render: the switch needs authService.phase 2 or higher. It has its own value,
// goApi.authProxy.impersonation, because the chart refuses every AUTH_PROXY_* name through goApi.extraEnv. Off, it
// renders nothing.

const impersonationEnv = "AUTH_PROXY_IMPERSONATION"

func impersonationArgs(phase string, sets ...string) []string {
	head := []string{"--set", "authService.phase=" + phase}
	switch phase {
	case "3":
		head = append(head, "--set", "authService.loginStage=goApi")
	case "4":
		head = append(head, "--set", "authService.usersOwnedStage=owned")
	}
	return goAPIWithAuth(append(append(head, phase3Secrets...), sets...)...)
}

func TestImpersonationOffRendersByteEqualToNoValueAtAllPhases(t *testing.T) {
	for _, phase := range []string{"0", "1", "2", "3", "4"} {
		base, baseOut, err := renderAuth(t, impersonationArgs(phase)...)
		if err != nil {
			t.Fatalf("phase %s: render failed: %v\n%.400s", phase, err, baseOut)
		}
		_, offOut, err := renderAuth(t, impersonationArgs(phase, "--set", "goApi.authProxy.impersonation=false")...)
		if err != nil {
			t.Fatalf("phase %s, explicit off: render failed: %v\n%.400s", phase, err, offOut)
		}
		if offOut != baseOut {
			t.Errorf("phase %s: the render with impersonation=false differs from the default render", phase)
		}
		if env := proxyEnv(t, base); env[impersonationEnv] != "" {
			t.Errorf("phase %s: the default render sets %s=%q", phase, impersonationEnv, env[impersonationEnv])
		}
		if strings.Contains(baseOut, impersonationEnv) {
			t.Errorf("phase %s: the default render mentions %s", phase, impersonationEnv)
		}
	}
}

func TestImpersonationOnRendersTheSwitchTrueAndNothingElse(t *testing.T) {
	for _, phase := range []string{"2", "3", "4"} {
		_, offOut, err := renderAuth(t, impersonationArgs(phase)...)
		if err != nil {
			t.Fatalf("phase %s: render failed: %v\n%.400s", phase, err, offOut)
		}
		docs, onOut, err := renderAuth(t, impersonationArgs(phase, "--set", "goApi.authProxy.impersonation=true")...)
		if err != nil {
			t.Fatalf("phase %s, on: render failed: %v\n%.400s", phase, err, onOut)
		}
		env := proxyEnv(t, docs)
		if got := env[impersonationEnv]; got != "true" {
			t.Fatalf("phase %s: %s = %q, want \"true\"", phase, impersonationEnv, got)
		}
		if got := env["AUTH_PROXY_SESSIONS"]; got != "true" {
			t.Fatalf("phase %s: the switch is on and AUTH_PROXY_SESSIONS = %q", phase, got)
		}
		off, on := strings.Split(offOut, "\n"), strings.Split(onOut, "\n")
		seen := map[string]int{}
		for _, line := range off {
			seen[line]++
		}
		extra := 0
		for _, line := range on {
			if seen[line] > 0 {
				seen[line]--
				continue
			}
			extra++
			if !strings.Contains(line, impersonationEnv) && strings.TrimSpace(line) != `value: "true"` {
				t.Errorf("phase %s: the switch added the line %q", phase, line)
			}
		}
		if extra != 2 || len(on) != len(off)+2 {
			t.Errorf("phase %s: the switch added %d lines (%d in total), want exactly the 2 of one env entry", phase, extra, len(on)-len(off))
		}
	}
}

func TestImpersonationRefusals(t *testing.T) {
	for name, tc := range map[string]struct {
		args []string
		want string
	}{
		"on at phase 0": {goAPIWithAuth("--set", "goApi.authProxy.impersonation=true"),
			"goApi.authProxy.impersonation needs authService.phase 2"},
		"on at phase 1": {goAPIWithAuth("--set", "authService.phase=1", "--set", "goApi.authProxy.impersonation=true"),
			"goApi.authProxy.impersonation needs authService.phase 2"},
		"a string": {impersonationArgs("2", "--set-string", "goApi.authProxy.impersonation=true"),
			"goApi.authProxy.impersonation must be true or false"},
		"a number": {impersonationArgs("2", "--set", "goApi.authProxy.impersonation=1"),
			"goApi.authProxy.impersonation must be true or false"},
		"null": {impersonationArgs("2", "--set", "goApi.authProxy.impersonation=null", "--set", "goApi.authProxy.orgProvisioning=false"),
			"goApi.authProxy.impersonation must be true or false"},
		"another key beside it": {impersonationArgs("2", "--set", "goApi.authProxy.impersonation=true", "--set", "goApi.authProxy.sessions=true"),
			"goApi.authProxy is replaced by authService.phase"},
		"a near-miss key": {impersonationArgs("2", "--set", "goApi.authProxy.Impersonation=true"),
			"goApi.authProxy is replaced by authService.phase"},
		"extraEnv": {impersonationArgs("2", "--set", "goApi.extraEnv[0].name="+impersonationEnv, "--set", "goApi.extraEnv[0].value=true"),
			"goApi.extraEnv must not set AUTH_PROXY_*"},
	} {
		t.Run(name, func(t *testing.T) {
			_, out, err := renderAuth(t, tc.args...)
			if err == nil {
				t.Fatal("the render succeeded; it must refuse")
			}
			if !strings.Contains(out, tc.want) {
				t.Fatalf("the refusal does not say %q: %.400s", tc.want, out)
			}
		})
	}
}

// The two goApi.authProxy keys are independent: both on at phase 4 render both switches.
func TestImpersonationBesideOrgProvisioning(t *testing.T) {
	docs, out, err := renderAuth(t, impersonationArgs("4", append([]string{"--set", "goApi.authProxy.impersonation=true", "--set", "goApi.authProxy.orgProvisioning=true"}, membershipRoutesOn...)...)...)
	if err != nil {
		t.Fatalf("both keys on: render failed: %v\n%.400s", err, out)
	}
	env := proxyEnv(t, docs)
	if env[impersonationEnv] != "true" || env[orgProvisioningEnv] != "true" {
		t.Fatalf("%s=%q %s=%q, want both \"true\"", impersonationEnv, env[impersonationEnv], orgProvisioningEnv, env[orgProvisioningEnv])
	}
}
