package devhealth_test

import (
	"strings"
	"testing"
)

// CHAOS-11858: AUTH_PROXY_ORG_PROVISIONING, the go-api switch that sends an
// organization reactivation (PATCH is_active=true) to auth-service first. It
// has its own chart value, goApi.authProxy.orgProvisioning, because the chart
// refuses every AUTH_PROXY_* name through goApi.extraEnv. Off, it renders
// nothing: the manifests are what they were before the value existed.

const orgProvisioningEnv = "AUTH_PROXY_ORG_PROVISIONING"

func orgProvisioningArgs(phase string, sets ...string) []string {
	return goAPIWithAuth(append(append([]string{"--set", "authService.phase=" + phase}, phase3Secrets...), sets...)...)
}

func TestOrgProvisioningOffRendersByteEqualToNoValueAtAllPhases(t *testing.T) {
	for _, phase := range []string{"0", "1", "2", "3"} {
		base, baseOut, err := renderAuth(t, orgProvisioningArgs(phase)...)
		if err != nil {
			t.Fatalf("phase %s: render failed: %v\n%.400s", phase, err, baseOut)
		}
		_, offOut, err := renderAuth(t, orgProvisioningArgs(phase, "--set", "goApi.authProxy.orgProvisioning=false")...)
		if err != nil {
			t.Fatalf("phase %s, explicit off: render failed: %v\n%.400s", phase, err, offOut)
		}
		if offOut != baseOut {
			t.Errorf("phase %s: the render with orgProvisioning=false differs from the default render", phase)
		}
		if env := proxyEnv(t, base); env[orgProvisioningEnv] != "" {
			t.Errorf("phase %s: the default render sets %s=%q", phase, orgProvisioningEnv, env[orgProvisioningEnv])
		}
		if strings.Contains(baseOut, orgProvisioningEnv) {
			t.Errorf("phase %s: the default render mentions %s", phase, orgProvisioningEnv)
		}
	}
}

func TestOrgProvisioningOnRendersTheSwitchTrueAndNothingElse(t *testing.T) {
	for _, phase := range []string{"2", "3"} {
		_, offOut, err := renderAuth(t, orgProvisioningArgs(phase)...)
		if err != nil {
			t.Fatalf("phase %s: render failed: %v\n%.400s", phase, err, offOut)
		}
		docs, onOut, err := renderAuth(t, orgProvisioningArgs(phase, "--set", "goApi.authProxy.orgProvisioning=true")...)
		if err != nil {
			t.Fatalf("phase %s, on: render failed: %v\n%.400s", phase, err, onOut)
		}
		if got := proxyEnv(t, docs)[orgProvisioningEnv]; got != "true" {
			t.Fatalf("phase %s: %s = %q, want \"true\"", phase, orgProvisioningEnv, got)
		}
		off, on := strings.Split(offOut, "\n"), strings.Split(onOut, "\n")
		if len(on) != len(off)+2 {
			t.Fatalf("phase %s: the switch changed %d lines, want exactly the 2 of one env entry", phase, len(on)-len(off))
		}
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
			if !strings.Contains(line, orgProvisioningEnv) && strings.TrimSpace(line) != `value: "true"` {
				t.Errorf("phase %s: the switch added the line %q", phase, line)
			}
		}
		if extra != 2 {
			t.Errorf("phase %s: %d added lines, want 2", phase, extra)
		}
		if token := sessionsContainerEnv(t, docs, "Deployment", "a-dev-health-go-api")["AUTH_SESSION_ISSUE_TOKEN"]; token == nil {
			t.Errorf("phase %s: the switch is on and go-api has no AUTH_SESSION_ISSUE_TOKEN", phase)
		}
	}
}

func TestOrgProvisioningRefusals(t *testing.T) {
	for name, tc := range map[string]struct {
		args []string
		want string
	}{
		"on at phase 0": {goAPIWithAuth("--set", "goApi.authProxy.orgProvisioning=true"),
			"goApi.authProxy.orgProvisioning needs authService.phase 2 or higher"},
		"on at phase 1": {goAPIWithAuth("--set", "authService.phase=1", "--set", "goApi.authProxy.orgProvisioning=true"),
			"goApi.authProxy.orgProvisioning needs authService.phase 2 or higher"},
		"a string": {orgProvisioningArgs("2", "--set-string", "goApi.authProxy.orgProvisioning=true"),
			"goApi.authProxy.orgProvisioning must be true or false"},
		"a number": {orgProvisioningArgs("2", "--set", "goApi.authProxy.orgProvisioning=1"),
			"goApi.authProxy.orgProvisioning must be true or false"},
		"another key beside it": {orgProvisioningArgs("2", "--set", "goApi.authProxy.orgProvisioning=true", "--set", "goApi.authProxy.sessions=true"),
			"goApi.authProxy is replaced by authService.phase"},
		"extraEnv": {orgProvisioningArgs("2", "--set", "goApi.extraEnv[0].name="+orgProvisioningEnv, "--set", "goApi.extraEnv[0].value=true"),
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
