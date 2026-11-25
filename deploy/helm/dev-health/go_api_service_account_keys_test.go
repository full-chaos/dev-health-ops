package devhealth_test

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/apiservice"
)

// CHAOS-14689: AUTH_PROXY_SERVICE_ACCOUNT_KEYS, the go-api switch of the admin service account key and lifecycle
// routes (go-api calls auth-service with its workload token). It needs AUTH_PROXY_URL, which the chart renders only
// from authService.phase 2, and auth-service's key routes (AUTH_MEMBERSHIP_ROUTES=true), so the render refuses it
// without them. It has its own value, goApi.authProxy.serviceAccountKeys, because the chart refuses every AUTH_PROXY_*
// name through goApi.extraEnv. Off, it renders nothing.

const serviceAccountKeysEnv = "AUTH_PROXY_SERVICE_ACCOUNT_KEYS"

func serviceAccountKeysArgs(phase string, sets ...string) []string {
	return impersonationArgs(phase, sets...)
}

func TestServiceAccountKeysOffRendersByteEqualToNoValueAtAllPhases(t *testing.T) {
	t.Parallel()

	for _, phase := range []string{"0", "1", "2", "3", "4"} {
		base, baseOut, err := renderAuth(t, serviceAccountKeysArgs(phase)...)
		if err != nil {
			t.Fatalf("phase %s: render failed: %v\n%.400s", phase, err, baseOut)
		}
		_, offOut, err := renderAuth(t, serviceAccountKeysArgs(phase, "--set", "goApi.authProxy.serviceAccountKeys=false")...)
		if err != nil {
			t.Fatalf("phase %s, explicit off: render failed: %v\n%.400s", phase, err, offOut)
		}
		if offOut != baseOut {
			t.Errorf("phase %s: the render with serviceAccountKeys=false differs from the default render", phase)
		}
		if env := proxyEnv(t, base); env[serviceAccountKeysEnv] != "" {
			t.Errorf("phase %s: the default render sets %s=%q", phase, serviceAccountKeysEnv, env[serviceAccountKeysEnv])
		}
		if strings.Contains(baseOut, serviceAccountKeysEnv) {
			t.Errorf("phase %s: the default render mentions %s", phase, serviceAccountKeysEnv)
		}
	}
}

func TestServiceAccountKeysOnRendersTheSwitchTrueAndNothingElse(t *testing.T) {
	t.Parallel()

	for _, phase := range []string{"2", "3", "4"} {
		_, offOut, err := renderAuth(t, serviceAccountKeysArgs(phase, membershipRoutesOn...)...)
		if err != nil {
			t.Fatalf("phase %s: render failed: %v\n%.400s", phase, err, offOut)
		}
		docs, onOut, err := renderAuth(t, serviceAccountKeysArgs(phase, append([]string{"--set", "goApi.authProxy.serviceAccountKeys=true"}, membershipRoutesOn...)...)...)
		if err != nil {
			t.Fatalf("phase %s, on: render failed: %v\n%.400s", phase, err, onOut)
		}
		if got := proxyEnv(t, docs)[serviceAccountKeysEnv]; got != "true" {
			t.Fatalf("phase %s: %s = %q, want \"true\"", phase, serviceAccountKeysEnv, got)
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
			if !strings.Contains(line, serviceAccountKeysEnv) && strings.TrimSpace(line) != `value: "true"` {
				t.Errorf("phase %s: the switch added the line %q", phase, line)
			}
		}
		if extra != 2 || len(on) != len(off)+2 {
			t.Errorf("phase %s: the switch added %d lines (%d in total), want exactly the 2 of one env entry", phase, extra, len(on)-len(off))
		}
		goAPI := authDoc(docs, "Deployment", "a-dev-health-go-api")
		if goAPI == nil {
			t.Fatalf("phase %s: no go-api Deployment", phase)
		}
		proxy, err := apiservice.ParseAuthProxyConfig(containerLookup(t, goAPI, "go-api"))
		if err != nil {
			t.Fatalf("phase %s: go-api refuses the rendered proxy settings: %v", phase, err)
		}
		if !proxy.ServiceAccountKeysOn() {
			t.Errorf("phase %s: go-api reads the rendered switch as off", phase)
		}
	}
}

func TestServiceAccountKeysRefusals(t *testing.T) {
	t.Parallel()

	on := []string{"--set", "goApi.authProxy.serviceAccountKeys=true"}
	for name, tc := range map[string]struct {
		args []string
		want string
	}{
		"on at phase 0": {goAPIWithAuth(append(on, membershipRoutesOn...)...),
			"goApi.authProxy.serviceAccountKeys needs authService.phase 2"},
		"on at phase 1": {goAPIWithAuth(append(append([]string{"--set", "authService.phase=1"}, on...), membershipRoutesOn...)...),
			"goApi.authProxy.serviceAccountKeys needs authService.phase 2"},
		"on without membership routes": {serviceAccountKeysArgs("2", on...),
			"goApi.authProxy.serviceAccountKeys needs authService.extraEnv AUTH_MEMBERSHIP_ROUTES=true"},
		"on with membership routes false": {serviceAccountKeysArgs("2", "--set", "goApi.authProxy.serviceAccountKeys=true",
			"--set", "authService.extraEnv[0].name=AUTH_MEMBERSHIP_ROUTES", "--set-string", "authService.extraEnv[0].value=false"),
			"goApi.authProxy.serviceAccountKeys needs authService.extraEnv AUTH_MEMBERSHIP_ROUTES=true"},
		"a string": {serviceAccountKeysArgs("2", "--set-string", "goApi.authProxy.serviceAccountKeys=true"),
			"goApi.authProxy.serviceAccountKeys must be true or false"},
		"a number": {serviceAccountKeysArgs("2", "--set", "goApi.authProxy.serviceAccountKeys=1"),
			"goApi.authProxy.serviceAccountKeys must be true or false"},
		"null": {serviceAccountKeysArgs("2", "--set", "goApi.authProxy.serviceAccountKeys=null", "--set", "goApi.authProxy.impersonation=false"),
			"goApi.authProxy.serviceAccountKeys must be true or false"},
		"a near-miss key": {serviceAccountKeysArgs("2", "--set", "goApi.authProxy.ServiceAccountKeys=true"),
			"goApi.authProxy is replaced by authService.phase"},
		"extraEnv": {serviceAccountKeysArgs("2", "--set", "goApi.extraEnv[0].name="+serviceAccountKeysEnv, "--set-string", "goApi.extraEnv[0].value=true"),
			"goApi.extraEnv must not set AUTH_PROXY_*"},
		"on with go-api off": {serviceAccountKeysArgs("2", append(append([]string{"--set", "goApi.enabled=false"}, on...), membershipRoutesOn...)...),
			"goApi.authProxy.serviceAccountKeys needs goApi.enabled"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

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

// The three goApi.authProxy keys are independent: all on at phase 4 render all three switches.
func TestServiceAccountKeysBesideTheOtherAuthProxyKeys(t *testing.T) {
	t.Parallel()

	docs, out, err := renderAuth(t, serviceAccountKeysArgs("4", append([]string{"--set", "goApi.authProxy.serviceAccountKeys=true",
		"--set", "goApi.authProxy.impersonation=true", "--set", "goApi.authProxy.orgProvisioning=true"}, membershipRoutesOn...)...)...)
	if err != nil {
		t.Fatalf("all keys on: render failed: %v\n%.400s", err, out)
	}
	env := proxyEnv(t, docs)
	for _, name := range []string{serviceAccountKeysEnv, impersonationEnv, orgProvisioningEnv} {
		if env[name] != "true" {
			t.Errorf("%s=%q, want \"true\"", name, env[name])
		}
	}
}
