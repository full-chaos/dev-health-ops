package devhealth_test

import (
	"strings"
	"testing"
)

const ssoProvidersGuardText = "goApi.extraEnv sets AUTH_SSO_PROVIDERS_OWNED on"

func ssoProvidersExtraEnv(entry string) []string {
	return []string{"--set", "goApi.enabled=true", "--set-json", "goApi.extraEnv=[" + entry + "]"}
}

func ssoProvidersEnvEntry(name, value string) string {
	return `{"name":"` + name + `","value":` + value + `}`
}

// go-api reads the switch with strings.ToLower(strings.TrimSpace(v)) == "true" (ParseSSOProvidersOwned); each spelling it reads as on is refused while auth-service does not own the providers.
func TestGoAPISSOProvidersOwnedRefusedWhileAuthServiceDoesNotOwn(t *testing.T) {
	t.Parallel()

	for _, value := range []string{`"true"`, `"TRUE"`, `"True"`, `" true "`, `"\ttrue\n"`, `true`} {
		_, out, err := renderAuth(t, phaseArgs(4, ssoProvidersExtraEnv(ssoProvidersEnvEntry(ssoProvidersEnv, value))...)...)
		if err == nil || !strings.Contains(out, ssoProvidersGuardText) {
			t.Errorf("value %s: want the render refused, err=%v out=%.300s", value, err, out)
		}
	}
	for _, off := range [][]string{nil, {"--set", "authService.ssoProvidersOwned=false"}} {
		args := phaseArgs(4, append(ssoProvidersExtraEnv(ssoProvidersEnvEntry(ssoProvidersEnv, `"true"`)), off...)...)
		if _, out, err := renderAuth(t, args...); err == nil || !strings.Contains(out, ssoProvidersGuardText) {
			t.Errorf("A off %v: want refused, err=%v out=%.300s", off, err, out)
		}
	}
}

func TestGoAPISSOProvidersOwnedValueFromRefusedWhileAuthServiceDoesNotOwn(t *testing.T) {
	t.Parallel()

	entry := `{"name":"AUTH_SSO_PROVIDERS_OWNED","valueFrom":{"configMapKeyRef":{"name":"x","key":"y"}}}`
	_, out, err := renderAuth(t, phaseArgs(4, ssoProvidersExtraEnv(entry)...)...)
	if err == nil || !strings.Contains(out, ssoProvidersGuardText) || !strings.Contains(out, "valueFrom") {
		t.Fatalf("valueFrom entry rendered with A off: err=%v out=%.300s", err, out)
	}
	_, out, err = renderAuth(t, phaseArgs(4, append(ssoProvidersExtraEnv(entry), "--set", "authService.ssoProvidersOwned=true")...)...)
	if err != nil {
		t.Fatalf("valueFrom entry with A on: %v\n%.400s", err, out)
	}
}

// A value the parser reads as off, and a name go-api never reads (os.Getenv is case-sensitive), render as before.
func TestGoAPISSOProvidersOwnedNotRefusedWhenOffOrNotTheName(t *testing.T) {
	t.Parallel()

	for _, entry := range []string{
		ssoProvidersEnvEntry(ssoProvidersEnv, `"false"`),
		ssoProvidersEnvEntry(ssoProvidersEnv, `""`),
		ssoProvidersEnvEntry(ssoProvidersEnv, `"yes"`),
		ssoProvidersEnvEntry(ssoProvidersEnv, `"1"`),
		ssoProvidersEnvEntry("auth_sso_providers_owned", `"true"`),
		ssoProvidersEnvEntry("AUTH_SSO_PROVIDERS_OWNED_X", `"true"`),
	} {
		if _, out, err := renderAuth(t, phaseArgs(4, ssoProvidersExtraEnv(entry)...)...); err != nil {
			t.Errorf("%s: want it rendered, err=%v out=%.300s", entry, err, out)
		}
	}
}

// A on with B on, A on with B off, both off, and the default (no extraEnv): all render, with the env where each switch puts it.
func TestGoAPISSOProvidersOwnedRenderedStates(t *testing.T) {
	t.Parallel()

	b := ssoProvidersExtraEnv(ssoProvidersEnvEntry(ssoProvidersEnv, `"true"`))
	for _, state := range []struct {
		name                string
		args                []string
		wantGoAPI, wantAuth bool
	}{
		{"A on B on", append(append([]string{}, b...), "--set", "authService.ssoProvidersOwned=true"), true, true},
		{"A on B off", []string{"--set", "goApi.enabled=true", "--set", "authService.ssoProvidersOwned=true"}, false, true},
		{"both off", []string{"--set", "goApi.enabled=true", "--set", "authService.ssoProvidersOwned=false"}, false, false},
		{"default", []string{"--set", "goApi.enabled=true"}, false, false},
	} {
		docs, out, err := renderAuth(t, phaseArgs(4, state.args...)...)
		if err != nil {
			t.Errorf("%s: render: %v\n%.400s", state.name, err, out)
			continue
		}
		goAPI, auth := authDoc(docs, "Deployment", "a-dev-health-go-api"), authDoc(docs, "Deployment", "a-dev-health-auth-service")
		if value, set := containerEnvValue(t, goAPI, ssoProvidersEnv); set != state.wantGoAPI || (set && value != "true") {
			t.Errorf("%s: go-api %s=%q set=%v, want set=%v", state.name, ssoProvidersEnv, value, set, state.wantGoAPI)
		}
		if value, set := containerEnvValue(t, auth, ssoProvidersEnv); set != state.wantAuth || (set && value != "true") {
			t.Errorf("%s: auth-service %s=%q set=%v, want set=%v", state.name, ssoProvidersEnv, value, set, state.wantAuth)
		}
	}
}
