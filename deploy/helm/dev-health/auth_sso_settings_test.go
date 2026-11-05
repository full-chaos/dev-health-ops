package devhealth_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
)

const ssoPeers = `authService.networkPolicy.egress.sso.to=[{"podSelector":{"matchLabels":{"app":"idp-egress"}}}]`

// ssoKeyNames is the chart's key-name input per setting and the env name it renders.
var ssoKeyNames = map[string]string{
	"stateKey": "AUTH_SSO_STATE_KEY", "appBaseURL": "APP_BASE_URL",
	"encryptionKey": "SETTINGS_ENCRYPTION_KEY", "encryptionSalt": "SETTINGS_ENCRYPTION_SALT",
}

// ssoSets turns the switches on at phase 2 with the Secret that holds the four settings and the IdP egress peers.
func ssoSets(modes []string, extra ...string) []string {
	out := []string{"--set", "authService.sso.secretName=a-sso", "--set-json", ssoPeers}
	for _, mode := range modes {
		out = append(out, "--set", "authService.sso."+mode+"=true")
	}
	return append(out, extra...)
}

func ssoEnv(t *testing.T, docs []map[string]any) []map[string]any {
	t.Helper()
	service := authDoc(docs, "Deployment", "a-dev-health-auth-service")
	if service == nil {
		t.Fatal("no auth-service Deployment")
	}
	var out []map[string]any
	for _, c := range podContainers(service) {
		if at(c, "name") != "auth-service" {
			continue
		}
		list, _ := at(c, "env").([]any)
		for _, e := range list {
			if m, ok := e.(map[string]any); ok {
				out = append(out, m)
			}
		}
	}
	return out
}

func envNamed(env []map[string]any, name string) []map[string]any {
	var out []map[string]any
	for _, e := range env {
		if e["name"] == name {
			out = append(out, e)
		}
	}
	return out
}

func goAPIProxyFlag(t *testing.T, docs []map[string]any, name string) string {
	t.Helper()
	value, set := containerEnvValue(t, authDoc(docs, "Deployment", "a-dev-health-go-api"), name)
	if !set {
		t.Fatalf("go-api renders no %s", name)
	}
	return value
}

// A complete set renders the settings on auth-service by secretKeyRef only, and go-api forwards the mode.
func TestSSOCompleteSetRendersSecretRefsAndProxyFlags(t *testing.T) {
	for name, tc := range map[string]struct {
		modes      []string
		names      []string
		oidc, saml string
	}{
		"oidc": {[]string{"oidc"}, []string{"AUTH_SSO_STATE_KEY", "APP_BASE_URL", "SETTINGS_ENCRYPTION_KEY", "SETTINGS_ENCRYPTION_SALT"}, "true", "false"},
		"saml": {[]string{"saml"}, []string{"APP_BASE_URL", "SETTINGS_ENCRYPTION_KEY", "SETTINGS_ENCRYPTION_SALT"}, "false", "true"},
		"both": {[]string{"oidc", "saml"}, []string{"AUTH_SSO_STATE_KEY", "APP_BASE_URL", "SETTINGS_ENCRYPTION_KEY", "SETTINGS_ENCRYPTION_SALT"}, "true", "true"},
	} {
		t.Run(name, func(t *testing.T) {
			docs, out, err := renderAuth(t, phaseArgs(2, ssoSets(tc.modes)...)...)
			if err != nil {
				t.Fatalf("render: %v\n%.500s", err, out)
			}
			env := ssoEnv(t, docs)
			for _, want := range tc.names {
				got := envNamed(env, want)
				if len(got) != 1 {
					t.Fatalf("%s rendered %d times; want once", want, len(got))
				}
				ref := at(got[0], "valueFrom", "secretKeyRef")
				if _, literal := got[0]["value"]; literal || ref == nil || at(ref, "name") != "a-sso" || at(ref, "optional") != nil {
					t.Errorf("%s must be a required secretKeyRef of a-sso with no value: %v", want, got[0])
				}
			}
			for _, absent := range []string{"AUTH_SSO_STATE_KEY"} {
				if !slices.Contains(tc.names, absent) && len(envNamed(env, absent)) != 0 {
					t.Errorf("%s rendered without the oidc switch", absent)
				}
			}
			if v := goAPIProxyFlag(t, docs, "AUTH_PROXY_OIDC"); v != tc.oidc {
				t.Errorf("go-api AUTH_PROXY_OIDC=%s; want %s", v, tc.oidc)
			}
			if v := goAPIProxyFlag(t, docs, "AUTH_PROXY_SAML"); v != tc.saml {
				t.Errorf("go-api AUTH_PROXY_SAML=%s; want %s", v, tc.saml)
			}
		})
	}
}

// With the switches off nothing renders, whatever the Secret input says.
func TestSSOOffRendersNothing(t *testing.T) {
	docs, out, err := renderAuth(t, phaseArgs(2, "--set", "authService.sso.secretName=a-sso", "--set-json", ssoPeers)...)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	env := ssoEnv(t, docs)
	for _, name := range []string{"AUTH_SSO_STATE_KEY", "APP_BASE_URL", "SETTINGS_ENCRYPTION_KEY", "SETTINGS_ENCRYPTION_SALT"} {
		if len(envNamed(env, name)) != 0 {
			t.Errorf("%s rendered with both switches off", name)
		}
	}
	if strings.Contains(out, "idp-egress") {
		t.Error("the IdP egress rule rendered with both switches off")
	}
	if v := goAPIProxyFlag(t, docs, "AUTH_PROXY_OIDC"); v != "false" {
		t.Errorf("go-api AUTH_PROXY_OIDC=%s with the switch off", v)
	}
}

// A switch on with a setting missing fails the render and names the key.
func TestSSOMissingSettingFailsClosed(t *testing.T) {
	need := map[string][]string{
		"oidc": {"stateKey", "appBaseURL", "encryptionKey", "encryptionSalt"},
		"saml": {"appBaseURL", "encryptionKey", "encryptionSalt"},
	}
	for mode, keys := range need {
		for _, key := range keys {
			t.Run(mode+"/"+key, func(t *testing.T) {
				_, out, err := renderAuth(t, phaseArgs(2, ssoSets([]string{mode}, "--set", "authService.sso.keys."+key+"=")...)...)
				if err == nil || !strings.Contains(out, "authService.sso.keys."+key) || !strings.Contains(out, ssoKeyNames[key]) {
					t.Fatalf("rendered or the refusal does not name the key: err=%v out=%.400s", err, out)
				}
			})
		}
		t.Run(mode+"/secretName", func(t *testing.T) {
			_, out, err := renderAuth(t, phaseArgs(2, ssoSets([]string{mode}, "--set", "authService.sso.secretName=")...)...)
			if err == nil || !strings.Contains(out, "authService.sso.secretName") {
				t.Fatalf("rendered or the refusal does not name the Secret: err=%v out=%.400s", err, out)
			}
		})
	}
	// saml does not read the state key: an empty name is fine.
	if _, out, err := renderAuth(t, phaseArgs(2, ssoSets([]string{"saml"}, "--set", "authService.sso.keys.stateKey=")...)...); err != nil {
		t.Errorf("saml refused for an empty state key name: %v\n%.300s", err, out)
	}
}

// go-api must reach auth-service for the SSO routes to be forwarded.
func TestSSONeedsGoAPIToReachAuthService(t *testing.T) {
	_, out, err := renderAuth(t, phaseArgs(1, ssoSets([]string{"oidc"})...)...)
	if err == nil || !strings.Contains(out, "authService.phase >= 2") {
		t.Fatalf("phase 1: rendered or wrong refusal: err=%v out=%.300s", err, out)
	}
}

// APP_BASE_URL has one source: SSO owns it when on, the mail block yields only when it names the same Secret key.
func TestSSOAppBaseURLHasOneSource(t *testing.T) {
	t.Run("below phase 4 the SSO Secret is the source", func(t *testing.T) {
		docs, out, err := renderAuth(t, phaseArgs(2, ssoSets([]string{"saml"})...)...)
		if err != nil {
			t.Fatalf("render: %v\n%.300s", err, out)
		}
		got := envNamed(ssoEnv(t, docs), "APP_BASE_URL")
		if len(got) != 1 || at(got[0], "valueFrom", "secretKeyRef", "name") != "a-sso" {
			t.Errorf("APP_BASE_URL: %v; want one reference to a-sso", got)
		}
	})
	t.Run("phase 4 with the mail Secret as the SSO Secret renders it once", func(t *testing.T) {
		docs, out, err := renderAuth(t, phaseArgs(4, append(ssoSets([]string{"saml"}, "--set", "authService.sso.secretName=a-mail"), mailSets()...)...)...)
		if err != nil {
			t.Fatalf("render: %v\n%.300s", err, out)
		}
		got := envNamed(ssoEnv(t, docs), "APP_BASE_URL")
		if len(got) != 1 || at(got[0], "valueFrom", "secretKeyRef", "name") != "a-mail" {
			t.Errorf("APP_BASE_URL: %v; want one reference to a-mail", got)
		}
	})
	t.Run("phase 4 with two different sources fails", func(t *testing.T) {
		_, out, err := renderAuth(t, phaseArgs(4, append(ssoSets([]string{"saml"}), mailSets()...)...)...)
		if err == nil || !strings.Contains(out, "two sources") {
			t.Fatalf("rendered or wrong refusal: err=%v out=%.300s", err, out)
		}
	})
}

// The IdP is reached through the auth-service egress rule, value-driven and rendered only with a switch on.
func TestSSOEgress(t *testing.T) {
	_, out, err := renderAuth(t, phaseArgs(2, "--set", "authService.sso.secretName=a-sso", "--set", "authService.sso.saml=true")...)
	if err == nil || !strings.Contains(out, "authService.networkPolicy.egress.sso.to") {
		t.Fatalf("policy on, no peers: rendered or wrong refusal: err=%v out=%.300s", err, out)
	}
	docs, out, err := renderAuth(t, phaseArgs(2, ssoSets([]string{"saml"})...)...)
	if err != nil {
		t.Fatalf("render: %v\n%.300s", err, out)
	}
	policy := authDoc(docs, "NetworkPolicy", "a-dev-health-auth-service")
	if policy == nil || !strings.Contains(fmt.Sprint(at(policy, "spec", "egress")), "idp-egress") {
		t.Errorf("no egress rule for the IdP peers: %v", at(policy, "spec", "egress"))
	}
	if _, out, err := renderAuth(t, phaseArgs(2, append(ssoSets([]string{"saml"}), "--set", "authService.networkPolicy.enabled=false", "--set-json", "authService.networkPolicy.egress.sso.to=[]")...)...); err != nil {
		t.Errorf("policy off needs no peers: %v\n%.300s", err, out)
	}
}

// The four names have one source, the chart values.
func TestSSOExtraEnvCannotSetTheSettings(t *testing.T) {
	for _, name := range []string{"AUTH_SSO_STATE_KEY", "APP_BASE_URL", "SETTINGS_ENCRYPTION_KEY", "SETTINGS_ENCRYPTION_SALT"} {
		_, out, err := renderAuth(t, phaseArgs(2, "--set-json", fmt.Sprintf(`authService.extraEnv=[{"name":%q,"valueFrom":{"secretKeyRef":{"name":"x","key":"y"}}}]`, name))...)
		if err == nil || !strings.Contains(out, "authService.extraEnv must not set "+name) {
			t.Errorf("%s: rendered or wrong refusal: err=%v out=%.300s", name, err, out)
		}
	}
}

// What the chart requires per mode is what auth-service's own loader needs to mount the routes: dropping any one
// rendered setting turns the mode off, and the chart requires nothing less than the loader (the salt is the one setting the chart
// requires beyond it: it shapes the key go-api encrypted with).
func TestSSOChartRequiredSetMatchesTheLoader(t *testing.T) {
	loaderNeeds := map[string][]string{
		"oidc": {authconfig.EnvSSOStateKey, authconfig.EnvAppBaseURL, authconfig.EnvSettingsEncryptionKey},
		"saml": {authconfig.EnvAppBaseURL, authconfig.EnvSettingsEncryptionKey},
	}
	for mode, needs := range loaderNeeds {
		t.Run(mode, func(t *testing.T) {
			docs, out, err := renderAuth(t, phaseArgs(2, ssoSets([]string{mode})...)...)
			if err != nil {
				t.Fatalf("render: %v\n%.300s", err, out)
			}
			service := authDoc(docs, "Deployment", "a-dev-health-auth-service")
			load := func(skip string) authconfig.SSO {
				lookup := containerLookup(t, service, "auth-service")
				cfg, err := authconfig.Load(authconfig.Spec{Overrides: authServiceOverrides(t, service), LookupEnv: func(name string) (string, bool) {
					if name == skip {
						return "", false
					}
					return lookup(name)
				}})
				if err != nil {
					t.Fatalf("auth-service refuses the rendered settings: %v", err)
				}
				return cfg.SSO
			}
			enabled := func(s authconfig.SSO) bool {
				return map[string]bool{"oidc": s.OIDCEnabled(), "saml": s.SAMLEnabled()}[mode]
			}
			if !enabled(load("")) {
				t.Fatalf("%s routes are not mounted by the rendered settings", mode)
			}
			env := ssoEnv(t, docs)
			for _, name := range needs {
				if len(envNamed(env, name)) != 1 {
					t.Errorf("the loader needs %s for %s and the chart does not render it", name, mode)
				}
				if enabled(load(name)) {
					t.Errorf("dropping %s leaves %s mounted: the loader does not need it", name, mode)
				}
			}
		})
	}
}

func ssoSnapshot(t *testing.T, docs []map[string]any) map[string]string {
	t.Helper()
	out := authSnapshot(t, docs)
	for _, e := range ssoEnv(t, docs) {
		name, _ := e["name"].(string)
		if name == "APP_BASE_URL" || strings.HasPrefix(name, "SETTINGS_ENCRYPTION_") {
			out["Deployment/a-dev-health-auth-service/auth-service/"+name] = envEntryValue(e)
		}
	}
	return out
}

func TestSSOGoldens(t *testing.T) {
	for name, modes := range map[string][]string{"oidc": {"oidc"}, "saml": {"saml"}, "both": {"oidc", "saml"}} {
		t.Run(name, func(t *testing.T) {
			raw, err := json.MarshalIndent(ssoSnapshot(t, renderPhase(t, 2, ssoSets(modes)...)), "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			raw = append(raw, '\n')
			path := filepath.Join("testdata", "auth_sso", name+"-complete.golden.json")
			if os.Getenv("AUTH_PHASE_UPDATE_GOLDEN") == "1" {
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, raw, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read golden (AUTH_PHASE_UPDATE_GOLDEN=1 writes it): %v", err)
			}
			if string(want) != string(raw) {
				t.Fatalf("auth surface differs from %s:\n got %s\nwant %s", path, raw, want)
			}
		})
	}
	t.Run("missing key is refused", func(t *testing.T) {
		_, out, err := renderAuth(t, phaseArgs(2, ssoSets([]string{"oidc"}, "--set", "authService.sso.keys.encryptionKey=")...)...)
		want := "authService.sso.keys.encryptionKey"
		if err == nil || !strings.Contains(out, want) {
			t.Fatalf("rendered or wrong refusal: err=%v out=%.300s", err, out)
		}
		path := filepath.Join("testdata", "auth_sso", "oidc-missing-key.golden.txt")
		line := out[strings.Index(out, "authService.sso.keys.encryptionKey"):]
		line = strings.SplitN(line, "\n", 2)[0] + "\n"
		if os.Getenv("AUTH_PHASE_UPDATE_GOLDEN") == "1" {
			if err := os.WriteFile(path, []byte(line), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		golden, err := os.ReadFile(path)
		if err != nil || string(golden) != line {
			t.Fatalf("refusal differs from %s: got %q want %q (%v)", path, line, golden, err)
		}
	})
}
