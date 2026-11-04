package apiservice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/workloadtoken"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

const shadowTestBearer = "aaaa.bbbb.cccc-dddd-eeee-ffff"

type recordingLookup struct {
	values map[string]string
	mu     sync.Mutex
	read   []string
}

func (r *recordingLookup) lookup(key string) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.read = append(r.read, key)
	v, ok := r.values[key]
	return v, ok
}

func (r *recordingLookup) reads(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, k := range r.read {
		if k == key {
			return true
		}
	}
	return false
}

func shadowRowKey(t *testing.T) string {
	t.Helper()
	rows, err := policy.ParseShadowRows(authv1.GoAPIEndpointProfiles, goAPIAccessService)
	if err != nil {
		t.Fatal(err)
	}
	for key, row := range rows {
		if row.ResourceType == "organization" && !row.HighRisk {
			return key
		}
	}
	t.Fatal("no low-risk organization row")
	return ""
}

type shadowPDPServer struct {
	URL      string
	mu       sync.Mutex
	auths    []string
	services []string
	actions  []string
	exchange int
}

func newShadowPDPServer(t *testing.T) *shadowPDPServer {
	t.Helper()
	s := &shadowPDPServer{}
	token := testWorkloadToken(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		defer s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == workloadtoken.ExchangePath {
			s.exchange++
			body, _ := json.Marshal(map[string]any{"access_token": token, "issued_token_type": workloadtoken.TokenTypeJWT, "token_type": "Bearer", "expires_in": 300})
			_, _ = w.Write(body)
			return
		}
		var body struct {
			Action  string `json:"action"`
			Context struct {
				ClientService string `json:"client_service"`
			} `json:"context"`
		}
		_ = json.Unmarshal(raw, &body)
		s.auths = append(s.auths, r.Header.Get("Authorization"))
		s.services = append(s.services, body.Context.ClientService)
		s.actions = append(s.actions, body.Action)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"schema_version": "authorization_decision.v1", "decision_id": "dec_" + strings.Repeat("A", 26), "allowed": true,
			"reason_code": "allowed_by_role", "principal_id": "prn_TESTPRINCIPAL", "policy_revision": 1,
			"expires_at": time.Now().Add(time.Hour).UTC().Format("2006-01-02T15:04:05Z"),
		})
	}))
	t.Cleanup(srv.Close)
	s.URL = srv.URL
	return s
}

func (s *shadowPDPServer) snapshot() (auths, services, actions []string, exchanges int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.auths...), append([]string(nil), s.services...), append([]string(nil), s.actions...), s.exchange
}

func shadowEnvOn(t *testing.T, upstream string, extra map[string]string) map[string]string {
	t.Helper()
	env := map[string]string{policy.EnvShadowPDP: "true", EnvAuthProxyURL: upstream,
		EnvAuthWorkloadIssuer: testWorkloadIssuer, EnvAuthWorkloadBootstrapFile: bootstrapFile(t)}
	for k, v := range extra {
		env[k] = v
	}
	return env
}

func runNewShadow(t *testing.T, env map[string]string, shared *workloadtoken.Client) (*policy.Shadow, string, *recordingLookup) {
	t.Helper()
	rec := &recordingLookup{values: env}
	cfg, err := ParseAuthProxyConfig(rec.lookup)
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	s := newShadow(cfg, rec.lookup, shared, slog.New(slog.NewTextHandler(&logs, nil)))
	return s, logs.String(), rec
}

func TestShadowEnvNames(t *testing.T) {
	for got, want := range map[string]string{
		policy.EnvShadowPDP: "AUTH_SHADOW_PDP", policy.EnvShadowPDPHighRisk: "AUTH_SHADOW_PDP_HIGH_RISK",
		policy.EnvShadowPDPSample: "AUTH_SHADOW_PDP_SAMPLE", policy.EnvShadowPDPInFlight: "AUTH_SHADOW_PDP_MAX_IN_FLIGHT",
	} {
		if got != want {
			t.Errorf("env name %q, want %q", got, want)
		}
	}
}

func TestShadowOffByDefaultReadsNoWorkloadInputAndBuildsNoClient(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"unset":      {},
		"empty":      {policy.EnvShadowPDP: ""},
		"false":      {policy.EnvShadowPDP: "false"},
		"false+junk": {policy.EnvShadowPDP: "false", policy.EnvShadowPDPSample: "x", EnvAuthWorkloadIssuer: "x", EnvAuthWorkloadBootstrapFile: "/nonexistent"},
	} {
		s, logs, rec := runNewShadow(t, env, nil)
		if s != nil {
			t.Errorf("%s: a shadow was built", name)
		}
		for _, key := range []string{EnvAuthWorkloadIssuer, EnvAuthWorkloadBootstrapFile, EnvAuthWorkloadSubjectFile} {
			if rec.reads(key) {
				t.Errorf("%s: off read %s", name, key)
			}
		}
		if strings.Contains(logs, "level=ERROR") {
			t.Errorf("%s: off logged an error: %s", name, logs)
		}
	}
}

func TestShadowBadOrMissingInputIsOffWithAnErrorAndNoSecret(t *testing.T) {
	dir := t.TempDir()
	secretToken := filepath.Join(dir, "planted-secret-name")
	if err := os.WriteFile(secretToken, []byte("planted-secret-token-value"), 0o600); err != nil {
		t.Fatal(err)
	}
	proxy := "http://auth-service:8095"
	for name, env := range map[string]map[string]string{
		"no proxy url": shadowEnvOn(t, "", nil),
		"no workload file": func() map[string]string {
			e := shadowEnvOn(t, proxy, nil)
			delete(e, EnvAuthWorkloadBootstrapFile)
			return e
		}(),
		"both workload files": shadowEnvOn(t, proxy, map[string]string{EnvAuthWorkloadSubjectFile: secretToken}),
		"missing file":        shadowEnvOn(t, proxy, map[string]string{EnvAuthWorkloadBootstrapFile: filepath.Join(dir, "absent-secret-name")}),
		"no issuer":           func() map[string]string { e := shadowEnvOn(t, proxy, nil); delete(e, EnvAuthWorkloadIssuer); return e }(),
		"issuer not https":    shadowEnvOn(t, proxy, map[string]string{EnvAuthWorkloadIssuer: "http://auth.example.test"}),
		"bad switch":          shadowEnvOn(t, proxy, map[string]string{policy.EnvShadowPDP: "yes please"}),
		"bad high risk":       shadowEnvOn(t, proxy, map[string]string{policy.EnvShadowPDPHighRisk: "maybe"}),
		"sample zero":         shadowEnvOn(t, proxy, map[string]string{policy.EnvShadowPDPSample: "0"}),
		"sample 101":          shadowEnvOn(t, proxy, map[string]string{policy.EnvShadowPDPSample: "101"}),
		"sample text":         shadowEnvOn(t, proxy, map[string]string{policy.EnvShadowPDPSample: "ten"}),
		"in flight zero":      shadowEnvOn(t, proxy, map[string]string{policy.EnvShadowPDPInFlight: "0"}),
		"in flight text":      shadowEnvOn(t, proxy, map[string]string{policy.EnvShadowPDPInFlight: "many"}),
		"in flight negative":  shadowEnvOn(t, proxy, map[string]string{policy.EnvShadowPDPInFlight: "-3"}),
	} {
		s, logs, _ := runNewShadow(t, env, nil)
		if s != nil {
			t.Errorf("%s: a shadow was built", name)
		}
		if !strings.Contains(logs, "level=ERROR") {
			t.Errorf("%s: no ERROR line: %q", name, logs)
		}
		for _, secret := range []string{"planted-secret", dir, "bootstrap", "absent-secret-name"} {
			if strings.Contains(logs, secret) {
				t.Errorf("%s: log holds %q: %s", name, secret, logs)
			}
		}
	}
}

func TestShadowOnAsksThePDPWithTheWorkloadTokenAndNamesTheService(t *testing.T) {
	pdp := newShadowPDPServer(t)
	s, logs, _ := runNewShadow(t, shadowEnvOn(t, pdp.URL, nil), nil)
	if s == nil {
		t.Fatalf("no shadow: %s", logs)
	}
	key := shadowRowKey(t)
	s.Observe(context.Background(), policy.ShadowObservation{Pattern: key, Bearer: shadowTestBearer, OrgID: "11111111-1111-4111-8111-111111111111",
		Caller: policy.ShadowCallerAdmin, UserAccessToken: true, LegacyAllowed: true})
	deadline := time.Now().Add(5 * time.Second)
	for {
		auths, services, _, exchanges := pdp.snapshot()
		if len(auths) == 1 {
			if exchanges != 1 || !strings.HasPrefix(auths[0], "Bearer ") || strings.Contains(auths[0], shadowTestBearer) || services[0] != "dev-health-ops-api" {
				t.Fatalf("auth %q service %q exchanges %d: want the workload token, no user token, service dev-health-ops-api, one exchange", auths[0], services[0], exchanges)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the PDP saw %d calls, want 1", len(auths))
		}
		time.Sleep(10 * time.Millisecond)
	}
	if strings.Contains(logs, shadowTestBearer) {
		t.Errorf("log holds the user token: %s", logs)
	}
}

func TestShadowReusesTheSharedWorkloadClient(t *testing.T) {
	pdp := newShadowPDPServer(t)
	env := shadowEnvOn(t, pdp.URL, nil)
	delete(env, EnvAuthWorkloadIssuer)
	delete(env, EnvAuthWorkloadBootstrapFile)
	cfg := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: pdp.URL, EnvAuthProxyOrgProvisioning: "true"})
	provisioning, _, err := newCredentials(cfg, issueEnv(workloadEnv(t, map[string]string{EnvAuthProvisioningCredential: "workload"})))
	if err != nil || provisioning.workload == nil {
		t.Fatalf("shared client: %v %v", provisioning.workload, err)
	}
	s, logs, rec := runNewShadow(t, env, provisioning.workload)
	if s == nil {
		t.Fatalf("no shadow although a shared workload client exists: %s", logs)
	}
	if rec.reads(EnvAuthWorkloadBootstrapFile) || rec.reads(EnvAuthWorkloadIssuer) {
		t.Error("the shadow read workload inputs although a shared client exists")
	}
}

func configureShadow(t *testing.T, env map[string]string) (enabled bool, err error) {
	t.Helper()
	t.Setenv("ENVIRONMENT", "development")
	for _, key := range []string{policy.EnvShadowPDP, policy.EnvShadowPDPHighRisk, policy.EnvShadowPDPSample, policy.EnvShadowPDPInFlight, EnvAuthProxyURL,
		EnvAuthWorkloadIssuer, EnvAuthWorkloadBootstrapFile, EnvAuthWorkloadSubjectFile} {
		t.Setenv(key, "")
		os.Unsetenv(key)
	}
	for k, v := range env {
		t.Setenv(k, v)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	components, err := configureWith(ctx, reachableDatabase(config.Config{APIAddress: "127.0.0.1:0"}), health.NewRegistry(time.Second), quietLogger(),
		func(deps *Deps) { enabled = deps.Guard != nil && deps.Guard.ShadowEnabled() })
	closeComponents(components)
	return enabled, err
}

func TestConfigureShadowOffByDefaultAndBadInputNeverFailsAStart(t *testing.T) {
	if on, err := configureShadow(t, nil); err != nil || on {
		t.Fatalf("unset: shadow %v, err %v; want off and no error", on, err)
	}
	for name, env := range map[string]map[string]string{
		"on, no proxy":    {policy.EnvShadowPDP: "true"},
		"bad switch":      {policy.EnvShadowPDP: "banana"},
		"on, no workload": {policy.EnvShadowPDP: "true", EnvAuthProxyURL: "http://auth-service:8095"},
		"bad sample":      {policy.EnvShadowPDP: "true", EnvAuthProxyURL: "http://auth-service:8095", policy.EnvShadowPDPSample: "500"},
	} {
		if on, err := configureShadow(t, env); err != nil || on {
			t.Errorf("%s: shadow %v, err %v; want off and no error", name, on, err)
		}
	}
}

func TestConfigureShadowOnInstallsTheShadow(t *testing.T) {
	env := map[string]string{policy.EnvShadowPDP: "true", EnvAuthProxyURL: "http://auth-service:8095",
		EnvAuthWorkloadIssuer: testWorkloadIssuer, EnvAuthWorkloadBootstrapFile: bootstrapFile(t)}
	if on, err := configureShadow(t, env); err != nil || !on {
		t.Fatalf("on with good inputs: shadow %v, err %v; want on and no error", on, err)
	}
}

func TestShadowSetupErrorNamesTheBadInputAndNoValue(t *testing.T) {
	dir := t.TempDir()
	proxy := "http://auth-service:8095"
	both := EnvAuthWorkloadSubjectFile + "," + EnvAuthWorkloadBootstrapFile
	subject := filepath.Join(dir, "planted-subject-file")
	if err := os.WriteFile(subject, []byte("planted-subject-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		env  map[string]string
		drop string
		want string
	}{
		"missing issuer":       {nil, EnvAuthWorkloadIssuer, "variable=" + EnvAuthWorkloadIssuer},
		"issuer not https":     {map[string]string{EnvAuthWorkloadIssuer: "http://auth.example.test"}, "", "variable=" + EnvAuthWorkloadIssuer},
		"missing proxy url":    {map[string]string{EnvAuthProxyURL: ""}, "", "variable=" + EnvAuthProxyURL},
		"no workload file":     {nil, EnvAuthWorkloadBootstrapFile, "variable=" + both},
		"both workload files":  {map[string]string{EnvAuthWorkloadSubjectFile: subject}, "", "variable=" + both},
		"unreadable bootstrap": {map[string]string{EnvAuthWorkloadBootstrapFile: filepath.Join(dir, "absent-file")}, "", "variable=" + EnvAuthWorkloadBootstrapFile},
		"unreadable subject":   {map[string]string{EnvAuthWorkloadBootstrapFile: "", EnvAuthWorkloadSubjectFile: filepath.Join(dir, "absent-file")}, "", "variable=" + EnvAuthWorkloadSubjectFile},
	} {
		env := shadowEnvOn(t, proxy, tc.env)
		if tc.drop != "" {
			delete(env, tc.drop)
		}
		s, logs, _ := runNewShadow(t, env, nil)
		if s != nil {
			t.Errorf("%s: a shadow was built", name)
		}
		if !strings.Contains(logs, tc.want) || strings.Contains(logs, "variable=decider") {
			t.Errorf("%s: log %q, want %s", name, logs, tc.want)
		}
		for _, secret := range []string{dir, "planted-subject", "absent-file", "auth.example.test"} {
			if strings.Contains(logs, secret) {
				t.Errorf("%s: log holds %q", name, secret)
			}
		}
	}
}

var malformedProxyURLs = map[string]string{
	"parse error": "http://[::1", "wrong scheme": "ftp://auth-service:8095", "empty host": "http://", "credentials": "http://user:pw-planted@auth-service:8095",
	"path": "http://auth-service:8095/x", "query": "http://auth-service:8095?x=1", "fragment": "http://auth-service:8095#f", "opaque": "http:auth-service",
}

func configureShadowLogged(t *testing.T, env map[string]string) (enabled bool, logs string, err error) {
	t.Helper()
	t.Setenv("ENVIRONMENT", "development")
	for _, key := range []string{policy.EnvShadowPDP, policy.EnvShadowPDPHighRisk, policy.EnvShadowPDPSample, policy.EnvShadowPDPInFlight, EnvAuthProxyURL,
		EnvAuthWorkloadIssuer, EnvAuthWorkloadBootstrapFile, EnvAuthWorkloadSubjectFile, AuthProxyFlagEnv(AuthSessions), EnvAuthProxyOrgProvisioning} {
		t.Setenv(key, "")
		os.Unsetenv(key)
	}
	for k, v := range env {
		t.Setenv(k, v)
	}
	var buf bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	components, err := configureWith(ctx, reachableDatabase(config.Config{APIAddress: "127.0.0.1:0"}), health.NewRegistry(time.Second),
		slog.New(slog.NewTextHandler(&buf, nil)), func(deps *Deps) { enabled = deps.Guard != nil && deps.Guard.ShadowEnabled() })
	closeComponents(components)
	return enabled, buf.String(), err
}

func TestConfigureMalformedProxyURLWithTheShadowOnTurnsOnlyTheShadowOff(t *testing.T) {
	for name, raw := range malformedProxyURLs {
		on, logs, err := configureShadowLogged(t, map[string]string{policy.EnvShadowPDP: "true", EnvAuthProxyURL: raw})
		if err != nil || on {
			t.Errorf("%s: shadow %v, err %v; want off and no start error", name, on, err)
		}
		if !strings.Contains(logs, "variable="+EnvAuthProxyURL) || strings.Contains(logs, "pw-planted") {
			t.Errorf("%s: log %q, want an ERROR naming %s and no credential", name, logs, EnvAuthProxyURL)
		}
	}
}

// Without the shadow, or with a flag that needs the URL, a malformed AUTH_PROXY_URL still stops the start (authproxy.go ParseAuthProxyConfig).
func TestConfigureMalformedProxyURLStillStopsTheStartForItsOtherConsumers(t *testing.T) {
	for name, raw := range malformedProxyURLs {
		for label, env := range map[string]map[string]string{
			"shadow off":       {EnvAuthProxyURL: raw},
			"shadow false":     {policy.EnvShadowPDP: "false", EnvAuthProxyURL: raw},
			"sessions flag on": {policy.EnvShadowPDP: "true", EnvAuthProxyURL: raw, AuthProxyFlagEnv(AuthSessions): "true"},
			"provisioning on":  {policy.EnvShadowPDP: "true", EnvAuthProxyURL: raw, EnvAuthProxyOrgProvisioning: "true"},
		} {
			_, _, err := configureShadowLogged(t, env)
			var coded interface{ DependencyReason() string }
			if !errors.As(err, &coded) || coded.DependencyReason() != "api_auth_proxy_config_invalid" {
				t.Errorf("%s / %s: err %v, want reason api_auth_proxy_config_invalid", name, label, err)
			}
		}
	}
}
