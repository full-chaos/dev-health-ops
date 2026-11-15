package pdp

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/pep"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/auth/workloadinput"
	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp/pdptest"
)

const (
	testProfile = "GET /api/v1/home"
	testAction  = "analytics.read"
	testOrg     = "0B6A1C9E-55D4-4A43-9E34-6F4C1D2B3A10"
	// testBearer stands for a user access token: the PDP resolves it, the gate never reads it.
	testBearer = "eyJhbGciOiJFZERTQSIsInR5cCI6ImF0K2p3dCJ9.eyJzdWIiOiJ1In0.c2lnbmF0dXJl"
)

type meterReader struct {
	reader *sdkmetric.ManualReader
}

func newMeter(t *testing.T) (*meterReader, Config) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	return &meterReader{reader: reader}, Config{Meter: provider.Meter("test"), Logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))}
}

// points is every data point of metric name as "k=v,k=v" -> value.
func (m *meterReader) points(t *testing.T, name string) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := m.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	out := map[string]int64{}
	for _, scope := range rm.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != name {
				continue
			}
			add := func(set attribute.Set, v int64) {
				var parts []string
				for _, kv := range set.ToSlice() {
					parts = append(parts, string(kv.Key)+"="+kv.Value.Emit())
				}
				sort.Strings(parts)
				out[strings.Join(parts, ",")] += v
			}
			switch data := metric.Data.(type) {
			case metricdata.Sum[int64]:
				for _, p := range data.DataPoints {
					add(p.Attributes, p.Value)
				}
			case metricdata.Gauge[int64]:
				for _, p := range data.DataPoints {
					add(p.Attributes, p.Value)
				}
			}
		}
	}
	return out
}

func pepClient(t *testing.T, base string) pep.Decider {
	t.Helper()
	client, err := pep.New(pep.Config{BaseURL: base, AllowPlainHTTP: true, HTTPClient: &http.Client{}, ClientService: ClientService, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

type recordingLookup struct {
	mu     sync.Mutex
	values map[string]string
	read   []string
}

func (r *recordingLookup) lookup(key string) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.read = append(r.read, key)
	v, ok := r.values[key]
	return v, ok
}

func onEnv(t *testing.T, mode, upstream string) map[string]string {
	t.Helper()
	return map[string]string{EnvMode: mode, EnvAuthProxyURL: upstream, workloadinput.EnvIssuer: pdptest.Issuer,
		workloadinput.EnvBootstrapFile: pdptest.BootstrapFile(t)}
}

// Off (unset, empty, "off", padded) reads only the two switches, builds no client and asks nothing.
func TestPDPOffReadsOnlyTheSwitchAndAsksNothing(t *testing.T) {
	srv := pdptest.New(t)
	for name, values := range map[string]map[string]string{
		"unset":  {},
		"empty":  {EnvMode: ""},
		"off":    {EnvMode: "off"},
		"padded": {EnvMode: " off "},
	} {
		t.Run(name, func(t *testing.T) {
			for k, v := range onEnv(t, "", srv.URL) {
				if k != EnvMode {
					values[k] = v
				}
			}
			rec := &recordingLookup{values: values}
			var logs bytes.Buffer
			g := FromSettings(rec.lookup, slog.New(slog.NewTextHandler(&logs, nil)))
			if g.Mode() != ModeOff {
				t.Fatalf("mode = %q, want off", g.Mode())
			}
			if _, err := g.Authorize(context.Background(), testProfile, testBearer, testOrg); err != nil {
				t.Fatalf("off refused: %v", err)
			}
			g.Wait()
			if !slices.Equal(rec.read, []string{EnvMode, authzenforce.QueryAPI.Env(), authzenforce.QueryAPIHeaders.Env()}) {
				t.Fatalf("off read %v, want only %s and the two 5.17 cell lists", rec.read, EnvMode)
			}
			if n := len(srv.Requests()); n != 0 || srv.Exchanges() != 0 {
				t.Fatalf("off asked the PDP %d times, exchanged %d times", n, srv.Exchanges())
			}
			if logs.Len() != 0 {
				t.Fatalf("off logged: %s", logs.String())
			}
		})
	}
}

// With the query-api workload credential: one exchange, the PDP call carries the workload token (never the user's),
// client_service dev-health-query-api, the user token as the principal reference, the row action and the org.
func TestPDPEnforceAsksWithTheQueryAPIWorkloadToken(t *testing.T) {
	srv := pdptest.New(t)
	env := onEnv(t, "enforce", srv.URL)
	env[authzenforce.QueryAPI.Env()] = "analytics"
	g := FromSettings((&recordingLookup{values: env}).lookup, nil)
	if g.Mode() != ModeEnforce {
		t.Fatalf("mode = %q, want enforce", g.Mode())
	}
	for range 2 {
		if _, err := g.Authorize(context.Background(), testProfile, testBearer, seamOrg); err != nil {
			t.Fatalf("allow answered %v", err)
		}
	}
	got := srv.Requests()
	if len(got) != 2 || srv.Exchanges() != 1 {
		t.Fatalf("requests %d exchanges %d, want 2 and 1", len(got), srv.Exchanges())
	}
	want := pdptest.Request{Authorization: "Bearer " + srv.WorkloadToken, PrincipalReference: testBearer, Action: testAction,
		ResourceType: "organization", ResourceKey: seamOrg, OrganizationID: seamOrg, ClientService: "dev-health-query-api"}
	if got[0] != want {
		t.Fatalf("PDP call %+v, want %+v", got[0], want)
	}
}

// Enforce: the reason auth-service sends for a platform admin's allow is an allow for query-api: the real pep client reads
// allowed_by_platform_admin, and the same code on a deny is refused (it is an allow reason only).
func TestPDPEnforceAcceptsThePlatformAdminAllowReason(t *testing.T) {
	srv := pdptest.New(t)
	srv.Allow("allowed_by_platform_admin")
	env := onEnv(t, "enforce", srv.URL)
	env[authzenforce.QueryAPI.Env()] = "analytics"
	g := FromSettings((&recordingLookup{values: env}).lookup, nil)
	if _, err := g.Authorize(context.Background(), testProfile, testBearer, seamOrg); err != nil {
		t.Fatalf("an allow by allowed_by_platform_admin answered %v, want an allow", err)
	}
	srv.Deny("allowed_by_platform_admin")
	if _, err := g.Authorize(context.Background(), testProfile, testBearer, seamOrg); err == nil {
		t.Fatal("a deny naming allowed_by_platform_admin was allowed")
	}
}

// Enforce (the cell on): a PDP deny is a typed deny with its wire reason, and is counted.
func TestPDPEnforceDenyIsATypedDeny(t *testing.T) {
	srv := pdptest.New(t)
	srv.Deny("role_lacks_action")
	m, cfg := newMeter(t)
	cfg.Mode, cfg.Decider, cfg.Switch = ModeEnforce, pepClient(t, srv.URL), cells("analytics")
	_, err := New(cfg).Authorize(context.Background(), testProfile, testBearer, testOrg)
	denied, ok := err.(*DenyError)
	if !ok || denied.Local || denied.Reason != pep.ReasonRoleLacksAction {
		t.Fatalf("deny answered %#v, want a PDP DenyError role_lacks_action", err)
	}
	if got := m.points(t, resultMetric); got["action=analytics.read,mode=enforce,result=deny"] != 1 {
		t.Fatalf("results %v", got)
	}
}

// refusedTransport refuses every dial. A closed httptest port is not a refusal: another listener can take the freed
// port and answer.
type refusedTransport struct{}

func (refusedTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}
}

// Enforce (the cell on): PDP down (refused connection, 503, timeout) is a typed local deny, never a pass.
func TestPDPEnforceDownIsATypedLocalDeny(t *testing.T) {
	refused, err := pep.New(pep.Config{BaseURL: "http://pdp.invalid", AllowPlainHTTP: true, HTTPClient: &http.Client{Transport: refusedTransport{}}, ClientService: ClientService, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	failing := pdptest.New(t)
	failing.Fail(http.StatusServiceUnavailable)
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { time.Sleep(1500 * time.Millisecond) }))
	t.Cleanup(slow.Close)
	for name, tc := range map[string]struct {
		decider pep.Decider
		reason  pep.Reason
	}{
		"refused": {refused, pep.ReasonUnreachable},
		"503":     {pepClient(t, failing.URL), pep.ReasonUnavailable},
		"timeout": {pepClient(t, slow.URL), pep.ReasonTimeout},
	} {
		t.Run(name, func(t *testing.T) {
			m, cfg := newMeter(t)
			cfg.Mode, cfg.Decider, cfg.Switch = ModeEnforce, tc.decider, cells("analytics")
			_, err := New(cfg).Authorize(context.Background(), testProfile, testBearer, testOrg)
			denied, ok := err.(*DenyError)
			if !ok || !denied.Local || denied.Reason != tc.reason {
				t.Fatalf("PDP down answered %#v, want a local DenyError %s", err, tc.reason)
			}
			if got := m.points(t, resultMetric); got["action=analytics.read,mode=enforce,result=unavailable"] != 1 {
				t.Fatalf("results %v", got)
			}
		})
	}
}

// Shadow-mode Authorize asks nothing, counts nothing on the #5679 counter and never denies: the 5.12 core asks and
// counts (internal/queryapi/server/rest_shadow_test.go).
func TestPDPShadowAuthorizeAsksNothingAndCountsNothing(t *testing.T) {
	srv := pdptest.New(t)
	m, cfg := newMeter(t)
	cfg.Mode, cfg.Decider = ModeShadow, pepClient(t, srv.URL)
	g := New(cfg)
	if _, err := g.Authorize(context.Background(), testProfile, testBearer, testOrg); err != nil {
		t.Fatalf("shadow refused: %v", err)
	}
	g.Wait()
	if n := len(srv.Requests()); n != 0 {
		t.Fatalf("shadow Authorize asked %d times, want 0", n)
	}
	if got := m.points(t, resultMetric); len(got) != 0 {
		t.Fatalf("results %v, want none", got)
	}
}

// Only shadow mode builds the 5.12 shadow, from the same decider; off, enforce and an invalid gate build none.
func TestPDPGateShadowIsBuiltOnlyInShadowMode(t *testing.T) {
	srv := pdptest.New(t)
	for name, tc := range map[string]struct {
		cfg  Config
		want bool
	}{
		"off":     {Config{Mode: ModeOff, Decider: pepClient(t, srv.URL)}, false},
		"shadow":  {Config{Mode: ModeShadow, Decider: pepClient(t, srv.URL)}, true},
		"enforce": {Config{Mode: ModeEnforce, Decider: pepClient(t, srv.URL)}, false},
		"bad":     {Config{Mode: "banana"}, false},
		"no pdp":  {Config{Mode: ModeShadow}, false},
	} {
		t.Run(name, func(t *testing.T) {
			if got := New(tc.cfg).Shadow() != nil; got != tc.want {
				t.Fatalf("shadow built = %t, want %t", got, tc.want)
			}
		})
	}
	var nilGate *Gate
	if nilGate.Shadow() != nil {
		t.Fatal("a nil gate has a shadow")
	}
}

// A bad mode, or a mode that is on with a missing or bad input, still builds: one ERROR naming the variable (never
// a value or a path), the config-invalid gauge 1, and every PDP-gated request refused as unavailable, asking nothing.
func TestPDPMisconfigurationRefusesAsUnavailableAndNeverFailsTheStart(t *testing.T) {
	srv := pdptest.New(t)
	unreadable := filepath.Join(t.TempDir(), "missing-secret-file")
	for name, tc := range map[string]struct {
		edit     func(map[string]string)
		variable string
	}{
		"bad mode":            {func(e map[string]string) { e[EnvMode] = "banana" }, EnvMode},
		"capitalised mode":    {func(e map[string]string) { e[EnvMode] = "Enforce" }, EnvMode},
		"shadow without url":  {func(e map[string]string) { e[EnvMode] = "shadow"; delete(e, EnvAuthProxyURL) }, EnvAuthProxyURL},
		"url with a path":     {func(e map[string]string) { e[EnvAuthProxyURL] = srv.URL + "/x" }, EnvAuthProxyURL},
		"url with secret":     {func(e map[string]string) { e[EnvAuthProxyURL] = "http://u:hunter2@auth:8095" }, EnvAuthProxyURL},
		"no workload file":    {func(e map[string]string) { delete(e, workloadinput.EnvBootstrapFile) }, workloadinput.EnvSubjectFile + "," + workloadinput.EnvBootstrapFile},
		"both workload files": {func(e map[string]string) { e[workloadinput.EnvSubjectFile] = unreadable }, workloadinput.EnvSubjectFile + "," + workloadinput.EnvBootstrapFile},
		"unreadable file":     {func(e map[string]string) { e[workloadinput.EnvBootstrapFile] = unreadable }, workloadinput.EnvBootstrapFile},
		"no issuer":           {func(e map[string]string) { delete(e, workloadinput.EnvIssuer) }, workloadinput.EnvIssuer},
		"http issuer":         {func(e map[string]string) { e[workloadinput.EnvIssuer] = "http://auth.example.test" }, workloadinput.EnvIssuer},
	} {
		t.Run(name, func(t *testing.T) {
			env := onEnv(t, "enforce", srv.URL)
			tc.edit(env)
			var logs bytes.Buffer
			g := FromSettings((&recordingLookup{values: env}).lookup, slog.New(slog.NewTextHandler(&logs, nil)))
			_, err := g.Authorize(context.Background(), testProfile, testBearer, testOrg)
			denied, ok := err.(*DenyError)
			if !ok || !denied.Local || denied.Reason != pep.ReasonNotConfigured {
				t.Fatalf("misconfigured gate answered %#v, want a local not_configured DenyError", err)
			}
			if n := len(srv.Requests()); n != 0 {
				t.Fatalf("a misconfigured gate asked the PDP %d times", n)
			}
			text := logs.String()
			if strings.Count(text, "level=ERROR") != 1 || !strings.Contains(text, "variable="+tc.variable) {
				t.Fatalf("log %q, want one ERROR naming %s", text, tc.variable)
			}
			for _, secret := range []string{"hunter2", unreadable, env[workloadinput.EnvBootstrapFile], "banana"} {
				if secret != "" && strings.Contains(text, secret) {
					t.Fatalf("the log carries a value: %q", text)
				}
			}
		})
	}
}

// The gauges: enabled 1 only for a built shadow or enforce gate; config_invalid 1 only for a misconfigured one.
func TestPDPStateGauges(t *testing.T) {
	srv := pdptest.New(t)
	for name, tc := range map[string]struct {
		cfg              Config
		enabled, invalid int64
	}{
		"off":     {Config{Mode: ModeOff}, 0, 0},
		"shadow":  {Config{Mode: ModeShadow, Decider: pepClient(t, srv.URL)}, 1, 0},
		"enforce": {Config{Mode: ModeEnforce, Decider: pepClient(t, srv.URL)}, 1, 0},
		"bad":     {Config{Mode: "banana"}, 0, 1},
		"no pdp":  {Config{Mode: ModeEnforce}, 0, 1},
	} {
		t.Run(name, func(t *testing.T) {
			m, base := newMeter(t)
			tc.cfg.Meter, tc.cfg.Logger = base.Meter, base.Logger
			New(tc.cfg)
			if got := m.points(t, enabledMetric)[""]; got != tc.enabled {
				t.Errorf("enabled = %d, want %d", got, tc.enabled)
			}
			if got := m.points(t, configInvalidMetric)[""]; got != tc.invalid {
				t.Errorf("config_invalid = %d, want %d", got, tc.invalid)
			}
		})
	}
}

// A route with no row action fails closed while a cell is on, and asks nothing.
func TestPDPEnforceUnknownRowFailsClosed(t *testing.T) {
	srv := pdptest.New(t)
	_, cfg := newMeter(t)
	cfg.Mode, cfg.Decider, cfg.Switch = ModeEnforce, pepClient(t, srv.URL), cells("analytics")
	_, err := New(cfg).Authorize(context.Background(), "GET /api/v1/no-such-route", testBearer, testOrg)
	if denied, ok := err.(*DenyError); !ok || !denied.Local {
		t.Fatalf("an unknown row answered %#v, want a local DenyError", err)
	}
	if n := len(srv.Requests()); n != 0 {
		t.Fatalf("asked the PDP %d times for a route with no action", n)
	}
}

// The exempt list is empty: the retired HS256 class, the retired envelope class and a user token never count, in
// any mode.
func TestPDPExemptListIsEmpty(t *testing.T) {
	if len(ExemptClasses) != 0 {
		t.Fatalf("exempt list %v, want none", ExemptClasses)
	}
	for _, mode := range []string{ModeOff, ModeShadow, ModeEnforce, "banana"} {
		t.Run(mode, func(t *testing.T) {
			m, cfg := newMeter(t)
			cfg.Mode = mode
			if mode == ModeShadow || mode == ModeEnforce {
				cfg.Decider = pepClient(t, pdptest.New(t).URL)
			}
			g := New(cfg)
			g.CountExempt(context.Background(), "ops_access_token_hs256", testProfile)
			g.CountExempt(context.Background(), "effective_principal_envelope", "POST /api/v1/drilldown/prs")
			g.CountExempt(context.Background(), "user_access_token", testProfile)
			if got := m.points(t, exemptMetric); len(got) != 0 {
				t.Fatalf("exempt %v, want no count", got)
			}
		})
	}
}

func mapsEqual(a, b map[string]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// Every label value is from a closed set: no request, token or organization value is ever a label.
func TestPDPLabelsAreClosed(t *testing.T) {
	srv := pdptest.New(t)
	m, cfg := newMeter(t)
	cfg.Mode, cfg.Decider, cfg.Switch = ModeEnforce, pepClient(t, srv.URL), cells("analytics,metrics")
	g := New(cfg)
	for _, profile := range []string{testProfile, "GET /api/v1/flame", "GET /x/" + testOrg} {
		_, _ = g.Authorize(context.Background(), profile, testBearer, testOrg)
		g.CountExempt(context.Background(), "effective_principal_envelope", profile)
	}
	actions := []string{"analytics.read", "metrics.read", "work_items.read", "git.read", "org.read", "unknown"}
	closed := map[string][]string{
		"mode": {"enforce", "invalid"}, "result": {"allow", "deny", "unavailable", "not_configured", "not_carried_out", "no_decision_point"},
		"class": ExemptClasses, "action": actions,
	}
	for _, name := range []string{resultMetric, exemptMetric} {
		for key := range m.points(t, name) {
			for _, pair := range strings.Split(key, ",") {
				k, v, _ := strings.Cut(pair, "=")
				if !slices.Contains(closed[k], v) {
					t.Errorf("%s label %s=%q is outside its closed set", name, k, v)
				}
			}
		}
	}
}

// The row map is every protected query-api row on the query or internal listener that names an action.
func TestPDPRowsCoverEveryProtectedRESTRow(t *testing.T) {
	actions, err := parseActions(authv1.QueryAPIEndpointProfiles)
	if err != nil {
		t.Fatal(err)
	}
	rest := 0
	for key, action := range actions {
		if strings.Contains(key, " /api/v1/") {
			rest++
		}
		if action == "" {
			t.Errorf("%s has no action", key)
		}
	}
	if rest != 29 || actions[testProfile] != testAction || actions["GET /api/v1/drilldown/prs"] != "git.read" {
		t.Fatalf("rows: %d REST rows, home=%q prs=%q", rest, actions[testProfile], actions["GET /api/v1/drilldown/prs"])
	}
}

// The subject file is the Kubernetes form of the credential: it is read only while a mode is on.
func TestPDPReadsTheSubjectFileOnlyWhenOn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "subject")
	if err := os.WriteFile(path, []byte("eyJhbGciOiJSUzI1NiJ9.c3ViamVjdA.c2ln"), 0o600); err != nil {
		t.Fatal(err)
	}
	srv := pdptest.New(t)
	env := map[string]string{EnvMode: "shadow", EnvAuthProxyURL: srv.URL, workloadinput.EnvIssuer: pdptest.Issuer, workloadinput.EnvSubjectFile: path}
	if g := FromSettings((&recordingLookup{values: env}).lookup, nil); g.Mode() != ModeShadow {
		t.Fatalf("mode = %q with a subject file, want shadow", g.Mode())
	}
}

// The fail-closed obligation table of the PDP client (pm, F-A), now answered by the pepgate seam with the cells of
// both actions on and the request organization in lowercase. Each row states the seam's answer: an allow passes only
// with the asked organization as tenant_scope and repository_scope organization (or an action with no repository
// member); every obligation the seam cannot apply yet is a local deny with the PEP's unknown_obligation; a tenant the
// seam cannot match is tenant_scope_mismatch; a repository-filtered allow with no repository member is
// repository_scope_absent; a document the PEP client refuses keeps the client's local reason (counted unavailable).
func TestPDPEnforceAppliesOrFailsClosedOnObligations(t *testing.T) {
	org := strings.ToLower(testOrg)
	for name, tc := range map[string]struct {
		profile    string
		obligation map[string]any
		pass       bool
		reason     pep.Reason
		result     string
	}{
		"organization scope, tenant match":     {testProfile, map[string]any{"tenant_scope": org, "repository_scope": "organization"}, true, "", ResultAllow},
		"scope none action, tenant only":       {"GET /buildinfo", map[string]any{"tenant_scope": org}, true, "", ResultAllow},
		"empty allowlist":                      {testProfile, map[string]any{"tenant_scope": org, "repository_allowlist": []string{}}, false, ReasonAllowlistRouteUnsupported, ResultNotCarriedOut},
		"allowlist of one":                     {testProfile, map[string]any{"tenant_scope": org, "repository_allowlist": []string{"acme/widgets"}}, false, ReasonAllowlistRouteUnsupported, ResultNotCarriedOut},
		"allowlist on a scope none action":     {"GET /buildinfo", map[string]any{"tenant_scope": org, "repository_allowlist": []string{"acme/widgets"}}, false, ReasonNotCarriedOut, ResultNotCarriedOut},
		"redaction class":                      {testProfile, map[string]any{"tenant_scope": org, "repository_scope": "organization", "redaction": []string{"free_text"}}, false, ReasonNotCarriedOut, ResultNotCarriedOut},
		"tenant mismatch":                      {testProfile, map[string]any{"tenant_scope": "other-org", "repository_scope": "organization"}, false, ReasonTenantMismatch, ResultNotCarriedOut},
		"no tenant scope":                      {testProfile, map[string]any{"repository_scope": "organization"}, false, ReasonTenantMismatch, ResultNotCarriedOut},
		"no repository member on filtered one": {acrTestProfile, map[string]any{"tenant_scope": org}, false, ReasonNoRepositoryRule, ResultNotCarriedOut},
		"tenant only on a metrics read":        {testProfile, map[string]any{"tenant_scope": org}, true, "", ResultAllow},
		"minimum assurance":                    {testProfile, map[string]any{"tenant_scope": org, "repository_scope": "organization", "minimum_assurance": "aal2"}, false, ReasonNotCarriedOut, ResultNotCarriedOut},
		"minimum assurance aal1":               {testProfile, map[string]any{"tenant_scope": org, "repository_scope": "organization", "minimum_assurance": "aal1"}, true, "", ResultAllow},
		"minimum assurance aal3":               {testProfile, map[string]any{"tenant_scope": org, "repository_scope": "organization", "minimum_assurance": "aal3"}, false, ReasonNotCarriedOut, ResultNotCarriedOut},
		"minimum assurance unknown":            {testProfile, map[string]any{"tenant_scope": org, "repository_scope": "organization", "minimum_assurance": "aal9"}, false, pep.ReasonUnknownObligation, ResultUnavailable},
		"step up":                              {testProfile, map[string]any{"tenant_scope": org, "repository_scope": "organization", "step_up": true}, false, ReasonNotCarriedOut, ResultNotCarriedOut},
		"unknown obligation key":               {testProfile, map[string]any{"tenant_scope": org, "repository_scope": "organization", "audit": true}, false, pep.ReasonUnknownObligation, ResultUnavailable},
		"unknown repository scope":             {testProfile, map[string]any{"tenant_scope": org, "repository_scope": "everything"}, false, pep.ReasonUnknownObligation, ResultUnavailable},
		"malformed block":                      {testProfile, map[string]any{"tenant_scope": 7}, false, pep.ReasonMalformed, ResultUnavailable},
		"allow with no obligations":            {testProfile, nil, false, ReasonTenantMismatch, ResultNotCarriedOut},
	} {
		t.Run(name, func(t *testing.T) {
			srv := pdptest.New(t)
			srv.Obligations(tc.obligation)
			m, cfg := newMeter(t)
			cfg.Mode, cfg.Decider, cfg.Switch = ModeEnforce, pepClient(t, srv.URL), cells("analytics,org,acr")
			g := New(cfg)
			g.actions[acrTestProfile] = "acr.context.read"
			_, err := g.Authorize(context.Background(), tc.profile, testBearer, org)
			got := m.points(t, resultMetric)
			if len(got) != 1 || got["action="+actionOf(tc.profile)+",mode=enforce,result="+tc.result] != 1 {
				t.Fatalf("results %v, want one %s", got, tc.result)
			}
			if tc.pass {
				if err != nil {
					t.Fatalf("answered %v, want a pass", err)
				}
				return
			}
			denied, ok := err.(*DenyError)
			if !ok || !denied.Local || denied.Reason != tc.reason {
				t.Fatalf("answered %#v, want a local DenyError %s", err, tc.reason)
			}
		})
	}
}

// acrTestProfile is a test-only row of a filtered action: catalog version 14 keeps only the ACR reads filtered, and no
// query-api row asks one.
const acrTestProfile = "GET /acr-test"

func actionOf(profile string) string {
	if profile == "GET /buildinfo" {
		return "org.read"
	}
	if profile == acrTestProfile {
		return "acr.context.read"
	}
	return testAction
}

// A hand-built allow (the fake gives the default organization allow) with a repository allowlist on a route not cut
// over is the case the reason is pinned to: the gate's own refusal, not the client's. testProfile is an M route, so
// it names allowlist_route_unsupported.
func TestPDPEnforceAllowlistIsRefusedByTheGateWithTheClosedReason(t *testing.T) {
	srv := pdptest.New(t)
	srv.Obligations(map[string]any{"tenant_scope": strings.ToLower(testOrg), "repository_allowlist": []string{"acme/widgets"}})
	_, cfg := newMeter(t)
	cfg.Mode, cfg.Decider, cfg.Switch = ModeEnforce, pepClient(t, srv.URL), cells("analytics")
	_, err := New(cfg).Authorize(context.Background(), testProfile, testBearer, strings.ToLower(testOrg))
	denied, ok := err.(*DenyError)
	if !ok || !denied.Local || denied.Reason != ReasonAllowlistRouteUnsupported {
		t.Fatalf("answered %#v, want a local DenyError allowlist_route_unsupported", err)
	}
}
