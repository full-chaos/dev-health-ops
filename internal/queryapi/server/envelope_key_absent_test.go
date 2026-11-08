package server

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

// A pod with no envelope key (GO_API_ENVELOPE_JWKS_PATH, _ISSUER, _AUDIENCE unset) keeps every route mounted and
// starts; the envelope class is refused with the bytes an unaccepted bearer gets.

// absentKeyBearer has no class query-api accepts: three segments, no readable JOSE header.
const absentKeyBearer = "bm90.YSB0b2tlbg.c2ln"

var envelopeKeyNames = []string{"GO_API_ENVELOPE_JWKS_PATH", "GO_API_ENVELOPE_ISSUER", "GO_API_ENVELOPE_AUDIENCE"}

// absentKeyEnv turns every REST route on and gives the builders ClickHouse and registry Postgres URIs (nothing is
// dialled by construction) and no envelope key. set adds or overrides names.
func absentKeyEnv(set map[string]string) getenvFunc {
	env := map[string]string{
		"CLICKHOUSE_URI":               "clickhouse://default:@127.0.0.1:1/default",
		"GO_API_REGISTRY_POSTGRES_URI": "postgres://u:p@127.0.0.1:1/db?sslmode=disable",
	}
	for _, name := range []string{
		"GO_API_DRILLDOWN_ISSUES_ENABLED", "GO_API_DRILLDOWN_PRS_ENABLED", "GO_API_EXPLAIN_ENABLED", "GO_API_FILTER_OPTIONS_ENABLED",
		"GO_API_FLAME_AGGREGATED_ENABLED", "GO_API_FLAME_ENABLED", "GO_API_HEATMAP_ENABLED", "GO_API_HOME_ENABLED",
		"GO_API_INVESTMENT_ENABLED", "GO_API_INVESTMENT_EXPLAIN_ENABLED",
		"GO_API_INVESTMENT_SUNBURST_ENABLED", "GO_API_META_ENABLED", "GO_API_OPPORTUNITIES_ENABLED",
		"GO_API_PEOPLE_DRILLDOWN_ISSUES_ENABLED", "GO_API_PEOPLE_DRILLDOWN_PRS_ENABLED", "GO_API_PEOPLE_METRIC_ENABLED",
		"GO_API_PEOPLE_SEARCH_ENABLED", "GO_API_PEOPLE_SUMMARY_ENABLED", "GO_API_QUADRANT_ENABLED", "GO_API_SANKEY_ENABLED",
		"GO_API_WORK_UNIT_EXPLAIN_ENABLED", "GO_API_WORK_UNITS_ENABLED",
	} {
		env[name] = "true"
	}
	for name, value := range set {
		env[name] = value
	}
	return func(name string) string { return env[name] }
}

// serveRecovered answers a panic as status -1, so one panicking cell does not stop the others.
func serveRecovered(handler http.Handler, req *http.Request) (got answer) {
	defer func() {
		if p := recover(); p != nil {
			got = answer{-1, fmt.Sprintf("panic: %v", p)}
		}
	}()
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	body, _ := io.ReadAll(rec.Result().Body)
	return answer{rec.Code, strings.TrimSpace(string(body))}
}

func TestRESTRowsMountWithoutEnvelopeKey(t *testing.T) {
	quietStdLog(t)
	for _, partial := range []map[string]string{nil, {"GO_API_ENVELOPE_ISSUER": iaIssuer}} {
		getenv := absentKeyEnv(partial)
		for _, group := range restGroups {
			if needsLiveWriteConnection[group.Builder] {
				continue
			}
			t.Run(fmt.Sprintf("%s/partial=%t", group.Builder, partial != nil), func(t *testing.T) {
				handlers, cleanup, ok, err := group.Build(getenv, nil)
				if cleanup != nil {
					defer cleanup()
				}
				if err != nil || !ok || len(handlers) != len(group.Mounts) {
					t.Fatalf("Build ok=%v err=%v handlers=%d without the envelope key, want mounted", ok, err, len(handlers))
				}
			})
		}
	}
}

// The two rows that open a live write connection at build cannot be built here; without the envelope key they must
// get past their config gate (the live dial then fails), not stop at "not configured".
func TestLiveWriteRowsPassTheConfigGateWithoutEnvelopeKey(t *testing.T) {
	quietStdLog(t)
	getenv := absentKeyEnv(nil)
	for _, group := range restGroups {
		if !needsLiveWriteConnection[group.Builder] {
			continue
		}
		t.Run(group.Builder, func(t *testing.T) {
			_, cleanup, ok, err := group.Build(getenv, nil)
			if cleanup != nil {
				cleanup()
			}
			if !ok && err == nil {
				t.Fatalf("stopped at the config gate (ok=false, err=nil) without the envelope key")
			}
		})
	}
}

func TestQueryRouteConfigWithoutEnvelopeKey(t *testing.T) {
	for name, tc := range map[string]struct {
		set  map[string]string
		want bool
	}{
		"no envelope key":       {nil, true},
		"partial envelope key":  {map[string]string{"GO_API_ENVELOPE_JWKS_PATH": "/nonexistent/jwks.json"}, true},
		"no ClickHouse":         {map[string]string{"CLICKHOUSE_URI": ""}, false},
		"no registry Postgres":  {map[string]string{"GO_API_REGISTRY_POSTGRES_URI": ""}, false},
		"neither data store":    {map[string]string{"CLICKHOUSE_URI": "", "GO_API_REGISTRY_POSTGRES_URI": ""}, false},
		"full envelope key set": {map[string]string{"GO_API_ENVELOPE_JWKS_PATH": "/x", "GO_API_ENVELOPE_ISSUER": "i", "GO_API_ENVELOPE_AUDIENCE": "a"}, true},
	} {
		t.Run(name, func(t *testing.T) {
			if _, ok := loadQueryRouteConfig(absentKeyEnv(tc.set)); ok != tc.want {
				t.Fatalf("loadQueryRouteConfig ok=%v, want %v (/query, /graphql, MCP and the home, opportunities, investment explain and work-unit explain gates)", ok, tc.want)
			}
		})
	}
}

func TestReadinessProbesWithoutEnvelopeKey(t *testing.T) {
	names := func(probes []ReadinessProbe) string {
		var out []string
		for _, probe := range probes {
			out = append(out, probe.Name)
		}
		return strings.Join(out, ",")
	}
	without := readinessProbes(fakePinger{}, fakePinger{}, nil, nil)
	if got, want := names(without), "query_clickhouse,query_postgres"; got != want {
		t.Fatalf("probes without an envelope key = %s, want %s", got, want)
	}
	for _, probe := range without {
		if err := probe.Check(t.Context()); err != nil {
			t.Fatalf("probe %s: %v", probe.Name, err)
		}
	}
	if got, want := names(readinessProbes(fakePinger{}, fakePinger{}, fakeJWKS{}, nil)), "query_clickhouse,query_postgres,query_jwks"; got != want {
		t.Fatalf("probes with an envelope key = %s, want %s", got, want)
	}
}

// Every bearer-authenticated REST row built without the envelope key answers a validly signed envelope with the
// bytes an unaccepted bearer gets: 401 and restRefusal. The meta row reads no bearer.
func TestRESTRowsRefuseTheEnvelopeWithoutEnvelopeKey(t *testing.T) {
	quietStdLog(t)
	_, priv := iaVerifier(t)
	envelope := iaEnvelope(t, priv, principal.Claims{OrgID: ecOrg.String(), Role: "admin"})
	getenv := absentKeyEnv(nil)
	refusal := answer{http.StatusUnauthorized, restRefusal}
	for _, group := range restGroups {
		if needsLiveWriteConnection[group.Builder] || group.Builder == "buildMetaRoute" {
			continue
		}
		t.Run(group.Builder, func(t *testing.T) {
			handlers, cleanup, ok, err := group.Build(getenv, nil)
			if cleanup != nil {
				defer cleanup()
			}
			if err != nil || !ok {
				t.Fatalf("Build ok=%v err=%v without the envelope key", ok, err)
			}
			for i, mount := range group.Mounts {
				path := pathParameter.ReplaceAllString(mount.Pattern, "x")
				send := func(bearer string) answer {
					req := httptest.NewRequest(mount.Methods[0], path, nil)
					req.Header.Set("Authorization", "Bearer "+bearer)
					return serveRecovered(handlers[i], req)
				}
				unaccepted := send(absentKeyBearer)
				if unaccepted != refusal {
					t.Errorf("%s %s unaccepted bearer: got %+v, want %+v", mount.Methods[0], mount.Pattern, unaccepted, refusal)
				}
				if got := send(envelope); got != unaccepted {
					t.Errorf("%s %s envelope: got %+v, want the unaccepted bearer's %+v", mount.Methods[0], mount.Pattern, got, unaccepted)
				}
			}
		})
	}
}

// absentKeyRESTMux is authenticateRESTRequest on /api/v1/home over verifier (nil = no envelope key) and an edge
// verifier routed for the user access token.
func absentKeyRESTMux(t *testing.T, verifier *principal.Verifier, setup *accessSetup) http.Handler {
	t.Helper()
	edge, err := principal.NewEdgeVerifier(newQueryAPIUserStoreFrom(newAccessStore()))
	if err != nil {
		t.Fatal(err)
	}
	if err := routeEdgeVerifier(edge, setup.opts); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/home", func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authenticateRESTRequest(w, r, verifier, edge, "home")
		if !ok {
			return
		}
		r = r.WithContext(authctx.WithClaims(r.Context(), claims))
		_, _ = fmt.Fprintf(w, `{"org":%q}`, claims.OrgID)
	})
	return mux
}

// With no envelope key the REST carrier refuses the envelope as an unaccepted bearer, and answers the user access
// token exactly as a pod with the key does.
func TestRESTCarrierWithoutEnvelopeKey(t *testing.T) {
	quietStdLog(t)
	m := newAccessMinter(t)
	verifier, priv := iaVerifier(t)
	envelope := iaEnvelope(t, priv, principal.Claims{OrgID: ecOrg.String(), Role: "admin"})
	without := absentKeyRESTMux(t, nil, testAccess(m.keys, nil, accessTestNow))
	with := absentKeyRESTMux(t, verifier, testAccess(m.keys, nil, accessTestNow))
	send := func(handler http.Handler, bearer string) answer {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/home", nil)
		req.Header.Set("Authorization", "Bearer "+bearer)
		return serveRecovered(handler, req)
	}
	refusal := answer{http.StatusUnauthorized, restRefusal}
	if got := send(without, absentKeyBearer); got != refusal {
		t.Fatalf("unaccepted bearer: got %+v, want %+v", got, refusal)
	}
	if got := send(without, envelope); got != refusal {
		t.Errorf("envelope without the key: got %+v, want the unaccepted bearer's %+v", got, refusal)
	}
	member := m.mint(ecUser, nil, nil)
	served := answer{http.StatusOK, `{"org":"` + ecOrg.String() + `"}`}
	if got, before := send(without, member), send(with, member); got != before || got != served {
		t.Errorf("user access token without the key: got %+v, with the key %+v, want both %+v", got, before, served)
	}
	orgless := m.mint(ecUser, map[string]any{"org_id": nil}, nil)
	if got, before := send(without, orgless), send(with, orgless); got != before {
		t.Errorf("org-less user access token without the key: got %+v, with the key %+v", got, before)
	}
}

// With no envelope key the /query carrier refuses the envelope with the bare 401 an unaccepted bearer gets.
func TestQueryCarrierRefusesTheEnvelopeWithoutEnvelopeKey(t *testing.T) {
	quietStdLog(t)
	_, priv := iaVerifier(t)
	envelope := iaEnvelope(t, priv, principal.Claims{OrgID: "org-1", Role: "admin"})
	handler, seen := iaDispatch(t, nil)
	send := func(bearer string) answer {
		body, err := json.Marshal(map[string]string{"query": iaDocument})
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+bearer)
		got := serveRecovered(handler, req)
		got.Body += "\n"
		return got
	}
	unaccepted := send(absentKeyBearer)
	if unaccepted != (answer{http.StatusUnauthorized, queryRefusal}) {
		t.Fatalf("unaccepted bearer: got %+v, want 401 %q", unaccepted, queryRefusal)
	}
	if got := send(envelope); got != unaccepted {
		t.Errorf("envelope without the key: got %+v, want the unaccepted bearer's %+v", got, unaccepted)
	}
	if len(*seen) != 0 {
		t.Errorf("a refused request ran %d time(s)", len(*seen))
	}
}

// writeAbsentKeyJWKS writes a JWKS document with one usable Ed25519 key (usable) or none, and returns its path.
func writeAbsentKeyJWKS(t *testing.T, usable bool) string {
	t.Helper()
	keys := []map[string]any{}
	if usable {
		pub, _, err := ed25519.GenerateKey(nil)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, map[string]any{
			"kty": "OKP", "crv": "Ed25519", "x": base64.RawURLEncoding.EncodeToString(pub),
			"kid": iaKID, "use": "sig", "alg": "EdDSA",
		})
	}
	doc, err := json.Marshal(map[string]any{"keys": keys})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "jwks.json")
	if err := os.WriteFile(path, doc, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func fullKeyEnv(jwksPath string) map[string]string {
	return map[string]string{"GO_API_ENVELOPE_JWKS_PATH": jwksPath, "GO_API_ENVELOPE_ISSUER": iaIssuer, "GO_API_ENVELOPE_AUDIENCE": iaAudience}
}

// buildLog runs Build over env and returns what it logged.
func buildLog(t *testing.T, env map[string]string) string {
	t.Helper()
	var buf bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(previous)
	plane, err := Build(func(name string) string { return env[name] })
	if err != nil {
		t.Fatal(err)
	}
	plane.Close()
	return buf.String()
}

// The process says once, at start, that the envelope class is retired, and names the unset keys (names only).
func TestEnvelopeRetiredLineAtStart(t *testing.T) {
	plantRows(t)
	const line = "envelope class retired: no envelope key configured"
	const pathValue = "/secret-looking/path/jwks.json"
	for name, tc := range map[string]struct {
		env     map[string]string
		lines   int
		names   []string
		unnamed []string
	}{
		"no key":                            {nil, 1, envelopeKeyNames, nil},
		"path only":                         {map[string]string{"GO_API_ENVELOPE_JWKS_PATH": pathValue}, 1, envelopeKeyNames[1:], envelopeKeyNames[:1]},
		"issuer only":                       {map[string]string{"GO_API_ENVELOPE_ISSUER": iaIssuer}, 1, []string{envelopeKeyNames[0], envelopeKeyNames[2]}, envelopeKeyNames[1:2]},
		"key set":                           {fullKeyEnv(writeAbsentKeyJWKS(t, true)), 0, nil, nil},
		"key set, JWKS holds no usable key": {fullKeyEnv(writeAbsentKeyJWKS(t, false)), 1, envelopeKeyNames[:1], envelopeKeyNames[1:]},
	} {
		t.Run(name, func(t *testing.T) {
			out := buildLog(t, tc.env)
			if got := strings.Count(out, line); got != tc.lines {
				t.Fatalf("%q lines = %d, want %d; log:\n%s", line, got, tc.lines, out)
			}
			var retired string
			for _, l := range strings.Split(out, "\n") {
				if strings.Contains(l, line) {
					retired = l
				}
			}
			for _, n := range tc.names {
				if !strings.Contains(retired, n) {
					t.Errorf("line %q does not name unset %s", retired, n)
				}
			}
			for _, n := range tc.unnamed {
				if strings.Contains(retired, n) {
					t.Errorf("line %q names %s, which is set", retired, n)
				}
			}
			if strings.Contains(out, pathValue) || strings.Contains(out, iaIssuer) {
				t.Errorf("the log carries a value: %s", out)
			}
		})
	}
}
