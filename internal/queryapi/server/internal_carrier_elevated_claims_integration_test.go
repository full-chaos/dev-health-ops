//go:build integration

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gqlgraphql "github.com/99designs/gqlgen/graphql"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp/pdptest"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/reports"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// CHAOS-13950: the internal listener's header carrier must not serve an elevated claim (superuser, impersonation, an
// operator role) with that scope, as the MCP listener refuses it (mcpAuthenticate). The plane is the internal listener
// over the query-api login role provisioned the way a deployment provisions it (NOSUPERUSER, the real migrate leg), the
// gate configured as prod (PDP mode enforce, the 7 user namespaces, no header cell), and acr's route,
// /query/run-operation, with the registered catalog documents.
func TestInternalCarrierElevatedClaimsAreNotServed(t *testing.T) {
	// The cell's own rows, red at ops f41bfa84 (lane cag-13945 log cell13950-20261116T2241Z.log).
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	fixture := startQueryAPIRoleFixture(t, ctx)
	orgA, orgB := pathsOrg, pathsTargetOrg

	if _, err := fixture.admin.Exec(ctx, `
INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, created_at, updated_at)
VALUES (gen_random_uuid(), $1, 'cell13950 connector A', 'github', '[]'::json, '{}'::json, TRUE, now(), now())`, orgA); err != nil {
		t.Fatalf("seed sync_configurations: %v", err)
	}
	writer := newReportWriter(fixture.admin, filepath.Join(repoRootFromHere(t), "contracts", "jobs", "v1"))
	cron := "0 9 * * 1"
	for org, name := range map[string]string{orgA: "cell13950 org A report", orgB: "cell13950 org B report"} {
		if _, err := writer.Create(ctx, org, reports.CreateInput{Name: name, ReportPlan: []byte(`{"steps":[]}`),
			Parameters: []byte(`{}`), ScheduleCron: &cron, ScheduleTimezone: "UTC"}); err != nil {
			t.Fatalf("seed the %s: %v", name, err)
		}
	}

	dataHealthDoc := readCellDocument(t, "data_health_connectors_captured.graphql")
	savedReportsDoc := readCellDocument(t, "saved_reports_captured.graphql")
	internal, roots := newElevatedClaimsPlane(t, fixture, dataHealthDoc, savedReportsDoc)
	cellRun := func(t *testing.T, internal http.Handler, document string, variables map[string]any, org, role, superuser, impersonation string) cellAnswer {
		t.Helper()
		roots.Store(0)
		a := cellRunOnce(t, internal, document, variables, org, role, superuser, impersonation)
		a.roots = int(roots.Load())
		return a
	}

	dataHealth := func(role, superuser, impersonation string) cellAnswer {
		return cellRun(t, internal, dataHealthDoc, map[string]any{"teamId": "ALL"}, orgA, role, superuser, impersonation)
	}
	savedReportsOfB := func(org, superuser, impersonation string) cellAnswer {
		return cellRun(t, internal, savedReportsDoc, map[string]any{"orgId": orgB, "limit": 10, "offset": 0}, org, "member", superuser, impersonation)
	}
	refusedElevated := func(a cellAnswer) bool { return a.code == http.StatusForbidden && a.roots == 0 }
	connectorServed := func(a cellAnswer) bool { return strings.Contains(a.body, "cell13950 connector A") }
	reportServed := func(a cellAnswer) bool { return strings.Contains(a.body, "cell13950 org B report") }

	// Controls: today's answer, must pass.
	if a := dataHealth("member", "false", "false"); connectorServed(a) || !strings.Contains(a.body, "AUTHORIZATION_ERROR") {
		t.Fatalf("control C1 (dataHealth, role member): %d %s, want the operator refusal", a.code, a.body)
	} else {
		t.Logf("C1 control dataHealth role=member su=false imp=false: %d %s", a.code, a.body)
	}
	if a := savedReportsOfB(orgA, "false", "false"); reportServed(a) || !strings.Contains(a.body, "Access denied: cannot query org") {
		t.Fatalf("control C2 (savedReports org B, header org A): %d %s, want the org guard refusal", a.code, a.body)
	} else {
		t.Logf("C2 control savedReports(orgId=B) header org=A su=false: %d %s", a.code, a.body)
	}
	t.Logf("C3 reference (K17, the stated org) savedReports(orgId=B) header org=B: %s", savedReportsOfB(orgB, "false", "false"))
	// C4: acr's real header set (role viewer, superuser false, impersonation false; acr directread queryapi_client.go:63-65).
	if a := cellRun(t, internal, savedReportsDoc, map[string]any{"orgId": orgA, "limit": 10, "offset": 0}, orgA, "viewer", "false", "false"); !strings.Contains(a.body, "cell13950 org A report") {
		t.Fatalf("control C4 (acr's header set, savedReports org A): %d %s, want org A's report served", a.code, a.body)
	} else {
		t.Logf("C4 control acr header set viewer/false/false savedReports(orgId=A): %s", a)
	}

	// Elevated claims: each must not be served with its scope.
	for _, row := range []struct {
		name, role, superuser string
	}{{"E1a role=admin", "admin", "false"}, {"E1b role=owner", "owner", "false"}, {"E1c role=operator", "operator", "false"}, {"E1d su=true", "member", "true"}} {
		a := dataHealth(row.role, row.superuser, "false")
		t.Logf("%s dataHealth header org=A: %s", row.name, a)
		if connectorServed(a) || !refusedElevated(a) {
			t.Errorf("%s: dataHealth on an elevated header claim: %d %s with %d root fields run, want 403 and nothing run", row.name, a.code, a.body, a.roots)
		}
	}
	if a := savedReportsOfB(orgA, "true", "false"); reportServed(a) || !refusedElevated(a) {
		t.Errorf("E2 su=true: savedReports(orgId=B) from header org A: %d %s with %d root fields run, want 403 and nothing run", a.code, a.body, a.roots)
	} else {
		t.Logf("E2 su=true savedReports(orgId=B) header org=A: %s", a)
	}

	// Impersonation: E3b is the elevated row; E3a and E3c stay refused.
	if a := dataHealth("admin", "false", "true"); connectorServed(a) || !refusedElevated(a) {
		t.Errorf("E3b dataHealth role=admin imp=true: %d %s with %d root fields run, want 403 and nothing run", a.code, a.body, a.roots)
	} else {
		t.Logf("E3b dataHealth role=admin imp=true: %s", a)
	}
	if a := dataHealth("member", "false", "true"); connectorServed(a) {
		t.Errorf("E3a dataHealth role=member imp=true served: %d %s", a.code, a.body)
	} else {
		t.Logf("E3a dataHealth role=member imp=true: %s", a)
	}
	if a := savedReportsOfB(orgA, "true", "true"); reportServed(a) {
		t.Errorf("E3c savedReports(orgId=B) header org=A su=true imp=true served: %d %s", a.code, a.body)
	} else {
		t.Logf("E3c savedReports(orgId=B) header org=A su=true imp=true: %s", a)
	}
}

type cellAnswer struct {
	code  int
	body  string
	roots int
}

func (a cellAnswer) String() string {
	body := a.body
	if len(body) > 400 {
		body = body[:400] + "..."
	}
	return fmt.Sprintf("%d %s (root fields run %d)", a.code, body, a.roots)
}

func readCellDocument(t *testing.T, name string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "wire_capture", name))
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// newElevatedClaimsPlane is newQMPlane's internal listener with the resolver reading Postgres as the provisioned role,
// and the count of root fields run.
func newElevatedClaimsPlane(t *testing.T, fixture *queryAPIRoleFixture, documents ...string) (http.Handler, *atomic.Int64) {
	t.Helper()
	quietStdLog(t)
	store := newQueryAPIUserStoreFrom(newAccessStore())
	m := newAccessMinter(t)
	auth := ecEdgeAuth(t, store)
	if err := routeAuthenticator(auth, testAccess(m.keys, nil, accessTestNow).opts); err != nil {
		t.Fatal(err)
	}
	verifier, _ := iaVerifier(t)
	getenv := getenvFunc(func(string) string { return "" })

	server := newGraphQLServerWithLimits(&graph.Resolver{ClickHouse: &teamScopeReachClient{}, Postgres: fixture.rolePool}, 1000, 10)
	roots := &atomic.Int64{}
	server.AroundRootFields(func(ctx context.Context, next gqlgraphql.RootResolver) gqlgraphql.Marshaler {
		roots.Add(1)
		return next(ctx)
	})
	routeMux := routeswitch.NewMux(routeswitch.StaticSwitch{"gql": true})
	routeMux.Register("gql", server)
	byDigest := map[string]string{}
	for _, document := range documents {
		byDigest[digestHex(document)] = "gql"
	}
	query := newDocumentDispatchHandler(getenv, routeMux, byDigest, verifier, auth, store, "", nil, newClassRowGate(allMCPRootsEnabled()))
	stub := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) }
	mux, internalMux, mcpMux := http.NewServeMux(), http.NewServeMux(), http.NewServeMux()
	mountQueryRouteSets(getenv, mux, internalMux, mcpMux, queryRouteHandlers{
		Query: query, Registry: stub, BuildInfo: stub, RunOperation: markClassGated(query),
		MCP: newMCPHandlerWithLimits(&countingMCPClient{}, nil, allMCPRootsEnabled(), getenv, mcpDefaultLimits()),
	}, graphQLEdgeDeps{auth: auth, maxBytes: defaultGraphQLMaxQueryBytes})
	env := qmEnv(t, pdptest.New(t), pdp.ModeEnforce, "analytics,metrics,git,work_items,integrations,reports,org", "")
	handler, internal, mcp, _ := gatedListenerHandlers(knobs(env), mux, internalMux, mcpMux)
	_, internalListener := Listeners("127.0.0.1:0", "127.0.0.1:0", &Plane{Handler: handler, InternalHandler: internal, MCPHandler: mcp}, nil, nil)
	if internalListener == nil {
		t.Fatal("expected the internal listener")
	}
	return internalListener.server.Handler, roots
}

// cellRunOnce posts document to /query/run-operation on the internal listener with the four internal identity headers.
func cellRunOnce(t *testing.T, internal http.Handler, document string, variables map[string]any, org, role, superuser, impersonation string) cellAnswer {
	t.Helper()
	body, err := json.Marshal(map[string]any{"query": document, "variables": variables})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, runOperationPath, strings.NewReader(string(body)))
	for key, values := range mcpHeaders(org, role, superuser, impersonation) {
		request.Header[key] = values
	}
	recorder := httptest.NewRecorder()
	internal.ServeHTTP(recorder, request)
	return cellAnswer{code: recorder.Code, body: recorder.Body.String()}
}
