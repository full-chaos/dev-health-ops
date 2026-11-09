package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/99designs/gqlgen/graphql"
	"github.com/full-chaos/dev-health-go/pep"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp/pdptest"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/reports"
)

// CHAOS-13334: under enforce, a root the PDP denies whose own legacy check (graph.fieldPrechecks) also refuses the
// caller answers with the legacy bytes (its resolver's refusal), not the gate's FORBIDDEN (product rule 1). A denied
// root whose legacy check admits keeps the FORBIDDEN refusal: the decision never widens.

var (
	enforceDocDataHealth = `query Q { dataHealth(team: "t1") { __typename } }`
	enforceDocMixed      = `query Q { securityOverview(orgId: "` + ecOrg.String() + `") { __typename } dataHealth(team: "t1") { __typename } }`
)

// legacyDataHealthRefusal is the legacy answer to Query.dataHealth for a caller RequireOperator refuses.
const legacyDataHealthRefusal = `{"errors":[{"message":"Data health requires operator access","path":["dataHealth"],"extensions":{"code":"AUTHORIZATION_ERROR"}}],"data":null}`

// integrationsDenied denies integrations.read and allows every other action.
func integrationsDenied(t *testing.T) pep.Decider {
	allow, deny := pdptest.New(t), pdptest.New(t)
	deny.Deny("role_lacks_action")
	return funcDecider{func(ctx context.Context, req pep.Request) pep.Decision {
		if req.Action == "integrations.read" {
			return pepOf(t, deny.URL).Decide(ctx, req)
		}
		return pepOf(t, allow.URL).Decide(ctx, req)
	}}
}

// runQueryEnforce sends one document through the GraphQL server of internal POST /query: the claims the internal
// ingress verified (role, today's scope) and a user access token as the bearer.
func runQueryEnforce(t *testing.T, gate *pdp.Gate, role, document string) enforceRun {
	t.Helper()
	return runQueryEnforceWriting(t, gate, role, document, nil)
}

// runQueryEnforceWriting is runQueryEnforce with the saved-report writer writer.
func runQueryEnforceWriting(t *testing.T, gate *pdp.Gate, role, document string, writer *reports.Writer) enforceRun {
	t.Helper()
	client := &teamScopeReachClient{}
	server := newGraphQLServerWithLimits(&graph.Resolver{ClickHouse: client, ReportWriter: writer}, 1000, 10)
	var roots atomic.Int64
	server.AroundRootFields(func(ctx context.Context, next graphql.RootResolver) graphql.Marshaler {
		roots.Add(1)
		return next(ctx)
	})
	req := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(`{"query":`+jsonQuote(t, document)+`}`))
	req.Header.Set("Content-Type", "application/json")
	ctx := graph.WithShadowSubject(req.Context(), graphQLShadowSubject(newAccessMinter(t).mint(ecUser, nil, nil)))
	claims := authctx.Claims{OrgID: ecOrg.String(), Role: role, Scope: pepgate.Legacy(ecOrg.String())}
	rec := httptest.NewRecorder()
	w, r := withRefusalStatus(rec, req.WithContext(pdp.WithGate(authctx.WithClaims(ctx, claims), gate)))
	server.ServeHTTP(w, r)
	gate.Wait()
	client.mu.Lock()
	defer client.mu.Unlock()
	return enforceRun{code: rec.Code, body: rec.Body.String(), statements: len(client.statements), roots: int(roots.Load())}
}

// Cell 1: a member and a viewer, PDP deny on Query.dataHealth: exactly the legacy body, on /graphql and on /query.
func TestGraphQLEnforceDeniedDataHealthIsTheLegacyRefusal(t *testing.T) {
	for _, role := range []string{"member", "viewer"} {
		t.Run(role+" /graphql", func(t *testing.T) {
			caller := enforceCaller{user: true, role: role}
			legacy := runGraphQLEnforce(t, nil, caller, enforceDocDataHealth)
			if legacy.code != http.StatusOK || legacy.body != legacyDataHealthRefusal || legacy.statements != 0 {
				t.Fatalf("legacy answer %+v, want 200 %s and no read", legacy, legacyDataHealthRefusal)
			}
			gate, reader := enforceGate(t, pdp.ModeEnforce, "integrations", integrationsDenied(t))
			got := runGraphQLEnforce(t, gate, caller, enforceDocDataHealth)
			if got.code != legacy.code || got.body != legacy.body || got.statements != 0 {
				t.Fatalf("enforce answer %d %s with %d statements, want the legacy bytes %d %s and no read", got.code, got.body, got.statements, legacy.code, legacy.body)
			}
			deny := (&gatedREST{reader: reader}).counter(t, "dev_health_query_api_pdp_results_total")["action=integrations.read,mode=enforce,result=deny"]
			if deny != 1 {
				t.Fatalf("deny counted %d times, want 1", deny)
			}
		})
		t.Run(role+" /query", func(t *testing.T) {
			legacy := runQueryEnforce(t, nil, role, enforceDocDataHealth)
			if legacy.code != http.StatusOK || legacy.body != legacyDataHealthRefusal {
				t.Fatalf("legacy answer %+v, want 200 %s", legacy, legacyDataHealthRefusal)
			}
			gate, _ := enforceGate(t, pdp.ModeEnforce, "integrations", integrationsDenied(t))
			if got := runQueryEnforce(t, gate, role, enforceDocDataHealth); got.code != legacy.code || got.body != legacy.body || got.statements != 0 {
				t.Fatalf("enforce answer %+v, want the legacy bytes %+v", got, legacy)
			}
		})
	}
}

// Cell 2: a caller whose legacy check admits (admin) and whom the PDP denies keeps the FORBIDDEN refusal: no root runs.
func TestGraphQLEnforceDeniedDataHealthAdmittedByLegacyStaysForbidden(t *testing.T) {
	gate, _ := enforceGate(t, pdp.ModeEnforce, "integrations", integrationsDenied(t))
	runGraphQLEnforce(t, gate, callerUser, enforceDocDataHealth).forbidden(t)
	gate, _ = enforceGate(t, pdp.ModeEnforce, "integrations", integrationsDenied(t))
	if got := runQueryEnforce(t, gate, "owner", enforceDocDataHealth); got.code != http.StatusOK ||
		got.body != `{"errors":[{"message":"Not authorized","extensions":{"code":"FORBIDDEN"}}],"data":null}` || got.roots != 0 {
		t.Fatalf("/query owner: got %+v, want the FORBIDDEN refusal and no root run", got)
	}
}

// Cell 3: a PDP allow changes nothing: the answer is the legacy one for a refused member and for an admitted admin.
func TestGraphQLEnforceAllowedDataHealthIsUnchanged(t *testing.T) {
	for _, role := range []string{"member", "admin"} {
		t.Run(role, func(t *testing.T) {
			caller := enforceCaller{user: true, role: role}
			legacy := runGraphQLEnforce(t, nil, caller, enforceDocDataHealth)
			gate, _ := enforceGate(t, pdp.ModeEnforce, "integrations", pepOf(t, pdptest.New(t).URL))
			if got := runGraphQLEnforce(t, gate, caller, enforceDocDataHealth); got != legacy {
				t.Fatalf("an allow changed the answer:\n got %+v\nwant %+v", got, legacy)
			}
		})
	}
}

// Cell 4: a member's operation of dataHealth (denied, legacy refuses) and securityOverview (allowed): both resolvers
// run, as on the legacy path. dataHealth is non-null (DataHealth!), so its refusal nulls the whole data: the answer is
// the legacy one, byte for byte, with the analytics read made.
func TestGraphQLEnforceMixedOperationIsTheLegacyAnswer(t *testing.T) {
	caller := enforceCaller{user: true, role: "member"}
	legacy := runGraphQLEnforce(t, nil, caller, enforceDocMixed)
	if legacy.body != legacyDataHealthRefusal || legacy.statements == 0 || legacy.roots != 2 {
		t.Fatalf("legacy answer %+v, want the dataHealth refusal with the analytics read made and both roots run", legacy)
	}
	decider := &countingDecider{next: integrationsDenied(t)}
	gate, _ := enforceGate(t, pdp.ModeEnforce, "analytics,integrations", decider)
	if got := runGraphQLEnforce(t, gate, caller, enforceDocMixed); got != legacy {
		t.Fatalf("mixed operation:\n got %+v\nwant %+v", got, legacy)
	}
	actions := map[string]bool{}
	for _, req := range decider.requests() {
		actions[req.Action] = true
	}
	if len(actions) != 2 || !actions["analytics.read"] || !actions["integrations.read"] {
		t.Fatalf("asked %v, want analytics.read and integrations.read", actions)
	}
	// The other root denied too: the whole operation is refused, as today.
	deny := pdptest.New(t)
	deny.Deny("role_lacks_action")
	gate, _ = enforceGate(t, pdp.ModeEnforce, "analytics,integrations", pepOf(t, deny.URL))
	runGraphQLEnforce(t, gate, caller, enforceDocMixed).forbidden(t)
}

// Cell 5: a local refusal (no decision point, the PDP down) is unchanged: 503, no root run.
func TestGraphQLEnforceDataHealthLocalRefusalIsUnchanged(t *testing.T) {
	caller := enforceCaller{user: true, role: "member"}
	noClient, _ := enforceGate(t, pdp.ModeOff, "integrations", nil)
	runGraphQLEnforce(t, noClient, caller, enforceDocDataHealth).code503(t, string(pdp.ReasonNoDecisionPoint))
	down := pdptest.New(t)
	down.Fail(http.StatusServiceUnavailable)
	downGate, _ := enforceGate(t, pdp.ModeEnforce, "integrations", pepOf(t, down.URL))
	runGraphQLEnforce(t, downGate, caller, enforceDocDataHealth).code503(t, string(pep.ReasonUnavailable))
}

// countingPool counts the saved-report transactions a mutation begins (a write attempt) and begins none.
type countingPool struct{ begins atomic.Int64 }

func (p *countingPool) Begin(context.Context) (pgx.Tx, error) {
	p.begins.Add(1)
	return nil, errors.New("countingPool: no database")
}

// acp (b): a PDP deny of a root whose legacy guard ADMITS the caller never reaches the resolver: FORBIDDEN, no root
// run, no read, no write. dataHealth as admin and owner (RequireOperator admits) on /graphql and /query; a member's
// deleteSavedReport for its own org (requireMutationOrg admits), whose legacy run does begin a write.
func TestGraphQLEnforceDeniedRootTheGuardAdmitsNeverRuns(t *testing.T) {
	const forbidden = `{"errors":[{"message":"Not authorized","extensions":{"code":"FORBIDDEN"}}],"data":null}`
	for _, role := range []string{"admin", "owner"} {
		t.Run(role+" dataHealth", func(t *testing.T) {
			gate, _ := enforceGate(t, pdp.ModeEnforce, "integrations", integrationsDenied(t))
			runGraphQLEnforce(t, gate, enforceCaller{user: true, role: role}, enforceDocDataHealth).forbidden(t)
			gate, _ = enforceGate(t, pdp.ModeEnforce, "integrations", integrationsDenied(t))
			if got := runQueryEnforce(t, gate, role, enforceDocDataHealth); got.code != http.StatusOK || got.body != forbidden || got.roots != 0 || got.statements != 0 {
				t.Fatalf("/query: got %+v, want 200 %s, no root run, no read", got, forbidden)
			}
		})
	}
	t.Run("member deleteSavedReport own org", func(t *testing.T) {
		document := `mutation M { deleteSavedReport(orgId: "` + ecOrg.String() + `", reportId: "55555555-5555-5555-5555-555555555555") }`
		legacyPool := &countingPool{}
		legacy := runQueryEnforceWriting(t, nil, "member", document, &reports.Writer{Pool: legacyPool})
		if legacy.roots != 1 || legacyPool.begins.Load() != 1 {
			t.Fatalf("legacy run: %+v with %d writes begun, want the resolver run and one write begun (the guard admits)", legacy, legacyPool.begins.Load())
		}
		deny := pdptest.New(t)
		deny.Deny("role_lacks_action")
		gate, _ := enforceGate(t, pdp.ModeEnforce, "reports", pepOf(t, deny.URL))
		pool := &countingPool{}
		got := runQueryEnforceWriting(t, gate, "member", document, &reports.Writer{Pool: pool})
		if got.code != http.StatusOK || got.body != forbidden || got.roots != 0 || got.statements != 0 || pool.begins.Load() != 0 {
			t.Fatalf("enforce: got %+v with %d writes begun, want 200 %s, no root run, no read, no write", got, pool.begins.Load(), forbidden)
		}
	})
}
