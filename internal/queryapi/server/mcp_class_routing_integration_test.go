//go:build integration

package server

// CHAOS-7214: the MCP class's routing rows end to end against a real,
// migrated Postgres. The state the verbs exist to reach is asserted at the
// listener's own reader (routeswitch.PostgresSwitch over mcpRoutingDigests,
// the object buildQueryRoute builds): after seed + proof + enable the root is
// served, and a root without an enabled row answers 404 root_field_not_enabled.

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	schemav1 "github.com/full-chaos/dev-health-ops/contracts/graphql/v1"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/internalidentity"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

const (
	classTestBuild  = "7214aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	classTestSchema = itTestSchemaDigest
)

func classSeed(t *testing.T, pool *pgxpool.Pool, operations ...string) {
	t.Helper()
	if _, err := goapiproof.Seed(t.Context(), pool, goapiproof.SeedRequest{
		SchemaDigest: classTestSchema, RunningBuild: classTestBuild, Operations: operations,
		DocumentDigest: mcpclass.Digests(), RecordedBy: "test", ReviewEvidence: "class routing", PrincipalID: "test-principal",
	}); err != nil {
		t.Fatalf("seed %v: %v", operations, err)
	}
}

func classReceipt(t *testing.T, pool *pgxpool.Pool, operation, route, binding string) {
	t.Helper()
	// CHAOS-7512: `enable` reads the class receipt's provenance; a fully measured proof lists no excluded shape.
	root, _ := mcpclass.Root(operation)
	provenance, err := json.Marshal(goapiproof.ReceiptProvenance{MeasurementRoute: route, EdgeBuildBinding: binding,
		MCPClass: &goapiproof.MCPClassProvenance{Root: root, Reference: "go_document_route", Executed: 1, Matched: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := goapiproof.WriteAtomic(t.Context(), pool, goapiproof.Receipt{
		SchemaDigest: classTestSchema, DocumentDigest: mcpclass.DocumentDigest(), SelectedOperation: operation,
		CandidateBuild: classTestBuild, RequestIdentity: "req-" + operation, Stage: goapiproof.EnablementProofStage,
		TerminalState: goapiproof.EnablementProofTerminalState, OrgID: "org-proof", RecordedBy: "test",
		ObservedAt: time.Now().UTC(), MeasurementRoute: route, BuildBinding: binding, ReviewEvidence: string(provenance),
	}); err != nil {
		t.Fatalf("receipt %s: %v", operation, err)
	}
}

func classEnable(pool *pgxpool.Pool, operations ...string) ([]goapiproof.EnableOutcome, error) {
	kinds := map[string]string{}
	for _, operation := range operations {
		kinds[operation] = goapiproof.OperationKindMCPClass
	}
	return goapiproof.Enable(ctxBackground(), pool, goapiproof.EnableRequest{
		SchemaDigest: classTestSchema, RunningBuild: classTestBuild, Operations: operations,
		OperationKinds: kinds, DocumentDigest: mcpclass.Digests(), Mode: goapiproof.TargetModeCanary,
		RolloutPercentage: goapiproof.EnforcedRolloutPercentage, RecordedBy: "test", ReviewEvidence: "class routing",
		PrincipalID: "test-principal",
	})
}

func classListener(t *testing.T, pool *pgxpool.Pool) (http.Handler, *countingMCPClient) {
	t.Helper()
	ch := &countingMCPClient{}
	sw := routeswitch.NewClassSwitch(pool, classTestSchema, mcpRoutingDigests())
	getenv := getenvFunc(func(string) string { return "" })
	return internalidentity.MCP(newMCPHandlerWithLimits(ch, nil, sw, getenv, mcpDefaultLimits())), ch
}

func classHotspots(t *testing.T, handler http.Handler) *httptest.ResponseRecorder {
	t.Helper()
	return mcpDo(handler, http.MethodPost, validMCPHeaders(), mcpBody(t, mcpHotspotsQuery, mcpHotspotsVariables(mcpTestOrg)))
}

// The state the system exists to reach: after seed, proof and enable, the
// listener serves the root; a root with no enabled row, and a root whose row is
// only seeded (shadow), answers root_field_not_enabled.
func TestMCPClassRowServesOnlyAfterSeedProofAndEnable(t *testing.T) {
	pool := startTestRegistryPostgres(t)
	listener, ch := classListener(t, pool)
	hotspots, overview := mcpclass.Operation("hotspots"), mcpclass.Operation("securityOverview")

	// Dormant: no rows at all.
	assertMCPRefused(t, classHotspots(t, listener), ch, http.StatusNotFound, mcpReasonRootFieldNotEnabled)

	// Seeded (shadow) is still dark on the MCP listener.
	classSeed(t, pool, hotspots, overview)
	assertMCPRefused(t, classHotspots(t, listener), ch, http.StatusNotFound, mcpReasonRootFieldNotEnabled)

	// Enable without a per-root receipt is refused, and nothing is served.
	if _, err := classEnable(pool, hotspots); err == nil || !strings.Contains(err.Error(), "no deployed_executed/match proof run") {
		t.Fatalf("enable without a receipt: err = %v, want the unproven refusal", err)
	}
	assertMCPRefused(t, classHotspots(t, listener), ch, http.StatusNotFound, mcpReasonRootFieldNotEnabled)

	classReceipt(t, pool, hotspots, goapiproof.RouteProof, goapiproof.EdgeBuildPresent)
	if _, err := classEnable(pool, hotspots); err != nil {
		t.Fatalf("enable with a receipt: %v", err)
	}
	rec := classHotspots(t, listener)
	if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), mcpReasonRootFieldNotEnabled) {
		t.Fatalf("enabled root: status %d body %s, want it served", rec.Code, rec.Body.String())
	}
	// The sibling root is seeded but has no receipt and no enable: still dark.
	securityOverview := mcpBody(t, `query O($orgId: String!) { securityOverview(orgId: $orgId) { __typename } }`, map[string]any{"orgId": mcpTestOrg})
	ch2 := ch.calls.Load()
	rec = mcpDo(listener, http.MethodPost, validMCPHeaders(), securityOverview)
	if rec.Code != http.StatusNotFound || ch.calls.Load() != ch2 {
		t.Fatalf("a root with no enabled row: status %d body %s, want 404 and no ClickHouse call", rec.Code, rec.Body.String())
	}
}

// CHAOS-8704 (owner ruling D4789): a class row at another schema digest is served. The listener that
// computes a different digest than the row's still lights the root; no carry has to move the row.
func TestMCPClassRowAtAnotherSchemaDigestIsServed(t *testing.T) {
	pool := startTestRegistryPostgres(t)
	classEnabled(t, pool, "hotspots")
	if !classServedAt(t, pool, "sha256:"+strings.Repeat("c", 64)) {
		t.Fatal("a canary class row at another schema digest does not serve the root")
	}
}

// The other three states stay dark: no class row at any digest, a non-served mode at another digest,
// and a non-served row at the LIVE digest over an older canary row (the live digest decides first, so
// `disable` still darks a root).
func TestMCPClassRowStatesThatStayDark(t *testing.T) {
	const moved = "sha256:7214-moved-schema-digest"
	hotspots := mcpclass.Operation("hotspots")
	t.Run("no class row", func(t *testing.T) {
		pool := startTestRegistryPostgres(t)
		if classServedAt(t, pool, moved) {
			t.Fatal("a root with no class row is served")
		}
	})
	t.Run("a python row at another digest", func(t *testing.T) {
		pool := startTestRegistryPostgres(t)
		pgseed.RoutingState(t.Context(), t, pool, classTestSchema, mcpclass.DocumentDigest(), hotspots, "python")
		if classServedAt(t, pool, moved) {
			t.Fatal("a python class row at another digest serves the root")
		}
	})
	t.Run("a live shadow row over an older canary row", func(t *testing.T) {
		pool := startTestRegistryPostgres(t)
		pgseed.RoutingState(t.Context(), t, pool, "sha256:an-older-digest", mcpclass.DocumentDigest(), hotspots, "canary")
		pgseed.RoutingState(t.Context(), t, pool, moved, mcpclass.DocumentDigest(), hotspots, "shadow")
		if classServedAt(t, pool, moved) {
			t.Fatal("the live digest's shadow row did not dark the root over an older canary row")
		}
	})
}

// Enable admits a class root by its class receipt in BOTH directions' terms: a
// receipt on the wrong route, an unbound one, or one for another document digest
// admits nothing.
func TestClassEnableAdmitsOnlyAProofRouteBoundClassReceipt(t *testing.T) {
	cases := map[string]struct {
		route, binding string
	}{
		"edge route":    {goapiproof.RouteEdge, goapiproof.EdgeBuildPresent},
		"unbound build": {goapiproof.RouteProof, goapiproof.EdgeBuildAbsent},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			pool := startTestRegistryPostgres(t)
			op := mcpclass.Operation("hotspots")
			classSeed(t, pool, op)
			classReceipt(t, pool, op, tc.route, tc.binding)
			if _, err := classEnable(pool, op); err == nil {
				t.Fatalf("a class receipt on %s was admitted as enablement proof", name)
			}
		})
	}
}

func proofHandlers(t *testing.T, pool *pgxpool.Pool) (mcp, proof, bare http.Handler, token func(org string) string, ch *countingMCPClient) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := principal.NewVerifier(writeTestJWKS(t, pub), itTestIssuer, itTestAudience)
	if err != nil {
		t.Fatal(err)
	}
	ch = &countingMCPClient{}
	getenv := getenvFunc(func(string) string { return "" })
	base := newMCPHandlerWithLimits(ch, nil, routeswitch.NewPostgresSwitch(pool, classTestSchema, mcpRoutingDigests()), getenv, mcpDefaultLimits())
	proofSwitch := routeswitch.NewProofSwitch(pool, classTestSchema, mcpRoutingDigests())
	proofHandler := newMCPProofHandler(base, proofSwitch, verifier, newProofOrgAllowed(pool))
	if proofHandler == nil {
		t.Fatal("newMCPProofHandler returned nil")
	}
	token = func(org string) string {
		return signDataHealthEnvelope(t, priv, principal.Claims{OrgID: org, Role: "admin"}, time.Now().Add(time.Hour))
	}
	return internalidentity.MCP(base), internalidentity.Internal(proofHandler), proofHandler, token, ch
}

// The proof route measures a shadow root through the SAME handler; the MCP
// listener never serves that root. Both gates of the proof route (the
// envelope, the proof-org allowlist) are executed.
func TestMCPProofRouteServesAShadowRootThatTheMCPListenerRefuses(t *testing.T) {
	pool := startTestRegistryPostgres(t)
	hotspots := mcpclass.Operation("hotspots")
	classSeed(t, pool, hotspots) // shadow
	if err := goapiproof.AddProofOrg(t.Context(), pool, goapiproof.ProofOrgRequest{OrgID: mcpTestOrg, RecordedBy: "test", ReviewEvidence: "class proof"}); err != nil {
		t.Fatal(err)
	}
	mcp, proof, bare, token, ch := proofHandlers(t, pool)

	// The MCP listener cannot read a shadow root.
	assertMCPRefused(t, classHotspots(t, mcp), ch, http.StatusNotFound, mcpReasonRootFieldNotEnabled)

	bearer := http.Header{}
	bearer.Set("Content-Type", "application/json")
	bearer.Set("Authorization", "Bearer "+token(mcpTestOrg))
	rec := mcpDo(proof, http.MethodPost, bearer, mcpBody(t, mcpHotspotsQuery, mcpHotspotsVariables(mcpTestOrg)))
	if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), mcpReasonRootFieldNotEnabled) {
		t.Fatalf("proof route, allowlisted org, shadow root: status %d body %s, want it served", rec.Code, rec.Body.String())
	}

	// Another org is not on the allowlist.
	other := http.Header{}
	other.Set("Content-Type", "application/json")
	other.Set("Authorization", "Bearer "+token("org-not-allowed"))
	calls := ch.calls.Load()
	rec = mcpDo(proof, http.MethodPost, other, mcpBody(t, mcpHotspotsQuery, mcpHotspotsVariables("org-not-allowed")))
	if rec.Code != http.StatusForbidden || ch.calls.Load() != calls {
		t.Fatalf("a non-allowlisted org: status %d body %s, want 403 with no ClickHouse call", rec.Code, rec.Body.String())
	}
	if reason, _ := mcpReason(t, rec); reason != mcpReasonProofOrg {
		t.Fatalf("reason = %q, want %q", reason, mcpReasonProofOrg)
	}

	// The internal identity headers are not a carrier here even beside a valid
	// envelope, and no carrier at all is refused.
	both := validMCPHeaders()
	both.Set("Authorization", "Bearer "+token(mcpTestOrg))
	if rec = mcpDo(proof, http.MethodPost, both, mcpBody(t, mcpHotspotsQuery, mcpHotspotsVariables(mcpTestOrg))); rec.Code != http.StatusUnauthorized {
		t.Fatalf("identity headers beside a valid envelope: status %d, want 401", rec.Code)
	}
	for name, header := range map[string]http.Header{"identity headers": validMCPHeaders(), "no carrier": {"Content-Type": {"application/json"}}} {
		rec = mcpDo(proof, http.MethodPost, header, mcpBody(t, mcpHotspotsQuery, mcpHotspotsVariables(mcpTestOrg)))
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s: status %d, want 401", name, rec.Code)
		}
	}

	// Off the internal listener the proof handler is not reachable at all.
	rec = mcpDo(bare, http.MethodPost, bearer, mcpBody(t, mcpHotspotsQuery, mcpHotspotsVariables(mcpTestOrg)))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("proof handler off the internal listener: status %d, want 404", rec.Code)
	}
}

func ctxBackground() context.Context { return context.Background() }

// classEnabled seeds, proves and enables ONE class root at classTestSchema.
func classEnabled(t *testing.T, pool *pgxpool.Pool, root string) {
	t.Helper()
	op := mcpclass.Operation(root)
	classSeed(t, pool, op)
	classReceipt(t, pool, op, goapiproof.RouteProof, goapiproof.EdgeBuildPresent)
	if _, err := classEnable(pool, op); err != nil {
		t.Fatalf("enable %s: %v", op, err)
	}
}

func classServedAt(t *testing.T, pool *pgxpool.Pool, schemaDigest string) bool {
	t.Helper()
	ch := &countingMCPClient{}
	sw := routeswitch.NewClassSwitch(pool, schemaDigest, mcpRoutingDigests())
	listener := internalidentity.MCP(newMCPHandlerWithLimits(ch, nil, sw, getenvFunc(func(string) string { return "" }), mcpDefaultLimits()))
	rec := classHotspots(t, listener)
	return rec.Code == http.StatusOK
}

// Repoint moves provenance only: a class row keeps serving and now names the
// running build, so the next roll's carry does not hit a stale build.
func TestMCPClassRepointKeepsTheRowServingAndNamesTheRunningBuild(t *testing.T) {
	pool := startTestRegistryPostgres(t)
	classEnabled(t, pool, "hotspots")
	const next = "7214bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if _, err := goapiproof.Repoint(t.Context(), pool, goapiproof.RepointRequest{
		SchemaDigest: classTestSchema, RunningBuild: next, RecordedBy: "test", ReviewEvidence: "after the roll", PrincipalID: "test-principal",
	}); err != nil {
		t.Fatalf("repoint: %v", err)
	}
	var build string
	if err := pool.QueryRow(t.Context(), `SELECT current_candidate_build FROM go_api_routing_state WHERE selected_operation = $1`, mcpclass.Operation("hotspots")).Scan(&build); err != nil || build != next {
		t.Fatalf("class row build = %q (err %v), want %q", build, err, next)
	}
	if !classServedAt(t, pool, classTestSchema) {
		t.Fatal("a repoint made the class root dark")
	}
}

// Disable by class name turns the root off at the listener.
func TestMCPClassDisableTurnsTheRootOff(t *testing.T) {
	pool := startTestRegistryPostgres(t)
	classEnabled(t, pool, "hotspots")
	if _, err := goapiproof.Disable(t.Context(), pool, goapiproof.DisableRequest{
		SchemaDigest: classTestSchema, Operations: []string{mcpclass.Operation("hotspots")}, DocumentDigest: mcpclass.Digests(),
		NewMode: "python", RecordedBy: "test", ReviewEvidence: "off", Apply: true,
	}); err != nil {
		t.Fatalf("disable: %v", err)
	}
	if classServedAt(t, pool, classTestSchema) {
		t.Fatal("a disabled class root is still served")
	}
}

// status names a dark root and does not list a class row as an UNREGISTERED operation.
func TestMCPClassStatusNamesDarkRootsAndProof(t *testing.T) {
	pool := startTestRegistryPostgres(t)
	classEnabled(t, pool, "hotspots")
	classSeed(t, pool, mcpclass.Operation("analytics")) // shadow: dark
	served, err := mcpclass.ServedRoots(schemav1.SDL)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := goapiproof.MCPClassStatusRows(t.Context(), pool, classTestSchema, served)
	if err != nil {
		t.Fatal(err)
	}
	by := map[string]goapiproof.MCPClassRootStatus{}
	for _, row := range rows {
		by[row.Root] = row
	}
	if len(rows) != len(mcpclass.SortedRoots()) {
		t.Fatalf("%d class roots reported, want one per allowlisted root (%d)", len(rows), len(mcpclass.SortedRoots()))
	}
	if h := by["hotspots"]; h.DigestState != goapiproof.DigestMatch || !h.Reachable || !h.Proven || h.Dark {
		t.Fatalf("hotspots = %+v, want a reachable, proven live row", h)
	}
	if a := by["analytics"]; a.DigestState != goapiproof.DigestMatch || a.Reachable || a.Proven || !a.Dark {
		t.Fatalf("analytics (shadow, no receipt) = %+v, want dark and unproven", a)
	}
	if m := by["catalog"]; m.DigestState != goapiproof.DigestMissing || !m.Dark {
		t.Fatalf("catalog (no row) = %+v, want MISSING and dark", m)
	}
	statuses, err := goapiproof.RoutingStatusRows(t.Context(), pool, classTestSchema, "", map[string]string{"featureFlags": "sha256:" + strings.Repeat("a", 64)})
	if err != nil {
		t.Fatal(err)
	}
	for _, status := range statuses {
		if mcpclass.IsOperation(status.Operation) {
			t.Fatalf("class row %s listed in the per-operation table as %s", status.Operation, status.DigestState)
		}
	}
}

// CHAOS-7833: the two routing-row families govern DIFFERENT routes and neither stands in for the other.
//   - document rows (selected_operation = the named document operation, e.g. "hotspots") govern the named-operation route (/query on :8090/:8091,
//     the route acr's run_operation calls): query_route.go builds that switch from the document digests;
//   - class rows (selected_operation = "mcp:<root>") govern the MCP listener (:8092, acr's graphql_query): mcp_route.go asks sw.Enabled("mcp:"+root).
//
// A canary DOCUMENT row for hotspots therefore does not serve the MCP listener, and an enabled CLASS row does not enable the document operation.
func TestDocumentRowsAndMCPClassRowsGovernSeparateRoutes(t *testing.T) {
	pool := startTestRegistryPostgres(t)
	ctx := t.Context()
	const docOp, docDigest = "hotspots", "sha256:7833aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if _, err := pool.Exec(ctx, `INSERT INTO public.go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
		VALUES ($1, $2, $3, $4)`, classTestSchema, docDigest, docOp, classTestBuild); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO public.go_api_routing_state
		(schema_digest, document_digest, selected_operation, current_candidate_build, owner, mode, rollout_percentage, review_evidence, recorded_by)
		VALUES ($1, $2, $3, $4, 'go', 'canary', 100, 'a document row', 'test')`, classTestSchema, docDigest, docOp, classTestBuild); err != nil {
		t.Fatal(err)
	}
	documentSwitch := routeswitch.NewPostgresSwitch(pool, classTestSchema, map[string]string{docOp: docDigest})
	if !documentSwitch.Enabled(docOp) {
		t.Fatal("the document row is canary: the document-route switch must serve it")
	}

	// 1. The canary DOCUMENT row does not enable the MCP listener's root: it asks for "mcp:hotspots", which has no row.
	listener, ch := classListener(t, pool)
	assertMCPRefused(t, classHotspots(t, listener), ch, http.StatusNotFound, mcpReasonRootFieldNotEnabled)

	// ...and an enabled CLASS row for the same root is what serves it, while the document row stays what it was.
	hotspotsClass := mcpclass.Operation("hotspots")
	classSeed(t, pool, hotspotsClass)
	classReceipt(t, pool, hotspotsClass, goapiproof.RouteProof, goapiproof.EdgeBuildPresent)
	if _, err := classEnable(pool, hotspotsClass); err != nil {
		t.Fatalf("enable mcp:hotspots: %v", err)
	}
	if rec := classHotspots(t, listener); rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), mcpReasonRootFieldNotEnabled) {
		t.Fatalf("enabled class row: status %d body %s, want the MCP listener to serve it", rec.Code, rec.Body.String())
	}
	if !documentSwitch.Enabled(docOp) {
		t.Fatal("enabling the class row changed the document row's reachability")
	}

	// 2. An enabled CLASS row does not enable a document operation of the same name on the document route.
	overview := mcpclass.Operation("securityOverview")
	classSeed(t, pool, overview)
	classReceipt(t, pool, overview, goapiproof.RouteProof, goapiproof.EdgeBuildPresent)
	if _, err := classEnable(pool, overview); err != nil {
		t.Fatalf("enable the class row: %v", err)
	}
	classSwitch := routeswitch.NewPostgresSwitch(pool, classTestSchema, mcpRoutingDigests())
	if !classSwitch.Enabled(overview) {
		t.Fatal("the class row is canary: the MCP switch must serve it")
	}
	otherDocSwitch := routeswitch.NewPostgresSwitch(pool, classTestSchema, map[string]string{"securityOverview": docDigest})
	if otherDocSwitch.Enabled("securityOverview") {
		t.Fatal("an enabled mcp:securityOverview class row must not enable the document operation securityOverview")
	}
}
