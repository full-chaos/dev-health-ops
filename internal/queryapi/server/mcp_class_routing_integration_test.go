//go:build integration

package server

// CHAOS-7214: the MCP class's routing rows end to end against a real,
// migrated Postgres. The state the verbs exist to reach is asserted at the
// listener's own reader (routeswitch.PostgresSwitch over mcpRoutingDigests,
// the object buildQueryRoute builds): after seed + proof + enable the root is
// served, and a root without an enabled row answers 404 root_field_not_enabled.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	schemav1 "github.com/full-chaos/dev-health-ops/contracts/graphql/v1"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/internalidentity"
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
	sw := routeswitch.NewClassDecisionSwitch(pool, mcpRoutingDigests())
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
func TestMCPClassRowServesOnlyAfterSeedAndEnable(t *testing.T) {
	pool := startTestRegistryPostgres(t)
	listener, ch := classListener(t, pool)
	hotspots, overview := mcpclass.Operation("hotspots"), mcpclass.Operation("securityOverview")

	// Dormant: no rows at all.
	assertMCPRefused(t, classHotspots(t, listener), ch, http.StatusNotFound, mcpReasonRootFieldNotEnabled)

	// Seeded (shadow) is still dark on the MCP listener.
	classSeed(t, pool, hotspots, overview)
	assertMCPRefused(t, classHotspots(t, listener), ch, http.StatusNotFound, mcpReasonRootFieldNotEnabled)

	// Enable is the operator's recorded decision, with no receipt in front of it (D6024).
	if _, err := classEnable(pool, hotspots); err != nil {
		t.Fatalf("enable: %v", err)
	}
	rec := classHotspots(t, listener)
	if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), mcpReasonRootFieldNotEnabled) {
		t.Fatalf("enabled root: status %d body %s, want it served", rec.Code, rec.Body.String())
	}
	// The sibling root is seeded but not enabled: still dark.
	securityOverview := mcpBody(t, `query O($orgId: String!) { securityOverview(orgId: $orgId) { __typename } }`, map[string]any{"orgId": mcpTestOrg})
	ch2 := ch.calls.Load()
	rec = mcpDo(listener, http.MethodPost, validMCPHeaders(), securityOverview)
	if rec.Code != http.StatusNotFound || ch.calls.Load() != ch2 {
		t.Fatalf("a root with no enabled row: status %d body %s, want 404 and no ClickHouse call", rec.Code, rec.Body.String())
	}
}

func ctxBackground() context.Context { return context.Background() }

// classEnabled seeds, proves and enables ONE class root at classTestSchema.
func classEnabled(t *testing.T, pool *pgxpool.Pool, root string) {
	t.Helper()
	op := mcpclass.Operation(root)
	classSeed(t, pool, op)
	if _, err := classEnable(pool, op); err != nil {
		t.Fatalf("enable %s: %v", op, err)
	}
}

func classServed(t *testing.T, pool *pgxpool.Pool) bool {
	t.Helper()
	ch := &countingMCPClient{}
	sw := routeswitch.NewClassDecisionSwitch(pool, mcpRoutingDigests())
	listener := internalidentity.MCP(newMCPHandlerWithLimits(ch, nil, sw, getenvFunc(func(string) string { return "" }), mcpDefaultLimits()))
	rec := classHotspots(t, listener)
	return rec.Code == http.StatusOK
}

const movedSchemaDigest = "sha256:7214-moved-schema-digest"

// CHAOS-8735: the decision does not depend on the schema digest. After a schema move nothing has to be carried
// and the root is still served.
func TestMCPClassDecisionSurvivesASchemaDigestMove(t *testing.T) {
	pool := startTestRegistryPostgres(t)
	classEnabled(t, pool, "hotspots")
	if !classServed(t, pool) {
		t.Fatal("precondition: the enabled root is served")
	}
	var rows int
	if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM go_api_class_decision`).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("class decisions = %d (err %v), want one row per operation", rows, err)
	}
}

// vet-2 repro: after a schema move `disable` still turns a root off (it acts on the decision, not on a row at the
// live digest), and `enable` at the new digest turns it on again.
func TestMCPClassDisableAndEnableWorkAfterASchemaMove(t *testing.T) {
	pool := startTestRegistryPostgres(t)
	hotspots := mcpclass.Operation("hotspots")
	classEnabled(t, pool, "hotspots")
	if _, err := goapiproof.Disable(t.Context(), pool, goapiproof.DisableRequest{
		SchemaDigest: movedSchemaDigest, Operations: []string{hotspots}, DocumentDigest: mcpclass.Digests(),
		NewMode: "disabled", RecordedBy: "test", ReviewEvidence: "off after the move", Apply: true,
	}); err != nil {
		t.Fatalf("disable after a schema move: %v", err)
	}
	if classServed(t, pool) {
		t.Fatal("a root disabled after a schema move is still served")
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
	if classServed(t, pool) {
		t.Fatal("a disabled class root is still served")
	}
}

// status names a dark root and a live one.
func TestMCPClassStatusNamesDarkRoots(t *testing.T) {
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
	if h := by["hotspots"]; h.DigestState != goapiproof.DigestMatch || !h.Reachable || h.Dark {
		t.Fatalf("hotspots = %+v, want a reachable live row", h)
	}
	if a := by["analytics"]; a.DigestState != goapiproof.DigestMatch || a.Reachable || !a.Dark {
		t.Fatalf("analytics (shadow) = %+v, want dark", a)
	}
	if m := by["catalog"]; m.DigestState != goapiproof.DigestMissing || !m.Dark {
		t.Fatalf("catalog (no row) = %+v, want MISSING and dark", m)
	}
}

// CHAOS-7833, CHAOS-8702: the two routes are decided apart, and neither is decided by a routing row of the other kind.
//   - the named-operation route (/query on :8090/:8091, the route acr's run_operation calls) serves every registered catalog operation, with no row:
//     query_route.go builds that switch from the document digests (routeswitch.NewCatalogSwitch);
//   - the MCP listener (:8092, acr's graphql_query) serves a root only when its class decision (go_api_class_decision, "mcp:<root>") says so:
//     mcp_route.go asks sw.Enabled("mcp:"+root).
//
// A catalog operation is therefore served with no class decision, and an enabled class decision does not change what the catalog switch answers.
func TestDocumentRoutesAndMCPClassDecisionsAreDecidedApart(t *testing.T) {
	pool := startTestRegistryPostgres(t)
	catalog := routeswitch.NewCatalogSwitch(map[string]string{"hotspots": "sha256:7833aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})
	if !catalog.Enabled("hotspots") {
		t.Fatal("a registered catalog operation must be served with no row at all")
	}
	if catalog.Enabled("mcp:hotspots") {
		t.Fatal("the catalog switch must not serve an operation it does not register: a class root is decided by its own table")
	}

	// 1. With no class decision the MCP listener refuses the root, whatever the catalog switch says of the document.
	listener, ch := classListener(t, pool)
	assertMCPRefused(t, classHotspots(t, listener), ch, http.StatusNotFound, mcpReasonRootFieldNotEnabled)

	// ...and an enabled class decision is what serves it.
	hotspotsClass := mcpclass.Operation("hotspots")
	classSeed(t, pool, hotspotsClass)
	if _, err := classEnable(pool, hotspotsClass); err != nil {
		t.Fatalf("enable mcp:hotspots: %v", err)
	}
	if rec := classHotspots(t, listener); rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), mcpReasonRootFieldNotEnabled) {
		t.Fatalf("enabled class decision: status %d body %s, want the MCP listener to serve it", rec.Code, rec.Body.String())
	}

	// 2. An enabled class decision for one root does not light another root.
	overview := mcpclass.Operation("securityOverview")
	classSeed(t, pool, overview)
	if _, err := classEnable(pool, overview); err != nil {
		t.Fatalf("enable the class decision: %v", err)
	}
	classSwitch := routeswitch.NewClassDecisionSwitch(pool, mcpRoutingDigests())
	if !classSwitch.Enabled(overview) {
		t.Fatal("the class decision is canary: the MCP switch must serve it")
	}
	if classSwitch.Enabled(mcpclass.Operation("catalog")) {
		t.Fatal("a root with no decision must stay dark")
	}
}

// vet-2 repro: `status` and the switch read the same decision. For every mode of the one decision row, and for no
// row at all, the status verdict is the switch's answer.
func TestMCPClassStatusAndTheSwitchAgreeForEveryState(t *testing.T) {
	pool := startTestRegistryPostgres(t)
	served, err := mcpclass.ServedRoots(schemav1.SDL)
	if err != nil {
		t.Fatal(err)
	}
	hotspots := mcpclass.Operation("hotspots")
	statusReachable := func() bool {
		rows, err := goapiproof.MCPClassStatusRows(t.Context(), pool, movedSchemaDigest, served)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			if row.Operation == hotspots {
				return row.Reachable
			}
		}
		t.Fatal("hotspots is not reported")
		return false
	}
	if statusReachable() || classServed(t, pool) {
		t.Fatal("no decision: status or the switch calls the root lit")
	}
	for _, mode := range []string{"canary", "primary", "shadow", "python", "disabled"} {
		pgseed.ClassDecision(t.Context(), t, pool, hotspots, mode)
		if got, want := statusReachable(), classServed(t, pool); got != want {
			t.Errorf("mode %s: status reachable=%t, the switch serves=%t", mode, got, want)
		}
		if want := mode == "canary" || mode == "primary"; classServed(t, pool) != want {
			t.Errorf("mode %s: the switch serves=%t, want %t", mode, classServed(t, pool), want)
		}
	}
}
