package routing

import (
	"bytes"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
)

func TestResolveClassScope(t *testing.T) {
	scope, isClass, err := resolveClassScope("mcp:hotspots,mcp:analytics")
	if err != nil || !isClass || len(scope.Operations) != 2 {
		t.Fatalf("scope = %+v isClass=%v err=%v", scope, isClass, err)
	}
	for _, op := range scope.Operations {
		if scope.Digests[op] != mcpclass.DocumentDigest() {
			t.Fatalf("%s digest = %q", op, scope.Digests[op])
		}
	}
	if all, isClass, err := resolveClassScope("all-mcp"); err != nil || !isClass || len(all.Operations) != len(mcpclass.SortedRoots()) {
		t.Fatalf("all-mcp = %+v %v %v", all, isClass, err)
	}
	// Document operations and the empty / default values are not class scope.
	for _, raw := range []string{"", "all-registered", "featureFlags", "featureFlags,hotspots"} {
		if _, isClass, err := resolveClassScope(raw); isClass || err != nil {
			t.Errorf("%q: isClass=%v err=%v, want the document path", raw, isClass, err)
		}
	}
	// Mixed, unknown and not-allowlisted roots are refused.
	for _, raw := range []string{"mcp:hotspots,featureFlags", "mcp:dataHealth", "mcp:home", "mcp:", "all-mcp,featureFlags"} {
		if _, isClass, err := resolveClassScope(raw); !isClass || err == nil {
			t.Errorf("%q: isClass=%v err=%v, want a refusal", raw, isClass, err)
		}
	}
	if _, _, err := resolveClassScope("mcp:hotspots,featureFlags"); err == nil || !strings.Contains(err.Error(), "separately") {
		t.Errorf("the mixed refusal does not say to run them separately: %v", err)
	}
}

func TestRequireClassRootsServedAcceptsEveryAllowlistedRoot(t *testing.T) {
	scope, _, err := resolveClassScope("all-mcp")
	if err != nil {
		t.Fatal(err)
	}
	if err := requireClassRootsServed(scope.Operations); err != nil {
		t.Fatalf("an allowlisted root is not a Query field of this binary's SDL: %v", err)
	}
	if err := requireClassRootsServed([]string{"mcp:notARoot"}); err == nil {
		t.Fatal("a root outside the SDL was accepted")
	}
}

// status prints what a proven class root's receipt rests on, and nothing for a root
// that is not proven.
func TestPrintMCPClassStatusNamesTheProofAndItsExcludedShapes(t *testing.T) {
	var out bytes.Buffer
	previous := stdout
	stdout = &out
	t.Cleanup(func() { stdout = previous })
	mode := "canary"
	printMCPClassStatus(statusReport{MCPClass: []statusReportMCPRoot{
		{Root: "analytics", Operation: "mcp:analytics", DigestState: "MATCH", Mode: &mode, Reachable: true, Proven: true,
			ProofReference: "go_document_route", ProofShapesCounted: 33, ProofShapesMatched: 32,
			ProofShapesExcluded: []string{"featureFlagTimeseries=doc_operation_not_receipt_backed", "other=x"}},
		{Root: "hotspots", Operation: "mcp:hotspots", DigestState: "MATCH", Mode: &mode, Reachable: true, Proven: false,
			ProofReference: "go_document_route", ProofShapesExcluded: []string{"must-not-print=x"}},
		// proven by a receipt written before the reference was recorded: its counts and
		// exclusions still show, with the reference said to be unrecorded
		{Root: "catalog", Operation: "mcp:catalog", DigestState: "MATCH", Mode: &mode, Reachable: true, Proven: true,
			ProofShapesCounted: 3, ProofShapesMatched: 3, ProofShapesExcluded: []string{"old-shape=known_refusal"}},
		// one signal each: counts only, exclusions only
		{Root: "hotspots", Operation: "mcp:hotspots", DigestState: "MATCH", Mode: &mode, Reachable: true, Proven: true, ProofShapesCounted: 2},
		{Root: "securityAlerts", Operation: "mcp:securityAlerts", DigestState: "MATCH", Mode: &mode, Reachable: true, Proven: true,
			ProofShapesExcluded: []string{"only-excluded=x"}},
		// proven with no provenance at all: nothing to print
		{Root: "workGraphFlow", Operation: "mcp:workGraphFlow", DigestState: "MATCH", Mode: &mode, Reachable: true, Proven: true,
			ProofShapesExcluded: nil},
	}})
	got := out.String()
	for _, want := range []string{
		"proof: reference=go_document_route shapes counted=33 matched=32 excluded=2",
		"excluded: featureFlagTimeseries=doc_operation_not_receipt_backed",
		"excluded: other=x",
		"proof: reference=(not recorded) shapes counted=3 matched=3 excluded=1",
		"excluded: old-shape=known_refusal",
		"proof: reference=(not recorded) shapes counted=2 matched=0 excluded=0",
		"proof: reference=(not recorded) shapes counted=0 matched=0 excluded=1",
		"excluded: only-excluded=x",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("output lacks %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "must-not-print") {
		t.Fatalf("an unproven root printed its proof:\n%s", got)
	}
	// Exactly four proof lines: analytics and the three older-receipt roots; not the unproven root, not the root with no provenance.
	if n := strings.Count(got, "    proof: reference="); n != 4 {
		t.Fatalf("%d proof lines, want 4 (analytics and the three older-receipt roots):\n%s", n, got)
	}
}

// CHAOS-7499: a root proven under the stochastic leaf class says so in `status`: counted, named, never matched.
func TestPrintMCPClassStatusNamesTheStochasticShapes(t *testing.T) {
	var out bytes.Buffer
	previous := stdout
	stdout = &out
	t.Cleanup(func() { stdout = previous })
	mode := "canary"
	printMCPClassStatus(statusReport{MCPClass: []statusReportMCPRoot{
		{Root: "capacityForecast", Operation: "mcp:capacityForecast", DigestState: "MATCH", Mode: &mode, Reachable: true, Proven: true,
			ProofReference: "go_document_route", ProofShapesCounted: 1, ProofShapesMatched: 0, ProofShapesStochastic: []string{"capacityForecast"}},
	}})
	got := out.String()
	for _, want := range []string{
		"proof: reference=go_document_route shapes counted=1 matched=0 excluded=0",
		"proven under the stochastic leaf class (not a match): capacityForecast",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("output lacks %q:\n%s", want, got)
		}
	}
}
