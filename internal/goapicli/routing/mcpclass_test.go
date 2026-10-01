package routing

import (
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
