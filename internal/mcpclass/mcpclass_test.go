package mcpclass

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	schemav1 "github.com/full-chaos/dev-health-ops/contracts/graphql/v1"
)

// The class digest is the sha256 of the key, computed here independently of the
// package's own digest function: a change to either side is visible.
func TestDocumentDigestIsTheSHA256OfTheClassKey(t *testing.T) {
	if DocumentKey != "dev-health-ops/mcp-freeform-class/v1" {
		t.Fatalf("DocumentKey = %q: a bump needs the tooling that writes the rows", DocumentKey)
	}
	sum := sha256.Sum256([]byte("dev-health-ops/mcp-freeform-class/v1"))
	want := hex.EncodeToString(sum[:])
	if got := strings.TrimPrefix(DocumentDigest(), "sha256:"); got != want {
		t.Fatalf("DocumentDigest = %q, want sha256 hex %q", DocumentDigest(), want)
	}
}

// Every allowlisted root is a Query root field of the SDL this binary embeds: an
// allowlist entry the SDL lacks would be a root the listener lists and cannot serve.
func TestEveryAllowlistedRootIsAQueryFieldOfTheEmbeddedSDL(t *testing.T) {
	served, err := ServedRoots(schemav1.SDL)
	if err != nil {
		t.Fatal(err)
	}
	for _, root := range SortedRoots() {
		if !served[root] {
			t.Errorf("allowlisted root %q is not a Query field of the embedded SDL", root)
		}
	}
	if len(served) != len(SortedRoots()) {
		t.Errorf("served %d roots, allowlist has %d", len(served), len(SortedRoots()))
	}
}

func TestIsClassRowNeedsBothTheOperationShapeAndTheClassDigest(t *testing.T) {
	op, digest := Operation("hotspots"), DocumentDigest()
	for name, tc := range map[string]struct {
		operation, digest string
		want              bool
	}{
		"class row":                   {op, digest, true},
		"prefix under another digest": {op, "sha256:" + strings.Repeat("0", 64), false},
		"class digest on a document":  {"hotspots", digest, false},
		"neither":                     {"hotspots", "x", false},
	} {
		if got := IsClassRow(tc.operation, tc.digest); got != tc.want {
			t.Errorf("%s: IsClassRow = %v, want %v", name, got, tc.want)
		}
	}
}

func TestResolveOperations(t *testing.T) {
	got, err := ResolveOperations("mcp:hotspots, analytics ,mcp:hotspots")
	if err != nil || len(got) != 2 || got[0] != "mcp:analytics" || got[1] != "mcp:hotspots" {
		t.Fatalf("ResolveOperations = %v, %v", got, err)
	}
	all, err := ResolveOperations("all-mcp")
	if err != nil || len(all) != len(SortedRoots()) {
		t.Fatalf("all-mcp = %v, %v", all, err)
	}
	for _, bad := range []string{"", " , ", "mcp:home", "mcp:recommendations", "mcp:workItemTeamAttributions", "mcp:nope", "dataHealth"} {
		if _, err := ResolveOperations(bad); err == nil {
			t.Errorf("ResolveOperations(%q) did not refuse", bad)
		}
	}
}

func TestAllowedOperationIsTheAllowlistAndNothingElse(t *testing.T) {
	for _, root := range SortedRoots() {
		if !AllowedOperation(Operation(root)) {
			t.Errorf("%s is not allowed", root)
		}
	}
	for _, op := range []string{"hotspots", "mcp:", "mcp:dataHealth", "mcp:home"} {
		if AllowedOperation(op) {
			t.Errorf("%q is allowed", op)
		}
	}
	if len(Digests()) != len(SortedRoots()) {
		t.Fatal("Digests does not hold one row per root")
	}
}
