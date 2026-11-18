package syncdispatchruntime

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// TestPostSyncFenceUsesTheSharedCommitSnapshot: the post_sync fence has no copy of the commit-time check. The row
// lock text lives only in transport_fence.go, native_post_sync.go carries no fence SQL or refusal reason of its own,
// and it reaches the check through the shared predicate, snapshot read and decision.
func TestPostSyncFenceUsesTheSharedCommitSnapshot(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	lockText := []string{"FOR NO KEY UPDATE", "NOWAIT"}
	ownText := []string{"FOR NO KEY UPDATE", "NOWAIT", "outbox.attempts", "delivery_attempt_mismatch", "outbox_not_dispatched", "route_generation_mismatch"}
	used := map[string]bool{"transportReferencePredicate": false, "readCommitSnapshot": false, "decideCommit": false}
	parsed := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		parsed++
		ast.Inspect(file, func(node ast.Node) bool {
			switch node := node.(type) {
			case *ast.BasicLit:
				if node.Kind != token.STRING {
					return true
				}
				if name != "transport_fence.go" {
					for _, text := range lockText {
						if strings.Contains(node.Value, text) {
							t.Errorf("%s: a string literal carries %q; the commit-time row lock is commitSnapshotSQL (transport_fence.go) only", name, text)
						}
					}
				}
				if name == "native_post_sync.go" {
					for _, text := range ownText {
						if strings.Contains(node.Value, text) {
							t.Errorf("native_post_sync.go: a string literal carries %q; the post_sync fence must use the shared predicate and decision", text)
						}
					}
				}
			case *ast.Ident:
				if _, ok := used[node.Name]; ok && name == "native_post_sync.go" {
					used[node.Name] = true
				}
			}
			return true
		})
	}
	if parsed < 40 {
		t.Fatalf("parsed %d package files, want the whole package", parsed)
	}
	for identifier, seen := range used {
		if !seen {
			t.Errorf("native_post_sync.go does not use %s", identifier)
		}
	}
}
