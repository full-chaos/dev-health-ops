package main

import (
	"os"
	"path/filepath"
	"testing"
)

// TestTheCommittedMatrixSatisfiesItsOwnContract runs the real -check against
// the real repository, so `go test ./...` fails the moment the committed page
// disagrees with the committed sources or a row claims a status its evidence
// column cannot support.
//
// This is separate from internal/migrationmatrix's unit tests on purpose:
// those prove the RULES work on synthetic rows, this proves the SHIPPED
// DOCUMENT obeys them. A repo can have a perfect validator and a lying page.
func TestTheCommittedMatrixSatisfiesItsOwnContract(t *testing.T) {
	root := repoRoot(t)
	if err := runCheck(root); err != nil {
		t.Fatalf("the committed migration matrix fails its own contract: %v\n\n"+
			"Re-render with:\n"+
			"  go run ./cmd/dev-health-migration-matrix -render -root . -routing <snapshot.json>\n"+
			"(-routing takes a JSON dump of go_api_routing_state; see routingFilePayload.)", err)
	}
}

// TestCheckFailsWhenTheDocIsEditedByHand is the RED half. A generated block is
// only as good as the check that notices someone improved it in place -- which
// is precisely how the ic_finalize citation came to name a file that had been
// deleted weeks earlier.
func TestCheckFailsWhenTheDocIsEditedByHand(t *testing.T) {
	root := t.TempDir()
	realRoot := repoRoot(t)

	for _, relative := range []string{
		docRelative,
		statusRelative,
		renderRelative,
		nativeRelative,
		digestPinRelative,
	} {
		copyInto(t, filepath.Join(realRoot, relative), filepath.Join(root, relative))
	}

	// Sanity: the copy still passes, so a failure below is caused by the
	// edit and not by the copying.
	if err := runCheck(root); err != nil {
		t.Fatalf("the copied tree should pass before it is tampered with: %v", err)
	}

	docPath := filepath.Join(root, docRelative)
	raw, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("read copied doc: %v", err)
	}
	// The most tempting hand edit there is: soften the honest cell.
	tampered := replaceFirst(string(raw), "**unknown** (read", "`dc4194f58404` (read")
	if tampered == string(raw) {
		t.Fatal("expected the rendered doc to contain an **unknown** deployed-revision cell to tamper with")
	}
	if err := os.WriteFile(docPath, []byte(tampered), 0o644); err != nil {
		t.Fatalf("write tampered doc: %v", err)
	}

	if err := runCheck(root); err == nil {
		t.Fatal("hand-editing a generated cell must fail the check; it passed")
	}
}

func replaceFirst(haystack, old, replacement string) string {
	index := indexOf(haystack, old)
	if index < 0 {
		return haystack
	}
	return haystack[:index] + replacement + haystack[index+len(old):]
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}

func copyInto(t *testing.T, from, to string) {
	t.Helper()
	raw, err := os.ReadFile(from) //nolint:gosec // test-local path
	if err != nil {
		t.Fatalf("read %s: %v", from, err)
	}
	if err := os.MkdirAll(filepath.Dir(to), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(to), err)
	}
	if err := os.WriteFile(to, raw, 0o644); err != nil {
		t.Fatalf("write %s: %v", to, err)
	}
}

// repoRoot walks up from the test's working directory to the module root.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find the module root above the test's working directory")
		}
		dir = parent
	}
}
