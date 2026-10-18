//go:build authhistory

package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Needs full git history, so it is built only under -tags authhistory, which
// only `ci/check_go.sh auth history` (auth-test job, fetch-depth: 0) passes.
// It FAILS on a shallow checkout and never skips.
func TestEveryEndpointProfileInventoryDeclaresAVerifiableSourceCommit(t *testing.T) {
	root := testRoot(t)
	files, err := filepath.Glob(filepath.Join(ContractsDir(root), "endpoint-profiles.*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no endpoint-profiles.*.json inventory found (err=%v): the check would measure nothing", err)
	}
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		var document map[string]any
		if err := json.Unmarshal(raw, &document); err != nil {
			t.Fatalf("%s: %v", file, err)
		}
		if problems := problemsForSourceCommit(root, filepath.Base(file), document); len(problems) != 0 {
			t.Errorf("%s", strings.Join(problems, "\n"))
			continue
		}
		t.Logf("AUTH_SOURCE_COMMIT_VERIFIED %s", filepath.Base(file))
	}
}
