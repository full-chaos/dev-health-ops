package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/repokey/cells"
)

// The decision allowlist and the grant read serve the same key the request
// carries; each schema is held to the one cell table.
func TestRepositoryKeySchemasHoldTheRepositoryKeyCells(t *testing.T) {
	root, err := RepoRoot(".")
	if err != nil {
		t.Fatal(err)
	}
	examples := filepath.Join(ContractsDir(root), "examples")
	surfaces := []struct {
		surface, file string
		set           func(doc map[string]any, key string)
	}{
		{"authorization_decision.v1", "authorization_decision/decision-valid-allow-allowlist-several-keys.json", func(doc map[string]any, key string) {
			doc["obligations"].(map[string]any)["repository_allowlist"] = []any{key}
		}},
		{"grant-read.v1", "grant-read/valid-active-repository.json", func(doc map[string]any, key string) {
			doc["resource_key"] = key
		}},
	}
	for _, s := range surfaces {
		raw, err := os.ReadFile(filepath.Join(examples, s.file))
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range append(cells.Table(), cells.Cell{Name: "organization-wide", Key: "*", Valid: true}) {
			var doc map[string]any
			if err := json.Unmarshal(raw, &doc); err != nil {
				t.Fatal(err)
			}
			s.set(doc, c.Key)
			if err := Validate(root, s.surface, doc); (err == nil) != c.Valid {
				t.Errorf("%s %s: error = %v, want valid=%v", s.surface, c.Name, err, c.Valid)
			}
		}
	}
}
