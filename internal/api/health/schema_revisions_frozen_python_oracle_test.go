package health

import (
	"encoding/json"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

func TestSchemaRevisionsMatchFrozenPythonAlembic(t *testing.T) {
	output := frozenPython(t, "schema-revisions.golden.json", programoracle.Program{Name: "alembic revisions"})[0]
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var result struct {
		Minimum   string   `json:"minimum"`
		Revisions []string `json:"revisions"`
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &result); err != nil {
		t.Fatalf("decode: %v: %s", err, output)
	}
	var goRevisions []string
	for revision, satisfies := range satisfyingRevisions {
		// health.go reads the VALUE: an entry set to false is not a satisfying revision, so it is not listed.
		if satisfies {
			goRevisions = append(goRevisions, revision)
		}
	}
	sort.Strings(goRevisions)
	if result.Minimum != minimumSchemaRevision || strings.Join(result.Revisions, ",") != strings.Join(goRevisions, ",") {
		t.Fatalf("schema revisions drifted from the Alembic chain: regenerate schema_revisions.go\n Python minimum %s %v\n Go     minimum %s %v",
			result.Minimum, result.Revisions, minimumSchemaRevision, goRevisions)
	}
}
