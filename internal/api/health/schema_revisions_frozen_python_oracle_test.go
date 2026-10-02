package health

import (
	"encoding/json"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// pythonRevisionsProgram walks the REAL Alembic script directory the way
// migrate._database_has_revision does and prints every revision that has
// the minimum as an ancestor (itself included).
const pythonRevisionsProgram = `
import json
from alembic.script import ScriptDirectory
from dev_health_ops.migrate import _make_alembic_config, _APPLICATION_SCHEMA_MINIMUM_REVISION as minimum
scripts = ScriptDirectory.from_config(_make_alembic_config())
out = sorted(r.revision for r in scripts.walk_revisions()
             if minimum in {a.revision for a in scripts.iterate_revisions(r.revision, "base")})
print(json.dumps({"minimum": minimum, "revisions": out}))
`

func TestSchemaRevisionsMatchFrozenPythonAlembic(t *testing.T) {
	output := frozenPython(t, "schema-revisions.golden.json", programoracle.Program{Name: "alembic revisions", Text: pythonRevisionsProgram})[0]
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var result struct {
		Minimum   string   `json:"minimum"`
		Revisions []string `json:"revisions"`
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &result); err != nil {
		t.Fatalf("decode: %v: %s", err, output)
	}
	var goRevisions []string
	for revision := range satisfyingRevisions {
		goRevisions = append(goRevisions, revision)
	}
	sort.Strings(goRevisions)
	if result.Minimum != minimumSchemaRevision || strings.Join(result.Revisions, ",") != strings.Join(goRevisions, ",") {
		t.Fatalf("schema revisions drifted from the Alembic chain: regenerate schema_revisions.go\n Python minimum %s %v\n Go     minimum %s %v",
			result.Minimum, result.Revisions, minimumSchemaRevision, goRevisions)
	}
}
