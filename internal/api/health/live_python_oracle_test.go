package health

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
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

func TestSchemaRevisionsMatchLivePythonAlembic(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	command := exec.Command(python, "-c", pythonRevisionsProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live Python alembic: %v", pyoracle.RunError(python, err, output))
	}
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
	proofDir := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDir == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proofDir, "api-health-revisions"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
}
