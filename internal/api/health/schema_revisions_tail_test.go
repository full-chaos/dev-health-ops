package health

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
)

// missingTailRevisions are the Go-only chain revisions (above pgmigrate.FrozenHead) a database could be migrated to
// that satisfyingRevisions does not list: the api would report such a database as not at the schema it needs.
func missingTailRevisions(chain []pgmigrate.ChainFile) []string {
	var missing []string
	for _, file := range pgmigrate.ChainAbove(chain, pgmigrate.FrozenHead) {
		if !satisfyingRevisions[file.Revision] {
			missing = append(missing, file.Revision)
		}
	}
	return missing
}

func TestSchemaRevisionsCoverTheGoOnlyChainTail(t *testing.T) {
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	if missing := missingTailRevisions(chain); len(missing) > 0 {
		t.Errorf("chain revisions %v are not in satisfyingRevisions (schema_revisions.go): add them by hand", missing)
	}
}

func TestTheTailCoverageCheckNamesAMissingRevision(t *testing.T) {
	planted := []pgmigrate.ChainFile{{Revision: pgmigrate.FrozenHead}, {Revision: "9998"}}
	if got := missingTailRevisions(planted); len(got) != 1 || got[0] != "9998" {
		t.Errorf("missing = %v, want [9998]", got)
	}
	if got := missingTailRevisions([]pgmigrate.ChainFile{{Revision: pgmigrate.FrozenHead}}); len(got) != 0 {
		t.Errorf("a chain ending at FrozenHead misses %v", got)
	}
}
