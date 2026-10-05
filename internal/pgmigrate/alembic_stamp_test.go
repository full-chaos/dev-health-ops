package pgmigrate_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// alembicScriptsStamp is the digest of the Alembic scripts tree (src/dev_health_ops/alembic/versions: every file,
// sorted by path, one "path sha256" line each) the CHAOS-7797 goldens were recorded from: the pinned build
// (statesPythonBuild) holds exactly this tree. The record verb already proves the recording ran on the pinned
// build's whole src (the golden header's producer_digest); this stamp is the check a run with NO Python can make:
// a script that changes here changes what the states, the walk and the old-script heads describe, and the goldens
// must then be re-recorded on the build that holds the new scripts. Freshness is by digest, not by re-execution.
const alembicScriptsStamp = "11b978cc90d79a112af44be1b3cacb24984d8c34f3dbb958df170cce4de837af"

// alembicScriptsDigest is the digest of the Alembic scripts under root (a checkout root).
func alembicScriptsDigest(root string) (string, error) {
	base := filepath.Join(root, "src", "dev_health_ops", "alembic", "versions")
	var lines []string
	err := filepath.WalkDir(base, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == "__pycache__" {
				return filepath.SkipDir
			}
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(base, path)
		sum := sha256.Sum256(raw)
		lines = append(lines, fmt.Sprintf("%s %s", filepath.ToSlash(rel), hex.EncodeToString(sum[:])))
		return nil
	})
	if err != nil {
		return "", err
	}
	if len(lines) == 0 {
		return "", fmt.Errorf("%s holds no Alembic script: the digest would be of nothing", base)
	}
	sort.Strings(lines)
	total := sha256.Sum256([]byte(strings.Join(lines, "\n") + "\n"))
	return hex.EncodeToString(total[:]), nil
}

// requireAlembicStamp fails a RECORDING whose Python checkout holds other scripts than the stamp names.
func requireAlembicStamp(t *testing.T, pythonRoot string) {
	t.Helper()
	got, err := alembicScriptsDigest(pythonRoot)
	if err != nil {
		t.Fatal(err)
	}
	if got != alembicScriptsStamp {
		t.Fatalf("the Python checkout %s holds Alembic scripts with digest %s, the stamp is %s: record on the build the stamp names", pythonRoot, got, alembicScriptsStamp)
	}
}

// TestAlembicScriptsAreWhatTheGoldensWereRecordedFrom needs no Python: it fails when the Alembic scripts of this
// tree differ from the tree the goldens were recorded from (or are gone and the stamp says otherwise is not
// possible: an empty tree fails too, the measurement did not happen).
func TestAlembicScriptsAreWhatTheGoldensWereRecordedFrom(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	got, err := alembicScriptsDigest(root)
	if err != nil {
		t.Fatal(err)
	}
	if got != alembicScriptsStamp {
		t.Fatalf("the Alembic scripts (src/dev_health_ops/alembic/versions) digest is %s, the goldens of this package were recorded from %s: re-record testdata/golden/{baseline_states,hook_states,preflight_states,history_walk,old_script_heads}.json on the build that holds the new scripts, then update the stamp", got, alembicScriptsStamp)
	}
}
