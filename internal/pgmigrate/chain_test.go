package pgmigrate

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The chain after the baseline covers every Alembic revision above it: a PR that adds
// an Alembic revision fails here until it adds the revision's sql/ file, and a chain file
// must continue the revision its Alembic script says it does (CHAOS-6801: before, a
// database below the head could not be upgraded by dho at all). The Python scripts
// are read while they exist; once they are deleted the chain is the source and this
// test skips.
func TestChainCoversEveryAlembicRevision(t *testing.T) {
	versions := filepath.Join("..", "..", "src", "dev_health_ops", "alembic", "versions")
	files, err := filepath.Glob(filepath.Join(versions, "[0-9][0-9][0-9][0-9]_*.py"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Skip("the Alembic scripts are gone: the chain is the source")
	}
	baseline, err := LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	application := applicationHead(baseline)
	revisionLine := regexp.MustCompile(`(?m)^revision(?:: str)? = "([0-9]+)"`)
	downLine := regexp.MustCompile(`(?m)^down_revision(?:: [^=]+)? = "([0-9]+)"`)
	type script struct{ revision, down string }
	var above []script
	for _, file := range files {
		source, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		revision := revisionLine.FindStringSubmatch(string(source))
		down := downLine.FindStringSubmatch(string(source))
		if revision == nil {
			t.Fatalf("%s: no revision id found: the scan measures nothing", file)
		}
		if revision[1] > application {
			if down == nil {
				t.Fatalf("%s: revision %s above the baseline has no single down_revision", file, revision[1])
			}
			above = append(above, script{revision[1], down[1]})
		}
	}
	sort.Slice(above, func(i, j int) bool { return above[i].revision < above[j].revision })
	if len(above) == 0 && len(chain) == 0 {
		return
	}
	want := map[string]string{}
	for _, entry := range above {
		want[entry.revision] = entry.down
	}
	have := map[string]bool{}
	previous := application
	for _, file := range chain {
		have[file.Revision] = true
		down, isAlembic := want[file.Revision]
		if !isAlembic {
			t.Errorf("sql/%s has no Alembic revision above the baseline %s: rename it to its revision or remove it", file.Name, application)
			continue
		}
		if down != previous {
			t.Errorf("sql/%s continues %s, but its Alembic script says down_revision %s", file.Name, previous, down)
		}
		previous = file.Revision
		if !strings.HasPrefix(file.Name, file.Revision+"_") {
			t.Errorf("sql/%s is not named for revision %s", file.Name, file.Revision)
		}
	}
	for _, entry := range above {
		if !have[entry.revision] {
			t.Errorf("Alembic revision %s (down_revision %s) has no sql/%s_*.sql: render it with `alembic upgrade %s:%s --sql`, without the BEGIN/COMMIT and alembic_version lines", entry.revision, entry.down, entry.revision, entry.down, entry.revision)
		}
	}
}
