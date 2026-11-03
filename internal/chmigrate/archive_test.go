package chmigrate_test

import (
	"encoding/json"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

// archiveDir holds what the baseline replaced when it moved to the chain head (CHAOS-11751): the 098 baseline and
// the chain files 099..144. Nothing applies these files; only tests read them.
const archiveDir = "archive"

// loadArchivedBaseline returns the 098 baseline the head replaced.
func loadArchivedBaseline(t *testing.T) chmigrate.Baseline {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(archiveDir, "baseline-098", "head.json"))
	if err != nil {
		t.Fatalf("read the archived baseline: %v", err)
	}
	var baseline chmigrate.Baseline
	if err := json.Unmarshal(raw, &baseline); err != nil {
		t.Fatalf("decode the archived baseline: %v", err)
	}
	if len(baseline.Versions) == 0 || len(baseline.Objects) == 0 {
		t.Fatalf("the archived baseline holds %d versions and %d objects", len(baseline.Versions), len(baseline.Objects))
	}
	return baseline
}

// loadArchivedChain returns the archived chain files in apply order.
func loadArchivedChain(t *testing.T) []chmigrate.ChainFile {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(archiveDir, "sql", "*.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatalf("%s/sql holds no chain file", archiveDir)
	}
	sort.Strings(paths)
	chain := make([]chmigrate.ChainFile, 0, len(paths))
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		chain = append(chain, chmigrate.ChainFile{Version: filepath.Base(path), SQL: string(raw)})
	}
	return chain
}

// The head baseline records every version the archived baseline and the archived chain record, and the live chain
// holds only files after the last archived one: a database at the archived head is at this baseline.
func TestBaselineRecordsEveryArchivedVersion(t *testing.T) {
	head, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	live, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	archived := loadArchivedChain(t)
	recorded := map[string]bool{}
	for _, version := range head.Versions {
		recorded[version] = true
	}
	var want []string
	want = append(want, loadArchivedBaseline(t).Versions...)
	for _, file := range archived {
		want = append(want, file.Version)
	}
	for _, version := range want {
		if !recorded[version] {
			t.Errorf("the head baseline does not record the archived version %s", version)
		}
	}
	if len(head.Versions) != len(want) {
		t.Errorf("the head baseline records %d versions, the archive %d", len(head.Versions), len(want))
	}
	last := archived[len(archived)-1].Version
	for _, file := range live {
		if file.Version <= last {
			t.Errorf("live chain file %s is not after the archived head %s", file.Version, last)
		}
	}
	if len(live) == 0 && chmigrate.HeadVersion(head, live) != last {
		t.Errorf("the head version is %s, want the archived head %s", chmigrate.HeadVersion(head, live), last)
	}
}

// The binary embeds no archived file: an archived chain file applied by a build would be a second schema path.
func TestNoNonTestSourceEmbedsTheArchive(t *testing.T) {
	sources, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, source := range sources {
		if strings.HasSuffix(source, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), source, nil, parser.ParseComments)
		if err != nil {
			t.Fatal(err)
		}
		checked++
		for _, group := range file.Comments {
			for _, comment := range group.List {
				if strings.HasPrefix(comment.Text, "//go:embed") && strings.Contains(comment.Text, archiveDir) {
					t.Errorf("%s embeds the archive: %s", source, comment.Text)
				}
			}
		}
	}
	if checked == 0 {
		t.Fatal("no non-test source read")
	}
}
