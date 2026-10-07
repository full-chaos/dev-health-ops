package chwrite

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// The two shadow tables are written by the worker and read by nothing that
// serves a user. dho api has an exact ClickHouse grant list that does not
// name them, but query-api has NO ClickHouse posture, so the database does not
// stop it from reading any table. This test is the only barrier for query-api:
// a non-test Go file under the serving trees must not name either table.
// Neither table goes into a Latest...Source() helper either.
var shadowTables = []string{"work_unit_investment_shadow", "llm_categorization_attempts"}

// shadowReaderBanRoots are the serving trees, relative to the module root:
// query-api and its GraphQL resolvers, the query-api service, the HTTP api
// and the api service.
var shadowReaderBanRoots = []string{
	"internal/queryapi",
	"internal/queryapiservice",
	"internal/apiservice",
	"internal/api",
}

// filesNamingAShadowTable returns the non-test Go files under the roots that
// name a shadow table, as module-relative paths, and the number of files read.
func filesNamingAShadowTable(t *testing.T, moduleRoot string, roots []string) (offenders []string, read int) {
	t.Helper()
	for _, root := range roots {
		base := filepath.Join(moduleRoot, filepath.FromSlash(root))
		if _, err := os.Stat(base); err != nil {
			t.Fatalf("serving tree %s does not exist, so the ban measured nothing: %v", root, err)
		}
		err := filepath.WalkDir(base, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			content, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			read++
			for _, table := range shadowTables {
				if strings.Contains(string(content), table) {
					relative, _ := filepath.Rel(moduleRoot, path)
					offenders = append(offenders, filepath.ToSlash(relative)+" names "+table)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
	}
	return offenders, read
}

func TestNoServingFileReadsAShadowTable(t *testing.T) {
	moduleRoot, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	offenders, read := filesNamingAShadowTable(t, moduleRoot, shadowReaderBanRoots)
	// A measurement that did not happen must fail: an empty walk would pass.
	if read < 100 {
		t.Fatalf("the ban read only %d serving files, so it did not measure the serving trees", read)
	}
	if len(offenders) > 0 {
		t.Fatalf("the shadow tables are never read by a serving path (query-api has no ClickHouse posture, so this test is its only barrier):\n%s",
			strings.Join(offenders, "\n"))
	}
}

// The guard must fail on the planted defect: a serving-tree file that names a
// shadow table. The planted tree is a temp dir, never the real one.
func TestShadowReaderBanFailsOnAPlantedReader(t *testing.T) {
	for _, table := range shadowTables {
		root := t.TempDir()
		dir := filepath.Join(root, "internal", "queryapi", "investment")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		source := "package investment\n\nconst q = `SELECT * FROM " + table + "`\n"
		if err := os.WriteFile(filepath.Join(dir, "reader.go"), []byte(source), 0o600); err != nil {
			t.Fatal(err)
		}
		// A test file naming the table is allowed.
		if err := os.WriteFile(filepath.Join(dir, "reader_test.go"), []byte(source), 0o600); err != nil {
			t.Fatal(err)
		}
		offenders, read := filesNamingAShadowTable(t, root, []string{"internal/queryapi"})
		if read != 1 || len(offenders) != 1 || !strings.Contains(offenders[0], table) {
			t.Fatalf("planted reader of %s: read %d, offenders %v", table, read, offenders)
		}
	}
}
