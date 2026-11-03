package removedenv

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

// The removed switch has one reader, ObserveOrgsOwned. This census holds every other non-test Go file clear of the
// key and of the names its readers and grants had.
var removedNames = regexp.MustCompile(`AUTH_ORGS_OWNED|OrgsOwned|OwnsOrgs|ownsOrgs|OrgsOn\b|OrgsEnv|EnvOrgsOwned|orgsOwned|OrganizationWriteGrant|organizationWriteStatements|organizationWriteMissing|isOrganizationWriteColumn`)

func TestNoOtherFileReadsTheRemovedOrgsOwnedKey(t *testing.T) {
	_, here, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(here), "..", "..", "..")
	own := filepath.Join(filepath.Dir(here), "removedenv.go")
	var found []string
	for _, top := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") || path == own {
				return nil
			}
			body, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if match := removedNames.Find(body); match != nil {
				rel, _ := filepath.Rel(root, path)
				found = append(found, rel+" "+string(match))
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(found) > 0 {
		t.Fatalf("%d file(s) still name the removed switch or its grant:\n%s", len(found), strings.Join(found, "\n"))
	}
}
