package writerinventory

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var legacyLinkTokensSQL = regexp.MustCompile(`(?i)\b(FROM|JOIN|UPDATE|INTO|TABLE)\s+(public\.)?(email_verification_tokens|password_reset_tokens)\b`)

// go-api's public link-token tables have no reader and no writer left: the
// credential routes are auth-service's, so no production code runs SQL on them
// and the writer scan finds no writer of them.
func TestNoProductionCodeNamesTheLegacyLinkTokenTables(t *testing.T) {
	root := repoRoot(t)
	watched, err := Watched(root)
	if err != nil {
		t.Fatal(err)
	}
	found, err := Scan(root, watched)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range found {
		if w.Table == "public.email_verification_tokens" || w.Table == "public.password_reset_tokens" {
			t.Errorf("writer of %s: %s %s %s", w.Table, w.File, w.Func, w.Verb)
		}
	}
	for _, dir := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(root, path)
			if d.IsDir() {
				if filepath.ToSlash(rel) == "internal/testsupport" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			src, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			for i, line := range strings.Split(string(src), "\n") {
				if legacyLinkTokensSQL.MatchString(line) {
					t.Errorf("SQL on a legacy link-token table: %s:%d", filepath.ToSlash(rel), i+1)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
