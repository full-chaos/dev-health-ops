package writerinventory

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var refreshTokensSQL = regexp.MustCompile(`(?i)\b(FROM|JOIN|UPDATE|INTO)\s+public\.refresh_tokens\b`)

// restore-fence's legacy public.refresh_tokens section runs as the migration role (restorefence/doc.go), never as the
// auth runtime role this test is about.
var refreshTokensMigrationRoleRows = map[string]bool{
	"internal/auth/restorefence/specs.go|section.applySQL|public.refresh_tokens|UPDATE": true,
}

// auth-service holds no privilege on go-api's public.refresh_tokens, so no
// auth-service code reads or writes it.
func TestAuthServiceNeitherReadsNorWritesRefreshTokens(t *testing.T) {
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
		if w.Table == "public.refresh_tokens" && strings.HasPrefix(w.File, "internal/auth/") {
			t.Errorf("auth-service writer of public.refresh_tokens: %s %s %s", w.File, w.Func, w.Verb)
		}
	}
	dynamic := map[string]bool{}
	for _, e := range DynamicAllowlist {
		dynamic[e.Key()] = true
		if e.Table == "public.refresh_tokens" && strings.HasPrefix(e.File, "internal/auth/") && !refreshTokensMigrationRoleRows[e.Key()] {
			t.Errorf("auth-service dynamic writer of public.refresh_tokens: %s %s %s", e.File, e.Func, e.Verb)
		}
	}
	for k := range refreshTokensMigrationRoleRows {
		if !dynamic[k] {
			t.Errorf("stale migration-role row (not in DynamicAllowlist): %s", k)
		}
	}
	err = filepath.WalkDir(filepath.Join(root, "internal", "auth"), func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for i, line := range strings.Split(string(src), "\n") {
			if refreshTokensSQL.MatchString(line) {
				rel, _ := filepath.Rel(root, path)
				t.Errorf("auth-service SQL on public.refresh_tokens: %s:%d", rel, i+1)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
