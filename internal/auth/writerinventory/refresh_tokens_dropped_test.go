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

// acp ruling (CHAOS-13504): restore-fence runs as the owner login, never the auth runtime role (restorefence/doc.go:13).
// Exempt is only its one public.refresh_tokens section (specs.go:248), matched by file and statement text: a second
// section on that table, or a changed one, fails.
var refreshTokensMigrationRoleRows = map[string]bool{
	"internal/auth/restorefence/specs.go|section.applySQL|public.refresh_tokens|UPDATE": true,
}

const refreshTokensRestoreFenceSection = `{name: "public.refresh_tokens", legacy: true, table: "refresh_tokens", kind: kindRevoke, key: colUUID("id"), ` +
	`cols: []column{colTS("revoked_at")}, revoked: "s.revoked_at IS NOT NULL", active: "t.revoked_at IS NULL"}`

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
	texts, err := restoreFenceSectionTexts(root, "public.refresh_tokens")
	if err != nil {
		t.Fatal(err)
	}
	if len(texts) != 1 || texts[0] != refreshTokensRestoreFenceSection {
		t.Errorf("restore-fence must hold exactly the one exempt public.refresh_tokens section; it holds %d:\n%s", len(texts), strings.Join(texts, "\n"))
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
