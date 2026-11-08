package edgetoken

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// hs256MinterAllowed is the one production file that may sign HS256: the GitHub App install state, not an access
// token.
var hs256MinterAllowed = map[string]bool{"internal/api/githubapp/state.go": true}

// No production file mints an HS256 access token: edgetoken.NewSigner has no caller and no file signs with
// jwt.SigningMethodHS256 outside the allow list (CHAOS-13155).
func TestNoProductionFileMintsAnHS256AccessToken(t *testing.T) {
	root := "../../.."
	var bad []string
	err := filepath.WalkDir(root+"/internal", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		if strings.Contains(rel, "/testsupport/") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(raw)
		if !hs256MinterAllowed[rel] && strings.Contains(text, "SigningMethodHS256") {
			bad = append(bad, rel+": SigningMethodHS256")
		}
		if !strings.HasPrefix(rel, "internal/auth/edgetoken/") && strings.Contains(text, "edgetoken.NewSigner(") {
			bad = append(bad, rel+": edgetoken.NewSigner")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(bad) > 0 {
		t.Fatalf("production HS256 minters remain:\n%s", strings.Join(bad, "\n"))
	}
}
