package workgraph

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// rawErrorAttr matches a log attribute that carries an error value or its
// text under the key "error".
var rawErrorAttr = regexp.MustCompile(`slog\.Any\(\s*"error"|"error",\s*[A-Za-z_][A-Za-z0-9_.]*(\.Error\(\))?[,)}]`)

func TestNoWorkgraphLogCallCarriesARawError(t *testing.T) {
	var offenders []string
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for index, line := range strings.Split(string(source), "\n") {
			if rawErrorAttr.MatchString(line) {
				offenders = append(offenders, fmt.Sprintf("%s:%d: %s", path, index+1, strings.TrimSpace(line)))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(offenders) > 0 {
		t.Fatalf("a raw error reaches a log attribute (use ErrorAttr / withErrorAttrs):\n%s", strings.Join(offenders, "\n"))
	}
}
