package docstools_test

import (
	"os"
	"strings"
	"testing"
)

// The ported checks keep every path as the user wrote it (pathlib keeps ".."; the kernel resolves "link/../x" through
// the link). filepath.Join, Dir, Clean and Abs clean ".." lexically, which made the tree walk, the report parent and
// the walk below a linked site directory differ from Python. Only dc_facts.go may use filepath (its root is the
// working directory's physical path and fixed components).
func TestDocsChecksCleanNoUserPath(t *testing.T) {
	for _, name := range []string{"dc_args.go", "dc_links.go", "dc_pyhelp.go", "dc_pyjson.go", "dc_search.go"} {
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		for number, line := range strings.Split(string(raw), "\n") {
			if strings.Contains(line, "filepath.") || strings.Contains(line, `"path/filepath"`) || strings.Contains(line, "path.Clean") {
				t.Errorf("%s:%d cleans a path lexically: %s", name, number+1, strings.TrimSpace(line))
			}
		}
	}
}
