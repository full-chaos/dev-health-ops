package docstools_test

import (
	"os"
	"strings"
	"testing"
)

// The ported checks keep every path as the user wrote it (pathlib keeps ".."; the kernel resolves "link/../x" through
// the link). filepath.Join, Dir, Clean and Abs clean ".." lexically, which made the tree walk, the report parent and
// the walk below a linked site directory differ from Python. os.Getwd is banned everywhere (it returns the logical $PWD, Python's os.getcwd the physical one).
func TestDocsChecksCleanNoUserPath(t *testing.T) {
	for _, name := range []string{"a1_pyhelp.go", "dc_args.go", "dc_links.go", "dc_pyhelp.go", "dc_pyjson.go", "dc_search.go"} {
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		for number, line := range strings.Split(string(raw), "\n") {
			banned := []string{"os.Getwd("}
			for _, fn := range []string{"Clean", "Join", "Dir", "Base"} {
				if i := strings.Index(line, "path."+fn); i >= 0 && (i == 0 || !strings.ContainsRune("abcdefghijklmnopqrstuvwxyz", rune(line[i-1]))) {
					t.Errorf("%s:%d uses path.%s (a lexical clean): %s", name, number+1, fn, strings.TrimSpace(line))
				}
			}
			banned = append(banned, "filepath.", `"path/filepath"`)
			for _, b := range banned {
				if strings.Contains(line, b) {
					t.Errorf("%s:%d uses %s (a lexical clean or the logical working directory): %s", name, number+1, b, strings.TrimSpace(line))
				}
			}
		}
	}
}
