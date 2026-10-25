package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// An anchor that names a range must read forward: line_end is never before line. Range-aware consumers of the
// registries would otherwise mislocate the citation.
func TestRegistryAnchorRangesAreNotInverted(t *testing.T) {
	root := testRoot(t)
	files, err := filepath.Glob(filepath.Join(ContractsDir(root), "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no registries found: %v", err)
	}
	ranges := 0
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		var document any
		if json.Unmarshal(raw, &document) != nil {
			continue
		}
		var walk func(node any, where string)
		walk = func(node any, where string) {
			switch typed := node.(type) {
			case map[string]any:
				line, hasLine := typed["line"].(float64)
				end, hasEnd := typed["line_end"].(float64)
				if hasLine && hasEnd {
					ranges++
					if end < line {
						t.Errorf("%s %s: line_end %d is before line %d (%v)", filepath.Base(file), where, int(end), int(line), typed["path"])
					}
				}
				for key, child := range typed {
					walk(child, where+"/"+key)
				}
			case []any:
				for i, child := range typed {
					walk(child, where+"/"+string(rune('0'+i%10)))
				}
			}
		}
		walk(document, "")
	}
	if ranges == 0 {
		t.Fatal("no anchor range was measured: the walk found none")
	}
}
