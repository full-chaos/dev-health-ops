package graph

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Every non-test file of this package that builds the Home response calls home.ResolveScope first (CHAOS-6119): the filter's reason
// reads the resolved scope, and a caller that skips the call answers the scope it was handed.
func TestEveryHomeBuildingFileResolvesTheScopeFirst(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	building := 0
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		src := string(raw)
		build := strings.Index(src, "home.BuildResponse(")
		if build < 0 {
			continue
		}
		building++
		resolve := strings.Index(src, "home.ResolveScope(")
		if resolve < 0 || resolve > build {
			t.Errorf("%s calls home.BuildResponse without calling home.ResolveScope before it", file)
		}
	}
	if building == 0 {
		t.Fatal("no file builds the Home response: the census measured nothing")
	}
}
