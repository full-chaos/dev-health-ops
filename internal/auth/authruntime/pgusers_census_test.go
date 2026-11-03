package authruntime

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Every route set reads the go-api users through pgUsers, so none of them
// reads public is_superuser while auth-service owns the platform roles.
func TestEveryRouteSetReadsUsersThroughPGUsers(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if n := strings.Count(string(src), "authapi.PGUsers{"); n > 0 && !(file == "runtime.go" && n == 1 && strings.Contains(string(src), "func pgUsers(")) {
			t.Errorf("%s builds authapi.PGUsers directly %d time(s); use pgUsers(cfg, pool, schema)", file, n)
		}
	}
}
