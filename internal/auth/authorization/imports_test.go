package authorization

import (
	"os/exec"
	"strings"
	"testing"
)

// The decision core decides from the snapshot alone. Repository inventory
// (the ClickHouse repos list) and everything that serves it must stay out of
// the package's dependency closure, so a repository the inventory happens to
// hold can never widen a decision.
func TestPackageNeverImportsRepositoryInventory(t *testing.T) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Fatalf("go toolchain not found: %v", err)
	}
	out, err := exec.Command(goBin, "list", "-deps", "-f", "{{.ImportPath}}", ".").Output()
	if err != nil {
		t.Fatalf("go list -deps: %v", err)
	}
	banned := []string{
		"internal/repoinventory", "internal/queryapi", "internal/apiservice", "internal/storage/clickhouse",
		"internal/storage/postgres", "net/http", "database/sql",
	}
	deps := strings.Fields(string(out))
	if len(deps) < 3 {
		t.Fatalf("dependency list looks empty: %v", deps)
	}
	for _, d := range deps {
		for _, b := range banned {
			if strings.HasSuffix(d, b) || strings.Contains(d, b+"/") {
				t.Errorf("dependency closure holds %s (banned %s)", d, b)
			}
		}
	}
}
