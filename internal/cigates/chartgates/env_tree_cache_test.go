package chartgates_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func envTreeFixture(t *testing.T) (string, []string) {
	t.Helper()
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "cmd"), 0o755); err != nil {
		t.Fatal(err)
	}
	files := map[string]string{
		"go.mod":                    "module example.com/envfixture\n\ngo 1.22\n",
		"internal/x/base.go":        "package x\nfunc Base(lookup func(string) string) string { return lookup(\"BASE_KEY\") }\n",
		"internal/x/integration.go": "//go:build integration\n\npackage x\nfunc Integration(lookup func(string) string) string { return lookup(\"INTEGRATION_KEY\") }\n",
		"internal/x/base_test.go":   "package x\nvar TestOnly = 1\n",
	}
	for name, source := range files {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	env := append(os.Environ(), "GOFLAGS=-mod=mod", "GOPROXY=off", "GOSUMDB=off", "GOTOOLCHAIN=local", "GOWORK=off")
	return root, env
}

// Preserve unit boundaries and order as well as a sorted file-name/error census.
func envTreeDump(units []envUnit, errs []string, sorted bool) string {
	var rows []string
	for _, unit := range units {
		var files []string
		for name := range unit.files {
			files = append(files, name)
		}
		sort.Strings(files)
		row, _ := json.Marshal(files)
		rows = append(rows, "unit:"+string(row))
	}
	for _, err := range errs {
		rows = append(rows, "error:"+err)
	}
	if sorted {
		sort.Strings(rows)
	}
	return strings.Join(rows, "\n")
}

func TestEnvTreeConcurrentMatchesSerialFixture(t *testing.T) {
	root, env := envTreeFixture(t)
	serialUnits, serialErrs := loadEnvTree(root, append(env, "CHARTGATES_ENVTREE_SERIAL=1"))
	units, errs := loadEnvTree(root, append(env, "CHARTGATES_ENVTREE_SERIAL=0"))
	if len(errs) != 0 || len(serialErrs) != 0 {
		t.Fatalf("fixture errors: concurrent=%v serial=%v", errs, serialErrs)
	}
	// An independent serial reference catches dropping/reordering a set in both paths.
	defaults, defaultErrs := loadEnvUnits(root, nil, env)
	integration, integrationErrs := loadEnvUnits(root, []string{"-tags=integration"}, env)
	wantUnits := append(defaults, integration...)
	wantErrs := append(defaultErrs, integrationErrs...)
	for _, sorted := range []bool{false, true} {
		want := envTreeDump(wantUnits, wantErrs, sorted)
		if got := envTreeDump(units, errs, sorted); got != want {
			t.Fatalf("concurrent dump (sorted=%v):\n%s\nwant:\n%s", sorted, got, want)
		}
		if got := envTreeDump(serialUnits, serialErrs, sorted); got != want {
			t.Fatalf("serial dump (sorted=%v):\n%s\nwant:\n%s", sorted, got, want)
		}
	}
	if got := scanEnvUnits(defaults)["INTEGRATION_KEY"]; len(got) != 0 {
		t.Fatalf("default tags included integration-only reader: %v", got)
	}
	want := []string{"internal/x/integration.go"}
	if got := scanEnvUnits(units)["INTEGRATION_KEY"]; !reflect.DeepEqual(got, want) {
		t.Fatalf("integration readers = %v, want %v", got, want)
	}
	if got := scanEnvUnits(serialUnits); !reflect.DeepEqual(got, scanEnvUnits(units)) {
		t.Fatalf("serial/concurrent scans differ: %v", got)
	}
}

func TestEnvTreeErrorsKeepSerialOrderFixture(t *testing.T) {
	root, env := envTreeFixture(t)
	// Distinct errors in each build variant make lost/reordered errors observable.
	for name, source := range map[string]string{
		"default_error.go":     "//go:build !integration\n\npackage x\nvar A = missingDefault\nvar B = missingDefaultAgain\n",
		"integration_error.go": "//go:build integration\n\npackage x\nvar A = missingIntegration\nvar B = missingIntegrationAgain\n",
	} {
		if err := os.WriteFile(filepath.Join(root, "internal/x", name), []byte(source), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	defaults, defaultErrs := loadEnvUnits(root, nil, env)
	integration, integrationErrs := loadEnvUnits(root, []string{"-tags=integration"}, env)
	if len(defaultErrs) == 0 || len(integrationErrs) == 0 {
		t.Fatalf("fixture did not produce errors: %v / %v", defaultErrs, integrationErrs)
	}
	want := envTreeDump(append(defaults, integration...), append(defaultErrs, integrationErrs...), false)
	for _, serial := range []string{"0", "1"} {
		units, errs := loadEnvTree(root, append(env, "CHARTGATES_ENVTREE_SERIAL="+serial))
		if got := envTreeDump(units, errs, false); got != want {
			t.Fatalf("serial=%s dump:\n%s\nwant:\n%s", serial, got, want)
		}
	}
	// Driver failure must return just the Load error and no units.
	units, errs := loadEnvUnits(filepath.Join(root, "absent"), nil, env)
	if len(units) != 0 || len(errs) != 1 {
		t.Fatalf("Load error gave %d units and %v errors", len(units), errs)
	}
}

func TestEnvReaderCacheDeepCopyFixture(t *testing.T) {
	units := checkSources(map[string]string{
		"internal/x/a.go": "package x\nfunc A(lookup func(string) string) string { return lookup(\"KEY\") }\n",
		"internal/x/b.go": "package x\nfunc B(lookup func(string) string) string { return lookup(\"KEY\") }\n",
	})
	var cache envReaderCache
	want := map[string][]string{"KEY": {"internal/x/a.go", "internal/x/b.go"}}
	got := cache.scan(units)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("first scan = %v, want %v", got, want)
	}
	got["KEY"][0] = "corrupted"
	got["KEY"] = append(got["KEY"], "appended")
	got["ADDED"] = []string{"new"}
	// Nil units also proves that the second call uses the memoized scan.
	if again := cache.scan(nil); !reflect.DeepEqual(again, want) {
		t.Fatalf("second scan = %v, want %v", again, want)
	}
}
