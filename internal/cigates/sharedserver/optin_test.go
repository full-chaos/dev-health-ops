package sharedserver_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	containersImportPath = "github.com/full-chaos/dev-health-ops/internal/testsupport/containers"

	// sharedTestMain is the TestMain of a package that runs on shared servers.
	sharedTestMain = "package opted\n\nimport (\n\t\"os\"\n\t\"testing\"\n\n\t\"" + containersImportPath + "\"\n)\n\n" +
		"func TestMain(m *testing.M) { os.Exit(containers.RunWithSharedServers(m.Run)) }\n"
)

// optsIn reports whether a test file holds a TestMain that runs the package
// on shared servers.
func optsIn(src string) (bool, error) {
	return strings.Contains(src, optInCall), nil
}

// missingFromBatch returns the packages of a batch (internal/<dir>) that are
// not among the opted-in directories (relative to internal).
func missingFromBatch(optedIn, batch []string) []string {
	have := map[string]bool{}
	for _, dir := range optedIn {
		have["internal/"+dir] = true
	}
	var missing []string
	for _, pkg := range batch {
		if !have[pkg] {
			missing = append(missing, pkg)
		}
	}
	return missing
}

func mainFile(imports, body string) string {
	return "package opted\n\nimport (\n" + imports + ")\n\n" + body
}

const (
	stdImports  = "\t\"os\"\n\t\"testing\"\n\n\t\"" + containersImportPath + "\"\n"
	venueImport = "\t\"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle\"\n"
)

func TestOptInNeedsATestMainThatRunsTheTestsInsideTheSharedCall(t *testing.T) {
	cases := []struct {
		name string
		src  string
		want bool
	}{
		{"m.Run passed to the call", sharedTestMain, true},
		{"a func literal around a runner", mainFile(stdImports+venueImport,
			"func TestMain(m *testing.M) {\n\tos.Exit(containers.RunWithSharedServers(func() int { return venueoracle.RunTests(m) }))\n}\n"), true},
		{"setup before the call", mainFile(stdImports,
			"func TestMain(m *testing.M) {\n\tsetup()\n\tos.Exit(containers.RunWithSharedServers(m.Run))\n}\n\nfunc setup() {}\n"), true},
		{"an aliased import", mainFile("\t\"os\"\n\t\"testing\"\n\n\tc \""+containersImportPath+"\"\n",
			"func TestMain(m *testing.M) { os.Exit(c.RunWithSharedServers(m.Run)) }\n"), true},
		{"the marker only in a comment", mainFile(stdImports,
			"// TestMain would call containers.RunWithSharedServers(m.Run) one day.\nfunc TestMain(m *testing.M) {\n\t// containers.RunWithSharedServers(\n\tos.Exit(m.Run())\n}\n"), false},
		{"the marker in a string literal", mainFile(stdImports,
			"func TestMain(m *testing.M) {\n\t_ = \"containers.RunWithSharedServers(\"\n\tos.Exit(m.Run())\n}\n"), false},
		{"an unused reference", mainFile(stdImports,
			"func TestMain(m *testing.M) {\n\t_ = containers.RunWithSharedServers\n\tos.Exit(m.Run())\n}\n"), false},
		{"the call in a func TestMain never calls", mainFile(stdImports,
			"func runShared(m *testing.M) int { return containers.RunWithSharedServers(m.Run) }\n\nfunc TestMain(m *testing.M) { os.Exit(m.Run()) }\n"), false},
		{"m.Run again outside the call", mainFile(stdImports,
			"func TestMain(m *testing.M) {\n\t_ = containers.RunWithSharedServers(m.Run)\n\tos.Exit(m.Run())\n}\n"), false},
		{"a runner given m outside the call", mainFile(stdImports+venueImport,
			"func TestMain(m *testing.M) {\n\tcode := venueoracle.RunTests(m)\n\tos.Exit(containers.RunWithSharedServers(func() int { return code }))\n}\n"), false},
		{"the call result is dropped", mainFile(stdImports,
			"func TestMain(m *testing.M) {\n\tcontainers.RunWithSharedServers(m.Run)\n}\n"), false},
		{"the call runs nothing of m", mainFile(stdImports,
			"func TestMain(m *testing.M) {\n\tos.Exit(containers.RunWithSharedServers(func() int { return 0 }))\n}\n"), false},
		{"a package that is not the harness", mainFile("\t\"os\"\n\t\"testing\"\n\n\t\"example.com/other/containers\"\n",
			"func TestMain(m *testing.M) { os.Exit(containers.RunWithSharedServers(m.Run)) }\n"), false},
		{"no TestMain", "package opted\n\nfunc helper() {}\n", false},
	}
	for _, tc := range cases {
		got, err := optsIn(tc.src)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if got != tc.want {
			t.Errorf("%s: optsIn = %v, want %v\n%s", tc.name, got, tc.want, tc.src)
		}
	}
}

func TestABatchRowFailsWhenItsTestMainOnlyMentionsTheCall(t *testing.T) {
	root := t.TempDir()
	write := func(rel, body string) {
		t.Helper()
		writeFixture(t, filepath.Join(root, rel), body)
	}
	write("internal/real/main_test.go", strings.ReplaceAll(sharedTestMain, "package opted", "package real"))
	write("internal/commented/main_test.go", "package commented\n\nimport (\n\t\"os\"\n\t\"testing\"\n)\n\n// containers.RunWithSharedServers(m.Run)\nfunc TestMain(m *testing.M) { os.Exit(m.Run()) }\n")
	write("internal/unused/main_test.go", mainFile(stdImports, "func TestMain(m *testing.M) {\n\t_ = containers.RunWithSharedServers\n\tos.Exit(m.Run())\n}\n"))
	write("internal/outside/main_test.go", mainFile(stdImports, "func TestMain(m *testing.M) {\n\t_ = containers.RunWithSharedServers(m.Run)\n\tos.Exit(m.Run())\n}\n"))
	optedIn, _ := scan(t, filepath.Join(root, "internal"))
	batch := []string{"internal/real", "internal/commented", "internal/unused", "internal/outside"}
	got := strings.Join(missingFromBatch(optedIn, batch), ",")
	if want := "internal/commented,internal/unused,internal/outside"; got != want {
		t.Errorf("batch rows missing from the opted-in packages = %q, want %q (opted in: %v)", got, want, optedIn)
	}
}

func writeFixture(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}
