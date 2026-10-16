package venuechanged

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// scratchModule writes the module the test above lists and returns its root.
func scratchModule(t *testing.T) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	write := func(rel, body string) {
		t.Helper()
		full := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("go.mod", "module example.com/x\n\ngo 1.22\n\nrequire example.com/local v0.0.0\n\nreplace example.com/local => ./third_party/local\n")
	write("third_party/local/go.mod", "module example.com/local\n\ngo 1.22\n")
	write("third_party/local/local.go", "package local\n\nfunc Value() string { return \"local\" }\n")
	write("internal/dep_test/dep.go", "package dep\n\nimport (\n\t_ \"embed\"\n\n\t\"example.com/local\"\n)\n\n//go:embed a,b/value.txt\nvar comma string\n\n//go:embed \"assets/a b/value.txt\"\nvar space string\n\nfunc Value() string { return comma + space + local.Value() }\n")
	write("internal/dep_test/a,b/value.txt", "comma")
	write("internal/dep_test/assets/a b/value.txt", "space")
	write("internal/venue/venue_test.go", "package venue\n\nimport (\n\t\"testing\"\n\n\tdep \"example.com/x/internal/dep_test\"\n)\n\nfunc TestVenueOracleFixture(t *testing.T) { _ = dep.Value() }\n")
	return root
}

// TestTheClosureOfARealModuleKeepsAwkwardDirectoryNames runs the real `go list -json -deps -test` over a scratch module whose
// registered test links a package in a directory ending _test, a package embedding a file whose name holds a comma and one whose
// directory holds a space, and a repo-local module reached by a replace directive. Each used to be lost or corrupted by the text
// edits of the earlier derivation (suffix removal, comma-joined lists, an in-module-prefix filter) while the freshness check stayed
// green.
func TestTheClosureOfARealModuleKeepsAwkwardDirectoryNames(t *testing.T) {
	root := scratchModule(t)
	cmd := exec.Command("go", "list", "-buildvcs=false", "-mod=mod", "-json", "-deps", "-test", "./internal/venue")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off", "GOFLAGS=-mod=mod", "GOPROXY=off")
	out, err := cmd.Output()
	if err != nil {
		detail := ""
		if exit, ok := err.(*exec.ExitError); ok {
			detail = string(exit.Stderr)
		}
		t.Fatalf("go list over the scratch module: %v\n%s", err, detail)
	}
	listing, err := decodeGoList(strings.NewReader(string(out)))
	if err != nil {
		t.Fatal(err)
	}
	got, err := closuresFromListing(root, []string{"internal/venue"}, listing)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"internal/dep_test",
		"third_party/local",
		"~internal/dep_test/a,b",
		"~internal/dep_test/assets/a b",
	}
	if !reflect.DeepEqual(got["internal/venue"], want) {
		t.Fatalf("closure of internal/venue = %q, want %q", got["internal/venue"], want)
	}
}
