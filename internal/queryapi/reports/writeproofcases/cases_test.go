package writeproofcases

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof/writeproof"
)

// A build that links this package registers all five cases: a case without a
// committed baseline is skipped at init (so the oracle that produces it can
// run), and this test is what makes that skip loud.
func TestEveryMutationHasARegisteredCase(t *testing.T) {
	want := map[string]bool{}
	for _, c := range Build(nil) {
		want[c.Name] = true
		if _, ok := writeproof.Lookup(c.Name); !ok {
			t.Errorf("case %q is not registered: baselines.json has no digest for it", c.Name)
		}
	}
	for _, name := range writeproof.Names() {
		if strings.HasPrefix(name, "saved-report-") && !want[name] {
			t.Errorf("registered case %q is not built by this package", name)
		}
	}
	for name, digest := range Baselines() {
		if !want[name] {
			t.Errorf("baselines.json names %q, which no case builds", name)
		}
		if !strings.HasPrefix(digest, "sha256:") || len(digest) != len("sha256:")+64 {
			t.Errorf("baseline of %q is not a sha256 digest: %q", name, digest)
		}
	}
}

// Every mutation the catalog marks kind=mutation has a write case, and every
// case belongs to one: a mutation added to the catalog without a proof form
// case cannot be enabled, and this names it before an operator finds out.
func TestTheCasesCoverExactlyTheCatalogMutations(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(file), "..", "..", "..", "..")
	raw, err := os.ReadFile(filepath.Join(root, "src", "dev_health_ops", "api", "graphql", "go_api_operations.json"))
	if err != nil {
		t.Fatal(err)
	}
	var catalog []struct {
		Operation string `json:"operation"`
		Kind      string `json:"kind"`
	}
	if err := json.Unmarshal(raw, &catalog); err != nil {
		t.Fatal(err)
	}
	var mutations []string
	for _, entry := range catalog {
		if entry.Kind == "mutation" {
			mutations = append(mutations, entry.Operation)
		}
	}
	var proven []string
	for _, c := range Build(nil) {
		proven = append(proven, c.Operation)
	}
	sort.Strings(mutations)
	sort.Strings(proven)
	if !reflect.DeepEqual(mutations, proven) {
		t.Fatalf("catalog mutations %v, write cases prove %v", mutations, proven)
	}
	if !reflect.DeepEqual(mutations, sortedCopy(Operations)) {
		t.Fatalf("Operations %v is not the catalog's mutations %v", Operations, mutations)
	}
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

// The variables are what the mutation is sent: valid JSON, carrying the org and
// the run, never a value fixed at build time.
func TestVariablesCarryTheOrgAndTheRun(t *testing.T) {
	for _, c := range Build(map[string]string{}) {
		text := c.VariablesText("org-under-test", "run-under-test")
		var decoded map[string]any
		if err := json.Unmarshal([]byte(text), &decoded); err != nil {
			t.Errorf("%s: variables are not a JSON object: %v\n%s", c.Name, err, text)
			continue
		}
		if decoded["orgId"] != "org-under-test" {
			t.Errorf("%s: orgId is %v, want the org the verb runs in", c.Name, decoded["orgId"])
		}
		other := c.VariablesText("org-under-test", "another-run")
		if other == text {
			t.Errorf("%s: variables do not depend on the run tag, so two runs would touch the same rows", c.Name)
		}
	}
}

// Every table is addressed by org and run: $1 and $2 both appear, so a read can
// never return another run's rows.
func TestEveryTableIsAddressedByOrgAndRun(t *testing.T) {
	for _, c := range Build(nil) {
		for _, table := range c.Tables {
			if !strings.Contains(table.SQL, "$1") || !strings.Contains(table.SQL, "$2") {
				t.Errorf("%s/%s: the statement does not use both $1 (org) and $2 (run tag)", c.Name, table.Label)
			}
		}
	}
}
