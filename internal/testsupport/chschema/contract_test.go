package chschema

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"go/build"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	"github.com/full-chaos/dev-health-ops/internal/operationalbackfill"
)

const modulePath = "github.com/full-chaos/dev-health-ops/"

// TestAppliedChainIsTheContractConsumersExpect pins that the schema Apply
// builds is the contract production runs and that the operational readers
// classify: chmigrate's head is contract 2, `dho migrate clickhouse` refuses
// any other contract, and every operational table of that head is contract 2
// to the Go reader the writers use (operationalbackfill.TableContract). The
// retired Python chain built contract 1 when OPERATIONAL_ORDERING_CONTRACT was
// unset; a head that slipped back to it goes red here.
func TestAppliedChainIsTheContractConsumersExpect(t *testing.T) {
	head, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	if head.Contract != 2 {
		t.Fatalf("chmigrate head is contract %d, want production's contract 2", head.Contract)
	}
	if err := chmigrate.CheckContract(1, head); err == nil {
		t.Fatal("the migrator accepts contract 1 against the contract-2 head")
	}
	if err := chmigrate.CheckContract(2, head); err != nil {
		t.Fatalf("the migrator refuses production's contract: %v", err)
	}
	tables := operationalTables(t, head)
	if len(tables) != 12 {
		t.Fatalf("head holds %d operational tables, want the 12 of OPERATIONAL_ENTITY_TABLES", len(tables))
	}
	for _, object := range tables {
		got, err := operationalbackfill.TableContract(object.Create, object.Name)
		if err != nil || got != operationalbackfill.ContractCurrent {
			t.Errorf("%s: the head's DDL is contract %v (%v), want contract 2", object.Name, got, err)
		}
	}
}

// TestContract1HeadDiffersOnlyInTheOperationalTables pins the contract-1
// overlay against the contract-2 head: it replaces exactly the operational
// tables, each with DDL the Go reader classifies as contract 1, and omits only
// migration 067, which the head records.
func TestContract1HeadDiffersOnlyInTheOperationalTables(t *testing.T) {
	head, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := LoadContract1Baseline()
	if err != nil {
		t.Fatal(err)
	}
	if legacy.Contract != 1 {
		t.Fatalf("overlay contract %d, want 1", legacy.Contract)
	}
	if len(legacy.Objects) != len(head.Objects) {
		t.Fatalf("overlay holds %d objects, head %d", len(legacy.Objects), len(head.Objects))
	}
	operational := map[string]bool{}
	for _, object := range operationalTables(t, head) {
		operational[object.Name] = true
	}
	for index, object := range legacy.Objects {
		original := head.Objects[index]
		if object.Name != original.Name {
			t.Fatalf("object %d is %s, head holds %s there", index, object.Name, original.Name)
		}
		if !operational[object.Name] {
			if object != original {
				t.Errorf("%s differs from the head but is not an operational table", object.Name)
			}
			continue
		}
		if object.Create == original.Create {
			t.Errorf("%s: the contract-1 DDL equals the contract-2 DDL", object.Name)
		}
		got, err := operationalbackfill.TableContract(object.Create, object.Name)
		if err != nil || got != operationalbackfill.ContractLegacy {
			t.Errorf("%s: the overlay DDL is contract %v (%v), want contract 1", object.Name, got, err)
		}
	}
	const ordering = "067_operational_ordering_contract.py"
	recorded := func(versions []string) bool {
		for _, version := range versions {
			if version == ordering {
				return true
			}
		}
		return false
	}
	if !recorded(head.Versions) || recorded(legacy.Versions) || len(legacy.Versions) != len(head.Versions)-1 {
		t.Errorf("the overlay must omit exactly %s (head records it: %v, overlay: %v)", ordering, recorded(head.Versions), recorded(legacy.Versions))
	}
}

func operationalTables(t *testing.T, head chmigrate.Baseline) []chmigrate.Object {
	t.Helper()
	var tables []chmigrate.Object
	for _, object := range head.Objects {
		if strings.HasPrefix(object.Name, "operational_") && !object.IsView() {
			tables = append(tables, object)
		}
	}
	return tables
}

func TestApplyRefusesAnotherContractInTheEnvironment(t *testing.T) {
	for name, tc := range map[string]struct {
		value   string
		set     bool
		refused bool
	}{
		"unset":    {"", false, false},
		"two":      {"2", true, false},
		"one":      {"1", true, true},
		"blank":    {"", true, true},
		"three":    {"3", true, true},
		"two+pad":  {" 2", true, true},
		"legacy 0": {"0", true, true},
	} {
		if err := checkContractEnv(tc.value, tc.set); (err != nil) != tc.refused {
			t.Errorf("%s: err = %v, want refused=%v", name, err, tc.refused)
		}
	}
}

// TestChschemaStartsNoProcess is the guard for the CHAOS-7332 change: the
// package's non-test files neither import os/exec (or a Python resolver) nor
// reach one through the module packages they import, so the schema cannot be
// applied by a child process, Python or otherwise.
func TestChschemaStartsNoProcess(t *testing.T) {
	directory, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Clean(filepath.Join(directory, "..", "..", ".."))

	pkg, err := build.ImportDir(directory, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(pkg.GoFiles) == 0 {
		t.Fatal("no non-test Go files found: the guard measured nothing")
	}
	fileSet := token.NewFileSet()
	for _, name := range pkg.GoFiles {
		file, err := parser.ParseFile(fileSet, filepath.Join(directory, name), nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, spec := range file.Imports {
			path := strings.Trim(spec.Path.Value, `"`)
			if path == "os/exec" || strings.HasSuffix(path, "/pyoracle") || strings.HasSuffix(path, "/venueoracle") {
				t.Errorf("%s imports %s: chschema must not start a process", name, path)
			}
		}
	}

	// The module packages reachable from the non-test files: none may import a
	// Python resolver.
	seen := map[string]bool{}
	var walk func(path, via string)
	walk = func(path, via string) {
		if seen[path] {
			return
		}
		seen[path] = true
		if strings.HasSuffix(path, "/pyoracle") || strings.HasSuffix(path, "/venueoracle") {
			t.Errorf("%s reaches %s", via, path)
			return
		}
		imported, err := build.ImportDir(filepath.Join(root, strings.TrimPrefix(path, modulePath)), 0)
		if err != nil {
			t.Errorf("read %s: %v", path, err)
			return
		}
		for _, next := range imported.Imports {
			if strings.HasPrefix(next, modulePath) {
				walk(next, path)
			}
		}
	}
	checked := 0
	for _, path := range pkg.Imports {
		if strings.HasPrefix(path, modulePath) {
			walk(path, "chschema")
			checked++
		}
	}
	if checked == 0 || len(seen) < 3 {
		t.Fatalf("the guard walked %d packages from %d module imports: it measured nothing", len(seen), checked)
	}
}

// TestContract1HeadMatchesItsPin fails when the recorded contract-1 overlay is
// edited without its pin (the Python build it was recorded on and its sha256).
func TestContract1HeadMatchesItsPin(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "contract1_head.pin.json"))
	if err != nil {
		t.Fatal(err)
	}
	var pin struct {
		PythonBuild string            `json:"python_build"`
		SHA256      map[string]string `json:"sha256"`
	}
	if err := json.Unmarshal(data, &pin); err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^[0-9a-f]{40}$`).MatchString(pin.PythonBuild) {
		t.Fatalf("python_build %q is not a 40-hex commit", pin.PythonBuild)
	}
	sum := sha256.Sum256(contract1Head)
	if got := hex.EncodeToString(sum[:]); got != pin.SHA256["contract1_head.json"] {
		t.Errorf("contract1_head.json sha256 %s, pinned %s: re-record it and update the pin together", got, pin.SHA256["contract1_head.json"])
	}
}
