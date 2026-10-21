package contracts

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	santhosh "github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/santhosh-tekuri/jsonschema/v6/kind"
)

// The wire validator (google/jsonschema-go, contracts.Validate) returns one
// error that points into the SCHEMA, so a golden test cannot learn WHERE in
// the document a reject fixture fails from it. This file is the instance
// location source (CHAOS-10203): it compiles the same schema file with
// santhosh-tekuri/jsonschema, whose errors carry the instance location, and
// every reject fixture asserts the keyword AND the location its manifest
// declares. It is a test-side oracle only; production validates with
// contracts.Validate.

type locationLoader struct{ root string }

// Load resolves a $ref to the file beneath the contracts directory and refuses
// everything else, as localLoader does for the production validator.
func (l locationLoader) Load(url string) (any, error) {
	rel, ok := strings.CutPrefix(url, wireIDPrefix)
	if !ok || rel == "" || strings.Contains(rel, "..") {
		return nil, fmt.Errorf("refusing $ref %q: only %s... is loadable", url, wireIDPrefix)
	}
	path := ContractsDir(l.root) + "/" + rel
	if err := requireInsideContracts(l.root, path); err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading referenced schema %s: %w", path, err)
	}
	return santhosh.UnmarshalJSON(strings.NewReader(string(raw)))
}

var locationSchemas = struct {
	mu   sync.Mutex
	byID map[string]*santhosh.Schema
}{byID: map[string]*santhosh.Schema{}}

func locationSchema(t *testing.T, root, surface string) *santhosh.Schema {
	t.Helper()
	locationSchemas.mu.Lock()
	defer locationSchemas.mu.Unlock()
	id := root + "|" + surface
	if schema, ok := locationSchemas.byID[id]; ok {
		return schema
	}
	compiler := santhosh.NewCompiler()
	compiler.UseLoader(locationLoader{root})
	schema, err := compiler.Compile(wireIDPrefix + "jsonschema/" + surface + ".schema.json")
	if err != nil {
		t.Fatalf("compiling %s for instance locations: %v", surface, err)
	}
	locationSchemas.byID[id] = schema
	return schema
}

// pointer renders an instance location as a JSON Pointer; the document root is
// "/", the form the manifests declare.
func pointer(location []string) string { return "/" + strings.Join(location, "/") }

// propertyNamesLocation derives where a propertyNames violation sits from the
// schema path of the violation. The validator reports such a violation at the
// property name's own (empty) location, so the object holding the name is the
// instance reached by the `properties/<name>` steps in front of `propertyNames`
// on the schema path; structural keywords on the path (allOf, then, $ref, ...)
// do not move the instance. A path step this cannot place is an error, never a
// guess.
func propertyNamesLocation(keywordLocation string) ([]string, error) {
	tokens := strings.Split(strings.TrimPrefix(keywordLocation, "/"), "/")
	var at []string
	for i := 0; i < len(tokens); i++ {
		switch token := tokens[i]; token {
		case "propertyNames":
			return at, nil
		case "properties":
			if i+1 >= len(tokens) {
				return nil, fmt.Errorf("schema path %q ends after properties", keywordLocation)
			}
			i++
			at = append(at, strings.NewReplacer("~1", "/", "~0", "~").Replace(tokens[i]))
		case "allOf", "anyOf", "oneOf", "$defs", "definitions":
			i++
		case "if", "then", "else", "$ref", "$dynamicRef", "not", "":
		default:
			return nil, fmt.Errorf("cannot place schema path step %q of %q on the instance", token, keywordLocation)
		}
	}
	return nil, fmt.Errorf("schema path %q has no propertyNames step", keywordLocation)
}

// rejectLeaves returns every violation of document as "keyword@pointer". The
// error tree gives the keyword (the `not` keyword is named explicitly) and the
// instance location of every violation except one under propertyNames, whose
// location the validator reports as the property name's own empty location;
// those come from the detailed output instead: the keyword is the last step of
// the schema path and the location is propertyNamesLocation's. The document
// root is "/".
func rejectLeaves(t *testing.T, root, surface string, document any) []string {
	t.Helper()
	err := locationSchema(t, root, surface).Validate(document)
	if err == nil {
		return nil
	}
	validation, ok := err.(*santhosh.ValidationError)
	if !ok {
		t.Fatalf("%s: validation failed without a location: %v", surface, err)
	}
	var out []string
	var walk func(node *santhosh.ValidationError)
	walk = func(node *santhosh.ValidationError) {
		if _, isNames := node.ErrorKind.(*kind.PropertyNames); isNames {
			return
		}
		if len(node.Causes) > 0 {
			for _, cause := range node.Causes {
				walk(cause)
			}
			return
		}
		keyword := ""
		if path := node.ErrorKind.KeywordPath(); len(path) > 0 {
			keyword = path[len(path)-1]
		}
		if _, isNot := node.ErrorKind.(*kind.Not); isNot {
			keyword = "not"
		}
		out = append(out, keyword+"@"+pointer(node.InstanceLocation))
	}
	walk(validation)
	var names func(unit santhosh.OutputUnit)
	names = func(unit santhosh.OutputUnit) {
		if len(unit.Errors) > 0 {
			for _, child := range unit.Errors {
				names(child)
			}
			return
		}
		if !strings.Contains(unit.KeywordLocation, "/propertyNames/") {
			return
		}
		at, err := propertyNamesLocation(unit.KeywordLocation)
		if err != nil {
			t.Fatalf("%s: %v", surface, err)
		}
		steps := strings.Split(unit.KeywordLocation, "/")
		out = append(out, steps[len(steps)-1]+"@"+pointer(at))
	}
	names(*validation.DetailedOutput())
	sort.Strings(out)
	return out
}

// declaredViolation is nil when leaves holds the declared keyword at the
// declared location. Both validators stop at different first errors, so the
// declared pair must be among the violations, not the only one.
func declaredViolation(leaves []string, keyword, location string) error {
	want := keyword + "@" + location
	for _, leaf := range leaves {
		if leaf == want {
			return nil
		}
	}
	return fmt.Errorf("no violation %s; violations are %v", want, leaves)
}

// assertRejectLocation fails unless document violates surface's schema with the
// declared keyword at the declared instance location.
func assertRejectLocation(t *testing.T, root, surface string, document any, keyword, location string) {
	t.Helper()
	if err := declaredViolation(rejectLeaves(t, root, surface, document), keyword, location); err != nil {
		t.Fatalf("%s: %v", surface, err)
	}
}

// decodeFixture reads a JSON fixture without losing number tokens.
func decodeFixture(t *testing.T, raw []byte) any {
	t.Helper()
	var document any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	return document
}

// TestEveryRejectFixtureFailsAtItsDeclaredLocation walks every examples/*/
// manifest.json reject entry, whatever the surface, and holds it to the keyword
// and instance location the manifest declares, so a surface whose own golden
// test does not call assertRejectLocation is still covered.
func TestEveryRejectFixtureFailsAtItsDeclaredLocation(t *testing.T) {
	root := testRoot(t)
	manifests, err := filepath.Glob(ContractsDir(root) + "/examples/*/manifest.json")
	if err != nil || len(manifests) == 0 {
		t.Fatalf("no manifests found (err=%v)", err)
	}
	checked := 0
	for _, manifest := range manifests {
		dir := manifest[:strings.LastIndex(manifest, "/")]
		surface := dir[strings.LastIndex(dir, "/")+1:] + ".v1"
		raw, err := os.ReadFile(manifest)
		if err != nil {
			t.Fatal(err)
		}
		var entries struct {
			Reject []struct {
				File     string `json:"file"`
				Keyword  string `json:"expect_keyword"`
				Location string `json:"expect_instance_location"`
			} `json:"reject"`
		}
		if err := json.Unmarshal(raw, &entries); err != nil {
			t.Fatalf("%s: %v", manifest, err)
		}
		for _, entry := range entries.Reject {
			t.Run(surface+"/"+entry.File, func(t *testing.T) {
				fixture, err := os.ReadFile(dir + "/" + entry.File)
				if err != nil {
					t.Fatal(err)
				}
				assertRejectLocation(t, root, surface, decodeFixture(t, fixture), entry.Keyword, entry.Location)
			})
			checked++
		}
	}
	if checked < 400 {
		t.Fatalf("only %d reject fixtures were checked: the walk stopped seeing the manifests", checked)
	}
}

// TestARejectFixtureAtTheWrongLocationFails is the planted control: a real
// reject fixture held to its declared pair passes, and the same fixture held to
// a different location, a different keyword or a location one level deeper
// fails.
func TestARejectFixtureAtTheWrongLocationFails(t *testing.T) {
	root := testRoot(t)
	raw, err := os.ReadFile(ContractsDir(root) + "/examples/session/invalid-amr-unknown.json")
	if err != nil {
		t.Fatal(err)
	}
	leaves := rejectLeaves(t, root, "session.v1", decodeFixture(t, raw))
	if len(leaves) == 0 {
		t.Fatal("the fixture validated")
	}
	keyword, location, _ := strings.Cut(leaves[0], "@")
	if err := declaredViolation(leaves, keyword, location); err != nil {
		t.Fatalf("the real violation was refused: %v", err)
	}
	for name, declared := range map[string][2]string{
		"another location": {keyword, "/not-a-member"},
		"deeper location":  {keyword, location + "/planted"},
		"another keyword":  {"planted-keyword", location},
	} {
		if declaredViolation(leaves, declared[0], declared[1]) == nil {
			t.Errorf("%s: %v was accepted for violations %v", name, declared, leaves)
		}
	}
}

// TestPropertyNamesViolationsAreLocatedByTheirSchemaPath: the object a
// propertyNames violation sits at comes from the schema path, so an empty
// member name does not read as the document root and a second object that holds
// the same member name elsewhere is not mistaken for the violating one.
func TestPropertyNamesViolationsAreLocatedByTheirSchemaPath(t *testing.T) {
	root := testRoot(t)
	raw, err := os.ReadFile(ContractsDir(root) + "/examples/provisioning-event/invalid-change-arbitrary-key.json")
	if err != nil {
		t.Fatal(err)
	}
	fixture := decodeFixture(t, raw).(map[string]any)
	change := fixture["payload"].(map[string]any)["change"].(map[string]any)
	offending := ""
	for name := range change {
		offending = name
	}
	delete(change, offending)
	change[""] = "x"
	leaves := rejectLeaves(t, root, "provisioning-event.v1", fixture)
	if err := declaredViolation(leaves, "enum", "/payload/change"); err != nil {
		t.Fatalf("an empty member name is not placed at its object: %v", err)
	}
	if declaredViolation(leaves, "enum", "/") == nil {
		t.Fatalf("an empty member name was accepted at the document root: %v", leaves)
	}

	change[offending] = "x"
	delete(change, "")
	fixture["payload"].(map[string]any)["decoy"] = map[string]any{offending: "x"}
	leaves = rejectLeaves(t, root, "provisioning-event.v1", fixture)
	if declaredViolation(leaves, "enum", "/payload/decoy") == nil {
		t.Fatalf("an object that merely holds the same member name was accepted: %v", leaves)
	}
	if err := declaredViolation(leaves, "enum", "/payload/change"); err != nil {
		t.Fatalf("the violating object is not reported: %v", err)
	}
}
