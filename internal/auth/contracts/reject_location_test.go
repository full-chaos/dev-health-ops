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

// objectsWithKey lists the pointer of every object in document that has a
// member named name.
func objectsWithKey(document any, name string) []string {
	var out []string
	var walk func(node any, at []string)
	walk = func(node any, at []string) {
		switch value := node.(type) {
		case map[string]any:
			if _, ok := value[name]; ok {
				out = append(out, pointer(at))
			}
			for key, child := range value {
				walk(child, append(append([]string(nil), at...), key))
			}
		case []any:
			for index, child := range value {
				walk(child, append(append([]string(nil), at...), fmt.Sprint(index)))
			}
		}
	}
	walk(document, nil)
	sort.Strings(out)
	return out
}

// rejectLeaves returns every violation of document as "keyword@pointer". A
// violation of propertyNames is reported at the object that holds the offending
// member: the validator reports the member name's own (empty) location, so the
// object is found by the member name.
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
	var walk func(node *santhosh.ValidationError, propertyName string)
	walk = func(node *santhosh.ValidationError, propertyName string) {
		if names, isNames := node.ErrorKind.(*kind.PropertyNames); isNames {
			propertyName = names.Property
		}
		if len(node.Causes) > 0 {
			for _, cause := range node.Causes {
				walk(cause, propertyName)
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
		if propertyName != "" {
			for _, at := range objectsWithKey(document, propertyName) {
				out = append(out, keyword+"@"+at)
			}
			return
		}
		out = append(out, keyword+"@"+pointer(node.InstanceLocation))
	}
	walk(validation, "")
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
