package teamattribution

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// linkedIssueDonorGates are the inheritable-relationship gates: a function that reads one walks dependency edges
// to a donor.
var linkedIssueDonorGates = map[string]bool{
	"GithubWorkItemDerivationInheritableRelationships": true,
}

// linkedIssueDonorSites are the walks that take a team from a donor. Each must call LinkedIssueDonorRefusal.
var linkedIssueDonorSites = []string{
	"internal/teamattribution/cascade.go\x00BuildLinkedIssueIndexFromRows",
	"internal/jobs/metrics/remaining/work_item_attribution_donor_change.go\x00donorChangedDependents",
	"internal/providersync/team_repo_ownership_derivation.go\x00buildDonorTeamIDResolver",
}

// linkedIssueDonorGateExemptions read a gate but take no team from a donor. Each must still read one.
var linkedIssueDonorGateExemptions = map[string]string{
	"internal/jobs/metrics/remaining/work_item_attribution_native_clickhouse.go\x00workItemAttributionDonorTargets":           "loads the donor targets' rows; the team is decided by BuildLinkedIssueIndexFromRows",
	"internal/jobs/metrics/remaining/work_item_attribution_native_clickhouse.go\x00loadInheritableDependencySourcesTargeting": "scope expansion: the sources of edges to a changed item, no team read",
	"internal/teamattribution/cascade.go\x00LatestDependencyEdgeWins":                                                         "edge tie rule (latest edge per pair, an inheritable type wins a tie); no donor or team read",
}

type linkedIssueDonorFunc struct {
	readsGate, callsRefusal bool
}

// linkedIssueDonorCensus parses every production Go file of the module (overlay replaces or adds files by
// module-relative path) and returns each function that reads a gate, and each relationship map literal outside
// the gates' own declarations.
func linkedIssueDonorCensus(t *testing.T, root string, overlay map[string][]byte) (map[string]linkedIssueDonorFunc, []string) {
	t.Helper()
	sources := map[string][]byte{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		if entry.IsDir() {
			if strings.HasPrefix(entry.Name(), ".") || rel == "third_party" || rel == "node_modules" || entry.Name() == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(rel, ".go") || strings.HasSuffix(rel, "_test.go") {
			return nil
		}
		content, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		sources[rel] = content
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for rel, content := range overlay {
		sources[rel] = content
	}
	funcs := map[string]linkedIssueDonorFunc{}
	var copiedGates []string
	for rel, content := range sources {
		if !bytes.Contains(content, []byte("InheritableRelationship")) && !bytes.Contains(content, []byte(`"relates_to"`)) {
			continue
		}
		fileSet := token.NewFileSet()
		file, parseErr := parser.ParseFile(fileSet, rel, content, 0)
		if parseErr != nil {
			t.Fatalf("parse %s: %v", rel, parseErr)
		}
		for _, decl := range file.Decls {
			switch decl := decl.(type) {
			case *ast.FuncDecl:
				if decl.Body == nil {
					continue
				}
				var site linkedIssueDonorFunc
				ast.Inspect(decl.Body, func(node ast.Node) bool {
					switch node := node.(type) {
					case *ast.Ident:
						site.readsGate = site.readsGate || linkedIssueDonorGates[node.Name]
						site.callsRefusal = site.callsRefusal || node.Name == "LinkedIssueDonorRefusal"
					case *ast.CompositeLit:
						if linkedIssueRelationshipMap(node) {
							copiedGates = append(copiedGates, fileSet.Position(node.Pos()).String())
						}
					}
					return true
				})
				if site.readsGate {
					funcs[rel+"\x00"+decl.Name.Name] = site
				}
			case *ast.GenDecl:
				for _, spec := range decl.Specs {
					value, ok := spec.(*ast.ValueSpec)
					if !ok {
						continue
					}
					gate := len(value.Names) == 1 && linkedIssueDonorGates[value.Names[0].Name]
					ast.Inspect(value, func(node ast.Node) bool {
						if literal, ok := node.(*ast.CompositeLit); ok && !gate && linkedIssueRelationshipMap(literal) {
							copiedGates = append(copiedGates, fileSet.Position(literal.Pos()).String())
						}
						return true
					})
				}
			}
		}
	}
	sort.Strings(copiedGates)
	return funcs, copiedGates
}

// linkedIssueRelationshipMap reports whether a literal keys "relates_to" and "external_issue_key": a copy of a
// gate.
func linkedIssueRelationshipMap(literal *ast.CompositeLit) bool {
	keys := map[string]bool{}
	for _, element := range literal.Elts {
		pair, ok := element.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		if key, ok := pair.Key.(*ast.BasicLit); ok && key.Kind == token.STRING {
			keys[strings.Trim(key.Value, "\"`")] = true
		}
	}
	return keys["relates_to"] && keys["external_issue_key"]
}

func linkedIssueDonorCensusViolations(funcs map[string]linkedIssueDonorFunc, copiedGates []string) []string {
	var violations []string
	required := map[string]bool{}
	for _, key := range linkedIssueDonorSites {
		required[key] = true
		site, ok := funcs[key]
		switch {
		case !ok:
			violations = append(violations, "donor site "+strings.ReplaceAll(key, "\x00", ": ")+" not found or reads no gate")
		case !site.callsRefusal:
			violations = append(violations, "donor site "+strings.ReplaceAll(key, "\x00", ": ")+" does not call LinkedIssueDonorRefusal")
		}
	}
	for key, reason := range linkedIssueDonorGateExemptions {
		if _, ok := funcs[key]; !ok {
			violations = append(violations, "stale exemption ("+reason+"): "+strings.ReplaceAll(key, "\x00", ": ")+" reads no gate")
		}
	}
	for key, site := range funcs {
		if required[key] || site.callsRefusal || linkedIssueDonorGateExemptions[key] != "" {
			continue
		}
		violations = append(violations, "new donor walk "+strings.ReplaceAll(key, "\x00", ": ")+
			": call LinkedIssueDonorRefusal, or add it to linkedIssueDonorGateExemptions with a reason")
	}
	for _, position := range copiedGates {
		violations = append(violations, "copy of an inheritable-relationship gate at "+position+": read the gate instead")
	}
	sort.Strings(violations)
	return violations
}

// TestEveryLinkedIssueDonorWalkCallsTheDonorRefusal holds every Go walk that takes a team from a dependency donor
// to the one donor rule, LinkedIssueDonorRefusal: the cascade, the donor-change backstop and the
// team_repo_ownership derivation. A function that reads an inheritable-relationship gate is a donor walk: it calls
// the predicate or is a named exemption.
func TestEveryLinkedIssueDonorWalkCallsTheDonorRefusal(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	funcs, copiedGates := linkedIssueDonorCensus(t, root, nil)
	for _, violation := range linkedIssueDonorCensusViolations(funcs, copiedGates) {
		t.Error(violation)
	}
	if len(funcs) < len(linkedIssueDonorSites)+len(linkedIssueDonorGateExemptions) {
		t.Errorf("census found %d gate readers: the walk did not reach the donor walks", len(funcs))
	}
}

// Red pairs: the census fails when one donor site drops the call, when a new walk reads a gate without it, and
// when a walk copies a gate.
func TestLinkedIssueDonorCensusRedPairs(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range linkedIssueDonorSites {
		rel, name, _ := strings.Cut(key, "\x00")
		t.Run("drop the call in "+name, func(t *testing.T) {
			content, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
			if err != nil {
				t.Fatal(err)
			}
			funcs, copiedGates := linkedIssueDonorCensus(t, root, map[string][]byte{rel: dropDonorRefusalIn(t, rel, content, name)})
			want := "donor site " + rel + ": " + name + " does not call LinkedIssueDonorRefusal"
			if violations := linkedIssueDonorCensusViolations(funcs, copiedGates); !containsString(violations, want) {
				t.Fatalf("violations %q, want %q", violations, want)
			}
		})
	}
	t.Run("a new walk without the call", func(t *testing.T) {
		walk := []byte("package fake\n\nimport \"github.com/full-chaos/dev-health-ops/internal/teamattribution\"\n\n" +
			"func newDonorWalk(kind string) bool {\n\treturn teamattribution.GithubWorkItemDerivationInheritableRelationships[kind]\n}\n")
		funcs, copiedGates := linkedIssueDonorCensus(t, root, map[string][]byte{"internal/fake/walk.go": walk})
		want := "new donor walk internal/fake/walk.go: newDonorWalk: call LinkedIssueDonorRefusal, or add it to linkedIssueDonorGateExemptions with a reason"
		if violations := linkedIssueDonorCensusViolations(funcs, copiedGates); !containsString(violations, want) {
			t.Fatalf("violations %q, want %q", violations, want)
		}
	})
	t.Run("the repository-ownership copy of the gate put back", func(t *testing.T) {
		rel := "internal/providersync/team_repo_ownership_derivation.go"
		content, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatal(err)
		}
		planted := append(append([]byte{}, content...), []byte("\nvar teamRepoOwnershipInheritableRelationshipTypes = map[string]bool{\n"+
			"\t\"relates_to\": true, \"relates\": true, \"duplicates\": true, \"external_issue_key\": true,\n}\n")...)
		funcs, copiedGates := linkedIssueDonorCensus(t, root, map[string][]byte{rel: planted})
		want := "copy of an inheritable-relationship gate at " + rel + ":"
		for _, violation := range linkedIssueDonorCensusViolations(funcs, copiedGates) {
			if strings.HasPrefix(violation, want) {
				return
			}
		}
		t.Fatalf("no violation starts with %q", want)
	})
	t.Run("a copied gate", func(t *testing.T) {
		walk := []byte("package fake\n\nvar inheritable = map[string]bool{\"relates_to\": true, \"external_issue_key\": true}\n")
		funcs, copiedGates := linkedIssueDonorCensus(t, root, map[string][]byte{"internal/fake/walk.go": walk})
		if violations := linkedIssueDonorCensusViolations(funcs, copiedGates); !containsString(violations,
			"copy of an inheritable-relationship gate at internal/fake/walk.go:3:19: read the gate instead") {
			t.Fatalf("violations %q, want the copied gate", violations)
		}
	})
}

// dropDonorRefusalIn renames every LinkedIssueDonorRefusal identifier inside function name of content.
func dropDonorRefusalIn(t *testing.T, rel string, content []byte, name string) []byte {
	t.Helper()
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, rel, content, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != name || fn.Body == nil {
			continue
		}
		start, end := fileSet.Position(fn.Body.Pos()).Offset, fileSet.Position(fn.Body.End()).Offset
		body := bytes.ReplaceAll(content[start:end], []byte("LinkedIssueDonorRefusal("), []byte("linkedIssueDonorRefusalDropped("))
		if bytes.Equal(body, content[start:end]) {
			t.Fatalf("%s: %s has no LinkedIssueDonorRefusal call to drop", rel, name)
		}
		return append(append(append([]byte{}, content[:start]...), body...), content[end:]...)
	}
	t.Fatalf("%s: no func %s", rel, name)
	return nil
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
