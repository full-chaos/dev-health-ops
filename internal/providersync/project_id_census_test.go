package providersync

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// projectIDConstructorFile is the one file that may build a ProjectID.
const projectIDConstructorFile = "internal/providersync/project_id.go"

// projectIDIdentityConstructors are the constructors of a project's identity
// form. Every other exported constructor in the file is a NAMED exception and
// is listed in projectIDNamedExceptions.
var projectIDIdentityConstructors = []string{"GitLabCatalogProjectID", "JiraProjectID", "LinearProjectID"}

// projectIDExceptionCallers names, for each named exception, the only
// production files that may call its constructor.
var projectIDExceptionCallers = map[string][]string{
	"GitLabPathOwnershipProjectID": {"internal/providersync/gitlab_team_catalog.go"},
	"GitHubRepoProjectID":          {"internal/providersync/github_team_catalog_effects_clickhouse.go"},
	"LinearTeamKeyProjectID":       {"internal/providersync/linear_reference_catalog_route.go"},
}

// projectIDMarkerLiterals are the literal pieces a hand-built project id is
// made of. The files below may hold one for a reason other than building an
// id: they recognize or retire a form no writer produces any more.
var projectIDMarkerAllowed = map[string]string{
	"internal/providersync/jira_team_catalog.go": "jiraKeyBuiltProjectIDPrefix: recognizes (to refuse and to retire) the key-built Jira form",
}

// projectIDSinkFields are the row fields that persist a project id. Each one
// must have the ProjectID type: a bare string would let a writer build the id
// by hand and still compile.
var projectIDSinkFields = []struct{ file, typeName, field string }{
	{"internal/providersync/jira_team_catalog.go", "jiraTeamCatalogOwnershipRow", "ProjectID"},
	{"internal/providersync/jira_team_catalog.go", "jiraTeamCatalogProjectRow", "ID"},
	{"internal/providersync/gitlab_team_catalog.go", "gitlabTeamCatalogOwnershipRow", "ProjectID"},
	{"internal/providersync/gitlab_team_catalog.go", "gitlabTeamCatalogProjectRow", "ID"},
	{"internal/providersync/linear_reference_catalog.go", "linearReferenceOwnershipRow", "ProjectID"},
	{"internal/providersync/linear_reference_catalog.go", "linearReferenceProjectRow", "ID"},
	{"internal/providersync/ownership_snapshot.go", "OwnershipSnapshotRow", "ProjectID"},
	{"internal/atlassianteams/collect.go", "OwnershipRow", "ProjectID"},
	{"internal/atlassianteams/write.go", "openOwnership", "projectID"},
}

var projectIDProviderMarkers = map[string]bool{":jira:": true, ":gitlab:": true, ":github:": true, ":linear:": true}

// projectIDPackage says whether a file is in a package that holds project
// ids. The x+":"+p+":" shape is a marker only there: other packages build
// lock keys and cache keys the same way.
func projectIDPackage(relative string) bool {
	return strings.HasPrefix(relative, "internal/providersync/") || strings.HasPrefix(relative, "internal/atlassianteams/") ||
		strings.HasPrefix(relative, "internal/projectmembership/")
}

type projectIDCensus struct {
	files int
	// literals: files holding a ProjectID composite literal with elements.
	literals []string
	// markers: files holding a literal that is a provider marker, or a
	// ":" + x + ":" concatenation.
	markers []string
	// callers: exception constructor -> files that call it.
	callers map[string]map[string]bool
	// constructors: exported funcs of the constructor file.
	constructors []string
	// fieldTypes: file|type|field -> the source text of the field type.
	fieldTypes map[string]string
}

func exprText(expr ast.Expr) string {
	switch typed := expr.(type) {
	case *ast.Ident:
		return typed.Name
	case *ast.SelectorExpr:
		return exprText(typed.X) + "." + typed.Sel.Name
	case *ast.StarExpr:
		return "*" + exprText(typed.X)
	case *ast.ArrayType:
		return "[]" + exprText(typed.Elt)
	}
	return "<expr>"
}

func scanProjectIDCensus(t *testing.T, repoRoot string, roots ...string) projectIDCensus {
	t.Helper()
	census := projectIDCensus{callers: map[string]map[string]bool{}, fieldTypes: map[string]string{}}
	for _, root := range roots {
		err := filepath.WalkDir(filepath.Join(repoRoot, root), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				if entry.Name() == "testdata" || entry.Name() == "vendor" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			relative, err := filepath.Rel(repoRoot, path)
			if err != nil {
				return err
			}
			relative = filepath.ToSlash(relative)
			census.files++
			literal, marker := false, false
			ast.Inspect(file, func(node ast.Node) bool {
				switch typed := node.(type) {
				case *ast.CompositeLit:
					if exprText(typed.Type) == "ProjectID" || exprText(typed.Type) == "providersync.ProjectID" {
						if len(typed.Elts) > 0 {
							literal = true
						}
					}
				case *ast.BasicLit:
					if typed.Kind == token.STRING {
						if text, err := strconv.Unquote(typed.Value); err == nil && projectIDProviderMarkers[text] {
							marker = true
						}
					}
				case *ast.BinaryExpr:
					// x + ":" + provider + ":" : the shape of a built marker.
					if typed.Op == token.ADD && projectIDPackage(relative) {
						// ((org + ":") + provider) + ":" parses left to right.
						if middle, ok := typed.X.(*ast.BinaryExpr); ok && middle.Op == token.ADD {
							if inner, ok := middle.X.(*ast.BinaryExpr); ok && inner.Op == token.ADD {
								first, firstOK := inner.Y.(*ast.BasicLit)
								last, lastOK := typed.Y.(*ast.BasicLit)
								if firstOK && lastOK && strings.Contains(strings.ToLower(exprText(inner.X)), "org") && first.Kind == token.STRING && first.Value == `":"` &&
									last.Kind == token.STRING && last.Value == `":"` {
									marker = true
								}
							}
						}
					}
				case *ast.CallExpr:
					name := ""
					switch callee := typed.Fun.(type) {
					case *ast.Ident:
						name = callee.Name
					case *ast.SelectorExpr:
						name = callee.Sel.Name
					}
					if _, named := projectIDExceptionCallers[name]; named {
						if census.callers[name] == nil {
							census.callers[name] = map[string]bool{}
						}
						census.callers[name][relative] = true
					}
				case *ast.TypeSpec:
					if structType, ok := typed.Type.(*ast.StructType); ok {
						for _, field := range structType.Fields.List {
							for _, name := range field.Names {
								census.fieldTypes[relative+"|"+typed.Name.Name+"|"+name.Name] = exprText(field.Type)
							}
						}
					}
				}
				return true
			})
			if literal && relative != projectIDConstructorFile {
				census.literals = append(census.literals, relative)
			}
			if marker && relative != projectIDConstructorFile {
				census.markers = append(census.markers, relative)
			}
			if relative == projectIDConstructorFile {
				for _, declaration := range file.Decls {
					if function, ok := declaration.(*ast.FuncDecl); ok && function.Recv == nil && function.Name.IsExported() {
						census.constructors = append(census.constructors, function.Name.Name)
					}
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("scan %s: %v", root, err)
		}
	}
	sort.Strings(census.literals)
	sort.Strings(census.markers)
	sort.Strings(census.constructors)
	return census
}

// TestProjectIDConstructionCensus fails when a production file builds a
// project id any way but the constructors of project_id.go: a ProjectID
// literal, a hand-built "{org}:{provider}:{id}" string, a named exception
// called from a file that is not named for it, a constructor that is neither
// an identity form nor a listed exception, or a sink row field that is a bare
// string again.
func TestProjectIDConstructionCensus(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	census := scanProjectIDCensus(t, root, "internal", "cmd")
	if census.files < 100 || len(census.constructors) == 0 || len(census.fieldTypes) == 0 {
		t.Fatalf("the scan read %d files, %d constructors, %d fields: it measured nothing",
			census.files, len(census.constructors), len(census.fieldTypes))
	}

	if len(census.literals) > 0 {
		t.Errorf("ProjectID literals outside %s: %v -- build the id with a constructor", projectIDConstructorFile, census.literals)
	}

	var strayMarkers []string
	for _, file := range census.markers {
		if _, allowed := projectIDMarkerAllowed[file]; !allowed {
			strayMarkers = append(strayMarkers, file)
		}
	}
	if len(strayMarkers) > 0 {
		t.Errorf("hand-built project id markers (a \":<provider>:\" literal or x+\":\"+p+\":\") outside %s: %v -- "+
			"use the constructor of the form, or name the file in projectIDMarkerAllowed with its reason",
			projectIDConstructorFile, strayMarkers)
	}
	for file, reason := range projectIDMarkerAllowed {
		held := false
		for _, marker := range census.markers {
			held = held || marker == file
		}
		if !held || strings.TrimSpace(reason) == "" {
			t.Errorf("projectIDMarkerAllowed names %s but it holds no marker (or no reason): a stale allowance hides a future one", file)
		}
	}

	wantConstructors := append([]string(nil), projectIDIdentityConstructors...)
	for _, exception := range projectIDNamedExceptions {
		wantConstructors = append(wantConstructors, exception.Constructor)
		if strings.TrimSpace(exception.Reason) == "" {
			t.Errorf("named exception %s has no reason", exception.Constructor)
		}
		if _, ok := projectIDExceptionCallers[exception.Constructor]; !ok {
			t.Errorf("named exception %s has no caller entry in projectIDExceptionCallers", exception.Constructor)
		}
	}
	sort.Strings(wantConstructors)
	if strings.Join(census.constructors, ",") != strings.Join(wantConstructors, ",") {
		t.Errorf("exported functions of %s:\n got  %v\n want %v\nA new form is either an identity constructor "+
			"(projectIDIdentityConstructors) or a named exception (projectIDNamedExceptions, with a reason).",
			projectIDConstructorFile, census.constructors, wantConstructors)
	}

	for constructor, want := range projectIDExceptionCallers {
		var got []string
		for file := range census.callers[constructor] {
			got = append(got, file)
		}
		sort.Strings(got)
		sort.Strings(want)
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("%s is called from %v, want only %v: a named exception is not a second way to build the identity form",
				constructor, got, want)
		}
	}

	for _, sink := range projectIDSinkFields {
		key := sink.file + "|" + sink.typeName + "|" + sink.field
		got, found := census.fieldTypes[key]
		if !found {
			t.Errorf("sink field %s.%s not found in %s: the census table is stale", sink.typeName, sink.field, sink.file)
			continue
		}
		if got != "ProjectID" && got != "providersync.ProjectID" {
			t.Errorf("sink field %s.%s has type %s, want ProjectID: a bare string lets a writer build the id by hand", sink.typeName, sink.field, got)
		}
	}
}
