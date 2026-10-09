package providersync

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// plainBoolPageEnds are the production struct fields in internal/providersync
// that decode a page-end signal (hasNextPage / isLast) into a plain bool. A
// plain bool reads an absent field as "no more pages", which is an end nobody
// proved. Every one listed feeds NO ownership snapshot (work-item and
// pull-request fetches, where a missing page end is a limit of the data, not
// a fact closed by a snapshot). A path that feeds a snapshot decodes into a
// pointer (a stated end) or a typed end (linearReferencePageEnd).
var plainBoolPageEnds = map[string]struct {
	count  int
	reason string
}{
	"linear_work_items_route.go":        {1, "Linear work-item connections: no ownership snapshot reads them"},
	"github_work_items_social_fetch.go": {1, "GitHub pull-request social fetch: no ownership snapshot"},
	"github_pr_reviews_fetch.go":        {1, "GitHub review fetch: no ownership snapshot"},
	"jira_atlassian_route.go":           {1, "Jira work-item route: no ownership snapshot"},
}

// snapshotPageInfoFields are the PageInfo fields of the Linear catalog payloads
// that feed ownership and membership snapshots. Each must be a linearReferencePageEnd.
var snapshotPageInfoFields = []struct{ file, typeName string }{
	{"linear_reference_catalog.go", "linearReferenceProjectTeamsPayload"},
	{"linear_reference_catalog.go", "linearReferenceCatalogTeamMembersPayload"},
	{"linear_reference_catalog.go", "linearReferenceCatalogTeamPagePayload"},
}

func TestPageInfoDecodeCensus(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "internal", "providersync")
	plain := map[string]int{}
	pageInfoTypes := map[string]string{}
	files := 0
	err = filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if path != dir {
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
		files++
		base := filepath.Base(path)
		ast.Inspect(file, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.TypeSpec:
				if structType, ok := typed.Type.(*ast.StructType); ok {
					for _, field := range structType.Fields.List {
						for _, name := range field.Names {
							if name.Name == "PageInfo" {
								pageInfoTypes[base+"|"+typed.Name.Name] = exprText(field.Type)
							}
						}
					}
				}
			case *ast.Field:
				for _, name := range typed.Names {
					if name.Name != "HasNextPage" && name.Name != "IsLast" {
						continue
					}
					if ident, ok := typed.Type.(*ast.Ident); ok && ident.Name == "bool" {
						plain[base]++
					}
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if files < 100 {
		t.Fatalf("the scan read %d files: it measured nothing", files)
	}
	var names []string
	for name := range plain {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		allowed, ok := plainBoolPageEnds[name]
		if !ok || allowed.count != plain[name] || strings.TrimSpace(allowed.reason) == "" {
			t.Errorf("%s decodes a page end into a plain bool %d time(s) and is not named in plainBoolPageEnds: an absent field reads as "+
				"\"no more pages\". Use a pointer or linearReferencePageEnd on a path that feeds a snapshot, or name the file with its reason.", name, plain[name])
		}
	}
	for name := range plainBoolPageEnds {
		if plain[name] == 0 {
			t.Errorf("plainBoolPageEnds names %s but it holds no plain bool page end: a stale allowance hides a future one", name)
		}
	}
	for _, field := range snapshotPageInfoFields {
		got, found := pageInfoTypes[field.file+"|"+field.typeName]
		if !found {
			t.Errorf("%s.%s has no PageInfo field: the census table is stale", field.file, field.typeName)
			continue
		}
		if got != "linearReferencePageEnd" {
			t.Errorf("%s.PageInfo has type %s, want linearReferencePageEnd (a stated end)", field.typeName, got)
		}
	}
}
