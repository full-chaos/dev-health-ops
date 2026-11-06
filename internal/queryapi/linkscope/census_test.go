package linkscope

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// K1 (CHAOS-13085): every statement constant of a narrowed item-grain route that reads a work_item_* table or
// work_items is on this list with the reason it cannot read an item outside the ceiling, and each file calls
// linkscope.ItemCondition. A new read, or a listed read that is gone, fails: a work-item read of a registry route
// either passes through linkscope or is named here with its proof.
var workItemReads = map[string]map[string]string{
	"internal/queryapi/drilldown/issues.go": {
		"fetchIssuesQuery":                        "wct is the read; the link condition is spliced into its WHERE",
		"latestBlockedItemDaysSource":             "the blocked window source that wraps it carries the link condition on its outer WHERE",
		"primaryWorkItemTeamAttributionSource":    "LEFT JOIN on the ids of the narrowed read; it adds no item",
		"teamScopedWorkItemTeamAttributionSource": "LEFT JOIN on the ids of the narrowed read; it adds no item",
	},
	"internal/queryapi/people/drilldownissues.go": {
		"fetchPersonIssuesQuery":                        "wct is the read; the link condition is spliced into its WHERE",
		"primaryWorkItemTeamAttributionSourceForPerson": "LEFT JOIN on the ids of the narrowed read; it adds no item",
		"workItemTitlesQuery":                           "reads the titles of the ids the narrowed read returned",
	},
	"internal/queryapi/flame/clickhouse.go": {
		"fetchIssueQuery":         "the link condition is spliced into its WHERE",
		"fetchIssueIdentityQuery": "read only for an unrestricted ceiling; fetchIssueIdentity hands a restricted one to fetchCeilingIssueIdentity",
		"fetchIssueTitleQuery":    "title only, no repository column",
	},
}

var workItemTable = regexp.MustCompile(`FROM\s+(work_item_\w+|work_items)\b`)

func TestK1_EveryWorkItemReadOfANarrowedRouteIsNarrowedOrNamed(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	for rel, listed := range workItemReads {
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, rel), nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", rel, err)
		}
		found := map[string]bool{}
		callsItemCondition := false
		ast.Inspect(file, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.ValueSpec:
				for i, name := range node.Names {
					if i >= len(node.Values) {
						continue
					}
					if lit, ok := node.Values[i].(*ast.BasicLit); ok && lit.Kind == token.STRING {
						if text, err := strconv.Unquote(lit.Value); err == nil && workItemTable.MatchString(text) {
							found[name.Name] = true
						}
					}
				}
			case *ast.SelectorExpr:
				if pkg, ok := node.X.(*ast.Ident); ok && pkg.Name == "linkscope" && node.Sel.Name == "ItemCondition" {
					callsItemCondition = true
				}
			}
			return true
		})
		if !callsItemCondition {
			t.Errorf("%s never calls linkscope.ItemCondition", rel)
		}
		var problems []string
		for name := range found {
			if _, ok := listed[name]; !ok {
				problems = append(problems, name+" reads a work-item table and is not on the K1 list")
			}
		}
		for name := range listed {
			if !found[name] {
				problems = append(problems, name+" is on the K1 list but no longer reads a work-item table")
			}
		}
		sort.Strings(problems)
		if len(problems) > 0 {
			t.Errorf("%s:\n  %s", rel, strings.Join(problems, "\n  "))
		}
	}
}
