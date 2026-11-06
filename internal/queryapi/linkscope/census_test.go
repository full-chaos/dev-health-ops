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
type censusFile struct {
	needsLink bool
	reads     map[string]string
}

var workItemReads = map[string]censusFile{
	"internal/queryapi/drilldown/issues.go": {true, map[string]string{
		"fetchIssuesQuery":                        "wct is the read; the link condition is spliced into its WHERE",
		"latestBlockedItemDaysSource":             "the blocked window source that wraps it carries the link condition on its outer WHERE",
		"primaryWorkItemTeamAttributionSource":    "LEFT JOIN on the ids of the narrowed read; it adds no item",
		"teamScopedWorkItemTeamAttributionSource": "LEFT JOIN on the ids of the narrowed read; it adds no item",
	}},
	"internal/queryapi/people/drilldownissues.go": {true, map[string]string{
		"fetchPersonIssuesQuery":                        "wct is the read; the link condition is spliced into its WHERE",
		"primaryWorkItemTeamAttributionSourceForPerson": "LEFT JOIN on the ids of the narrowed read; it adds no item",
		"workItemTitlesQuery":                           "reads the titles of the ids the narrowed read returned",
	}},
	"internal/queryapi/flame/clickhouse.go": {true, map[string]string{
		"fetchIssueQuery":         "the link condition is spliced into its WHERE",
		"fetchIssueIdentityQuery": "read only for an unrestricted ceiling; fetchIssueIdentity hands a restricted one to fetchCeilingIssueIdentity",
		"fetchIssueTitleQuery":    "title only, no repository column",
	}},
	// CHAOS-13086: the work-item reads of home (and opportunities, which builds on home.BuildResponse).
	"internal/queryapi/home/queries_freshness.go": {true, map[string]string{
		"fetchCoverage":       "the two work_item_cycle_times reads carry linkscope.ItemCondition",
		"fetchSourceStatuses": "the work_items read carries linkscope.ItemCondition",
	}},
	"internal/queryapi/home/queries_signal_attribution.go": {true, map[string]string{
		"signalAttributionSQL": "under a ceiling the filter is linkscope.ItemConditionIDs on wct.work_item_id, never wi.repo_id",
	}},
	"internal/queryapi/home/repolinked.go": {true, map[string]string{
		"readCoverage": "under a ceiling the item scan is held to linkeditems.Subquery over the view's repository ids",
		"readBlocked":  "work_item_id IN linkeditems.Subquery over the view's repository ids",
	}},
	"internal/queryapi/home/queries_metrics.go": {false, map[string]string{
		"fetchBlockedHours": "reads work_item_state_durations_daily by team: unreachable under a ceiling, where blocked_work takes the repo-linked path (repoFilterRequested is true for a restricted ceiling)",
	}},
}

var workItemTable = regexp.MustCompile(`FROM\s+(work_item_\w+|work_items)\b`)

// linkSelectors are the ways a file narrows an item read to a link.
var linkSelectors = map[string]bool{"linkscope.ItemCondition": true, "linkscope.ItemConditionIDs": true, "linkeditems.Subquery": true}

func TestK1_EveryWorkItemReadOfANarrowedRouteIsNarrowedOrNamed(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	for rel, entry := range workItemReads {
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, rel), nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", rel, err)
		}
		found := map[string]bool{}
		mark := func(name string, node ast.Node) {
			ast.Inspect(node, func(n ast.Node) bool {
				if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
					if text, err := strconv.Unquote(lit.Value); err == nil && workItemTable.MatchString(text) {
						found[name] = true
					}
				}
				return true
			})
		}
		for _, decl := range file.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				mark(d.Name.Name, d.Body)
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					if v, ok := spec.(*ast.ValueSpec); ok && len(v.Names) > 0 {
						mark(v.Names[0].Name, v)
					}
				}
			}
		}
		linked := false
		ast.Inspect(file, func(n ast.Node) bool {
			if sel, ok := n.(*ast.SelectorExpr); ok {
				if pkg, ok := sel.X.(*ast.Ident); ok && linkSelectors[pkg.Name+"."+sel.Sel.Name] {
					linked = true
				}
			}
			return true
		})
		if entry.needsLink && !linked {
			t.Errorf("%s never narrows through linkscope.ItemCondition, linkscope.ItemConditionIDs or linkeditems.Subquery", rel)
		}
		var problems []string
		for name := range found {
			if _, ok := entry.reads[name]; !ok {
				problems = append(problems, name+" reads a work-item table and is not on the K1 list")
			}
		}
		for name := range entry.reads {
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
