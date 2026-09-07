package providersync

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// investmentRealConfigRuleCount is the only place the "44 rules" figure quoted
// in the PR body and in this package's comments is allowed to live, and it is
// ASSERTED against the real file below rather than narrated. A count that only
// appears in prose is a claim; a count the build checks is a measurement.
const investmentRealConfigRuleCount = 44

// investmentWorkItemArtifactLiteralType is the Go type name this file reflects
// the work-item investment call site from -- see investmentReflectGoCallSite.
const investmentWorkItemArtifactLiteralType = "InvestmentArtifact"

// investmentGoCallSitePremise is what the production Go source says about the
// artifact the work-item call site builds. Both fields are REFLECTED from the
// real Go source (github_work_item_engine_destinations.go) via go/ast, never
// transcribed.
//
// CHAOS-5351: this used to reflect PYTHON (job_work_items.py's now-deleted
// compute_work_item_engine_destinations_daily, via
// testdata/python_investment_call_site.py's dict-literal reflection). Python
// is deleted; Go's own InvestmentArtifact{...} composite literal in
// buildGitHubInvestmentDestinationsDaily is the production call site now, so
// the premise must derive from IT.
type investmentGoCallSitePremise struct {
	// ArtifactKeys are the lower-cased field names set in the InvestmentArtifact{}
	// composite literal (e.g. "labels", "component", "title", "provider").
	ArtifactKeys []string
	// ItemSourcedFields are the subset of ArtifactKeys whose value expression
	// reads from the enclosing range loop's item variable (a SelectorExpr like
	// `item.Labels`), as opposed to a local/constant (like `&emptyComponent`).
	// A field NOT in this set cannot vary per work item -- that is what keeps
	// "component is always empty" (and therefore data_component's
	// unreachability) true regardless of what fields a work-item-shaped Go
	// struct declares.
	ItemSourcedFields []string
}

func (premise investmentGoCallSitePremise) hasArtifactKey(name string) bool {
	return investmentContains(premise.ArtifactKeys, name)
}

func (premise investmentGoCallSitePremise) isItemSourced(name string) bool {
	return investmentContains(premise.ItemSourcedFields, name)
}

func investmentContains(values []string, name string) bool {
	for _, value := range values {
		if value == name {
			return true
		}
	}
	return false
}

// investmentParseGoCallSitePremise is the pure, t-free core of the reflection:
// it parses src (Go source text) looking for EXACTLY ONE composite literal of
// type investmentWorkItemArtifactLiteralType, inside a range loop, and returns
// an error -- never panics, never calls testing.T -- for every way the premise
// could fail to derive cleanly. Kept separate from investmentReflectGoCallSite
// so TestInvestmentGoCallSiteReflectorFailsWhenLiteralMissing can drive it with
// a synthetic source and assert on the error without the failure aborting that
// test itself.
func investmentParseGoCallSitePremise(
	path string, src []byte,
) (investmentGoCallSitePremise, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, src, 0)
	if err != nil {
		return investmentGoCallSitePremise{}, fmt.Errorf("parse %s: %w", path, err)
	}

	var literals []*ast.CompositeLit
	ast.Inspect(file, func(n ast.Node) bool {
		lit, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		ident, ok := lit.Type.(*ast.Ident)
		if ok && ident.Name == investmentWorkItemArtifactLiteralType {
			literals = append(literals, lit)
		}
		return true
	})
	if len(literals) != 1 {
		return investmentGoCallSitePremise{}, fmt.Errorf(
			"expected exactly one %s{...} composite literal in %s, found %d -- "+
				"the work-item investment call-site premise cannot be derived "+
				"without exactly one production call site",
			investmentWorkItemArtifactLiteralType, path, len(literals),
		)
	}
	literal := literals[0]

	rangeVar := investmentEnclosingRangeVar(file, literal)
	if rangeVar == "" {
		return investmentGoCallSitePremise{}, fmt.Errorf(
			"%s{...} literal in %s is not inside a range loop -- the call-site "+
				"premise assumes one item is classified per loop iteration",
			investmentWorkItemArtifactLiteralType, path,
		)
	}

	var artifactKeys, itemSourced []string
	for _, elt := range literal.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			return investmentGoCallSitePremise{}, fmt.Errorf(
				"%s{...} literal in %s has a positional (non-keyed) field -- "+
					"the reflector requires keyed fields to name them",
				investmentWorkItemArtifactLiteralType, path,
			)
		}
		key, ok := kv.Key.(*ast.Ident)
		if !ok {
			return investmentGoCallSitePremise{}, fmt.Errorf(
				"%s{...} literal in %s has a non-identifier key",
				investmentWorkItemArtifactLiteralType, path,
			)
		}
		name := strings.ToLower(key.Name)
		artifactKeys = append(artifactKeys, name)
		if investmentExprReadsFrom(kv.Value, rangeVar) {
			itemSourced = append(itemSourced, name)
		}
	}
	sort.Strings(artifactKeys)
	sort.Strings(itemSourced)
	return investmentGoCallSitePremise{
		ArtifactKeys:      artifactKeys,
		ItemSourcedFields: itemSourced,
	}, nil
}

// investmentEnclosingRangeVar returns the loop variable name of the innermost
// `for _, X := range ...` whose body contains target, or "" if target is not
// inside any range loop's body.
func investmentEnclosingRangeVar(file *ast.File, target ast.Node) string {
	var best *ast.RangeStmt
	ast.Inspect(file, func(n ast.Node) bool {
		rs, ok := n.(*ast.RangeStmt)
		if !ok || rs.Body == nil {
			return true
		}
		if rs.Body.Pos() > target.Pos() || target.End() > rs.Body.End() {
			return true
		}
		if best == nil || (rs.Body.Pos() >= best.Body.Pos() && rs.Body.End() <= best.Body.End()) {
			best = rs
		}
		return true
	})
	if best == nil {
		return ""
	}
	ident, ok := best.Value.(*ast.Ident)
	if !ok {
		return ""
	}
	return ident.Name
}

// investmentExprReadsFrom reports whether expr contains a selector expression
// (e.g. `item.Labels`) whose base identifier is name.
func investmentExprReadsFrom(expr ast.Expr, name string) bool {
	found := false
	ast.Inspect(expr, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if ident, ok := sel.X.(*ast.Ident); ok && ident.Name == name {
			found = true
		}
		return true
	})
	return found
}

// investmentReflectGoCallSite executes the reflector against the real
// production source.
func investmentReflectGoCallSite(t *testing.T) investmentGoCallSitePremise {
	t.Helper()
	root := investmentRepoRoot(t)
	path := filepath.Join(root, "internal/providersync/github_work_item_engine_destinations.go")
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	premise, err := investmentParseGoCallSitePremise(path, src)
	if err != nil {
		t.Fatal(err)
	}
	if len(premise.ArtifactKeys) == 0 {
		t.Fatalf("call-site reflection of %s came back empty; every assertion "+
			"derived from it would be vacuously true", path)
	}
	return premise
}

// TestInvestmentGoCallSiteReflectorFailsWhenLiteralMissing is the tripwire's
// own tripwire: if a refactor ever moves the InvestmentArtifact{...} literal
// out of github_work_item_engine_destinations.go (or duplicates it, or turns
// it positional), investmentParseGoCallSitePremise must return an error, not
// silently report an empty or partial premise that would make every
// assertion below it vacuously true. Driven directly against synthetic
// source -- never against the real file -- so it exercises the failure paths
// without needing to break production code to prove they fire.
func TestInvestmentGoCallSiteReflectorFailsWhenLiteralMissing(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name string
		src  string
	}{
		{
			name: "no literal at all",
			src: `package providersync
func f() {}
`,
		},
		{
			name: "literal not inside a range loop",
			src: `package providersync
func f() {
	_ = InvestmentArtifact{Labels: nil}
}
`,
		},
		{
			name: "two literals",
			src: `package providersync
func f(items []item) {
	for _, item := range items {
		_ = InvestmentArtifact{Labels: item.Labels}
		_ = InvestmentArtifact{Labels: item.Labels}
	}
}
`,
		},
		{
			name: "positional (non-keyed) field",
			src: `package providersync
func f(items []item) {
	for _, item := range items {
		_ = InvestmentArtifact{item.Labels}
	}
}
`,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := investmentParseGoCallSitePremise("synthetic.go", []byte(testCase.src))
			if err == nil {
				t.Fatal("expected the reflector to fail loudly, got a premise instead")
			}
		})
	}
}

// TestInvestmentCallSiteArtifactPremiseHolds is the derivation the deadness
// tripwire below stands on, and the reason it is a separate test is that it
// must fail with its OWN name when the premise moves.
//
// Both facts come from production Go (github_work_item_engine_destinations.go):
//
//   - buildGitHubInvestmentDestinationsDaily's InvestmentArtifact{...} literal
//     sets exactly Labels, Component, Title and Provider -- so Paths is
//     absent, which is what kills the three path_prefix rules;
//   - Component is always `&emptyComponent` (a local, never `item.Component`),
//     so this call site's artifact can never carry a non-empty component --
//     which is what kills data_component.
//
// A previous version of this file (and, before that, of the Python producer
// it ported) hand-wrote the premise with no derivation at all. Adding a real
// field-read for Component at the call site would make data_component live
// while a hand-transcribed test went on passing, because it would be
// asserting against its own copy of the premise instead of against the
// premise. Deriving both facts from the real source via go/ast makes that
// change fail the tripwire loudly instead.
func TestInvestmentCallSiteArtifactPremiseHolds(t *testing.T) {
	premise := investmentReflectGoCallSite(t)

	want := []string{"component", "labels", "provider", "title"}
	got := append([]string(nil), premise.ArtifactKeys...)
	sort.Strings(got)
	if len(got) != len(want) || !investmentStringsEqual(got, want) {
		t.Errorf("the work-item call site now builds its artifact from %v, not %v. "+
			"Every deadness claim in this file is derived from that key set -- "+
			"re-derive them rather than adjusting this list to match.", got, want)
	}
	if premise.hasArtifactKey("paths") {
		t.Error("the call site now supplies `paths`, so the three path_prefix rules " +
			"this file calls dead are live. Update the tripwire and the PR body.")
	}
	if premise.isItemSourced("component") {
		t.Error("the call site now sources `component` from the work item itself " +
			"(rather than a constant empty local), so data_component can fire. " +
			"Update the tripwire and the PR body.")
	}
}

// investmentCallSiteArtifact builds the artifact the work-item call site
// actually produces, FROM the reflected premise rather than from a constant.
func investmentCallSiteArtifact(
	t *testing.T, premise investmentGoCallSitePremise, labels []string,
) InvestmentArtifact {
	t.Helper()
	artifact := InvestmentArtifact{Labels: labels}
	if premise.hasArtifactKey("component") {
		// Present in the literal, and never sourced from the work item, so it
		// is always "". The premise test above is what keeps that second half
		// true.
		artifact.Component = investmentString("")
	}
	if premise.hasArtifactKey("paths") {
		t.Fatal("the call site supplies `paths`, so this helper can no longer " +
			"derive the artifact it is meant to reproduce")
	}
	return artifact
}

// Four of the 44 rules in the checked-in investment config CANNOT fire from the
// work-item call site. This test DERIVES that set by running every rule in the
// real file against the real call-site artifact, and then asserts the derived
// set equals the four this lane claims -- rather than asserting only that four
// named rules fail, which would say nothing about the other forty.
//
// It is a TRIPWIRE, not documentation. Deadness asserted in a comment rots the
// moment someone edits the YAML; deadness asserted here fails the build. Set
// equality makes it two-directional for free: a rule that stops being dead
// fails, a rule that BECOMES dead fails, and a renamed or deleted rule fails,
// because all three change the derived set.
//
// It reads the REAL config. Pointing it at a fixture would make it agree with
// itself, which is the defect class this whole lane has been removing.
func TestInvestmentClassifierUnreachableRulesStayUnreachable(t *testing.T) {
	premise := investmentReflectGoCallSite(t)

	// Rule id -> why it cannot fire from the work-item call site. The reasons
	// are for the human reading a failure; the SET is what is asserted.
	unreachable := map[string]string{
		"infra_path":     "path_prefix, and the call site supplies no paths",
		"docs_path":      "path_prefix, and the call site supplies no paths",
		"test_path":      "path_prefix, and the call site supplies no paths",
		"data_component": "component list excludes \"\", the only value the call site can produce",
	}

	classifier, err := NewInvestmentClassifier(investmentConfigPath(t, "real"))
	if err != nil {
		t.Fatal(err)
	}
	if len(classifier.rules) != investmentRealConfigRuleCount {
		t.Fatalf("the real config now has %d rules, not %d -- the deadness set "+
			"below was derived against the %d-rule file and must be re-derived",
			len(classifier.rules), investmentRealConfigRuleCount,
			investmentRealConfigRuleCount)
	}

	var dead []string
	for _, rule := range classifier.rules {
		id, err := investmentRuleID(rule.id)
		if err != nil || id == nil {
			t.Fatalf("the real config has a rule whose id cannot be read (%v); "+
				"deadness cannot be attributed without one", err)
		}
		// Feed the rule EVERY label it declares, so the only thing that can
		// still reject it is a criterion the call site cannot satisfy. Without
		// this a rule might be failing for a boring reason and the assertion
		// would prove nothing about the criterion named in `unreachable`.
		artifact := investmentCallSiteArtifact(t, premise, investmentDeclaredLabels(t, rule.match))
		matched, err := investmentRuleMatches(rule.match, artifact)
		if err != nil {
			t.Fatalf("rule %q errored against the call-site artifact: %v -- the real "+
				"config must classify cleanly", *id, err)
		}
		if !matched {
			dead = append(dead, *id)
		}
	}
	sort.Strings(dead)

	want := make([]string, 0, len(unreachable))
	for id := range unreachable {
		want = append(want, id)
	}
	sort.Strings(want)
	if !investmentStringsEqual(dead, want) {
		t.Errorf("the rules unreachable from the work-item call site are now %v, "+
			"but this lane and the PR body both claim %v (reasons: %v). Either the "+
			"config changed or the call site did; update both, do not delete this "+
			"case.", dead, want, unreachable)
	}
}

// investmentDeclaredLabels reads a rule's own `label:` list so the deadness
// derivation can feed the rule everything it asks for.
func investmentDeclaredLabels(t *testing.T, match *yaml.Node) []string {
	t.Helper()
	if match == nil {
		return nil
	}
	node := investmentMappingValue(match, "label")
	if node == nil {
		return nil
	}
	entries, err := investmentIterate(node)
	if err != nil {
		t.Fatalf("read a real rule's label list: %v", err)
	}
	labels := make([]string, 0, len(entries))
	for _, entry := range entries {
		labels = append(labels, entry.Value)
	}
	return labels
}

func investmentStringsEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

// The counterpart assertion: the matcher arms those dead rules use are NOT
// broken, they are merely unfed. Without this, "dead" and "the matcher cannot
// match anything" would be indistinguishable, and a genuinely broken path or
// component arm would read as expected deadness.
func TestInvestmentClassifierDeadArmsWorkWhenFed(t *testing.T) {
	t.Parallel()
	classifier, err := NewInvestmentClassifier(investmentConfigPath(t, "real"))
	if err != nil {
		t.Fatal(err)
	}
	// data_component fires the moment a component it lists is supplied.
	got, err := classifier.Classify(
		InvestmentArtifact{Component: investmentString("Data Platform")})
	if err != nil {
		t.Fatal(err)
	}
	if got.RuleID == nil || *got.RuleID != "data_component" {
		t.Errorf("component arm: rule_id = %s, want data_component -- the arm itself "+
			"must work, or the unreachability claim above is about a broken matcher "+
			"rather than an unfed one", investmentDescribeString(got.RuleID))
	}
	// A path_prefix rule fires the moment paths are supplied. docs_path is used
	// because its prefixes are the least likely to collide with a label rule.
	got, err = classifier.Classify(InvestmentArtifact{Paths: []string{"docs/guide.md"}})
	if err != nil {
		t.Fatal(err)
	}
	if got.RuleID == nil || *got.RuleID != "docs_path" {
		t.Errorf("path arm: rule_id = %s, want docs_path -- no path_prefix rule fired "+
			"for a docs/ path, so the unreachability claim above may be masking a "+
			"broken path matcher", investmentDescribeString(got.RuleID))
	}
}
