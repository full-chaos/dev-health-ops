package providersync

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
	"sort"
	"testing"

	"gopkg.in/yaml.v3"
)

// investmentRealConfigRuleCount is the only place the "44 rules" figure quoted
// in the PR body and in this package's comments is allowed to live, and it is
// ASSERTED against the real file below rather than narrated. A count that only
// appears in prose is a claim; a count the build checks is a measurement.
const investmentRealConfigRuleCount = 44

// investmentCallSitePremise is what the production sources say about the
// artifact the work-item call site builds. Both fields are REFLECTED from real
// Python (testdata/python_investment_call_site.py), never transcribed.
type investmentCallSitePremise struct {
	CallSiteArtifactKeys []string `json:"call_site_artifact_keys"`
	WorkItemFields       []string `json:"work_item_fields"`
}

func (premise investmentCallSitePremise) hasArtifactKey(name string) bool {
	return investmentContains(premise.CallSiteArtifactKeys, name)
}

func investmentContains(values []string, name string) bool {
	for _, value := range values {
		if value == name {
			return true
		}
	}
	return false
}

// investmentReflectCallSite executes the reflector against the two real
// production files.
//
// It needs Python, so it runs under ci/check_go.sh's live-python-oracles stage
// rather than under a bare `go test` -- both are in the `fast` verb. That cost
// buys the thing an in-Go transcription could not: the premise below is the
// PRODUCTION code's, not this test's copy of it.
func investmentReflectCallSite(t *testing.T) investmentCallSitePremise {
	t.Helper()
	python := pythonExecutable(t)
	root := investmentRepoRoot(t)
	output, err := exec.Command(
		python,
		filepath.Join(root, "internal/providersync/testdata/python_investment_call_site.py"),
		filepath.Join(root, "src/dev_health_ops/metrics/job_work_items.py"),
		filepath.Join(root, "src/dev_health_ops/models/work_items.py"),
	).CombinedOutput()
	if err != nil {
		t.Fatalf("reflect the investment call site: %v: %s", err, output)
	}
	var premise investmentCallSitePremise
	if err := json.Unmarshal(output, &premise); err != nil {
		t.Fatalf("decode the call-site reflection: %v: %s", err, output)
	}
	if len(premise.CallSiteArtifactKeys) == 0 || len(premise.WorkItemFields) == 0 {
		t.Fatalf("call-site reflection came back empty (%s); every assertion "+
			"derived from it would be vacuously true", output)
	}
	return premise
}

// TestInvestmentCallSiteArtifactPremiseHolds is the derivation the deadness
// tripwire below stands on, and the reason it is a separate test is that it
// must fail with its OWN name when the premise moves.
//
// Both facts come from production Python:
//
//   - the call site (metrics/job_work_items.py:1377) passes exactly labels,
//     component, title and provider -- so `paths` is absent, which is what
//     kills the three path_prefix rules;
//   - WorkItem declares no `component` field, so that call site's
//     `getattr(item, "component", "")` cannot return anything but "" -- which
//     is what kills data_component.
//
// The previous version of this file hand-wrote `InvestmentArtifact{Component:
// "", Paths: nil}` with no derivation at all. Adding a `component` field to
// WorkItem would have made data_component live while this test went on passing,
// because it was asserting against its own transcription of the premise instead
// of against the premise.
func TestInvestmentCallSiteArtifactPremiseHolds(t *testing.T) {
	premise := investmentReflectCallSite(t)

	want := []string{"component", "labels", "provider", "title"}
	got := append([]string(nil), premise.CallSiteArtifactKeys...)
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
	if investmentContains(premise.WorkItemFields, "component") {
		t.Error("WorkItem now declares a `component` field, so the call site's " +
			"getattr(item, \"component\", \"\") no longer always returns \"\" and " +
			"data_component can fire. Update the tripwire and the PR body.")
	}
}

// investmentCallSiteArtifact builds the artifact the work-item call site
// actually produces, FROM the reflected premise rather than from a constant.
func investmentCallSiteArtifact(
	t *testing.T, premise investmentCallSitePremise, labels []string,
) InvestmentArtifact {
	t.Helper()
	artifact := InvestmentArtifact{Labels: labels}
	if premise.hasArtifactKey("component") {
		// Present in the dict literal, and WorkItem has no field to source it
		// from, so `getattr(item, "component", "")` yields the default. The
		// premise test above is what keeps that second half true.
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
	premise := investmentReflectCallSite(t)

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
