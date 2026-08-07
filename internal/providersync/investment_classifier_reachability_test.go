package providersync

import (
	"os"
	"testing"

	"gopkg.in/yaml.v3"
)

// Four of the 44 rules in the checked-in investment config CANNOT fire from the
// work-item call site (job_work_items.py:1377-1383), which supplies only
// labels, component, title and provider:
//
//   - infra_path, docs_path, test_path match on path_prefix, and the call site
//     passes no `paths` key at all, so the matcher's path arm rejects every
//     artifact reaching it from there.
//   - data_component matches component ['Data Platform', 'Analytics'], and
//     WorkItem has no `component` attribute, so `getattr(item, "component", "")`
//     is always "" -- which is not in that list.
//
// This is a TRIPWIRE, not documentation. Deadness asserted in a comment rots
// the moment someone edits the YAML; deadness asserted here fails the build.
// It is deliberately two-directional: a rule that stops being dead fails, AND a
// rule that disappears or is renamed fails, because either means this list has
// drifted from the config it describes.
//
// It reads the REAL config. Pointing it at a fixture would make it agree with
// itself, which is the defect class this whole lane has been removing.
func TestInvestmentClassifierUnreachableRulesStayUnreachable(t *testing.T) {
	// Rule id -> why it cannot fire from the work-item call site.
	unreachable := map[string]string{
		"infra_path":     "path_prefix, and the call site supplies no paths",
		"docs_path":      "path_prefix, and the call site supplies no paths",
		"test_path":      "path_prefix, and the call site supplies no paths",
		"data_component": "component list excludes \"\", the only value the call site can produce",
	}

	contents, err := os.ReadFile(investmentConfigPath(t, "real"))
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Rules []investmentRule `yaml:"rules"`
	}
	if err := yaml.Unmarshal(contents, &document); err != nil {
		t.Fatal(err)
	}
	if len(document.Rules) == 0 {
		t.Fatal("the real config parsed to zero rules; every assertion below would be vacuous")
	}

	// The artifact the work-item call site actually builds: labels vary, but
	// paths is ABSENT and component is ALWAYS "". Anything a rule needs beyond
	// this is unreachable from that path by construction.
	callSiteArtifact := func(labels []string) InvestmentArtifact {
		return InvestmentArtifact{Labels: labels, Component: "", Paths: nil}
	}

	seen := map[string]bool{}
	for _, rule := range document.Rules {
		reason, declaredDead := unreachable[rule.ID]
		if !declaredDead {
			continue
		}
		seen[rule.ID] = true

		// Feed the rule EVERY label it could want, so the only thing that can
		// still reject it is the criterion this test says is unsatisfiable.
		// Without this the rule might be failing for a boring reason and the
		// assertion would prove nothing about the criterion named in `reason`.
		var labels []string
		if rule.Match != nil && rule.Match.Label != nil {
			labels = append(labels, *rule.Match.Label...)
		}
		if investmentRuleMatches(rule.Match, callSiteArtifact(labels)) {
			t.Errorf("rule %q is now REACHABLE from the work-item call site, but this "+
				"test and the PR body both claim it is dead (%s). Either the config "+
				"changed or the call site did; update both, do not delete this case.",
				rule.ID, reason)
		}
	}

	for id := range unreachable {
		if !seen[id] {
			t.Errorf("rule %q is declared unreachable here but no longer exists in the "+
				"real config -- this list has drifted from the file it describes", id)
		}
	}
}

// The counterpart assertion: the matcher arms those dead rules use are NOT
// broken, they are merely unfed. Without this, "dead" and "the matcher cannot
// match anything" would be indistinguishable, and a genuinely broken path or
// component arm would read as expected deadness.
func TestInvestmentClassifierDeadArmsWorkWhenFed(t *testing.T) {
	classifier, err := NewInvestmentClassifier(investmentConfigPath(t, "real"))
	if err != nil {
		t.Fatal(err)
	}
	// data_component fires the moment a component it lists is supplied.
	if got := classifier.Classify(InvestmentArtifact{Component: "Data Platform"}); got.RuleID != "data_component" {
		t.Errorf("component arm: rule_id = %q, want data_component -- the arm itself "+
			"must work, or the unreachability claim above is about a broken matcher "+
			"rather than an unfed one", got.RuleID)
	}
	// A path_prefix rule fires the moment paths are supplied. docs_path is used
	// because its prefixes are the least likely to collide with a label rule.
	if got := classifier.Classify(InvestmentArtifact{Paths: []string{"docs/guide.md"}}); got.RuleID != "docs_path" {
		t.Error("path arm: no path_prefix rule fired for a docs/ path, so the " +
			"unreachability claim above may be masking a broken path matcher")
	}
}
