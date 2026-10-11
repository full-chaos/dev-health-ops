package investmentnormalize

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

func TestEveryMappedSubcategoryIsCanonical(t *testing.T) {
	for _, key := range KnownKeys() {
		if IsDefaultFallback(key[0], key[1]) {
			continue
		}
		sub, ok := MapSpecific(key[0], key[1])
		if !ok || !units.IsSubcategory(sub) {
			t.Errorf("%s/%s -> %q is not a canonical subcategory", key[0], key[1], sub)
		}
		if units.ThemeOf(sub) != Theme(sub) {
			t.Errorf("%s: theme %q differs from the taxonomy's %q", sub, Theme(sub), units.ThemeOf(sub))
		}
	}
	for _, name := range RuleNames() {
		_ = name
	}
	for _, r := range append(append([]estimateRule{}, conventionalRules...), keywordRules...) {
		if !units.IsSubcategory(r.subcategory) {
			t.Errorf("estimate rule %s names %q: not a canonical subcategory", r.name, r.subcategory)
		}
	}
}

// Every (area, stream) the legacy rule file can emit is a row of the mapping table (or the estimated
// product/general): a new rule in investment_areas.yaml fails here until the table holds it.
func TestEveryLegacyRuleOutputIsInTheMappingTable(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "src", "dev_health_ops", "config", "investment_areas.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	area := regexp.MustCompile(`investment_area:\s*"([^"]+)"`)
	stream := regexp.MustCompile(`project_stream:\s*"([^"]+)"`)
	pairs := map[string]bool{}
	for _, block := range strings.Split(string(raw), "output:")[1:] {
		end := strings.Index(block, "\n  -")
		if end < 0 {
			end = len(block)
		}
		block = block[:end]
		a, s := area.FindStringSubmatch(block), stream.FindStringSubmatch(block)
		if a != nil && s != nil {
			pairs[a[1]+"/"+s[1]] = true
		}
	}
	if len(pairs) < 30 {
		t.Fatalf("parsed only %d (area, stream) outputs of the rule file: the test lost its input", len(pairs))
	}
	var missing []string
	for pair := range pairs {
		parts := strings.SplitN(pair, "/", 2)
		if IsDefaultFallback(parts[0], parts[1]) {
			continue
		}
		if _, ok := MapSpecific(parts[0], parts[1]); !ok {
			missing = append(missing, pair)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("legacy rule outputs with no row in the mapping table: %v", missing)
	}
	// And the other way: no row of the table is dead (a rule file that cannot emit it).
	for _, key := range KnownKeys() {
		if !pairs[key[0]+"/"+key[1]] {
			t.Errorf("mapping row %s/%s is not an output of the rule file", key[0], key[1])
		}
	}
}

func TestMapSpecificRefusesAnUnknownKey(t *testing.T) {
	for _, key := range [][2]string{{"weird", "x"}, {"security", "nope"}, {"product", ""}, {"quality", ""}} {
		if sub, ok := MapSpecific(key[0], key[1]); ok {
			t.Errorf("%v mapped to %q: an unknown key must be refused, never defaulted", key, sub)
		}
	}
	if _, ok := MapSpecific("product", "general"); ok {
		t.Error("product/general is estimated from the item's text, never mapped")
	}
}

func str(s string) *string { return &s }

func TestEstimate(t *testing.T) {
	cases := []struct {
		name        string
		title       string
		description *string
		labels      []string
		want        string
		rule        string
	}{
		{"conventional fix", "fix(api): crash on login", nil, nil, "quality.bugfix", "cc:fix"},
		{"conventional feat", "feat: add export", nil, nil, "feature_delivery.roadmap", "cc:feat"},
		{"conventional refactor", "refactor(core): split the module", nil, nil, "maintenance.refactor", "cc:refactor"},
		{"conventional deps", "deps: bump lodash", nil, nil, "maintenance.upgrade", "cc:deps"},
		{"conventional test", "test: cover the parser", nil, nil, "quality.testing", "cc:test"},
		{"security word beats fix word", "Fix XSS in the comment box", nil, nil, "risk.vulnerability", "kw:vulnerability"},
		{"incident word", "Postmortem for the checkout outage", nil, nil, "operational.incident_response", "kw:incident"},
		{"on call", "Update the on-call rota", nil, nil, "operational.on_call", "kw:oncall"},
		{"support ticket", "Answer the customer support ticket", nil, nil, "operational.support", "kw:support"},
		{"flaky", "Flaky integration test", nil, nil, "quality.reliability", "kw:flaky"},
		{"bug", "Login button broken on mobile", nil, nil, "quality.bugfix", "kw:fix"},
		{"upgrade", "Upgrade Go to 1.27", nil, nil, "maintenance.upgrade", "kw:upgrade"},
		{"feature by description", "Dashboard", str("Implement the new dashboard"), nil, "feature_delivery.roadmap", "kw:feature"},
		{"label", "Dashboard", nil, []string{"tech debt"}, "maintenance.debt", "kw:debt"},
		{"description is read", "Task 42", str("this is a regression in billing"), nil, "quality.bugfix", "kw:fix"},
	}
	for _, c := range cases {
		sub, rule, ok := Estimate(c.title, c.description, c.labels)
		if !ok || sub != c.want || rule != c.rule {
			t.Errorf("%s: Estimate = (%q, %q, %v), want (%q, %q)", c.name, sub, rule, ok, c.want, c.rule)
		}
	}
	// NO default bucket: text no rule matches stays out.
	for _, title := range []string{"Quarterly planning", "Meeting notes", "", "Sync with the team"} {
		if sub, rule, ok := Estimate(title, nil, nil); ok {
			t.Errorf("%q matched (%q, %q): an unmatched item must stay out, never a default bucket", title, sub, rule)
		}
	}
	// Every estimate theme of D5941 is reachable.
	seen := map[string]bool{}
	for _, c := range cases {
		sub, _, _ := Estimate(c.title, c.description, c.labels)
		seen[Theme(sub)] = true
	}
	for _, theme := range []string{"feature_delivery", "operational", "maintenance", "quality", "risk"} {
		if !seen[theme] {
			t.Errorf("no estimate case reaches the theme %s", theme)
		}
	}
}

// The dry-run table carries the no-repository count and the unmatched share as own rows (lead, for chris).
func TestReportLinesCarryTheNoRepositoryCountAndTheUnmatchedShare(t *testing.T) {
	r := newReport()
	r.Items, r.SkippedNoRepo, r.Fallback, r.Unmatched = 10, 2, 4, 1
	joined := strings.Join(r.Lines(), "\n")
	for _, want := range []string{"skipped, no repository: 2", "product/general items (estimated): 4", "unmatched share of product/general: 25.0%"} {
		if !strings.Contains(joined, want) {
			t.Errorf("the report lacks the row %q:\n%s", want, joined)
		}
	}
	if got := strings.Join(newReport().Lines(), "\n"); !strings.Contains(got, "unmatched share of product/general: n/a") {
		t.Errorf("an empty window must say n/a, not divide by zero:\n%s", got)
	}
}
