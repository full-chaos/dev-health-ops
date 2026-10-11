package investmentnormalize

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

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
func TestReportLinesCarryEveryBucketAndTheUnmatchedShare(t *testing.T) {
	r := newReport()
	r.Items, r.NotCompleted, r.AlreadyCovered, r.NoRepoReachable, r.Fallback, r.Unmatched, r.Unchanged, r.Rows = 20, 1, 5, 2, 4, 1, 3, 8
	r.NoRepoReasons[reasonNoLink] = 2
	r.ByTier["native"] = 6
	joined := strings.Join(r.Lines(), "\n")
	for _, want := range []string{
		"items read: 20", "skipped, not completed on the classified day: 1", "skipped: covered by a canonical unit: 5",
		"skipped, no repository reachable: 2", "no repository reachable: no link: 2", "no repository reachable: all links churn 0: 0", "skipped: covered by a canonical unit: 5", "unmatched (product/general, no estimate rule): 1",
		"already stored, not rewritten: 3", "rows (normalized): 8", "by tier: native = 6",
		"product/general items (estimated): 4", "unmatched share of product/general: 25.0%",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("the report lacks the row %q:\n%s", want, joined)
		}
	}
	if got := r.Accounted(); got != 20 {
		t.Errorf("accounted = %d, want every item of the 20 counted once", got)
	}
	if got := strings.Join(newReport().Lines(), "\n"); !strings.Contains(got, "unmatched share of product/general: n/a") {
		t.Errorf("an empty window must say n/a, not divide by zero:\n%s", got)
	}
}

func TestAllocateReachesRepositoriesOnlyThroughTheEntityTree(t *testing.T) {
	a := uuid.MustParse("92270001-9227-4227-8227-922792279227")
	b := uuid.MustParse("92270002-9227-4227-8227-922792279227")
	links := map[string][]issueLink{
		"i-split":  {{repo: a, number: 1, tier: "heuristic", churn: 30}, {repo: b, number: 2, tier: "native", churn: 10}, {repo: a, number: 3, tier: "explicit_text", churn: 10}},
		"i-nochur": {{repo: a, number: 4, tier: "explicit_text", churn: 0}},
	}
	// A pull request / merge request item keeps its own repository, with its own tier.
	for _, kind := range []string{"pr", "merge_request"} {
		got, tier, reason := allocate(sourceRow{artifactID: "p", itemType: kind, ownRepo: &a}, links)
		if reason != "" || tier != tierOwnRepository || len(got) != 1 || got[0].repo != a || got[0].weight != 1 {
			t.Errorf("%s item: %+v %q %q", kind, got, tier, reason)
		}
	}
	if _, _, reason := allocate(sourceRow{artifactID: "p", itemType: "pr"}, links); reason != reasonNoOwnRepository {
		t.Errorf("a pull request item without a repository: reason %q", reason)
	}
	// An issue's own repository column is NOT the relation.
	if _, _, reason := allocate(sourceRow{artifactID: "i-own", itemType: "task", ownRepo: &a}, links); reason != reasonNoLink {
		t.Errorf("an issue with an own repository and no link: reason %q, want %q", reason, reasonNoLink)
	}
	// Several repositories: the churn weighs them (40 in a, 10 in b), the strongest tier is named.
	got, tier, reason := allocate(sourceRow{artifactID: "i-split", itemType: "task"}, links)
	if reason != "" || tier != "native" || len(got) != 2 || got[0].repo != a || got[1].repo != b || got[0].weight != 0.8 || got[1].weight != 0.2 {
		t.Errorf("split: %+v %q %q", got, tier, reason)
	}
	if _, tier, reason := allocate(sourceRow{artifactID: "i-nochur", itemType: "task"}, links); reason != reasonNoChurn || tier != "explicit_text" {
		t.Errorf("no churn: tier %q reason %q", tier, reason)
	}
}

func TestRecordSplitsTheEffortSoTheRepositoriesSumToTheItem(t *testing.T) {
	a := uuid.MustParse("92270001-9227-4227-8227-922792279227")
	b := uuid.MustParse("92270002-9227-4227-8227-922792279227")
	c := uuid.MustParse("92270003-9227-4227-8227-922792279227")
	points := 7.0
	allocs := []allocation{{repo: a, weight: 1.0 / 3, source: "pr_churn"}, {repo: b, weight: 1.0 / 3, source: "pr_churn"}, {repo: c, weight: 1.0 / 3, source: "pr_churn"}}
	rec, efforts := record(Options{Org: "o", RunID: "r"}, sourceRow{artifactID: "x", provider: "linear", points: &points, title: "t"}, allocs, "native", "maintenance.debt", "maintenance", "cc:chore", time.Unix(0, 0))
	sum := 0.0
	for _, e := range efforts {
		sum += e.EffortValue
	}
	if len(efforts) != 3 || sum != 7 || rec.EffortValue != 7 {
		t.Errorf("repository efforts %v sum %v for an item of %v", efforts, sum, rec.EffortValue)
	}
	if rec.RepoID != nil {
		t.Errorf("a unit over three repositories names repository %v, want none", rec.RepoID)
	}
	one, _ := record(Options{Org: "o", RunID: "r"}, sourceRow{artifactID: "x", provider: "linear", title: "t"}, allocs[:1], "native", "maintenance.debt", "maintenance", "cc:chore", time.Unix(0, 0))
	if one.RepoID == nil || *one.RepoID != a {
		t.Errorf("a unit over one repository names %v, want %v", one.RepoID, a)
	}
	// The input hash moves with the repositories: a changed split is a new row, an identical one is not.
	again, _ := record(Options{Org: "o", RunID: "other-run"}, sourceRow{artifactID: "x", provider: "linear", points: &points, title: "t"}, allocs, "native", "maintenance.debt", "maintenance", "cc:chore", time.Unix(99, 0))
	if again.CategorizationInputHash != rec.CategorizationInputHash {
		t.Errorf("the same input hashes differently across runs: %s vs %s", again.CategorizationInputHash, rec.CategorizationInputHash)
	}
	moved, _ := record(Options{Org: "o", RunID: "r"}, sourceRow{artifactID: "x", provider: "linear", points: &points, title: "t"}, allocs[:2], "native", "maintenance.debt", "maintenance", "cc:chore", time.Unix(0, 0))
	if moved.CategorizationInputHash == rec.CategorizationInputHash {
		t.Errorf("a changed split keeps the input hash")
	}
}

func TestRunIDForNamesTheWindowNotTheMoment(t *testing.T) {
	from, to := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	id := RunIDFor("org", from, to)
	if again := RunIDFor("org", from.Add(5*time.Hour), to.Add(7*time.Hour)); again != id {
		t.Errorf("the same days run as %q and %q", id, again)
	}
	if !strings.HasPrefix(id, Status+":") {
		t.Errorf("run id %q lacks the provenance prefix", id)
	}
	for name, other := range map[string]string{
		"another organization": RunIDFor("org2", from, to),
		"another first day":    RunIDFor("org", from.AddDate(0, 0, 1), to),
		"another last day":     RunIDFor("org", from, to.AddDate(0, 0, 1)),
	} {
		if other == id {
			t.Errorf("%s keeps the run id %q", name, id)
		}
	}
}
