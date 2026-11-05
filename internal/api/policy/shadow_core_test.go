package policy

import (
	"bytes"
	"context"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/pep"

	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

func catalogNamespaces() []string {
	seen := map[string]bool{}
	for _, action := range policycatalog.Default().Actions() {
		seen[strings.SplitN(action.ID, ".", 2)[0]] = true
	}
	var out []string
	for ns := range seen {
		out = append(out, ns)
	}
	sort.Strings(out)
	return out
}

var queryAPIActions = []string{"analytics.read", "git.read", "metrics.read", "work_items.read", "org.read", "reports.write"}

// 1g: the closed namespace set is the catalog's, sorted. A catalog namespace missing from it is red.
func TestShadowNamespaceSetIsTheCatalogsNamespaces(t *testing.T) {
	want := catalogNamespaces()
	if !slices.Equal(shadowNamespaces, want) {
		t.Fatalf("closed namespace set %v, catalog namespaces %v", shadowNamespaces, want)
	}
	for _, ns := range want {
		if !slices.Contains(shadowNamespaces, ns) {
			t.Errorf("catalog namespace %q is missing from the closed set", ns)
		}
	}
}

// 1a: a row for each query-api action is accepted by NewShadow.
func TestShadowAcceptsQueryAPIActionRows(t *testing.T) {
	rows := map[string]ShadowRow{}
	for _, id := range queryAPIActions {
		action, err := policycatalog.Default().Action(id)
		if err != nil {
			t.Fatal(err)
		}
		rows["Query."+id] = shadowRowOf(action)
	}
	if _, err := NewShadow(ShadowConfig{Decider: funcDecider{func(context.Context, pep.Request) pep.Decision { return pep.Decision{} }},
		Rows: rows, HighRisk: true}); err != nil {
		t.Fatalf("NewShadow refused the query-api rows: %v", err)
	}
}

// 1b: a mismatch on an analytics.read row counts namespace=analytics with the caller and class.
func TestShadowCountsAnAnalyticsMismatch(t *testing.T) {
	rec := recordShadow(t)
	rows := map[string]ShadowRow{"GET /api/v1/heatmap": {Action: "analytics.read", Namespace: "analytics", ResourceType: "organization"}}
	pdp := newTestPDP(t, denyWith("grant_absent"))
	sh, _ := newShadowFor(t, pdp.client(t), func(cfg *ShadowConfig) { cfg.Rows = rows })
	sh.Observe(context.Background(), ShadowObservation{Pattern: "GET /api/v1/heatmap", Bearer: shadowBearer, OrgID: ownOrg,
		Caller: ShadowCallerMember, UserAccessToken: true, LegacyAllowed: true})
	sh.wg.Wait()
	mismatch, results := rec.snapshot()
	if !slices.Equal(mismatch, []string{"analytics/member/unresolved"}) || !slices.Equal(results, []string{"mismatch"}) {
		t.Fatalf("mismatch %v results %v", mismatch, results)
	}
}

// 1c: a class row in a catalog namespace outside the old go-api set loads.
func TestShadowClassTableTakesAQueryAPINamespace(t *testing.T) {
	table := shadowClassHeader + "\nallow\tdeny\trole_lacks_action\tanalytics\tlegacy_bug\tfixture\n"
	if _, err := LoadShadowClasses([]byte(table)); err != nil {
		t.Fatalf("class table refused: %v", err)
	}
	bad := shadowClassHeader + "\nallow\tdeny\trole_lacks_action\tnot_a_namespace\tlegacy_bug\tfixture\n"
	if _, err := LoadShadowClasses([]byte(bad)); err == nil {
		t.Fatal("a namespace outside the catalog loaded")
	}
}

// 1d
func TestShadowRowForFollowsTheCatalog(t *testing.T) {
	for _, id := range append(slices.Clone(queryAPIActions), "teams.read", "admin.telemetry") {
		action, err := policycatalog.Default().Action(id)
		if err != nil {
			t.Fatal(err)
		}
		got, err := ShadowRowFor(id)
		if err != nil || got != shadowRowOf(action) {
			t.Errorf("%s: row %+v err %v, want %+v", id, got, err, shadowRowOf(action))
		}
	}
	if row, _ := ShadowRowFor("reports.write"); !row.HighRisk {
		t.Error("reports.write lost its high-risk flag")
	}
	if row, _ := ShadowRowFor("analytics.read"); row.Namespace != "analytics" || row.ResourceType != "organization" || row.HighRisk {
		t.Errorf("analytics.read row %+v", row)
	}
	for _, id := range []string{"analytics.nope", "", "acr.context.read"} {
		if _, err := ShadowRowFor(id); err == nil {
			t.Errorf("ShadowRowFor(%q) did not fail", id)
		}
	}
}

// 1e
func TestShadowCallerKinds(t *testing.T) {
	cases := []struct {
		holds bool
		role  string
		want  string
	}{
		{true, "member", ShadowCallerSuperuser}, {true, "owner", ShadowCallerSuperuser},
		{false, "owner", ShadowCallerAdmin}, {false, "admin", ShadowCallerAdmin},
		{false, "member", ShadowCallerMember}, {false, "", ShadowCallerMember}, {false, "operator", ShadowCallerMember},
	}
	for _, tc := range cases {
		if got := ShadowCaller(tc.holds, tc.role); got != tc.want {
			t.Errorf("ShadowCaller(%v,%q) = %q, want %q", tc.holds, tc.role, got, tc.want)
		}
	}
}

// 1f
func TestShadowFromEnvOnFromTheCaller(t *testing.T) {
	var logs bytes.Buffer
	deps := envDeps(&logs)
	deps.On = true
	state := recordState(t)
	if s := ShadowFromEnv(envLookup(nil), deps); s == nil {
		t.Fatal("On=true with AUTH_SHADOW_PDP unset built no shadow")
	}
	if len(*state) != 1 || (*state)[0] != (shadowState{true, false}) {
		t.Fatalf("state %v", *state)
	}
	if s := ShadowFromEnv(envLookup(map[string]string{EnvShadowPDP: "false", EnvShadowPDPSample: "25"}), deps); s == nil || s.percent != 25 {
		t.Fatalf("On=true must ignore AUTH_SHADOW_PDP, knobs still read: %+v", s)
	}

	calls := 0
	off := envDeps(&logs)
	inner := off.NewDecider
	off.NewDecider = func() (pep.Decider, error) { calls++; return inner() }
	if s := ShadowFromEnv(envLookup(nil), off); s != nil || calls != 0 {
		t.Fatalf("On=false and unset: shadow %v, decider builds %d", s, calls)
	}
	if s := ShadowFromEnv(envLookup(map[string]string{EnvShadowPDP: "true"}), off); s == nil || calls != 1 {
		t.Fatalf("On=false, AUTH_SHADOW_PDP=true must still build: %v %d", s, calls)
	}
	bad := envDeps(&logs)
	bad.On = true
	if s := ShadowFromEnv(envLookup(map[string]string{EnvShadowPDPSample: "0"}), bad); s != nil {
		t.Fatal("a bad knob with On=true built a shadow")
	}
}
