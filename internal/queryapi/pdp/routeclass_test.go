package pdp

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/pep"
	"github.com/full-chaos/dev-health-go/pep/pepfake"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
)

// CHAOS-13089 (PR-2a of CHAOS-10864, acp rulings Q1, Q5, Q6, Q9): the route-class registry is the one gate point of
// an allowlist decision.

var (
	narrowableClasses = []string{"heatmap", "investment", "investment_sunburst", "investment_explain", "drilldown_prs",
		"work_units", "work_unit_explain", "people_drilldown_prs", "drilldown_issues", "people_drilldown_issues", "flame", "home", "opportunities"}
	mixedClasses = []string{"quadrant", "sankey", "filteroptions", "people",
		"people_summary", "people_metric", "explain", "flame_aggregated"}
)

func restRows(t *testing.T) map[string]string {
	t.Helper()
	rows := map[string]string{}
	for profile, action := range New(Config{Mode: ModeOff}).actions {
		if strings.Contains(profile, " /api/v1/") {
			rows[profile] = action
		}
	}
	return rows
}

func TestRouteClassOfCoversExactlyTheRESTRows(t *testing.T) {
	rows := restRows(t)
	if len(rows) != 29 {
		t.Fatalf("%d REST rows with an action, want 29", len(rows))
	}
	for profile := range rows {
		if class, ok := RouteClassOf(profile); !ok || class == "" {
			t.Errorf("row %q has no route class", profile)
		}
	}
	for profile := range routeClassOf {
		if _, ok := rows[profile]; !ok {
			t.Errorf("route class entry %q is no REST row", profile)
		}
	}
	if len(routeClassOf) != len(rows) {
		t.Errorf("%d class entries for %d rows", len(routeClassOf), len(rows))
	}
	classes := map[string]bool{}
	for _, class := range routeClassOf {
		classes[class] = true
	}
	if len(classes) != 21 || len(RouteClassNames()) != 21 {
		t.Errorf("%d classes in the map, %d names, want 21", len(classes), len(RouteClassNames()))
	}
	for _, name := range RouteClassNames() {
		if !classes[name] {
			t.Errorf("class %q names no row", name)
		}
	}
}

// acp Q1: /api/v1/explain is M and /api/v1/heatmap is N. The N set is exactly the eight classes whose every response
// read is repository-keyed; every other class is M (Q6: a mixed route is whole-route M).
func TestRouteClassKindsArePinned(t *testing.T) {
	wantKind := map[string]string{}
	for _, class := range narrowableClasses {
		wantKind[class] = KindNarrowable
	}
	for _, class := range mixedClasses {
		wantKind[class] = KindMixed
	}
	names := RouteClassNames()
	if len(names) != len(wantKind) {
		t.Fatalf("%d classes, want %d", len(names), len(wantKind))
	}
	for _, class := range names {
		kind, ok := ClassKind(class)
		if !ok || kind != wantKind[class] {
			t.Errorf("class %s kind %q, want %q", class, kind, wantKind[class])
		}
	}
	if kind, _ := ClassKind("heatmap"); kind != KindNarrowable {
		t.Errorf("heatmap is %q, want N (Q1)", kind)
	}
	if kind, _ := ClassKind("explain"); kind != KindMixed {
		t.Errorf("explain is %q, want M (Q1)", kind)
	}
	for profile, want := range map[string]string{"GET /api/v1/heatmap": "heatmap", "GET /api/v1/explain": "explain", "POST /api/v1/explain": "explain",
		"POST /api/v1/work-units/{work_unit_id}/explain": "work_unit_explain", "GET /api/v1/people/{person_id}/drilldown/prs": "people_drilldown_prs"} {
		if class, _ := RouteClassOf(profile); class != want {
			t.Errorf("%s is class %q, want %q", profile, class, want)
		}
	}
}

func TestNarrowedClassesAreNarrowableOnly(t *testing.T) {
	for class := range allowlistNarrowed {
		if kind, ok := ClassKind(class); !ok || kind != KindNarrowable {
			t.Errorf("class %q is narrowed but is %q", class, kind)
		}
	}
	for profile, on := range allowlistCutOver {
		class, _ := RouteClassOf(profile)
		if on != allowlistNarrowed[class] {
			t.Errorf("cut-over of %s is %t, its class %s narrowed is %t", profile, on, class, allowlistNarrowed[class])
		}
	}
	want := 0
	for _, class := range routeClassOf {
		if allowlistNarrowed[class] {
			want++
		}
	}
	if len(allowlistCutOver) != want {
		t.Errorf("cut-over registry holds %d profiles, %d rows have a narrowed class", len(allowlistCutOver), want)
	}
}

func allowlistRule(action string, keys []string) pepfake.Rule {
	return pepfake.Rule{PrincipalReference: testBearer, Action: action, ResourceType: "organization", ResourceKey: seamOrg,
		OrganizationID: seamOrg, TenantScope: seamOrg, RepositoryAllowlist: keys}
}

func allClassGate(t *testing.T, rules ...pepfake.Rule) *Gate {
	t.Helper()
	_, cfg := newMeter(t)
	cfg.Mode, cfg.Decider, cfg.Switch = ModeEnforce, &countingDecider{Decider: pepfake.New(rules...)}, cells("analytics,metrics,git,work_items")
	return New(cfg)
}

// acp Q5: an allowlist caller on an M route is refused with the new closed reason, the class on the error (the
// log line carries it), and a refusal is a local 503.
func TestAllowlistOnAMixedRouteIsRefusedAsUnsupported(t *testing.T) {
	for profile, action := range restRows(t) {
		class, _ := RouteClassOf(profile)
		if kind, _ := ClassKind(class); kind != KindMixed {
			continue
		}
		for name, keys := range map[string][]string{"empty": {}, "one key": {"acme/a"}} {
			t.Run(profile+"/"+name, func(t *testing.T) {
				g := allClassGate(t, allowlistRule(action, keys))
				scope, err := g.Authorize(context.Background(), profile, testBearer, seamOrg)
				wantLocal(t, err, ReasonAllowlistRouteUnsupported)
				wantNoScope(t, scope)
				if denied, _ := err.(*DenyError); denied == nil || denied.Class != class {
					t.Fatalf("refusal class %#v, want %s", err, class)
				}
			})
		}
	}
}

// CHAOS-13090: the eight N classes narrow every read to a ceiling, so an allowlist decision is carried there and the
// scope is an Allowlist of the decision's keys.
func TestAllowlistOnANarrowableRouteIsCarried(t *testing.T) {
	seen := 0
	for profile, action := range restRows(t) {
		class, _ := RouteClassOf(profile)
		if kind, _ := ClassKind(class); kind != KindNarrowable {
			continue
		}
		seen++
		t.Run(profile, func(t *testing.T) {
			g := allClassGate(t, allowlistRule(action, []string{"acme/a"}))
			scope, err := g.Authorize(context.Background(), profile, testBearer, seamOrg)
			if err != nil || scope.Kind() != pepgate.KindAllowlist || !slices.Equal(scope.RepoKeys(), []string{"acme/a"}) {
				t.Fatalf("answered %v, %v, want an Allowlist scope of [acme/a]", scope.Kind(), err)
			}
		})
	}
	if seen != 19 {
		t.Fatalf("%d narrowable rows, want 19", seen)
	}
}

// The registry narrows exactly the N classes (the eight of CHAOS-13090 and the three item-grain classes of
// CHAOS-13085): no mixed class, no narrowable class left out.
func TestNarrowedClassesAreExactlyTheNarrowable(t *testing.T) {
	var narrowable, narrowed []string
	for _, class := range RouteClassNames() {
		if kind, _ := ClassKind(class); kind == KindNarrowable {
			narrowable = append(narrowable, class)
		}
		if AllowlistNarrowed(class) {
			narrowed = append(narrowed, class)
		}
	}
	if len(narrowable) != 13 || !slices.Equal(narrowable, narrowed) {
		t.Fatalf("narrowed %v, want the narrowable classes %v", narrowed, narrowable)
	}
}

// A narrowed class carries the allowlist (the registry derives the gate's copy), so the flip is one entry.
func TestNarrowedClassCarriesTheAllowlist(t *testing.T) {
	const profile = "GET /api/v1/drilldown/prs"
	g := allClassGate(t, allowlistRule("git.read", []string{"acme/a"}))
	g.carriesAllowlist = map[string]bool{profile: true}
	scope, err := g.Authorize(context.Background(), profile, testBearer, seamOrg)
	if err != nil || scope.Kind() != pepgate.KindAllowlist {
		t.Fatalf("narrowed class answered %v, %v", scope.Kind(), err)
	}
}

// R2: an Organization decision, or an allowlist holding "*", passes on every class: the registry only gates a ceiling.
func TestOrganizationScopePassesEveryClass(t *testing.T) {
	for profile, action := range restRows(t) {
		for name, edit := range map[string]func(*pepfake.Rule){
			"organization": func(r *pepfake.Rule) { r.RepositoryScope = "organization" },
			"star":         func(r *pepfake.Rule) { r.RepositoryAllowlist = []string{"*"} },
		} {
			t.Run(profile+"/"+name, func(t *testing.T) {
				rule := allowlistRule(action, nil)
				edit(&rule)
				scope, err := allClassGate(t, rule).Authorize(context.Background(), profile, testBearer, seamOrg)
				if err != nil || scope.Kind() != pepgate.KindOrganization {
					t.Fatalf("answered %v, %v, want an Organization scope", scope.Kind(), err)
				}
			})
		}
	}
}

// D6705: no redaction code. A redaction, minimum_assurance, step_up or unknown obligation stays unknown_obligation
// on every class, N or M (the obligation is refused before the repository member is read).
func TestObligationsStayRefusedOnEveryClass(t *testing.T) {
	for profile, action := range restRows(t) {
		for name, edit := range map[string]func(*pepfake.Rule){
			"redaction":         func(r *pepfake.Rule) { r.Redaction = []pep.RedactionClass{pep.RedactFreeText} },
			"minimum_assurance": func(r *pepfake.Rule) { r.MinimumAssurance = pep.AssuranceAAL2 },
			"step_up":           func(r *pepfake.Rule) { r.StepUp = true },
		} {
			t.Run(profile+"/"+name, func(t *testing.T) {
				rule := allowlistRule(action, nil)
				rule.RepositoryScope = "organization"
				edit(&rule)
				scope, err := allClassGate(t, rule).Authorize(context.Background(), profile, testBearer, seamOrg)
				wantLocal(t, err, ReasonNotCarriedOut)
				wantNoScope(t, scope)
			})
		}
	}
}

// acp Q5: the closed set of local reasons. A reason added or renamed fails here and in the docs.
func TestLocalReasonsAreAClosedSet(t *testing.T) {
	want := []string{"allowlist_route_unsupported", "authz_enforce_cell_off", "authz_enforce_no_decision_point",
		"pdp_unknown_obligation", "repository_scope_absent", "tenant_scope_mismatch"}
	var got []string
	for _, reason := range LocalReasons() {
		got = append(got, string(reason))
	}
	sort.Strings(got)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("local reasons %v, want %v", got, want)
	}
	file, err := parser.ParseFile(token.NewFileSet(), "pdp.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	declared := 0
	ast.Inspect(file, func(n ast.Node) bool {
		if spec, ok := n.(*ast.ValueSpec); ok && len(spec.Names) == 1 && strings.HasPrefix(spec.Names[0].Name, "Reason") {
			declared++
		}
		return true
	})
	routeclass, err := os.ReadFile("routeclass.go")
	if err != nil {
		t.Fatal(err)
	}
	if extra := strings.Count(string(routeclass), "pep.Reason ="); extra != 1 {
		t.Fatalf("routeclass.go declares %d reasons, want 1", extra)
	}
	if declared != len(want)-1 {
		t.Fatalf("pdp.go declares %d reasons, want %d (the 6th is in routeclass.go)", declared, len(want)-1)
	}
	docs, err := os.ReadFile("../../../docs/reference/auth/configuration.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, reason := range want {
		if !strings.Contains(string(docs), "`"+reason+"`") {
			t.Errorf("docs/reference/auth/configuration.md does not name `%s`", reason)
		}
	}
}

func TestMixedRefusalReasonIsNotUnknownObligation(t *testing.T) {
	if ReasonAllowlistRouteUnsupported == ReasonNotCarriedOut || string(ReasonAllowlistRouteUnsupported) != "allowlist_route_unsupported" {
		t.Fatalf("reason %q", ReasonAllowlistRouteUnsupported)
	}
}
