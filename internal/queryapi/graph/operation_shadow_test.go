package graph

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/ast"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
)

// CHAOS-13074: one OperationShadow tells the process's shadow what the legacy check decided for each root field.

const (
	shOwn   = "org-own"
	shOther = "org-other"
	shToken = "aaaa.bbbb.cccc-dddd-eeee-ffff"
)

type fakeSink struct {
	mu           sync.Mutex
	observed     []policy.ShadowObservation
	skipped      []string
	panicObserve bool
}

func (s *fakeSink) Observe(_ context.Context, o policy.ShadowObservation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.panicObserve {
		panic("observe boom")
	}
	s.observed = append(s.observed, o)
}

func (s *fakeSink) CountSkipped(_ context.Context, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.skipped = append(s.skipped, reason)
}

var userSubject = ShadowSubject{Bearer: shToken, UserAccessToken: true}

type shadowRun struct {
	sink    *fakeSink
	ran     int
	inner   context.Context
	outer   context.Context
	handled *graphql.Response
}

func runShadow(t *testing.T, document string, variables map[string]any, claims *authctx.Claims, subject *ShadowSubject, sink *fakeSink) *shadowRun {
	t.Helper()
	run := &shadowRun{sink: sink}
	ctx := context.Background()
	if claims != nil {
		ctx = authctx.WithClaims(ctx, *claims)
	}
	if subject != nil {
		ctx = WithShadowSubject(ctx, *subject)
	}
	ctx = graphql.WithOperationContext(ctx, guardOperation(t, document, variables))
	run.outer = ctx
	interceptor := OperationShadow{Sink: func(context.Context) ShadowSink {
		if sink == nil {
			return nil
		}
		return sink
	}}
	response := interceptor.InterceptOperation(ctx, func(inner context.Context) graphql.ResponseHandler {
		run.ran++
		run.inner = inner
		return func(context.Context) *graphql.Response { return &graphql.Response{Data: []byte(`{"ok":true}`)} }
	})
	run.handled = response(ctx)
	return run
}

func (r *shadowRun) one(t *testing.T) policy.ShadowObservation {
	t.Helper()
	if len(r.sink.observed) != 1 || len(r.sink.skipped) != 0 {
		t.Fatalf("observed %+v skipped %v, want exactly one observe", r.sink.observed, r.sink.skipped)
	}
	return r.sink.observed[0]
}

func rootDoc(key string, org string) (document string, variables map[string]any) {
	kind, name, _ := strings.Cut(key, ".")
	keyword, root := "query", parsedSchema.Query
	if kind == "Mutation" {
		keyword, root = "mutation", parsedSchema.Mutation
	}
	call := name
	if field := root.Fields.ForName(name); field != nil && field.Arguments.ForName("orgId") != nil && org != "" {
		call = name + "(orgId: $o)"
		return keyword + " Q($o: String) { " + call + " }", map[string]any{"o": org}
	}
	return keyword + " Q { " + call + " }", nil
}

func memberClaims() *authctx.Claims    { return &authctx.Claims{OrgID: shOwn} }
func adminClaims() *authctx.Claims     { return &authctx.Claims{OrgID: shOwn, Role: "admin"} }
func superuserClaims() *authctx.Claims { return &authctx.Claims{OrgID: shOwn, IsSuperuser: true} }

// C1: the legacy verdict of a field is the guard's AND the field's own pre-resolver check.
func TestOperationShadow_PerFieldVerdictIsGuardAndPrecheck(t *testing.T) {
	for _, tc := range []struct {
		name   string
		key    string
		org    string
		claims *authctx.Claims
		want   bool
	}{
		{"dataHealth member", "Query.dataHealth", "", memberClaims(), false},
		{"dataHealth admin role", "Query.dataHealth", "", adminClaims(), true},
		{"dataHealth superuser", "Query.dataHealth", "", superuserClaims(), true},
		{"telemetry platform dashboard member", "Query.productTelemetryPlatformDashboard", "", memberClaims(), false},
		{"telemetry platform dashboard superuser", "Query.productTelemetryPlatformDashboard", "", superuserClaims(), true},
		{"analytics field member own org", "Query.home", shOwn, memberClaims(), true},
		{"analytics field member other org (guard refuses)", "Query.home", shOther, memberClaims(), false},
		{"mutation own org", "Mutation.createSavedReport", shOwn, memberClaims(), true},
		{"mutation other org (guard refuses)", "Mutation.deleteSavedReport", shOther, memberClaims(), false},
		{"mutation of a superuser naming another org (rebound, runs)", "Mutation.triggerReport", shOther, superuserClaims(), true},
	} {
		document, variables := rootDoc(tc.key, tc.org)
		run := runShadow(t, document, variables, tc.claims, &userSubject, &fakeSink{})
		got := run.one(t)
		if got.LegacyAllowed != tc.want || got.Pattern != tc.key {
			t.Errorf("%s: observed %+v, want pattern %s legacy allowed %v", tc.name, got, tc.key, tc.want)
		}
	}
}

// C4: a root outside the table counts one skipped with its own reason and asks no PDP.
func TestOperationShadow_UnmappedRootCountsSkipped(t *testing.T) {
	sink := &fakeSink{}
	runShadow(t, `query Q { somethingNew }`, nil, memberClaims(), &userSubject, sink)
	if len(sink.observed) != 0 || !slices.Equal(sink.skipped, []string{policy.ShadowReasonUnmapped}) {
		t.Fatalf("observed %+v skipped %v, want one skipped unmapped", sink.observed, sink.skipped)
	}
	sink = &fakeSink{}
	runShadow(t, `query Q { somethingNew home }`, nil, memberClaims(), &userSubject, sink)
	if len(sink.observed) != 1 || !slices.Equal(sink.skipped, []string{policy.ShadowReasonUnmapped}) {
		t.Fatalf("a mapped root beside it: observed %d skipped %v", len(sink.observed), sink.skipped)
	}
}

// C5: introspection roots are silent: no call, no count.
func TestOperationShadow_IntrospectionSilent(t *testing.T) {
	for _, document := range []string{`query Q { __typename }`, `query Q { __schema { types { name } } }`, `query Q { __type(name: "Query") { name } }`} {
		sink := &fakeSink{}
		run := runShadow(t, document, nil, memberClaims(), &userSubject, sink)
		if len(sink.observed) != 0 || len(sink.skipped) != 0 || run.ran != 1 {
			t.Errorf("%s: observed %d skipped %v ran %d, want silence and one run", document, len(sink.observed), sink.skipped, run.ran)
		}
	}
}

// C6: the resource organization is the named orgId, whoever the caller is.
func TestOperationShadow_ResourceOrgIsNamedOrgId(t *testing.T) {
	document, variables := rootDoc("Query.home", shOther)
	if got := runShadow(t, document, variables, memberClaims(), &userSubject, &fakeSink{}).one(t); got.OrgID != shOther || got.LegacyAllowed {
		t.Errorf("member naming another org: %+v, want org %s and a legacy deny", got, shOther)
	}
	if got := runShadow(t, document, variables, superuserClaims(), &userSubject, &fakeSink{}).one(t); got.OrgID != shOther || !got.LegacyAllowed {
		t.Errorf("superuser naming another org: %+v, want org %s and a legacy allow", got, shOther)
	}
	document, variables = rootDoc("Query.dataHealth", "")
	if got := runShadow(t, document, variables, adminClaims(), &userSubject, &fakeSink{}).one(t); got.OrgID != shOwn {
		t.Errorf("no orgId named: %+v, want the caller's org", got)
	}
}

// C7: every one of the 52 root fields, alone and legacy-allowed, is observed once with its own key.
func TestOperationShadow_EveryLegacyAllowMappedFieldObserved(t *testing.T) {
	table := FieldActionTable()
	if len(table) != 52 {
		t.Fatalf("the field action table has %d roots, want 52", len(table))
	}
	for key := range table {
		document, variables := rootDoc(key, shOwn)
		got := runShadow(t, document, variables, superuserClaims(), &userSubject, &fakeSink{}).one(t)
		if got.Pattern != key || !got.LegacyAllowed || got.Bearer != shToken || !got.UserAccessToken {
			t.Errorf("%s: observed %+v", key, got)
		}
	}
}

// C8: a legacy deny reaches the shadow as a deny.
func TestOperationShadow_LegacyDenyObservedAsDeny(t *testing.T) {
	document, variables := rootDoc("Query.savedReports", shOther)
	got := runShadow(t, document, variables, memberClaims(), &userSubject, &fakeSink{}).one(t)
	if got.LegacyAllowed || got.OrgID != shOther {
		t.Fatalf("observed %+v, want a deny for %s", got, shOther)
	}
}

// Q4: the dedupe key is (action, resource org, verdict); two orgs of one action are two observes; over seven the rest
// count skipped with the reason cap.
func TestOperationShadow_DedupeKeyHasTheResourceOrgAndCapsAtSeven(t *testing.T) {
	sink := &fakeSink{}
	roots := []shadowRoot{
		{key: "Query.home", action: "analytics.read", org: "a", allowed: true},
		{key: "Query.catalog", action: "analytics.read", org: "a", allowed: true},
		{key: "Query.savedReport", action: "analytics.read", org: "b", allowed: true},
		{key: "Query.reportRuns", action: "analytics.read", org: "b", allowed: false},
		{key: "Query.pr", action: "git.read", org: "a", allowed: true},
	}
	observeRoots(context.Background(), sink, policy.ShadowObservation{Bearer: shToken, UserAccessToken: true}, roots)
	var got []string
	for _, o := range sink.observed {
		got = append(got, fmt.Sprintf("%s/%s/%v", o.Pattern, o.OrgID, o.LegacyAllowed))
	}
	want := []string{"Query.home/a/true", "Query.savedReport/b/true", "Query.reportRuns/b/false", "Query.pr/a/true"}
	if !slices.Equal(got, want) || len(sink.skipped) != 0 {
		t.Fatalf("observed %v skipped %v, want %v and none skipped", got, sink.skipped, want)
	}

	sink = &fakeSink{}
	var many []shadowRoot
	for i := range 10 {
		many = append(many, shadowRoot{key: "Query.home", action: "analytics.read", org: fmt.Sprintf("org-%d", i), allowed: true})
	}
	observeRoots(context.Background(), sink, policy.ShadowObservation{Bearer: shToken, UserAccessToken: true}, many)
	if len(sink.observed) != 7 || !slices.Equal(sink.skipped, []string{"cap", "cap", "cap"}) {
		t.Fatalf("observed %d skipped %v, want 7 observes and three skipped cap", len(sink.observed), sink.skipped)
	}
}

// C9: one op over every query root observes each distinct action once, never more than seven, and the first field of
// an action names the row.
func TestOperationShadow_CapAtSevenPerOp(t *testing.T) {
	var names []string
	for key := range FieldActionTable() {
		if name, ok := strings.CutPrefix(key, "Query."); ok {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	sink := &fakeSink{}
	runShadow(t, `query Q { `+strings.Join(names, " ")+` }`, nil, superuserClaims(), &userSubject, sink)
	if len(sink.observed) > 7 || len(sink.skipped) != 0 {
		t.Fatalf("observed %d skipped %v", len(sink.observed), sink.skipped)
	}
	actions := map[string]bool{}
	for _, o := range sink.observed {
		row, err := policy.ShadowRowFor(FieldActionTable()[o.Pattern])
		if err != nil || actions[row.Action] {
			t.Fatalf("%s: row %+v err %v, action seen twice or invalid", o.Pattern, row, err)
		}
		actions[row.Action] = true
	}
	if len(actions) != 6 {
		t.Fatalf("%d distinct query actions observed, want 6 (analytics, metrics, git, work_items, integrations, admin.telemetry)", len(actions))
	}
}

// Q3 and the shape rules: no user bearer (an MCP op), a credential class the PDP never decides, and an op the guard
// refuses for its shape each count ONE skipped for the op and ask nothing.
func TestOperationShadow_SkippedOncePerOpForNoBearerNonUserTokenAndOpShape(t *testing.T) {
	document, variables := rootDoc("Query.home", shOwn)
	for _, tc := range []struct {
		name    string
		subject *ShadowSubject
		doc     string
		vars    map[string]any
		want    string
	}{
		{"no subject", nil, document, variables, "no_user_bearer"},
		{"empty bearer", &ShadowSubject{}, document, variables, "no_user_bearer"},
		{"hs256 token", &ShadowSubject{Bearer: shToken, SkipReason: policy.ShadowReasonHS256}, document, variables, "hs256"},
		{"padded orgId", &userSubject, document, map[string]any{"o": " " + shOwn}, "op_refused"},
		{"empty orgId", &userSubject, document, map[string]any{"o": ""}, "op_refused"},
		{"two orgs", &userSubject, `query Q($a: String, $b: String) { home(orgId: $a) catalog(orgId: $b) }`, map[string]any{"a": shOwn, "b": shOther}, "op_refused"},
	} {
		sink := &fakeSink{}
		run := runShadow(t, tc.doc, tc.vars, memberClaims(), tc.subject, sink)
		if len(sink.observed) != 0 || !slices.Equal(sink.skipped, []string{tc.want}) || run.ran != 1 {
			t.Errorf("%s: observed %d skipped %v ran %d, want one skipped %s", tc.name, len(sink.observed), sink.skipped, run.ran, tc.want)
		}
	}
}

// Q5: a delegated session's mutation is a legacy deny and is told as impersonating (the core skips it).
func TestOperationShadow_DelegatedMutationIsImpersonating(t *testing.T) {
	document, variables := rootDoc("Mutation.createSavedReport", shOwn)
	got := runShadow(t, document, variables, &authctx.Claims{OrgID: shOwn, Delegated: true}, &userSubject, &fakeSink{}).one(t)
	if !got.Impersonating || got.LegacyAllowed {
		t.Fatalf("observed %+v, want impersonating and a legacy deny", got)
	}
}

// Q7: the platform dashboard has no org of its own: an org-less platform caller holding the action is allowed.
func TestOperationShadow_PlatformDashboardOfAnOrglessCaller(t *testing.T) {
	claims := &authctx.Claims{Platform: orglessSet(rule.ActionAdminTelemetry)}
	got := runShadow(t, orglessDashboard, map[string]any{"input": map[string]any{}}, claims, &userSubject, &fakeSink{}).one(t)
	if got.Pattern != "Query.productTelemetryPlatformDashboard" || got.OrgID != "" || !got.LegacyAllowed {
		t.Fatalf("observed %+v", got)
	}
}

// Q6: the bearer is on the context, never read from a header; no subject, nothing asked.
func TestOperationShadow_BearerComesFromTheContextSubject(t *testing.T) {
	document, variables := rootDoc("Query.home", shOwn)
	sink := &fakeSink{}
	runShadow(t, document, variables, memberClaims(), &userSubject, sink)
	if got := sink.one0(); got != shToken {
		t.Fatalf("bearer %q, want the context subject's", got)
	}
	sink = &fakeSink{}
	runShadow(t, document, variables, memberClaims(), nil, sink)
	if len(sink.observed) != 0 {
		t.Fatalf("an observe without a subject: %+v", sink.observed)
	}
	subject, ok := ShadowSubjectFrom(WithShadowSubject(context.Background(), userSubject))
	if !ok || subject != userSubject {
		t.Fatalf("subject round trip: %+v %v", subject, ok)
	}
	if _, ok := ShadowSubjectFrom(context.Background()); ok {
		t.Fatal("a subject out of nothing")
	}
}

func (s *fakeSink) one0() string {
	if len(s.observed) != 1 {
		return fmt.Sprintf("%d observes", len(s.observed))
	}
	return s.observed[0].Bearer
}

// C11: the operation runs once, with the same context, the same response, whatever the shadow does.
func TestOperationShadow_ResolverResultUnchanged(t *testing.T) {
	document, variables := rootDoc("Query.home", shOther)
	claims := superuserClaims()
	baseline := runShadow(t, document, variables, claims, &userSubject, nil)
	for name, sink := range map[string]*fakeSink{"shadow on": {}, "observe panics": {panicObserve: true}} {
		run := runShadow(t, document, variables, claims, &userSubject, sink)
		if run.ran != 1 || run.inner != run.outer {
			t.Errorf("%s: ran %d, context changed %v", name, run.ran, run.inner != run.outer)
		}
		if string(run.handled.Data) != string(baseline.handled.Data) || len(run.handled.Errors) != 0 {
			t.Errorf("%s: response %+v differs from %+v", name, run.handled, baseline.handled)
		}
	}
	ctx := graphql.WithOperationContext(authctx.WithClaims(context.Background(), *claims), guardOperation(t, document, variables))
	panicking := OperationShadow{Sink: func(context.Context) ShadowSink { panic("sink boom") }}
	ran := 0
	panicking.InterceptOperation(ctx, func(context.Context) graphql.ResponseHandler {
		ran++
		return func(context.Context) *graphql.Response { return &graphql.Response{} }
	})(ctx)
	if ran != 1 {
		t.Errorf("a panicking sink lookup ran the operation %d times", ran)
	}
	if baseline.ran != 1 {
		t.Errorf("shadow off ran %d times", baseline.ran)
	}
}

// C12: the table behind the rows is the 52 roots, each a catalog row the shadow accepts, each query or mutation of the schema.
func TestOperationShadow_RootTableIs51(t *testing.T) {
	table := FieldActionTable()
	if len(table) != 52 {
		t.Fatalf("%d roots, want 52", len(table))
	}
	for key, action := range table {
		if _, err := policy.ShadowRowFor(action); err != nil {
			t.Errorf("%s: %v", key, err)
		}
		kind, name, _ := strings.Cut(key, ".")
		root := map[string]*ast.Definition{"Query": parsedSchema.Query, "Mutation": parsedSchema.Mutation}[kind]
		if root == nil || root.Fields.ForName(name) == nil {
			t.Errorf("%s is not a root field of the schema", key)
		}
	}
	for key := range fieldPrechecks {
		if _, ok := table[key]; !ok {
			t.Errorf("pre-check %s is not a root of the table", key)
		}
	}
}

// A field orgId C while the scope is B: a non-impersonating holder of admin.orgs is rebound to C and served C, and the
// shadow judges C with the legacy answer allowed; anyone else gets "Access denied" and the shadow judges C with the
// legacy answer refused. It never judges B while serving C.
func TestOperationShadow_FieldOrgNotTheScopeIsJudgedAsTheFieldOrg(t *testing.T) {
	for _, tc := range []struct {
		name   string
		claims *authctx.Claims
		want   bool
	}{
		{"holder of admin.orgs is rebound to C and served C", superuserClaims(), true},
		{"member is refused", memberClaims(), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			document, variables := rootDoc("Mutation.triggerReport", shOther)
			run := runShadow(t, document, variables, tc.claims, &userSubject, &fakeSink{})
			got := run.one(t)
			if got.OrgID != shOther || got.LegacyAllowed != tc.want {
				t.Fatalf("observed %+v, want org %s (never the scope %s) legacy allowed %v", got, shOther, shOwn, tc.want)
			}
		})
	}
}
