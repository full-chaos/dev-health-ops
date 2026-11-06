package policy

import (
	"context"
	"io"
	"maps"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-go/pep"
	"github.com/full-chaos/dev-health-go/pep/pepfake"
	"github.com/golang-jwt/jwt/v5"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

// countingDecider records every question it is asked and answers from inner.
type countingDecider struct {
	inner   pep.Decider
	mu      sync.Mutex
	singles []pep.Request
	batches [][]pep.Request
}

func (c *countingDecider) Decide(ctx context.Context, req pep.Request) pep.Decision {
	c.mu.Lock()
	c.singles = append(c.singles, req)
	c.mu.Unlock()
	return c.inner.Decide(ctx, req)
}

func (c *countingDecider) DecideBatch(ctx context.Context, reqs []pep.Request) []pep.Decision {
	c.mu.Lock()
	c.batches = append(c.batches, append([]pep.Request(nil), reqs...))
	c.mu.Unlock()
	return c.inner.DecideBatch(ctx, reqs)
}

func (c *countingDecider) counts() (singles, batches int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.singles), len(c.batches)
}

// denyingFake allows what its rules allow and answers every other request with the decision point's deny. pepfake
// alone answers an unmatched request with a local no_decision, which go-api must treat as no verdict (503), so a deny
// cell built on it would pass through the no-verdict path.
type denyingFake struct {
	rules *pepfake.Fake
	deny  pep.Decision
}

func newDenyingFake(t *testing.T, rules ...pepfake.Rule) denyingFake {
	t.Helper()
	deny := newTestPDP(t, denyWith("platform_role_absent")).client(t).Decide(context.Background(),
		pep.Request{PrincipalReference: shadowBearer, Action: rule.ActionAdminOrgs, ResourceType: "platform", ResourceKey: shadowPlatformKey})
	if deny.Allowed() || deny.Local() {
		t.Fatalf("the test decision point's deny: allowed=%v local=%v reason=%s", deny.Allowed(), deny.Local(), deny.Reason())
	}
	return denyingFake{rules: pepfake.New(rules...), deny: deny}
}

func (f denyingFake) Decide(ctx context.Context, req pep.Request) pep.Decision {
	if d := f.rules.Decide(ctx, req); d.Allowed() || d.Reason() != pep.ReasonNoDecision {
		return d
	}
	return f.deny
}

func (f denyingFake) DecideBatch(ctx context.Context, reqs []pep.Request) []pep.Decision {
	out := make([]pep.Decision, len(reqs))
	for i, req := range reqs {
		out[i] = f.Decide(ctx, req)
	}
	return out
}

type panicDecider struct{}

func (panicDecider) Decide(context.Context, pep.Request) pep.Decision { panic("pdp") }
func (panicDecider) DecideBatch(context.Context, []pep.Request) []pep.Decision {
	panic("pdp")
}

// platformRule allows the cell bearer one platform action.
func platformRule(action string) pepfake.Rule {
	return pepfake.Rule{PrincipalReference: shadowBearer, Action: action, ResourceType: "platform", ResourceKey: shadowPlatformKey}
}

// orgAdminRule allows the cell bearer members.manage in its token organization, with the tenant scope a real allow
// carries.
func orgAdminRule() pepfake.Rule {
	return pepfake.Rule{PrincipalReference: shadowBearer, Action: adminOrOrgAction, ResourceType: "organization", ResourceKey: ownOrg,
		OrganizationID: ownOrg, TenantScope: ownOrg}
}

// adminUsersRows is the production action map with the /admin/users rows naming admin.users, as the profile rows
// of PR-2 will.
func adminUsersRows(t *testing.T) map[string]ShadowRow {
	t.Helper()
	rows := maps.Clone(productionGoAPIRows(t))
	for pattern, level := range platformAdminGapRoutes {
		if level == Admin {
			rows[pattern] = ShadowRow{Action: rule.ActionAdminUsers, Namespace: "admin", ResourceType: "platform", HighRisk: true}
		}
	}
	return rows
}

func seamEnforcer(t *testing.T, decider pep.Decider, rows map[string]ShadowRow, namespaces ...string) *Enforcer {
	t.Helper()
	enf, err := NewEnforcer(EnforceConfig{Decider: decider, Rows: rows, Switch: onFor(namespaces...), Logger: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	return enf
}

// seamEcho answers what HoldsPlatform says for action, twice, so a second ask in one request is seen.
func seamEcho(action string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user := UserFrom(r.Context())
		first, err := HoldsPlatform(r.Context(), user, action)
		if err != nil {
			WritePlatformDecisionRefusal(w, err)
			return
		}
		second, err := HoldsPlatform(r.Context(), user, action)
		if err != nil || second != first {
			http.Error(w, "the second ask disagreed", http.StatusInternalServerError)
			return
		}
		_, _ = io.WriteString(w, "platform="+strconv.FormatBool(first)+" bypass="+strconv.FormatBool(PlatformBypassesOrg(r.Context(), user, rule.ActionAdminOrgs)))
	})
}

func seamMux(guard *Guard) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("GET /api/v1/admin/users", guard.WrapAdminOr(rule.ActionAdminUsers, seamEcho(rule.ActionAdminUsers)))
	mux.Handle("GET /api/v1/admin/orgs", guard.WrapPlatform(rule.ActionAdminOrgs, seamEcho(rule.ActionAdminOrgs)))
	mux.Handle("POST /api/v1/telemetry/report", guard.Wrap(Authenticated, seamEcho(rule.ActionAdminTelemetry)))
	mux.Handle("GET /api/v1/admin/impersonate/status", guard.Wrap(Authenticated, seamEcho(rule.ActionAdminImpersonate)))
	// An admin-or row whose handler asks no platform question (the X-Org-Id paths of the users handlers).
	mux.Handle("GET /api/v1/admin/users/{user_id}", guard.WrapAdminOr(rule.ActionAdminUsers, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "served")
	})))
	// A handler that asks only whether a platform action stands in for organization power.
	mux.Handle("GET /api/v1/admin/seam/bypass", guard.Wrap(Authenticated, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "bypass="+strconv.FormatBool(PlatformBypassesOrg(r.Context(), UserFrom(r.Context()), rule.ActionAdminOrgs)))
	})))
	return mux
}

func seamHandler(t *testing.T, store *accessStore, claims *RoutedResult, enf *Enforcer) http.Handler {
	t.Helper()
	guard := NewGuard(routed(t, store, &fakeValidator{result: claims}), quiet())
	guard.SetEnforcer(enf)
	return seamMux(guard)
}

func seamServe(h http.Handler, method, path string) response {
	return serve(h, method, path, cellBearer())
}

// An admin-or row asks one batch: the platform action and members.manage in the token's organization. Either allow
// passes; the handler's platform answer is the platform question's, never the organization one's.
func TestAdminOrRowsAskOneBatchAndKeepThePlatformAnswer(t *testing.T) {
	for _, c := range []struct {
		name      string
		rules     []pepfake.Rule
		superuser bool
		role      string
		claims    *RoutedResult
		status    int
		body      string
		batchSize int
	}{
		{"platform holder, org member", []pepfake.Rule{platformRule(rule.ActionAdminUsers)}, false, "member", userClaims(nil), 200, "platform=true bypass=false", 2},
		{"org admin, no platform action", []pepfake.Rule{orgAdminRule()}, true, "admin", userClaims(nil), 200, "platform=false bypass=false", 2},
		{"both", []pepfake.Rule{platformRule(rule.ActionAdminUsers), orgAdminRule()}, false, "admin", userClaims(nil), 200, "platform=true bypass=false", 2},
		{"neither, legacy superuser", nil, true, "admin", userClaims(nil), 403, `{"detail":"Admin access required"}`, 2},
		{"org-less platform holder", []pepfake.Rule{platformRule(rule.ActionAdminUsers)}, true, "", orglessAccessClaims(), 200, "platform=true bypass=false", 1},
		{"org-less, no platform action", nil, true, "", orglessAccessClaims(), 403, `{"detail":"Admin access required"}`, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			rec := recordEnforce(t)
			decider := &countingDecider{inner: newDenyingFake(t, c.rules...)}
			got := seamServe(seamHandler(t, legacyStore(c.superuser, c.role), c.claims, seamEnforcer(t, decider, adminUsersRows(t), "admin")), http.MethodGet, "/api/v1/admin/users")
			if got.status != c.status || !strings.Contains(got.body, c.body) {
				t.Fatalf("%d %q, want %d %q", got.status, got.body, c.status, c.body)
			}
			singles, batches := decider.counts()
			if batches != 1 || singles != 0 || len(decider.batches[0]) != c.batchSize {
				t.Fatalf("%d single and %d batch calls (%v), want one batch of %d and no single call: the handler reuses the route's platform answer", singles, batches, decider.batches, c.batchSize)
			}
			if q := decider.batches[0][0]; q.Action != rule.ActionAdminUsers || q.ResourceType != "platform" || q.OrganizationID != "" {
				t.Fatalf("first question %+v, want the platform question for admin.users", q)
			}
			if c.batchSize == 2 {
				if q := decider.batches[0][1]; q.Action != adminOrOrgAction || q.ResourceType != "organization" || q.OrganizationID != ownOrg || q.ResourceKey != ownOrg {
					t.Fatalf("second question %+v, want members.manage in the token organization", q)
				}
			}
			want := "admin/allow"
			if c.status != 200 {
				want = "admin/deny"
			}
			if snap := rec.snapshot(); len(snap) != 1 || snap[0] != want {
				t.Fatalf("counted %v, want [%s]", snap, want)
			}
		})
	}
}

// An admin-or row with no verdict is refused with a 503: a panicking decision point, a local decision, and an allow
// whose obligation go-api does not carry out (a repository scope on a row with a repository dimension), even when the
// other question of the batch is a plain deny.
func TestAdminOrRowsFailClosedWithoutAVerdict(t *testing.T) {
	scoped := orgAdminRule()
	scoped.RepositoryScope = "organization"
	bad := platformRule(rule.ActionAdminUsers)
	bad.MinimumAssurance = "bogus"
	for name, decider := range map[string]pep.Decider{
		"panic":           panicDecider{},
		"local decision":  pepfake.New(bad, orgAdminRule()),
		"uncarried scope": newDenyingFake(t, scoped),
	} {
		t.Run(name, func(t *testing.T) {
			got := seamServe(seamHandler(t, legacyStore(true, "admin"), userClaims(nil), seamEnforcer(t, decider, adminUsersRows(t), "admin")), http.MethodGet, "/api/v1/admin/users")
			if got.status != http.StatusServiceUnavailable || got.header.Get("Retry-After") != enforceRetryAfter {
				t.Fatalf("%d %q, want 503 with Retry-After", got.status, got.body)
			}
		})
	}
}

// A platform row's own answer is the handler's answer for that action: no second question.
func TestHoldsPlatformReusesThePlatformRowsAnswer(t *testing.T) {
	decider := &countingDecider{inner: pepfake.New(platformRule(rule.ActionAdminOrgs))}
	got := seamServe(seamHandler(t, legacyStore(false, "member"), userClaims(nil), seamEnforcer(t, decider, productionGoAPIRows(t), "admin")), http.MethodGet, "/api/v1/admin/orgs")
	if got.status != 200 || got.body != "platform=true bypass=false" {
		t.Fatalf("%d %q, want 200 platform=true bypass=false", got.status, got.body)
	}
	if singles, batches := decider.counts(); singles != 1 || batches != 0 {
		t.Fatalf("%d single and %d batch calls, want 1 and 0", singles, batches)
	}
}

// With the admin cell on, a handler's platform question on a route with no row is the decision point's answer,
// asked once per request, whatever go-api's own flag says; no verdict is a 503 from the handler.
func TestHoldsPlatformAsksThePDPWithTheAdminCellOn(t *testing.T) {
	for _, c := range []struct {
		name      string
		decider   pep.Decider
		superuser bool
		status    int
		body      string
	}{
		{"pdp allows, flag off", pepfake.New(platformRule(rule.ActionAdminTelemetry)), false, 200, "platform=true bypass=false"},
		{"pdp denies, flag on", newDenyingFake(t), true, 200, "platform=false bypass=false"},
		{"no verdict", panicDecider{}, true, 503, ""},
		{"allow with an obligation", pepfake.New(pepfake.Rule{PrincipalReference: shadowBearer, Action: rule.ActionAdminTelemetry, ResourceType: "platform",
			ResourceKey: shadowPlatformKey, TenantScope: ownOrg}), false, 503, ""},
		{"local decision", pepfake.New(pepfake.Rule{PrincipalReference: shadowBearer, Action: rule.ActionAdminTelemetry, ResourceType: "platform",
			ResourceKey: shadowPlatformKey, MinimumAssurance: "bogus"}), false, 503, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			counting := &countingDecider{inner: c.decider}
			got := seamServe(seamHandler(t, legacyStore(c.superuser, "member"), userClaims(nil), seamEnforcer(t, counting, productionGoAPIRows(t), "admin")), http.MethodPost, "/api/v1/telemetry/report")
			if got.status != c.status || !strings.HasPrefix(got.body, c.body) {
				t.Fatalf("%d %q, want %d %q", got.status, got.body, c.status, c.body)
			}
			if c.status == 503 && got.header.Get("Retry-After") != enforceRetryAfter {
				t.Fatalf("503 without Retry-After")
			}
			if singles, batches := counting.counts(); singles != 1 || batches != 0 {
				t.Fatalf("%d single and %d batch calls, want one question for the request", singles, batches)
			}
		})
	}
}

// With no decision point client the handler's platform question has no verdict: 503, fail closed.
func TestHoldsPlatformWithNoDecisionPointIsA503(t *testing.T) {
	enf, err := NewEnforcer(EnforceConfig{Unconfigured: true, Rows: productionGoAPIRows(t), Switch: onFor("admin"), Logger: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	got := seamServe(seamHandler(t, legacyStore(true, "member"), userClaims(nil), enf), http.MethodPost, "/api/v1/telemetry/report")
	if got.status != http.StatusServiceUnavailable {
		t.Fatalf("%d %q, want 503", got.status, got.body)
	}
}

// With the admin cell off, or no Enforcer, the handler's platform answers are go-api's own and no question is asked.
func TestHoldsPlatformKeepsTheLocalSetWithTheAdminCellOff(t *testing.T) {
	for _, superuser := range []bool{true, false} {
		for name, enf := range map[string]func(*countingDecider) *Enforcer{
			"no enforcer":   func(*countingDecider) *Enforcer { return nil },
			"teams cell on": func(d *countingDecider) *Enforcer { return seamEnforcer(t, d, productionGoAPIRows(t), "teams") },
		} {
			decider := &countingDecider{inner: pepfake.New(platformRule(rule.ActionAdminTelemetry))}
			got := seamServe(seamHandler(t, legacyStore(superuser, "member"), userClaims(nil), enf(decider)), http.MethodPost, "/api/v1/telemetry/report")
			want := "platform=" + strconv.FormatBool(superuser) + " bypass=" + strconv.FormatBool(superuser)
			if got.status != 200 || got.body != want {
				t.Fatalf("%s, superuser %v: %d %q, want 200 %q", name, superuser, got.status, got.body, want)
			}
			if singles, batches := decider.counts(); singles+batches != 0 {
				t.Fatalf("%s: %d questions, want none", name, singles+batches)
			}
		}
	}
}

const legacyCredentialBody = `{"detail":{"message":"` + legacyCredentialDetail + `","reason_code":"legacy_credential"}}`

// With the admin cell on, a caller the decision point cannot judge (a legacy HS256 token) is refused its platform
// questions with a 403 legacy_credential, never answered from go-api's own set; with the cell off its answers are the
// local set.
func TestHoldsPlatformRefusesAnHS256CallerWithTheAdminCellOn(t *testing.T) {
	for _, c := range []struct {
		name   string
		cell   []string
		status int
		body   string
	}{
		{"admin cell on", []string{"admin"}, http.StatusForbidden, legacyCredentialBody},
		{"admin cell off", []string{"teams"}, http.StatusOK, "platform=true bypass=true"},
	} {
		t.Run(c.name, func(t *testing.T) {
			decider := &countingDecider{inner: pepfake.New(platformRule(rule.ActionAdminTelemetry))}
			store := newStore()
			store.users[userID] = UserState{IsActive: true, IsSuperuser: true}
			guard := NewGuard(authenticator(t, store), quiet())
			guard.SetEnforcer(seamEnforcer(t, decider, productionGoAPIRows(t), c.cell...))
			got := serve(seamMux(guard), http.MethodPost, "/api/v1/telemetry/report", bearer(t, superClaims))
			if got.status != c.status || got.body != c.body {
				t.Fatalf("%d %q, want %d %q", got.status, got.body, c.status, c.body)
			}
			if singles, batches := decider.counts(); singles+batches != 0 {
				t.Fatalf("%d questions, want none", singles+batches)
			}
		})
	}
}

// PlatformBypassesOrg outside a guard (no decisions in the context) is go-api's own set.
func TestPlatformBypassesOrgOutsideTheGuardIsTheLocalSet(t *testing.T) {
	ctx := context.Background()
	if !PlatformBypassesOrg(ctx, &User{IsSuperuser: true}, rule.ActionAdminOrgs) || PlatformBypassesOrg(ctx, &User{}, rule.ActionAdminOrgs) {
		t.Fatal("no decisions in the context: want the local set")
	}
	decider := &countingDecider{inner: pepfake.New(platformRule(rule.ActionAdminOrgs), platformRule("org.delete"))}
	judged := WithPlatformDecider(ctx, decider, shadowBearer)
	if PlatformBypassesOrg(judged, &User{IsSuperuser: true}, rule.ActionAdminOrgs) {
		t.Fatal("a judged caller with the admin cell on: no platform action stands in for organization power")
	}
	if held, err := HoldsPlatform(judged, &User{}, rule.ActionAdminOrgs); err != nil || !held {
		t.Fatalf("HoldsPlatform = %v, %v; want the decision point's allow", held, err)
	}
	if held, err := HoldsPlatform(judged, &User{IsSuperuser: true}, "org.delete"); err != nil || held {
		t.Fatalf("an organization action as a platform question = %v, %v; want false and no question", held, err)
	}
	if singles, batches := decider.counts(); singles != 1 || batches != 0 {
		t.Fatalf("%d single and %d batch calls, want only the admin.orgs question", singles, batches)
	}
}

// With the admin cell on, a live go-api impersonation row gets no platform action where the decision point does not
// decide the row (D6714: the bearer is the admin's own, judged as the admin, not as the target). An admin-or row with
// no action answers 403 before any question, whatever the admin's role; a handler's platform question is refused; the
// session's own status question (admin.impersonate) is still asked. With the cell off the answer is main's.
func TestPlatformQuestionsInAnImpersonationSessionAreRefusedWithTheAdminCellOn(t *testing.T) {
	for _, c := range []struct {
		name    string
		cell    string
		role    string
		method  string
		path    string
		status  int
		body    string
		singles int
	}{
		{"admin-or row, member", "admin", "member", http.MethodGet, "/api/v1/admin/users", 403, impersonationRefusalBody, 0},
		{"admin-or row, org admin", "admin", "admin", http.MethodGet, "/api/v1/admin/users", 403, impersonationRefusalBody, 0},
		{"handler question", "admin", "member", http.MethodPost, "/api/v1/telemetry/report", 403, impersonationRefusalBody, 0},
		{"own session status", "admin", "member", http.MethodGet, "/api/v1/admin/impersonate/status", 200, "platform=true bypass=false", 1},
		{"admin-or row the handler does not ask, org admin", "admin", "admin", http.MethodGet, "/api/v1/admin/users/" + targetUserID.String(), 403, impersonationRefusalBody, 0},
		{"admin-or row, cell off", "teams", "member", http.MethodGet, "/api/v1/admin/users", 200, "platform=true bypass=true", 0},
		{"admin-or row the handler does not ask, cell off", "teams", "admin", http.MethodGet, "/api/v1/admin/users/" + targetUserID.String(), 200, "served", 0},
		{"handler question, cell off", "teams", "member", http.MethodPost, "/api/v1/telemetry/report", 200, "platform=true bypass=true", 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			decider := &countingDecider{inner: pepfake.New(platformRule(rule.ActionAdminUsers), platformRule(rule.ActionAdminTelemetry),
				platformRule(rule.ActionAdminImpersonate))}
			h := impersonating(seamHandler(t, legacyStore(true, c.role), userClaims(nil), seamEnforcer(t, decider, productionGoAPIRows(t), c.cell)))
			got := seamServe(h, c.method, c.path)
			if got.status != c.status || got.body != c.body {
				t.Fatalf("%d %q, want %d %q", got.status, got.body, c.status, c.body)
			}
			if singles, batches := decider.counts(); singles != c.singles || batches != 0 {
				t.Fatalf("%d single and %d batch calls, want %d and 0", singles, batches, c.singles)
			}
		})
	}
}

// With the admin cell on, a legacy HS256 caller gets no platform stand-in for organization power either: its
// PlatformBypassesOrg is false, never go-api's own flag. With the cell off it is the flag.
func TestAnHS256CallerGetsNoOrgBypassWithTheAdminCellOn(t *testing.T) {
	for _, c := range []struct {
		cell string
		body string
	}{
		{"admin", "bypass=false"},
		{"teams", "bypass=true"},
	} {
		t.Run(c.cell, func(t *testing.T) {
			decider := &countingDecider{inner: pepfake.New(platformRule(rule.ActionAdminOrgs))}
			store := newStore()
			store.users[userID] = UserState{IsActive: true, IsSuperuser: true}
			guard := NewGuard(authenticator(t, store), quiet())
			guard.SetEnforcer(seamEnforcer(t, decider, productionGoAPIRows(t), c.cell))
			got := serve(seamMux(guard), http.MethodGet, "/api/v1/admin/seam/bypass", bearer(t, superClaims))
			if got.status != http.StatusOK || got.body != c.body {
				t.Fatalf("%d %q, want 200 %q", got.status, got.body, c.body)
			}
			if singles, batches := decider.counts(); singles+batches != 0 {
				t.Fatalf("%d questions, want none", singles+batches)
			}
		})
	}
}

// With the admin cell on, an organization-less caller reaches an organization route only through the decision point's
// answer at an admin-or or platform wrap, and never at a plain route, whatever go-api's own flag says. With the cell
// off the flag decides.
func TestAnOrglessCallerReachesOnlyByThePDPAnswerWithTheAdminCellOn(t *testing.T) {
	const orgRequired = `{"detail":"Organization context required"}`
	for _, c := range []struct {
		name    string
		cell    string
		method  string
		path    string
		status  int
		body    string
		singles int
	}{
		{"admin-or row, PDP denies, flag on", "admin", http.MethodGet, "/api/v1/admin/users", 403, orgRequired, 1},
		{"plain route, flag on", "admin", http.MethodPost, "/api/v1/telemetry/report", 403, orgRequired, 0},
		{"plain route, cell off", "teams", http.MethodPost, "/api/v1/telemetry/report", 200, "platform=true bypass=true", 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			decider := &countingDecider{inner: newDenyingFake(t)}
			got := seamServe(seamHandler(t, legacyStore(true, ""), orglessAccessClaims(), seamEnforcer(t, decider, productionGoAPIRows(t), c.cell)), c.method, c.path)
			if got.status != c.status || got.body != c.body {
				t.Fatalf("%d %q, want %d %q", got.status, got.body, c.status, c.body)
			}
			if singles, batches := decider.counts(); singles != c.singles || batches != 0 {
				t.Fatalf("%d single and %d batch calls, want %d and 0", singles, batches, c.singles)
			}
		})
	}
}

// With the admin cell on, the guard admits an HS256 superuser at an admin-or row by its membership row in the token's
// organization, never by its role claim: platform actions skip no membership read. A member who claims admin is not
// an admin, and its platform question is refused as a legacy credential. With the cell off the flag skips the read.
func TestAnHS256SuperuserIsAdmittedByTheMembershipRoleWithTheAdminCellOn(t *testing.T) {
	legacyBody := `{"detail":{"message":"` + legacyCredentialDetail + `","reason_code":"legacy_credential"}}`
	for _, c := range []struct {
		cell   string
		status int
		body   string
	}{
		{"admin", http.StatusForbidden, legacyBody},
		{"teams", http.StatusOK, "served"},
	} {
		t.Run(c.cell, func(t *testing.T) {
			decider := &countingDecider{inner: pepfake.New(platformRule(rule.ActionAdminUsers))}
			store := newStore()
			store.users[userID] = UserState{IsActive: true, IsSuperuser: true}
			guard := NewGuard(authenticator(t, store), quiet())
			guard.SetEnforcer(seamEnforcer(t, decider, productionGoAPIRows(t), c.cell))
			got := serve(seamMux(guard), http.MethodGet, "/api/v1/admin/users/"+targetUserID.String(), bearer(t, func(m jwt.MapClaims) {
				superClaims(m)
				m["role"] = "admin"
			}))
			if got.status != c.status || got.body != c.body {
				t.Fatalf("%d %q, want %d %q", got.status, got.body, c.status, c.body)
			}
			if singles, batches := decider.counts(); singles+batches != 0 {
				t.Fatalf("%d questions, want none", singles+batches)
			}
		})
	}
}

// With the admin cell on, an organization-less caller reaches the impersonation status route only by the decision
// point's admin.impersonate answer (in a live impersonation session too: the session's own status stays reachable); a
// deny is the organization-less refusal and no verdict a 503. With the cell off, go-api's own flag decides, no question.
func TestAnOrglessCallerReachesTheImpersonationStatusOnlyByThePDPAnswerWithTheAdminCellOn(t *testing.T) {
	const orgRequired = `{"detail":"Organization context required"}`
	for _, c := range []struct {
		name          string
		cell          string
		decider       func(t *testing.T) pep.Decider
		impersonating bool
		status        int
		body          string
		singles       int
	}{
		{"PDP allows", "admin", func(t *testing.T) pep.Decider { return pepfake.New(platformRule(rule.ActionAdminImpersonate)) }, false, 200, "platform=true bypass=false", 1},
		{"PDP allows, impersonating", "admin", func(t *testing.T) pep.Decider { return pepfake.New(platformRule(rule.ActionAdminImpersonate)) }, true, 200, "platform=true bypass=false", 1},
		{"PDP denies", "admin", func(t *testing.T) pep.Decider { return newDenyingFake(t) }, false, 403, orgRequired, 1},
		{"no verdict", "admin", func(*testing.T) pep.Decider { return panicDecider{} }, false, 503, `{"detail":"Authorization service temporarily unavailable"}`, 0},
		{"cell off", "teams", func(t *testing.T) pep.Decider { return newDenyingFake(t) }, false, 200, "platform=true bypass=true", 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			decider := &countingDecider{inner: c.decider(t)}
			h := seamHandler(t, legacyStore(true, ""), orglessAccessClaims(), seamEnforcer(t, decider, productionGoAPIRows(t), c.cell))
			if c.impersonating {
				h = impersonating(h)
			}
			got := seamServe(h, http.MethodGet, "/api/v1/admin/impersonate/status")
			if got.status != c.status || got.body != c.body {
				t.Fatalf("%d %q, want %d %q", got.status, got.body, c.status, c.body)
			}
			if c.status == 503 && got.header.Get("Retry-After") != enforceRetryAfter {
				t.Fatalf("503 without Retry-After")
			}
			if c.status != 503 {
				if singles, batches := decider.counts(); singles != c.singles || batches != 0 {
					t.Fatalf("%d single and %d batch calls, want %d and 0", singles, batches, c.singles)
				}
			}
		})
	}
}
