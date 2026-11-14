package policy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-go/pep"
	"github.com/full-chaos/dev-health-go/pep/pepfake"
	"github.com/golang-jwt/jwt/v5"

	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
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

// adminUsersRows is the production action map; it names admin.users on each /admin/users row.
func adminUsersRows(t *testing.T) map[string]ShadowRow {
	t.Helper()
	rows := productionGoAPIRows(t)
	want := ShadowRow{Action: rule.ActionAdminUsers, Namespace: "admin", ResourceType: "platform", HighRisk: true}
	for pattern := range adminOrUsersRoutes {
		if rows[pattern] != want {
			t.Fatalf("%s: production row %+v, want %+v", pattern, rows[pattern], want)
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
		_, _ = io.WriteString(w, "platform="+strconv.FormatBool(first))
	})
}

func seamMux(guard *Guard) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("GET /api/v1/admin/users", guard.WrapAdminOr(rule.ActionAdminUsers, seamEcho(rule.ActionAdminUsers)))
	mux.Handle("GET /api/v1/admin/orgs", guard.WrapPlatform(rule.ActionAdminOrgs, seamEcho(rule.ActionAdminOrgs)))
	mux.Handle("POST /api/v1/telemetry/opt-in", guard.Wrap(Authenticated, seamEcho(rule.ActionAdminTelemetry)))
	mux.Handle("GET /api/v1/admin/impersonate/status", guard.Wrap(Authenticated, seamEcho(rule.ActionAdminImpersonate)))
	// An admin-or row whose handler asks no platform question (the X-Org-Id paths of the users handlers).
	mux.Handle("GET /api/v1/admin/users/{user_id}", guard.WrapAdminOr(rule.ActionAdminUsers, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "served")
	})))
	// A platform wrap with no profile row: the decision point decides no route question, so the wrap itself asks.
	mux.Handle("GET /api/v1/admin/seam/platform", guard.WrapPlatform(rule.ActionAdminOrgs, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "served")
	})))
	// A plain admin route: no platform action of its own, so User.IsAdmin alone admits.
	mux.Handle("GET /api/v1/admin/seam/plain", guard.Wrap(Admin, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "served")
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
// passes; the handler's platform answer is the platform question's, never the organization one's. A refusal of a token
// with no organization is the legacy guard's 403 for it (product rule 1).
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
		{"platform holder, org member", []pepfake.Rule{platformRule(rule.ActionAdminUsers)}, false, "member", userClaims(nil), 200, "platform=true", 2},
		{"org admin, no platform action", []pepfake.Rule{orgAdminRule()}, true, "admin", userClaims(nil), 200, "platform=false", 2},
		{"both", []pepfake.Rule{platformRule(rule.ActionAdminUsers), orgAdminRule()}, false, "admin", userClaims(nil), 200, "platform=true", 2},
		{"neither, legacy superuser", nil, true, "admin", userClaims(nil), 403, `{"detail":"Admin access required"}`, 2},
		{"org-less platform holder", []pepfake.Rule{platformRule(rule.ActionAdminUsers)}, true, "", orglessAccessClaims(), 200, "platform=true", 1},
		{"org-less, no platform action", nil, true, "", orglessAccessClaims(), 403, `{"detail":"Organization context required"}`, 1},
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
	if got.status != 200 || got.body != "platform=true" {
		t.Fatalf("%d %q, want 200 platform=true", got.status, got.body)
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
		{"pdp allows, flag off", pepfake.New(platformRule(rule.ActionAdminTelemetry)), false, 200, "platform=true"},
		{"pdp denies, flag on", newDenyingFake(t), true, 200, "platform=false"},
		{"no verdict", panicDecider{}, true, 503, ""},
		{"allow with an obligation", pepfake.New(pepfake.Rule{PrincipalReference: shadowBearer, Action: rule.ActionAdminTelemetry, ResourceType: "platform",
			ResourceKey: shadowPlatformKey, TenantScope: ownOrg}), false, 503, ""},
		{"local decision", pepfake.New(pepfake.Rule{PrincipalReference: shadowBearer, Action: rule.ActionAdminTelemetry, ResourceType: "platform",
			ResourceKey: shadowPlatformKey, MinimumAssurance: "bogus"}), false, 503, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			counting := &countingDecider{inner: c.decider}
			got := seamServe(seamHandler(t, legacyStore(c.superuser, "member"), userClaims(nil), seamEnforcer(t, counting, productionGoAPIRows(t), "admin")), http.MethodPost, "/api/v1/telemetry/opt-in")
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
	got := seamServe(seamHandler(t, legacyStore(true, "member"), userClaims(nil), enf), http.MethodPost, "/api/v1/telemetry/opt-in")
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
			got := seamServe(seamHandler(t, legacyStore(superuser, "member"), userClaims(nil), enf(decider)), http.MethodPost, "/api/v1/telemetry/opt-in")
			want := "platform=" + strconv.FormatBool(superuser)
			if got.status != 200 || got.body != want {
				t.Fatalf("%s, superuser %v: %d %q, want 200 %q", name, superuser, got.status, got.body, want)
			}
			if singles, batches := decider.counts(); singles+batches != 0 {
				t.Fatalf("%s: %d questions, want none", name, singles+batches)
			}
		}
	}
}

// mainAnswer is main's answer for the request: the same guard and handlers with the admin cell off (go-api's own set,
// D6742: the cell-off predicates are main's).
func mainAnswer(t *testing.T, serveWith func(cell string) response) response {
	t.Helper()
	return serveWith("teams")
}

// platformAdminFake is the decision point for a platform admin (13209: platform_admin holds every platform action,
// with or without a membership row) and nothing else.
func platformAdminFake(t *testing.T) pep.Decider {
	t.Helper()
	var rules []pepfake.Rule
	for _, action := range rule.PlatformActions {
		rules = append(rules, platformRule(action))
	}
	return newDenyingFake(t, rules...)
}

// equalToMain fails unless the admin-cell answer is main's answer, status and body.
func equalToMain(t *testing.T, got, main response) {
	t.Helper()
	if got.status != main.status || got.body != main.body {
		t.Fatalf("admin cell %d %q, main %d %q: want main's answer", got.status, got.body, main.status, main.body)
	}
}

// refusalOnlyValidator is the production access validator for a token it refuses; an admitted token fails the cell.
type refusalOnlyValidator struct{ v *accessverify.Validator }

func (r refusalOnlyValidator) Validate(ctx context.Context, service, profile, bearer string) (*RoutedResult, error) {
	if _, err := r.v.Validate(ctx, service, profile, bearer); err != nil {
		return nil, err
	}
	return nil, errors.New("refusalOnlyValidator: the validator admitted a token this cell expects refused")
}

// Product rule 1: the caller of #5819's HS256 cell on its user access token. With the decision point knowing the
// caller as production state builds it (a users-row superuser is platform_admin with AUTH_PLATFORM_ROLES_OWNED off,
// prod's value: decisionstate.go, authapi/users.go, policy/store.go), the admin cell answers main's platform answer:
// true for the superuser, false for a member.
func TestAUserTokenCallerKeepsMainsPlatformAnswerWithTheAdminCellOn(t *testing.T) {
	for _, superuser := range []bool{true, false} {
		serveWith := func(cell string) (response, int) {
			inner := newDenyingFake(t)
			if superuser {
				inner = platformAdminFake(t).(denyingFake)
			}
			decider := &countingDecider{inner: inner}
			got := seamServe(seamHandler(t, legacyStore(superuser, "member"), userClaims(nil), seamEnforcer(t, decider, productionGoAPIRows(t), cell)), http.MethodPost, "/api/v1/telemetry/opt-in")
			singles, batches := decider.counts()
			return got, singles + batches
		}
		got, asked := serveWith("admin")
		main, _ := serveWith("teams")
		equalToMain(t, got, main)
		if want := "platform=" + strconv.FormatBool(superuser); got.status != http.StatusOK || got.body != want || asked == 0 {
			t.Fatalf("superuser %v: %d %q after %d questions, want 200 %q and the PDP asked", superuser, got.status, got.body, asked, want)
		}
	}
}

// HoldsPlatform outside a guard (no decisions in the context) is go-api's own set; with a decision point in the context
// it is that point's answer, and a non-platform action is no platform question.
func TestHoldsPlatformOutsideTheGuardIsTheLocalSet(t *testing.T) {
	ctx := context.Background()
	if held, _ := HoldsPlatform(ctx, &User{IsSuperuser: true}, rule.ActionAdminOrgs); !held {
		t.Fatal("no decisions in the context: want the local set")
	}
	decider := &countingDecider{inner: pepfake.New(platformRule(rule.ActionAdminOrgs), platformRule("org.delete"))}
	judged := WithPlatformDecider(ctx, decider, shadowBearer)
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

// With the admin cell on, a live go-api impersonation row keeps main's answer on the rows the decision point does not
// decide (13211 lists the only accepted narrowing; the /admin/users rows are not in it): the admin-or row and a
// handler's platform question are answered for the admin's own bearer, as main answers them for the admin.
func TestAnImpersonationSessionKeepsMainsAnswerWithTheAdminCellOn(t *testing.T) {
	for _, c := range []struct {
		name   string
		role   string
		method string
		path   string
	}{
		{"admin-or row, member", "member", http.MethodGet, "/api/v1/admin/users"},
		{"admin-or row, org admin", "admin", http.MethodGet, "/api/v1/admin/users"},
		{"admin-or row the handler does not ask", "member", http.MethodGet, "/api/v1/admin/users/" + targetUserID.String()},
		{"handler question", "member", http.MethodPost, "/api/v1/telemetry/opt-in"},
		{"own session status", "member", http.MethodGet, "/api/v1/admin/impersonate/status"},
	} {
		t.Run(c.name, func(t *testing.T) {
			serveWith := func(cell string) response {
				h := impersonating(seamHandler(t, legacyStore(true, c.role), userClaims(nil), seamEnforcer(t, platformAdminFake(t), productionGoAPIRows(t), cell)))
				return seamServe(h, c.method, c.path)
			}
			got := serveWith("admin")
			equalToMain(t, got, mainAnswer(t, serveWith))
			if got.status != http.StatusOK {
				t.Fatalf("%d %q: main serves the impersonating platform admin here", got.status, got.body)
			}
		})
	}
}

// With the admin cell on, an organization-less caller reaches a route as main lets it: a platform admin (no membership
// row) reaches the admin-or row, a plain route and its own impersonation status; a caller with no platform role is
// refused as on main.
func TestAnOrglessCallerKeepsMainsAnswerWithTheAdminCellOn(t *testing.T) {
	for _, c := range []struct {
		name          string
		platformAdmin bool
		impersonating bool
		method        string
		path          string
	}{
		{"platform admin, admin-or row", true, false, http.MethodGet, "/api/v1/admin/users"},
		{"platform admin, plain route", true, false, http.MethodPost, "/api/v1/telemetry/opt-in"},
		{"platform admin, own status", true, false, http.MethodGet, "/api/v1/admin/impersonate/status"},
		{"platform admin, own status while impersonating", true, true, http.MethodGet, "/api/v1/admin/impersonate/status"},
		{"no platform role, admin-or row", false, false, http.MethodGet, "/api/v1/admin/users"},
		{"no platform role, plain route", false, false, http.MethodPost, "/api/v1/telemetry/opt-in"},
		{"no platform role, status", false, false, http.MethodGet, "/api/v1/admin/impersonate/status"},
	} {
		t.Run(c.name, func(t *testing.T) {
			serveWith := func(cell string) response {
				decider := newDenyingFake(t)
				if c.platformAdmin {
					decider = platformAdminFake(t).(denyingFake)
				}
				h := seamHandler(t, legacyStore(c.platformAdmin, ""), orglessAccessClaims(), seamEnforcer(t, decider, productionGoAPIRows(t), cell))
				if c.impersonating {
					h = impersonating(h)
				}
				return seamServe(h, c.method, c.path)
			}
			equalToMain(t, serveWith("admin"), mainAnswer(t, serveWith))
		})
	}
}

// With the admin cell on, the guard admits a platform admin with no admin role in the token's organization at an
// admin level, as main's User.IsAdmin does; a member with no platform role is refused as on main. A question the wrap
// does ask (a member at an admin-or row) with no verdict is a 503: no fallback on a failure.
func TestAdminAdmissionKeepsMainsAnswerWithTheAdminCellOn(t *testing.T) {
	for _, c := range []struct {
		name          string
		platformAdmin bool
		role          string
		path          string
		status        int
	}{
		{"platform admin, member row, admin-or row", true, "member", "/api/v1/admin/users/" + targetUserID.String(), 200},
		{"platform admin, org admin, admin-or row", true, "admin", "/api/v1/admin/users/" + targetUserID.String(), 200},
		{"member, no platform role, admin-or row", false, "member", "/api/v1/admin/users/" + targetUserID.String(), 403},
		{"org admin, no platform role, admin-or row", false, "admin", "/api/v1/admin/users/" + targetUserID.String(), 200},
		{"platform admin, member row, plain admin route", true, "member", "/api/v1/admin/seam/plain", 200},
		{"member, no platform role, plain admin route", false, "member", "/api/v1/admin/seam/plain", 403},
	} {
		t.Run(c.name, func(t *testing.T) {
			serveWith := func(cell string) response {
				// The decision point's state is the caller's: platform_admin for a platform admin, members.manage in the
				// token's organization for an org admin.
				var rules []pepfake.Rule
				if c.platformAdmin {
					for _, action := range rule.PlatformActions {
						rules = append(rules, platformRule(action))
					}
				}
				if c.role == "admin" {
					rules = append(rules, orgAdminRule())
				}
				return seamServe(seamHandler(t, legacyStore(c.platformAdmin, c.role), userClaims(nil), seamEnforcer(t, newDenyingFake(t, rules...), productionGoAPIRows(t), cell)),
					http.MethodGet, c.path)
			}
			got := serveWith("admin")
			equalToMain(t, got, mainAnswer(t, serveWith))
			if got.status != c.status {
				t.Fatalf("%d %q, want main's %d", got.status, got.body, c.status)
			}
		})
	}
	got := seamServe(seamHandler(t, legacyStore(false, "member"), userClaims(nil), seamEnforcer(t, panicDecider{}, productionGoAPIRows(t), "admin")),
		http.MethodGet, "/api/v1/admin/users/"+targetUserID.String())
	if got.status != http.StatusServiceUnavailable || got.header.Get("Retry-After") != enforceRetryAfter {
		t.Fatalf("no verdict: %d %q, want 503 with Retry-After", got.status, got.body)
	}
}

// With the admin cell on, a superuser who claims admin while its membership row says member keeps main's answer at an
// admin-or row: main skips the membership read for a holder of admin.orgs, and the decision point, for which the
// superuser is a platform admin, allows admin.users. (The fixture's routed token is judged now that the /admin/users
// rows name admin.users.)
func TestAFixtureSuperuserKeepsMainsAdmissionWithTheAdminCellOn(t *testing.T) {
	headers := bearer(t, func(m jwt.MapClaims) {
		superClaims(m)
		m["role"] = "admin"
	})
	var rules []pepfake.Rule
	for _, action := range rule.PlatformActions {
		r := platformRule(action)
		r.PrincipalReference = strings.TrimPrefix(headers["Authorization"], "Bearer ")
		rules = append(rules, r)
	}
	serveWith := func(cell string) response {
		store := newStore()
		store.users[userID] = UserState{IsActive: true, IsSuperuser: true}
		guard := NewGuard(authenticator(t, store), quiet())
		guard.SetEnforcer(seamEnforcer(t, &countingDecider{inner: newDenyingFake(t, rules...)}, productionGoAPIRows(t), cell))
		return serve(seamMux(guard), http.MethodGet, "/api/v1/admin/users/"+targetUserID.String(), headers)
	}
	got := serveWith("admin")
	equalToMain(t, got, mainAnswer(t, serveWith))
	if got.status != http.StatusOK || got.body != "served" {
		t.Fatalf("%d %q: main serves it", got.status, got.body)
	}
}

// With the admin cell on, a fixture superuser whose token names an organization it has no membership row in keeps
// main's answer at an admin route: main skips the membership read for a holder of admin.orgs (product rule 1).
func TestAFixtureSuperuserWithNoMembershipRowKeepsMainsAnswerWithTheAdminCellOn(t *testing.T) {
	serveWith := func(cell string) response {
		store := newStore()
		store.users[userID] = UserState{IsActive: true, IsSuperuser: true}
		guard := NewGuard(authenticator(t, store), quiet())
		guard.SetEnforcer(seamEnforcer(t, &countingDecider{inner: newDenyingFake(t)}, productionGoAPIRows(t), cell))
		return serve(seamMux(guard), http.MethodGet, "/api/v1/admin/seam/plain", bearer(t, func(m jwt.MapClaims) {
			superClaims(m)
			m["org_id"] = strangerOrg
		}))
	}
	got := serveWith("admin")
	equalToMain(t, got, mainAnswer(t, serveWith))
	if got.status != http.StatusOK || got.body != "served" {
		t.Fatalf("%d %q: main serves it", got.status, got.body)
	}
}

// With the admin cell on, a platform wrap whose row the decision point does not decide takes the wrap action from the
// decision point: its allow admits a caller with no local flag, its deny refuses a caller with one.
func TestAPlatformWrapWithNoRowAsksThePDPWithTheAdminCellOn(t *testing.T) {
	for _, c := range []struct {
		name      string
		allow     bool
		superuser bool
		status    int
	}{
		{"pdp allows, flag off", true, false, http.StatusOK},
		{"pdp denies, flag on", false, true, http.StatusForbidden},
	} {
		t.Run(c.name, func(t *testing.T) {
			decider := &countingDecider{inner: newDenyingFake(t)}
			if c.allow {
				decider.inner = newDenyingFake(t, platformRule(rule.ActionAdminOrgs))
			}
			got := seamServe(seamHandler(t, legacyStore(c.superuser, "member"), userClaims(nil), seamEnforcer(t, decider, productionGoAPIRows(t), "admin")),
				http.MethodGet, "/api/v1/admin/seam/platform")
			if got.status != c.status {
				t.Fatalf("%d %q, want %d", got.status, got.body, c.status)
			}
			if singles, batches := decider.counts(); singles != 1 || batches != 0 {
				t.Fatalf("%d single and %d batch calls, want one question", singles, batches)
			}
		})
	}
}
