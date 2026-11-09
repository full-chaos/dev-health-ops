package policy

import (
	"context"
	"net/http"
	"slices"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-go/pep"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

// adminUsersShadowRows is the shadow's action map for the /admin/users rows alone: admin.users, as the profile names it.
func adminUsersShadowRows(t *testing.T) map[string]ShadowRow {
	t.Helper()
	rows := map[string]ShadowRow{}
	for pattern, row := range adminUsersRows(t) {
		if _, ok := adminOrUsersRoutes[pattern]; ok {
			rows[pattern] = row
		}
	}
	if len(rows) != len(adminOrUsersRoutes) {
		t.Fatalf("%d admin-or users rows, want %d", len(rows), len(adminOrUsersRoutes))
	}
	return rows
}

// adminOrShadowHandler mounts the /admin/users rows as the admin package does (WrapAdminOr(admin.users), body first
// on the writes) with the shadow beside the guard.
func adminOrShadowHandler(t *testing.T, store *accessStore, claims *RoutedResult, sh *Shadow) http.Handler {
	t.Helper()
	guard := NewGuard(routed(t, store, &fakeValidator{result: claims}), quiet())
	guard.SetShadow(sh)
	mux := http.NewServeMux()
	for pattern, route := range adminOrUsersRoutes {
		if route.bodyFirst {
			mux.Handle(pattern, guard.BodyFirstAdminOr(rule.ActionAdminUsers, echo))
		} else {
			mux.Handle(pattern, guard.WrapAdminOr(rule.ActionAdminUsers, echo))
		}
	}
	return mux
}

// The shadow judges an admin-or row as the Enforcer decides it: one batch of the row's platform action and
// members.manage in the token's organization, either allow passing. An org admin with no platform role is a MATCH on
// every /admin/users row (legacy allow, decision point allow by members.manage), as are a platform admin and a member
// (deny, deny). The planted divergence, a decision point that refuses the org admin members.manage, is still a
// MISMATCH: the readout sees a real divergence.
func TestAdminOrShadowJudgesTheUsersRowsAsTheEnforcerDecides(t *testing.T) {
	for _, c := range []struct {
		name    string
		store   *accessStore
		rules   func(t *testing.T) pep.Decider
		allowed bool
		want    string
		asked   []string
	}{
		{"org admin", personaStore(personaAdmin), func(t *testing.T) pep.Decider { return newDenyingFake(t, orgAdminRule()) }, true, resultMatch,
			[]string{rule.ActionAdminUsers, adminOrOrgAction}},
		{"platform admin, org member", personaStore(persona{role: "member", superuser: true}), platformAdminFake, true, resultMatch,
			[]string{rule.ActionAdminUsers, adminOrOrgAction}},
		{"org member", personaStore(personaMember), func(t *testing.T) pep.Decider { return newDenyingFake(t) }, false, resultMatch,
			[]string{rule.ActionAdminUsers, adminOrOrgAction}},
		{"planted: org admin refused members.manage", personaStore(personaAdmin), func(t *testing.T) pep.Decider { return newDenyingFake(t) }, true, resultMismatch,
			[]string{rule.ActionAdminUsers, adminOrOrgAction}},
	} {
		for pattern := range adminOrUsersRoutes {
			t.Run(c.name+"/"+pattern, func(t *testing.T) {
				rec := recordShadow(t)
				seen := &recordingDecider{next: c.rules(t)}
				sh, _ := newShadowFor(t, seen, func(cfg *ShadowConfig) { cfg.Rows = adminUsersShadowRows(t) })
				got := serveCell(adminOrShadowHandler(t, c.store, userClaims(nil), sh), pattern, cellBearer())
				sh.Wait()
				if (got.status == http.StatusOK) != c.allowed {
					t.Fatalf("legacy answer %d %q, want allowed=%v", got.status, got.body, c.allowed)
				}
				if _, results := rec.snapshot(); !slices.Equal(results, []string{c.want}) {
					t.Fatalf("shadow results %v, want [%s]", results, c.want)
				}
				if asked := seen.actions(); !slices.Equal(asked, c.asked) {
					t.Fatalf("shadow asked %v, want the admin-or batch %v", asked, c.asked)
				}
			})
		}
	}
}

// Product rule 1, legacy answer: on the /admin/users rows, a caller with no organization on its token and no platform
// role gets main's 403 "Organization context required" with the admin cell on as with it off; a platform admin with
// no organization is served on and off.
func TestAdminOrUsersRowsKeepTheLegacyOrglessAnswerWithTheAdminCellOn(t *testing.T) {
	for _, c := range []struct {
		name          string
		platformAdmin bool
		want          response
	}{
		{"no platform role", false, response{status: http.StatusForbidden, body: `{"detail":"Organization context required"}`}},
		{"platform admin", true, response{status: http.StatusOK}},
	} {
		for pattern := range adminOrUsersRoutes {
			t.Run(c.name+"/"+pattern, func(t *testing.T) {
				serveWith := func(cell string) response {
					decider := pep.Decider(newDenyingFake(t))
					if c.platformAdmin {
						decider = platformAdminFake(t)
					}
					h := cellHandler(t, legacyStore(c.platformAdmin, ""), orglessAccessClaims(), seamEnforcer(t, decider, adminUsersRows(t), cell))
					return serveCell(h, pattern, cellBearer())
				}
				on, off := serveWith("admin"), serveWith("teams")
				equalToMain(t, on, off)
				if off.status != c.want.status || (c.want.body != "" && off.body != c.want.body) {
					t.Fatalf("legacy %d %q, want %d %q", off.status, off.body, c.want.status, c.want.body)
				}
			})
		}
	}
}

// recordingDecider passes every question to next and keeps the actions asked, in order.
type recordingDecider struct {
	next pep.Decider
	mu   sync.Mutex
	seen []string
}

func (d *recordingDecider) note(reqs ...pep.Request) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, r := range reqs {
		d.seen = append(d.seen, r.Action)
	}
}

func (d *recordingDecider) Decide(ctx context.Context, req pep.Request) pep.Decision {
	d.note(req)
	return d.next.Decide(ctx, req)
}

func (d *recordingDecider) DecideBatch(ctx context.Context, reqs []pep.Request) []pep.Decision {
	d.note(reqs...)
	return d.next.DecideBatch(ctx, reqs)
}

func (d *recordingDecider) actions() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.seen)
}
