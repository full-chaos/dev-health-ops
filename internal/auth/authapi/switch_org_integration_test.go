//go:build integration

package authapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// switchOrgLive is the switch route over migrated ops and auth schemas, every
// read and write as the auth runtime role (authschema.Apply's grants): the
// real session store, PGUsers and the identity projections.
type switchOrgLive struct {
	*preflightStore
	owned  bool
	ids    *identity.Store
	issuer *Issuer
	server http.Handler
}

func newSwitchOrgLive(t *testing.T, ctx context.Context, owned bool, wrap func(SwitchOrgReads) SwitchOrgReads) *switchOrgLive {
	t.Helper()
	p := newPreflightStoreOwned(t, ctx, owned)
	ids, err := identity.New(identity.Config{Pool: p.runtime, Schema: p.schema, UsersOwnedByAuth: owned})
	if err != nil {
		t.Fatal(err)
	}
	issuer, err := NewIssuer(testSecret, testIssuer, testAudience)
	if err != nil {
		t.Fatal(err)
	}
	users := PGUsers{Pool: p.runtime, AuthSchema: p.schema, UsersOwned: owned}
	var reads SwitchOrgReads = users
	if wrap != nil {
		reads = wrap(users)
	}
	routes, err := compatRoutes(CompatDeps{
		Sessions: p.store, Users: users, Issuer: issuer, Projector: ids, Logger: slog.New(slog.DiscardHandler),
		AppendAudit: func(context.Context, audit.AuditEvent) error { return nil },
		Limits:      mustLimits(t, httpapi.NewMemoryCounters(nil)),
		SwitchOrg:   &SwitchOrgConfig{Sessions: p.store, Reads: reads},
	}, switchRows("ops_access_token_hs256"))
	if err != nil {
		t.Fatal(err)
	}
	return &switchOrgLive{preflightStore: p, owned: owned, ids: ids, issuer: issuer, server: serveRoutes(routes)}
}

func (l *switchOrgLive) exec(t *testing.T, ctx context.Context, sql string, args ...any) {
	t.Helper()
	if _, err := l.admin.Exec(ctx, sql, args...); err != nil {
		t.Fatalf("seed: %v", err)
	}
}

func (l *switchOrgLive) user(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	id := uuid.New()
	l.exec(t, ctx, `INSERT INTO public.users (id, email, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, 'local', true, true, false, 0, now(), now())`, id, "s-"+id.String()[:8]+"@example.test")
	if err := l.ids.Project(ctx, id); err != nil {
		t.Fatalf("Project: %v", err)
	}
	return id
}

func (l *switchOrgLive) org(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	id := uuid.New()
	l.exec(t, ctx, `INSERT INTO public.organizations (id, slug, name, is_active) VALUES ($1, $2, 'switch-test', true)`, id, "sw-"+id.String()[:8])
	if err := l.ids.ProjectOrganization(ctx, id); err != nil {
		t.Fatalf("ProjectOrganization: %v", err)
	}
	return id
}

func (l *switchOrgLive) member(t *testing.T, ctx context.Context, org, user uuid.UUID, role string) {
	t.Helper()
	l.exec(t, ctx, `INSERT INTO public.memberships (id, user_id, org_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, now(), now(), now())`, uuid.New(), user, org, role)
}

// signIn creates a session in org and the bearer of it.
func (l *switchOrgLive) signIn(t *testing.T, ctx context.Context, user, org uuid.UUID) (session.Issued, map[string]string) {
	t.Helper()
	issued, err := l.store.Create(ctx, user, &org, []string{"pwd"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	compact, err := l.issuer.Access(Grant{PrincipalID: user, OrganizationID: &org, Role: "admin", SessionID: issued.Session.ID,
		User: UserState{Email: "x@example.test", IsActive: true}})
	if err != nil {
		t.Fatal(err)
	}
	return issued, map[string]string{"Authorization": "Bearer " + compact}
}

func (l *switchOrgLive) liveIn(t *testing.T, ctx context.Context, user, org uuid.UUID) int {
	t.Helper()
	return l.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1 AND organization_id = $2
		AND revoked_at IS NULL AND expires_at > clock_timestamp()`, user.String(), org.String())
}

func (l *switchOrgLive) live(t *testing.T, ctx context.Context, id uuid.UUID) bool {
	t.Helper()
	return l.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE id = $1 AND revoked_at IS NULL`, id.String()) == 1
}

// switchCase is one user, member of A (admin) and B (member), signed in to A.
type switchCase struct {
	user, orgA, orgB uuid.UUID
	predecessor      session.Issued
	bearer           map[string]string
}

func (l *switchOrgLive) switchCase(t *testing.T, ctx context.Context) switchCase {
	t.Helper()
	c := switchCase{user: l.user(t, ctx), orgA: l.org(t, ctx), orgB: l.org(t, ctx)}
	l.member(t, ctx, c.orgA, c.user, "admin")
	l.member(t, ctx, c.orgB, c.user, "member")
	c.predecessor, c.bearer = l.signIn(t, ctx, c.user, c.orgA)
	return c
}

// TestSwitchOrgIntegration runs the switch cells on real rows, with the users
// owned (the membership read FOR SHARE) and not owned (a plain read: the role
// has no UPDATE on public.memberships, which a row lock needs).
func TestSwitchOrgIntegration(t *testing.T) {
	for _, owned := range []bool{false, true} {
		name := "users not owned"
		if owned {
			name = "users owned"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			l := newSwitchOrgLive(t, ctx, owned, nil)
			for _, cell := range []struct {
				name string
				run  func(*testing.T, context.Context, *switchOrgLive)
			}{
				{"SupersedesOnlyTheCallersSession", testSwitchOrgIntegrationSupersedesOnlyTheCallersSession},
				{"RemovedMemberRefused", testSwitchOrgIntegrationRemovedMemberRefused},
				{"SuspendedOrgRefused", testSwitchOrgIntegrationSuspendedOrgRefused},
				{"TwoSwitchesOneSession", testSwitchOrgIntegrationTwoSwitchesOneSession},
				{"SwitchBackAndForth", testSwitchOrgIntegrationSwitchBackAndForth},
			} {
				t.Run(cell.name, func(t *testing.T) { cell.run(t, ctx, l) })
			}
		})
	}
}

// O-D2: the switch supersedes the bearer's session only; the user's other
// session stays live. go-api's switch row commits with it, in B.
func testSwitchOrgIntegrationSupersedesOnlyTheCallersSession(t *testing.T, ctx context.Context, l *switchOrgLive) {
	c := l.switchCase(t, ctx)
	other, _ := l.signIn(t, ctx, c.user, c.orgA)
	r := post(l.server, PatternCompatSwitchOrg, orgBody(c.orgB), c.bearer)
	if r.Code != http.StatusOK {
		t.Fatalf("switch: %d %s", r.Code, r.Body)
	}
	if l.live(t, ctx, c.predecessor.Session.ID) || !l.live(t, ctx, other.Session.ID) || l.liveIn(t, ctx, c.user, c.orgB) != 1 {
		t.Fatal("want the predecessor revoked, the other session live and one live session in B")
	}
	if n := l.count(t, ctx, `SELECT count(*) FROM public.audit_logs WHERE org_id = $1 AND user_id = $2 AND action = 'login'
		AND resource_type = 'session' AND resource_id = $3 AND description = 'User switched active organization'`,
		c.orgB, c.user, c.user.String()); n != 1 {
		t.Fatalf("%d go-api switch rows in B, want 1", n)
	}
}

// O-C1: a member removed from B is refused; nothing changes.
func testSwitchOrgIntegrationRemovedMemberRefused(t *testing.T, ctx context.Context, l *switchOrgLive) {
	c := l.switchCase(t, ctx)
	l.exec(t, ctx, `DELETE FROM public.memberships WHERE user_id = $1 AND org_id = $2`, c.user, c.orgB)
	r := post(l.server, PatternCompatSwitchOrg, orgBody(c.orgB), c.bearer)
	if r.Code != http.StatusForbidden || r.Body.String() != switchNotMember {
		t.Fatalf("%d %s, want 403 %s", r.Code, r.Body, switchNotMember)
	}
	if !l.live(t, ctx, c.predecessor.Session.ID) || l.liveIn(t, ctx, c.user, c.orgB) != 0 {
		t.Fatal("a refused switch changed the sessions")
	}
}

// O-C2 (G1): a suspended organization is refused, on a membership that holds.
func testSwitchOrgIntegrationSuspendedOrgRefused(t *testing.T, ctx context.Context, l *switchOrgLive) {
	c := l.switchCase(t, ctx)
	l.exec(t, ctx, `UPDATE public.organizations SET is_active = false WHERE id = $1`, c.orgB)
	r := post(l.server, PatternCompatSwitchOrg, orgBody(c.orgB), c.bearer)
	if r.Code != http.StatusForbidden || r.Body.String() != switchNotMember {
		t.Fatalf("%d %s, want 403 %s", r.Code, r.Body, switchNotMember)
	}
	if !l.live(t, ctx, c.predecessor.Session.ID) || l.liveIn(t, ctx, c.user, c.orgB) != 0 {
		t.Fatal("a refused switch changed the sessions")
	}
}

// O-B4: switches raced from one session leave one live successor: one 200,
// the others the 401 of an ended session.
func testSwitchOrgIntegrationTwoSwitchesOneSession(t *testing.T, ctx context.Context, l *switchOrgLive) {
	c := l.switchCase(t, ctx)
	const n = 8
	codes := make([]int, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			codes[i] = post(l.server, PatternCompatSwitchOrg, orgBody(c.orgB), c.bearer).Code
		}()
	}
	close(start)
	wg.Wait()
	ok := 0
	for _, code := range codes {
		switch code {
		case http.StatusOK:
			ok++
		case http.StatusUnauthorized:
		default:
			t.Fatalf("codes %v: a switch answered %d", codes, code)
		}
	}
	if ok != 1 || l.liveIn(t, ctx, c.user, c.orgB) != 1 || l.live(t, ctx, c.predecessor.Session.ID) {
		t.Fatalf("codes %v, %d live in B: want one 200 and one live successor", codes, l.liveIn(t, ctx, c.user, c.orgB))
	}
}

// Multi-org navigation: A to B with the first bearer, then back to A with the
// successor's bearer; each switch leaves one live session.
func testSwitchOrgIntegrationSwitchBackAndForth(t *testing.T, ctx context.Context, l *switchOrgLive) {
	c := l.switchCase(t, ctx)
	r := post(l.server, PatternCompatSwitchOrg, orgBody(c.orgB), c.bearer)
	if r.Code != http.StatusOK {
		t.Fatalf("A to B: %d %s", r.Code, r.Body)
	}
	var answer struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(r.Body.Bytes(), &answer); err != nil {
		t.Fatal(err)
	}
	r = post(l.server, PatternCompatSwitchOrg, orgBody(c.orgA), map[string]string{"Authorization": "Bearer " + answer.AccessToken})
	if r.Code != http.StatusOK {
		t.Fatalf("B to A: %d %s", r.Code, r.Body)
	}
	if l.liveIn(t, ctx, c.user, c.orgB) != 0 || l.liveIn(t, ctx, c.user, c.orgA) != 1 {
		t.Fatalf("live in A %d, in B %d; want 1 and 0", l.liveIn(t, ctx, c.user, c.orgA), l.liveIn(t, ctx, c.user, c.orgB))
	}
}

// pausedReads is the reads with the membership read paused: it signals read
// once it has read (and, with the users owned, locked) the row, then waits for
// release.
type pausedReads struct {
	SwitchOrgReads
	read, release chan struct{}
}

func (p pausedReads) MemberRoleLockedOn(ctx context.Context, q session.Queryer, userID, orgID uuid.UUID) (*string, bool, error) {
	role, found, err := p.SwitchOrgReads.MemberRoleLockedOn(ctx, q, userID, orgID)
	close(p.read)
	<-p.release
	return role, found, err
}

// heldEnder is the session store a removal ends the pair's sessions through.
// With held set it pauses once: it closes held when the removal has locked
// and written the membership rows, and waits on proceed before it ends them.
type heldEnder struct {
	*session.Store
	once          sync.Once
	held, proceed chan struct{}
}

func (h *heldEnder) EndOrgInTx(ctx context.Context, tx audit.TxOps, principalID, organizationID uuid.UUID) (session.Ended, error) {
	if h.held != nil {
		h.once.Do(func() {
			close(h.held)
			select {
			case <-h.proceed:
			case <-ctx.Done():
			}
		})
	}
	return h.Store.EndOrgInTx(ctx, tx, principalID, organizationID)
}

// remove is auth-service's member removal itself (membership.Store.Remove),
// which writes the auth.memberships copy and so always changes guarded state,
// in its lock order: the principal FOR UPDATE (step 1), the public membership
// FOR UPDATE (5) and its delete, the auth copy (6), then the pair's sessions
// ended NOWAIT (6a). held and proceed pause it before 6a (heldEnder).
func (l *switchOrgLive) remove(ctx context.Context, user, org uuid.UUID, held, proceed chan struct{}) error {
	members, err := membership.New(membership.Config{Pool: l.runtime, Schema: l.schema, OwnsUsers: l.owned,
		Sessions: &heldEnder{Store: l.store, held: held, proceed: proceed}, Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		return err
	}
	_, err = members.Remove(ctx, membership.Change{OrgID: org, PrincipalID: user,
		Authorize: func(context.Context, audit.TxOps, membership.Locked) error { return nil }})
	return err
}

// O-B3 (G2): a switch and a member removal race in both orders. Removal
// first: the switch waits on the removal's principal lock, then reads no
// membership and is refused. Switch first: the removal waits on the switch's
// principal lock, then ends the new session. Never a live session in an org
// the member left. Users owned: auth-service is the remover.
func TestSwitchOrgIntegrationRaceWithRemoval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	t.Run("removal first", func(t *testing.T) {
		l := newSwitchOrgLive(t, ctx, true, nil)
		c := l.switchCase(t, ctx)
		held, proceed := make(chan struct{}), make(chan struct{})
		removed := make(chan error, 1)
		go func() { removed <- l.remove(ctx, c.user, c.orgB, held, proceed) }()
		select {
		case <-held:
		case err := <-removed:
			t.Fatalf("the removal ended (%v) before it held its locks", err)
		}
		answered := make(chan int, 1)
		go func() { answered <- post(l.server, PatternCompatSwitchOrg, orgBody(c.orgB), c.bearer).Code }()
		select {
		case code := <-answered:
			t.Fatalf("the switch answered %d while the removal held its locks", code)
		case <-time.After(300 * time.Millisecond):
		}
		close(proceed)
		if err := <-removed; err != nil {
			t.Fatalf("remove: %v", err)
		}
		if code := <-answered; code != http.StatusForbidden {
			t.Fatalf("switch after the removal: %d, want 403", code)
		}
		if l.liveIn(t, ctx, c.user, c.orgB) != 0 || !l.live(t, ctx, c.predecessor.Session.ID) {
			t.Fatal("want no session in B and the predecessor live")
		}
	})
	t.Run("switch first", func(t *testing.T) {
		read, release := make(chan struct{}), make(chan struct{})
		l := newSwitchOrgLive(t, ctx, true, func(r SwitchOrgReads) SwitchOrgReads {
			return pausedReads{SwitchOrgReads: r, read: read, release: release}
		})
		c := l.switchCase(t, ctx)
		answered := make(chan int, 1)
		go func() { answered <- post(l.server, PatternCompatSwitchOrg, orgBody(c.orgB), c.bearer).Code }()
		select {
		case <-read:
		case code := <-answered:
			t.Fatalf("the switch answered %d without reading the membership in its transaction", code)
		}
		removed := make(chan error, 1)
		go func() { removed <- l.remove(ctx, c.user, c.orgB, nil, nil) }()
		select {
		case err := <-removed:
			t.Fatalf("the removal finished (%v) while the switch held its locks", err)
		case <-time.After(300 * time.Millisecond):
		}
		close(release)
		if code := <-answered; code != http.StatusOK {
			t.Fatalf("switch: %d, want 200", code)
		}
		if err := <-removed; err != nil {
			t.Fatalf("remove: %v", err)
		}
		if n := l.liveIn(t, ctx, c.user, c.orgB); n != 0 {
			t.Fatalf("%d live sessions in B after the removal, want 0", n)
		}
	})
}
