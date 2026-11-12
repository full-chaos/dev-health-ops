package authapi

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
)

// The cells of POST /v1/authorize:repositories (CHAOS-13542): for a principal and an organization, the repositories it
// may read through acr.context.read and acr.evidence.read. The route literal is written out (not the constant) so the
// cells build, and fail, on a tree without the route.
const listingRoute = "/v1/authorize:repositories"

var listingActions = []string{"acr.context.read", "acr.evidence.read"}

// catalogTrap is a snapshot of an organization whose repository inventory (the identity rows of every repository of
// the organization, as a catalog would list them) is in reach. Reading the inventory fails the test: an org-wide reader's
// answer is the organization marker, never a list built from a catalog. Point reads of one grant's own repository id
// are counted, so an org-wide cell can also require none.
type catalogTrap struct {
	*fakeSnapshot
	t          *testing.T
	inventory  []string
	ids        map[string]string
	paths      map[string]string
	pointReads int
}

func newCatalogTrap(t *testing.T, s *fakeSnapshot, repos int) *catalogTrap {
	c := &catalogTrap{fakeSnapshot: s, t: t, ids: map[string]string{}, paths: map[string]string{}}
	for i := range repos {
		path := fmt.Sprintf("acme/catalog-%03d", i)
		c.inventory = append(c.inventory, path)
	}
	return c
}

// identity adds a live repository identity row: id names path.
func (c *catalogTrap) identity(id, path string) {
	c.ids[path], c.paths[id] = id, path
}

func (c *catalogTrap) RepositoryID(_, path string) (string, bool) {
	c.pointReads++
	id := c.ids[path]
	return id, id != ""
}

func (c *catalogTrap) PathForID(_, id string) (string, bool) {
	c.pointReads++
	path, ok := c.paths[id]
	return path, ok
}

// RepositoryPaths is the organization's repository catalog. No listing may call it.
func (c *catalogTrap) RepositoryPaths(string) ([]string, error) {
	c.t.Errorf("the listing read the organization's repository catalog (%d repositories)", len(c.inventory))
	return c.inventory, nil
}

// listingState serves the planted snapshots of the listing cells and the fixture's state for everything else.
type listingState struct {
	inner *fakeDecisionState
	snaps map[snapKey]authorization.GrantSnapshot
}

func (l *listingState) Read(ctx context.Context, fn func(DecisionTx) error) error {
	return l.inner.Read(ctx, func(tx DecisionTx) error { return fn(listingTx{tx.(fakeDecisionTx), l}) })
}

type listingTx struct {
	fakeDecisionTx
	l *listingState
}

func (t listingTx) Snapshot(ctx context.Context, principal uuid.UUID, org string) (authorization.GrantSnapshot, error) {
	if s, ok := t.l.snaps[snapKey{principal, org}]; ok {
		t.f.snapReads++
		if t.f.snapErr != nil {
			return nil, t.f.snapErr
		}
		return s, nil
	}
	return t.fakeDecisionTx.Snapshot(ctx, principal, org)
}

type listingFixture struct {
	*authorizeHandlerFixture
	ls *listingState
}

func newListingFixture(t *testing.T, opts ...func(*AuthorizeDeps)) *listingFixture {
	t.Helper()
	ls := &listingState{snaps: map[snapKey]authorization.GrantSnapshot{}}
	opts = append([]func(*AuthorizeDeps){func(d *AuthorizeDeps) {
		ls.inner = d.State.(*fakeDecisionState)
		d.State = ls
		d.ProfileRows = authorizeProfileDoc(t, PatternAuthorize, PatternAuthorizeBatch, listingRoute)
	}}, opts...)
	h := newAuthorizeHandlerFixture(t, opts...)
	h.routes[listingRoute] = h.routes[PatternAuthorize]
	return &listingFixture{authorizeHandlerFixture: h, ls: ls}
}

// member is an active membership snapshot of the fixture's principal in org with role and grants.
func (h *listingFixture) member(org, role string, grants []authorization.Grant) *fakeSnapshot {
	return &fakeSnapshot{
		principal: authorization.StatusActive, org: org, orgStatus: authorization.StatusActive,
		membership: &authorization.Membership{OrgID: org, Role: role, Status: authorization.StatusActive},
		grants:     grants, now: authorizeNow,
	}
}

func (h *listingFixture) plant(org string, s authorization.GrantSnapshot) {
	h.ls.snaps[snapKey{h.principal, org}] = s
}

func (h *listingFixture) listQuestion(action, org string) map[string]any {
	return h.question(action, "repository", "*", org)
}

func (h *listingFixture) list(q map[string]any) wireDecision {
	h.t.Helper()
	rec := h.post(listingRoute, h.workloadToken(nil), mustJSON(h.t, q))
	if rec.Code != http.StatusOK {
		h.t.Fatalf("POST %s: status %d body %s", listingRoute, rec.Code, rec.Body)
	}
	validateSurface(h.t, "authorization_decision.v1", rec.Body.Bytes())
	return decodeWire[wireDecision](h.t, rec)
}

func listedGrant(org, key string) authorization.Grant {
	return authorization.Grant{OrgID: org, Role: "member", ResourceKind: "repository", ResourceKey: key}
}

// ownGrants is n live grants of org, keys acme/repo-000.., with three rows that must never be listed: a revoked
// grant, an expired grant, and a live grant of another organization.
func ownGrants(org string, n int) ([]authorization.Grant, []string) {
	var rows []authorization.Grant
	var keys []string
	for i := n - 1; i >= 0; i-- {
		key := fmt.Sprintf("acme/repo-%03d", i)
		rows = append(rows, listedGrant(org, key))
		keys = append(keys, key)
	}
	revoked := listedGrant(org, "acme/revoked")
	revoked.Revoked = true
	expired := listedGrant(org, "acme/expired")
	expired.ExpiresAtUnixNano = authorizeNow
	rows = append(rows, revoked, expired, listedGrant(uuidOther(), "acme/other-org"))
	slices.Sort(keys)
	return rows, keys
}

func allowlistOf(t *testing.T, d wireDecision) []string {
	t.Helper()
	raw, ok := d.Obligations["repository_allowlist"].([]any)
	if !ok {
		t.Fatalf("no repository_allowlist in %+v", d)
	}
	out := make([]string, len(raw))
	for i, v := range raw {
		out[i] = v.(string)
	}
	return out
}

// listingAnswer is one route decision reduced to what a reader gets: allowed, reason, the repositories it reaches over
// the probe keys (all of them for repository_scope organization), the minimum assurance and the expiry.
type listingAnswer struct {
	Allowed   bool
	Reason    string
	Reach     []string
	Assurance string
	ExpiresAt string
}

func (a listingAnswer) String() string {
	return fmt.Sprintf("allowed=%v reason=%s reach=%d assurance=%q", a.Allowed, a.Reason, len(a.Reach), a.Assurance)
}

func (h *listingFixture) routeAnswer(t *testing.T, action, org string, probes []string) (listingAnswer, wireDecision) {
	t.Helper()
	d := h.list(h.listQuestion(action, org))
	a := listingAnswer{Allowed: d.Allowed, Reason: d.ReasonCode, Reach: []string{}, ExpiresAt: d.ExpiresAt}
	if d.Allowed {
		a.Assurance, _ = d.Obligations["minimum_assurance"].(string)
		if d.Obligations["repository_scope"] == "organization" {
			a.Reach = append(a.Reach, probes...)
		} else {
			listed := allowlistOf(t, d)
			for _, k := range probes {
				if slices.Contains(listed, k) {
					a.Reach = append(a.Reach, k)
				}
			}
		}
	}
	slices.Sort(a.Reach)
	return a, d
}

// perKeyAnswer is today's per-key read decision (POST /v1/authorize, resource repository <key>) for every probe key:
// the keys it allows, and the deny reasons, assurance levels and expiries it answers.
func (h *listingFixture) perKeyAnswer(t *testing.T, action, org string, probes []string) (allowed []string, reasons, assurance, expiry map[string]bool) {
	t.Helper()
	allowed, reasons, assurance, expiry = []string{}, map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, k := range probes {
		d := h.decideOne(h.question(action, "repository", k, org))
		if !d.Allowed {
			reasons[d.ReasonCode] = true
			continue
		}
		allowed = append(allowed, k)
		a, _ := d.Obligations["minimum_assurance"].(string)
		assurance[a], expiry[d.ExpiresAt] = true, true
	}
	slices.Sort(allowed)
	return allowed, reasons, assurance, expiry
}

// probeKeys is the key set both paths are asked about: the organization's inventory (as a catalog would list it), the
// reader's own keys, and keys the reader must never reach (revoked, expired, another organization's, never granted).
func probeKeys(inventory, own []string) []string {
	set := map[string]bool{"acme/revoked": true, "acme/expired": true, "acme/other-org": true, "acme/never-granted": true}
	for _, k := range append(slices.Clone(inventory), own...) {
		set[k] = true
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

func inventoryOf(n int) []string {
	out := make([]string, n)
	for i := range n {
		out[i] = fmt.Sprintf("acme/catalog-%03d", i)
	}
	return out
}

// differential asks both paths with the same fixture and the same session, for each read action: the listing route,
// and today's per-key read decision for every probe key. EQUAL: the route reaches exactly the keys the per-key reads
// allow; a route deny has the reason every per-key read answers; an allow carries the per-key reads' assurance and
// expiry. CAP: more than 256 own keys, the one difference the ruling names (the route answers the named cap error, each
// per-key read allows). Each row logs one DIFFERENTIAL line per action.
func (h *listingFixture) differential(t *testing.T, row, org string, probes []string, wantCap bool) listingAnswer {
	t.Helper()
	var first listingAnswer
	for i, action := range listingActions {
		route, _ := h.routeAnswer(t, action, org, probes)
		allowed, reasons, assurance, expiry := h.perKeyAnswer(t, action, org, probes)
		if i == 0 {
			first = route
		}
		verdict, why := "EQUAL", ""
		switch {
		case wantCap:
			verdict = "CAP"
			if route.Allowed || route.Reason != "repository_allowlist_too_large" || len(allowed) <= authorization.MaxRepositoryAllowlist {
				verdict, why = "DIFF", "want the route's repository_allowlist_too_large over more than 256 per-key allows"
			}
		case !slices.Equal(route.Reach, allowed):
			verdict, why = "DIFF", fmt.Sprintf("route reaches %d keys, per-key reads allow %d", len(route.Reach), len(allowed))
		case !route.Allowed && !(len(reasons) == 1 && reasons[route.Reason]):
			verdict, why = "DIFF", fmt.Sprintf("route deny %s, per-key deny reasons %v", route.Reason, reasons)
		case route.Allowed && len(allowed) > 0 && !(len(assurance) == 1 && assurance[route.Assurance]):
			verdict, why = "DIFF", fmt.Sprintf("route assurance %q, per-key %v", route.Assurance, assurance)
		case route.Allowed && len(allowed) > 0 && !(len(expiry) == 1 && expiry[route.ExpiresAt]):
			verdict, why = "DIFF", fmt.Sprintf("route expires_at %s, per-key %v", route.ExpiresAt, expiry)
		}
		if verdict == "DIFF" {
			t.Errorf("%s (%s): %s", row, action, why)
		}
		t.Logf("DIFFERENTIAL row=%q action=%s route={%v} per_key={allowed=%d/%d deny=%v assurance=%v} %s",
			row, action, route, len(allowed), len(probes), slices.Sorted(maps.Keys(reasons)), slices.Sorted(maps.Keys(assurance)), verdict)
	}
	return first
}

func (h *listingFixture) platformAdmin(org, role string, status authorization.Status) *fakeSnapshot {
	s := h.member(org, "", nil)
	s.membership, s.platformRole, s.orgStatus = nil, role, status
	return s
}

// delegateAs makes the fixture's session an impersonation of its principal in its organization; owner sets the
// actor's platform_owner role.
func (h *listingFixture) delegateAs(owner bool, permitted ...string) {
	h.delegate(permitted...)
	h.state.actors = map[uuid.UUID]ActorState{delegatedActor: {Active: true, CanImpersonate: true, Owner: owner}}
}

// D0: the route stays dark without its endpoint-profile row: the embedded document has none yet.
func TestRepositoryListingRouteIsDarkWithoutItsRow(t *testing.T) {
	f := newAuthorizeFixture(t)
	routes, err := AuthorizeRoutes(AuthorizeDeps{Callers: f.callers(), References: f.references(), State: &fakeDecisionState{},
		Evaluator: authorization.NewEvaluator(), Audit: (&fakeAudit{}).write, ProfileRows: authorizeProfileDoc(t, listingRoute)})
	if err != nil || len(routes) != 1 || routes[0].Pattern != listingRoute {
		t.Fatalf("a document with only the listing row mounted %v (%v), want the listing route", routes, err)
	}
	if routes, err = AuthorizeRoutes(AuthorizeDeps{Callers: f.callers(), References: f.references(), State: &fakeDecisionState{},
		Evaluator: authorization.NewEvaluator(), Audit: (&fakeAudit{}).write, ProfileRows: authorizeProfileDoc(t, PatternAuthorize)}); err != nil {
		t.Fatal(err)
	}
	for _, r := range routes {
		if r.Pattern == listingRoute {
			t.Fatalf("the listing route mounted without its row")
		}
	}
}

func wantOrgWide(t *testing.T, a listingAnswer, probes []string) {
	t.Helper()
	if !a.Allowed || len(a.Reach) != len(probes) {
		t.Fatalf("%v, want an org-wide allow over all %d probe keys", a, len(probes))
	}
}

func wantDeny(t *testing.T, a listingAnswer, reason string) {
	t.Helper()
	if a.Allowed || a.Reason != reason {
		t.Fatalf("%v, want deny %s", a, reason)
	}
}

// D1: the differential table (acp ruling of record on CHAOS-13542, 2026-11-11 11:05Z, point 2). For every caller class
// the route equals today's per-key READ decisions, for acr.context.read and acr.evidence.read, on the same fixture and
// session; each row also pins what that answer is.
func TestRepositoryListingEqualsThePerKeyReadDecisions(t *testing.T) {
	for _, tc := range []struct {
		role  string
		repos int
	}{{"viewer", 105}, {"member", 105}, {"admin", 105}, {"owner", 105}, {"admin", 300}} {
		t.Run(fmt.Sprintf("%s, membership default, %d-repository org", tc.role, tc.repos), func(t *testing.T) {
			h := newListingFixture(t)
			trap := newCatalogTrap(t, h.member(h.org, tc.role, nil), tc.repos)
			h.plant(h.org, trap)
			probes := probeKeys(inventoryOf(tc.repos), nil)
			before := trap.pointReads
			if _, d := h.routeAnswer(t, "acr.context.read", h.org, probes); d.Allowed && trap.pointReads != before {
				t.Fatalf("%d repository identity reads for an org-wide reader", trap.pointReads-before)
			}
			a := h.differential(t, fmt.Sprintf("%s default %d", tc.role, tc.repos), h.org, probes, false)
			if tc.role != "viewer" {
				wantOrgWide(t, a, probes)
			}
		})
	}
	for _, repos := range []int{105, 300} {
		t.Run(fmt.Sprintf("wildcard grant in a %d-repository org", repos), func(t *testing.T) {
			h := newListingFixture(t)
			h.plant(h.org, newCatalogTrap(t, h.member(h.org, "member", []authorization.Grant{listedGrant(h.org, "*")}), repos))
			probes := probeKeys(inventoryOf(repos), nil)
			wantOrgWide(t, h.differential(t, fmt.Sprintf("wildcard grant %d", repos), h.org, probes, false), probes)
		})
	}
	t.Run("exact grant on one repository", func(t *testing.T) {
		h := newListingFixture(t)
		h.plant(h.org, newCatalogTrap(t, h.member(h.org, "member", []authorization.Grant{listedGrant(h.org, "acme/catalog-007")}), 105))
		a := h.differential(t, "exact grant", h.org, probeKeys(inventoryOf(105), nil), false)
		if !a.Allowed || !slices.Equal(a.Reach, []string{"acme/catalog-007"}) {
			t.Fatalf("%v, want only acme/catalog-007", a)
		}
	})
	for _, n := range []int{99, 100, 101, 105, 256, 257} {
		t.Run(fmt.Sprintf("named reader with %d grants", n), func(t *testing.T) {
			h := newListingFixture(t)
			rows, want := ownGrants(h.org, n)
			h.plant(h.org, newCatalogTrap(t, h.member(h.org, "viewer", rows), 300))
			probes := probeKeys(inventoryOf(300), want)
			a := h.differential(t, fmt.Sprintf("named %d", n), h.org, probes, n > authorization.MaxRepositoryAllowlist)
			if n > authorization.MaxRepositoryAllowlist {
				if d := h.list(h.listQuestion("acr.context.read", h.org)); d.Obligations != nil {
					t.Fatalf("%d grants: %+v, want the cap deny with no obligations", n, d)
				}
				return
			}
			if got := allowlistOf(t, h.list(h.listQuestion("acr.context.read", h.org))); !a.Allowed || !slices.Equal(got, want) {
				t.Fatalf("%d grants: %v; allowlist of %d keys, want exactly the %d own keys", n, a, len(got), len(want))
			}
		})
	}
	t.Run("named reader with no live grant", func(t *testing.T) {
		h := newListingFixture(t)
		revoked := listedGrant(h.org, "acme/api")
		revoked.Revoked = true
		h.plant(h.org, h.member(h.org, "member", []authorization.Grant{revoked}))
		a := h.differential(t, "named no live grant", h.org, probeKeys(inventoryOf(105), []string{"acme/api"}), false)
		if got := allowlistOf(t, h.list(h.listQuestion("acr.context.read", h.org))); !a.Allowed || len(got) != 0 {
			t.Fatalf("no live grant: %v %v, want an allow with an empty allowlist", a, got)
		}
	})
	for _, role := range []string{"platform_admin", "platform_owner"} {
		t.Run(role+" with no membership", func(t *testing.T) {
			h := newListingFixture(t)
			h.plant(h.org, h.platformAdmin(h.org, role, authorization.StatusActive))
			wantDeny(t, h.differential(t, role+" no membership", h.org, probeKeys(inventoryOf(105), nil), false), "no_membership")
		})
	}
	for _, owner := range []bool{false, true} {
		actor := "admin actor"
		if owner {
			actor = "platform_owner actor"
		}
		t.Run("impersonation of an org-wide reader, "+actor, func(t *testing.T) {
			h := newListingFixture(t)
			h.plant(h.org, h.member(h.org, "admin", nil))
			h.delegateAs(owner, listingActions...)
			probes := probeKeys(inventoryOf(105), nil)
			a := h.differential(t, "impersonation org-wide, "+actor, h.org, probes, false)
			wantOrgWide(t, a, probes)
			if a.Assurance == "" {
				t.Fatalf("%v, want the session's minimum_assurance", a)
			}
		})
		t.Run("impersonation of a named reader, "+actor, func(t *testing.T) {
			h := newListingFixture(t)
			rows, want := ownGrants(h.org, 105)
			h.plant(h.org, h.member(h.org, "member", rows))
			h.delegateAs(owner, listingActions...)
			a := h.differential(t, "impersonation named 105, "+actor, h.org, probeKeys(inventoryOf(105), want), false)
			if !a.Allowed || !slices.Equal(a.Reach, want) || a.Assurance == "" {
				t.Fatalf("%v, want the 105 own keys with the session's minimum_assurance", a)
			}
		})
	}
	t.Run("impersonation without the read action permitted", func(t *testing.T) {
		h := newListingFixture(t)
		h.plant(h.org, h.member(h.org, "admin", nil))
		h.delegateAs(false, "acr.credential.issue")
		wantDeny(t, h.differential(t, "impersonation, read not permitted", h.org, probeKeys(inventoryOf(105), nil), false), "impersonation_denied")
	})
	t.Run("principal of another organization", func(t *testing.T) {
		h := newListingFixture(t)
		other := uuidOther()
		stranger := h.member(other, "admin", nil)
		stranger.membership = nil
		h.plant(other, stranger)
		wantDeny(t, h.differential(t, "other org", other, probeKeys(inventoryOf(105), nil), false), "no_membership")
	})
	t.Run("service account with grants", func(t *testing.T) {
		h := newListingFixture(t)
		account := h.member(h.org, "", []authorization.Grant{listedGrant(h.org, "acme/api")})
		account.membership = nil
		account.account = &authorization.ServiceAccount{OrgID: h.org, Status: authorization.StatusActive}
		h.plant(h.org, account)
		h.differential(t, "service account", h.org, probeKeys(inventoryOf(105), []string{"acme/api"}), false)
	})
	for name, s := range map[string]func(h *listingFixture) *fakeSnapshot{
		"member": func(h *listingFixture) *fakeSnapshot {
			s := h.member(h.org, "admin", nil)
			s.orgStatus = authorization.StatusSuspended
			return s
		},
		"platform_admin": func(h *listingFixture) *fakeSnapshot {
			return h.platformAdmin(h.org, "platform_admin", authorization.StatusSuspended)
		},
	} {
		t.Run("suspended organization, "+name, func(t *testing.T) {
			h := newListingFixture(t)
			h.plant(h.org, s(h))
			wantDeny(t, h.differential(t, "suspended org, "+name, h.org, probeKeys(inventoryOf(105), nil), false), "organization_inactive")
		})
	}
}

// D2: canonical keys: an id-bearing grant lists the live path of its id (never its stale stored path), an id with no
// live path lists nothing, and a key reached twice is listed once.
func TestRepositoryListingListsTheCanonicalKeys(t *testing.T) {
	h := newListingFixture(t)
	renamed := listedGrant(h.org, "acme/old-name")
	renamed.RepoID = "github:1"
	gone := listedGrant(h.org, "acme/deleted")
	gone.RepoID = "github:2"
	twice := listedGrant(h.org, "acme/api")
	twiceByID := listedGrant(h.org, "acme/api-before-rename")
	twiceByID.RepoID = "github:3"
	trap := newCatalogTrap(t, h.member(h.org, "member", []authorization.Grant{renamed, gone, twice, twiceByID}), 105)
	trap.identity("github:1", "acme/new-name")
	trap.identity("github:3", "acme/api")
	h.plant(h.org, trap)
	h.differential(t, "canonical keys", h.org, probeKeys(inventoryOf(105), []string{"acme/old-name", "acme/new-name", "acme/deleted", "acme/api", "acme/api-before-rename"}), false)
	got := allowlistOf(t, h.list(h.listQuestion("acr.context.read", h.org)))
	if want := []string{"acme/api", "acme/new-name"}; !slices.Equal(got, want) {
		t.Fatalf("allowlist %v, want %v", got, want)
	}
}

// D3: a store error fails closed: the call is a 503, never a list and never a deny.
func TestRepositoryListingFailsClosedOnAStoreError(t *testing.T) {
	h := newListingFixture(t)
	s := h.member(h.org, "member", nil)
	s.grantsErr = errors.New("grant read failed")
	h.plant(h.org, s)
	if rec := h.post(listingRoute, h.workloadToken(nil), mustJSON(t, h.listQuestion("acr.context.read", h.org))); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("grant read error: status %d body %s, want 503", rec.Code, rec.Body)
	}
	h2 := newListingFixture(t)
	h2.state.snapErr = errors.New("snapshot read failed")
	if rec := h2.post(listingRoute, h2.workloadToken(nil), mustJSON(t, h2.listQuestion("acr.context.read", h2.org))); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("snapshot read error: status %d body %s, want 503", rec.Code, rec.Body)
	}
}

// D4: the route lists; it never decides one repository, and an action that is not a repository read is
// resource_mismatch. A caller without a workload token is refused before any read.
func TestRepositoryListingRequestShape(t *testing.T) {
	h := newListingFixture(t)
	for name, q := range map[string]map[string]any{
		"one repository":     h.question("acr.context.read", "repository", "acme/api", h.org),
		"organization type":  h.question("acr.context.read", "organization", "acme", h.org),
		"no organization_id": h.question("acr.context.read", "repository", "*", ""),
		"platform type":      h.question("acr.context.read", "platform", "platform", ""),
	} {
		if rec := h.post(listingRoute, h.workloadToken(nil), mustJSON(t, q)); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status %d body %s, want 400", name, rec.Code, rec.Body)
		}
	}
	if d := h.list(h.listQuestion("acr.credential.issue", h.org)); d.Allowed || d.ReasonCode != "resource_mismatch" {
		t.Errorf("acr.credential.issue: %+v, want deny resource_mismatch", d)
	}
	before := h.state.snapReads
	if rec := h.post(listingRoute, "", mustJSON(t, h.listQuestion("acr.context.read", h.org))); rec.Code != http.StatusUnauthorized {
		t.Errorf("no bearer: status %d, want 401", rec.Code)
	}
	if rec := h.post(listingRoute, h.reference, mustJSON(t, h.listQuestion("acr.context.read", h.org))); rec.Code != http.StatusUnauthorized {
		t.Errorf("a user token as the caller: status %d, want 401", rec.Code)
	}
	if h.state.snapReads != before {
		t.Errorf("a refused caller read state")
	}
}

// D5: the listing never answers from the decision cache: a cached org-wide allow does not widen a named reader's list.
func TestRepositoryListingBypassesTheDecisionCache(t *testing.T) {
	planted := map[string]authorization.Decision{}
	h := newListingFixture(t, withPlanted(planted))
	for _, action := range listingActions {
		planted[action] = plantedAllow(&authorization.Obligations{TenantScope: h.org, RepositoryScope: authorization.RepositoryScopeOrganization})
	}
	rows, want := ownGrants(h.org, 3)
	h.plant(h.org, h.member(h.org, "member", rows))
	if got := allowlistOf(t, h.list(h.listQuestion("acr.context.read", h.org))); !slices.Equal(got, want) {
		t.Fatalf("allowlist %v, want the own keys %v", got, want)
	}
}

// B0: the embedded endpoint-profile document mounts the route.
func TestRepositoryListingRouteMountsFromTheEmbeddedDocument(t *testing.T) {
	f := newAuthorizeFixture(t)
	routes, err := AuthorizeRoutes(AuthorizeDeps{Callers: f.callers(), References: f.references(), State: &fakeDecisionState{},
		Evaluator: authorization.NewEvaluator(), Audit: (&fakeAudit{}).write})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range routes {
		if r.Method == http.MethodPost && r.Pattern == listingRoute {
			return
		}
	}
	t.Fatalf("POST %s is not mounted from the embedded endpoint-profile document", listingRoute)
}
