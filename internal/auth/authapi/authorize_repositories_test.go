package authapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
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

// issueQuestion is today's approval-path question: acr.credential.issue on the organization.
func (h *listingFixture) issueQuestion(org string) map[string]any {
	return h.question("acr.credential.issue", "organization", "acme", org)
}

// listingAnswer is the comparable part of one decision document: everything but the decision id.
type listingAnswer struct {
	Allowed     bool
	Reason      string
	ExpiresAt   string
	Obligations string
}

func listingAnswerOf(t *testing.T, d wireDecision) listingAnswer {
	t.Helper()
	return listingAnswer{Allowed: d.Allowed, Reason: d.ReasonCode, ExpiresAt: d.ExpiresAt, Obligations: string(mustJSON(t, d.Obligations))}
}

func (a listingAnswer) String() string {
	return fmt.Sprintf("allowed=%v reason=%s obligations=%s", a.Allowed, a.Reason, a.Obligations)
}

// differential asks both paths with the same fixture and the same session: the listing route for each read action and
// /v1/authorize for acr.credential.issue. Every answer must be equal; the issue answer is returned.
func (h *listingFixture) differential(t *testing.T, row string, org string) listingAnswer {
	t.Helper()
	issue := listingAnswerOf(t, h.decideOne(h.issueQuestion(org)))
	for _, action := range listingActions {
		route := listingAnswerOf(t, h.list(h.listQuestion(action, org)))
		verdict := "EQUAL"
		if route != issue {
			verdict = "DIFF"
			t.Errorf("%s (%s): route %v, issue %v", row, action, route, issue)
		}
		t.Logf("DIFFERENTIAL row=%q action=%s route={%v} issue={%v} %s", row, action, route, issue, verdict)
	}
	return issue
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

// D1: the differential table. For every caller class the listing answer equals today's acr.credential.issue answer
// (the approval path), on the same fixture and session; each row also pins what that answer is.
func TestRepositoryListingEqualsTheIssueDecision(t *testing.T) {
	t.Run("member org-wide (membership default) in a 105-repository org", func(t *testing.T) {
		h := newListingFixture(t)
		trap := newCatalogTrap(t, h.member(h.org, "admin", nil), 105)
		h.plant(h.org, trap)
		if a := h.differential(t, "org-wide default 105", h.org); !a.Allowed || !strings.Contains(a.Obligations, `"repository_scope":"organization"`) {
			t.Fatalf("org-wide: %v, want repository_scope organization", a)
		}
		if trap.pointReads != 0 {
			t.Fatalf("%d repository identity reads for an org-wide reader", trap.pointReads)
		}
	})
	t.Run("member org-wide (live * grant) in a 300-repository org", func(t *testing.T) {
		h := newListingFixture(t)
		h.plant(h.org, newCatalogTrap(t, h.member(h.org, "member", []authorization.Grant{listedGrant(h.org, "*")}), 300))
		if a := h.differential(t, "org-wide * grant 300", h.org); !a.Allowed || !strings.Contains(a.Obligations, `"repository_scope":"organization"`) {
			t.Fatalf("org-wide: %v, want repository_scope organization", a)
		}
	})
	for _, n := range []int{99, 100, 101, 105, 256, 257} {
		t.Run(fmt.Sprintf("named reader with %d grants", n), func(t *testing.T) {
			h := newListingFixture(t)
			rows, want := ownGrants(h.org, n)
			h.plant(h.org, newCatalogTrap(t, h.member(h.org, "viewer", rows), 300))
			a := h.differential(t, fmt.Sprintf("named %d", n), h.org)
			if n > authorization.MaxRepositoryAllowlist {
				if a.Allowed || a.Reason != "repository_allowlist_too_large" || a.Obligations != "null" {
					t.Fatalf("%d grants: %v, want deny repository_allowlist_too_large with no list", n, a)
				}
				return
			}
			got := allowlistOf(t, h.list(h.listQuestion("acr.context.read", h.org)))
			if !a.Allowed || !slices.Equal(got, want) {
				t.Fatalf("%d grants: %v; allowlist of %d keys, want exactly the %d own keys", n, a, len(got), len(want))
			}
		})
	}
	t.Run("named reader with no live grant", func(t *testing.T) {
		h := newListingFixture(t)
		revoked := listedGrant(h.org, "acme/api")
		revoked.Revoked = true
		h.plant(h.org, h.member(h.org, "member", []authorization.Grant{revoked}))
		if a := h.differential(t, "named no live grant", h.org); !a.Allowed || !strings.Contains(a.Obligations, `"repository_allowlist":[]`) {
			t.Fatalf("no live grant: %v, want an allow with an empty allowlist", a)
		}
	})
	for _, role := range []string{"platform_admin", "platform_owner"} {
		t.Run(role+" with no membership", func(t *testing.T) {
			h := newListingFixture(t)
			h.plant(h.org, h.platformAdmin(h.org, role, authorization.StatusActive))
			a := h.differential(t, role+" no membership", h.org)
			if role == "platform_admin" && (!a.Allowed || !strings.Contains(a.Obligations, `"repository_scope":"organization"`)) {
				t.Fatalf("platform admin: %v, want the issue path's org-wide allow", a)
			}
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
			h.delegateAs(owner, "acr.credential.issue", "acr.context.read", "acr.evidence.read")
			a := h.differential(t, "impersonation org-wide, "+actor, h.org)
			if !owner && (a.Allowed || a.Reason != "impersonation_denied") {
				t.Fatalf("%v, want the issue path's impersonation_denied", a)
			}
		})
		t.Run("impersonation of a named reader, "+actor, func(t *testing.T) {
			h := newListingFixture(t)
			rows, _ := ownGrants(h.org, 105)
			h.plant(h.org, h.member(h.org, "member", rows))
			h.delegateAs(owner, "acr.credential.issue", "acr.context.read", "acr.evidence.read")
			a := h.differential(t, "impersonation named 105, "+actor, h.org)
			if !owner && (a.Allowed || a.Reason != "impersonation_denied") {
				t.Fatalf("%v, want the issue path's impersonation_denied", a)
			}
		})
	}
	t.Run("principal of another organization", func(t *testing.T) {
		h := newListingFixture(t)
		other := uuidOther()
		stranger := h.member(other, "admin", nil)
		stranger.membership = nil
		h.plant(other, stranger)
		if a := h.differential(t, "other org", other); a.Allowed || a.Reason != "no_membership" {
			t.Fatalf("other org: %v, want deny no_membership", a)
		}
	})
	t.Run("service account with grants", func(t *testing.T) {
		h := newListingFixture(t)
		account := h.member(h.org, "", []authorization.Grant{listedGrant(h.org, "acme/api")})
		account.membership = nil
		account.account = &authorization.ServiceAccount{OrgID: h.org, Status: authorization.StatusActive}
		h.plant(h.org, account)
		h.differential(t, "service account", h.org)
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
		t.Run("disabled (suspended) organization, "+name, func(t *testing.T) {
			h := newListingFixture(t)
			h.plant(h.org, s(h))
			a := h.differential(t, "disabled org, "+name, h.org)
			if name == "member" && (a.Allowed || a.Reason != "organization_inactive") {
				t.Fatalf("%v, want deny organization_inactive", a)
			}
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
	h.differential(t, "canonical keys", h.org)
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
	for _, action := range append([]string{"acr.credential.issue"}, listingActions...) {
		planted[action] = plantedAllow(&authorization.Obligations{TenantScope: h.org, RepositoryScope: authorization.RepositoryScopeOrganization})
	}
	rows, want := ownGrants(h.org, 3)
	h.plant(h.org, h.member(h.org, "member", rows))
	if got := allowlistOf(t, h.list(h.listQuestion("acr.context.read", h.org))); !slices.Equal(got, want) {
		t.Fatalf("allowlist %v, want the own keys %v", got, want)
	}
}
