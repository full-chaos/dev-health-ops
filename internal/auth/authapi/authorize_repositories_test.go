package authapi

import (
	"context"
	"errors"
	"fmt"
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

// C0: the embedded endpoint-profile document mounts the route.
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

// C1: a named reader with 99, 100, 101, 105 and 256 grants gets exactly its own keys, sorted and canonical, in a
// 300-repository organization, with no catalog read.
func TestRepositoryListingNamedReaderGetsItsOwnKeys(t *testing.T) {
	for _, n := range []int{99, 100, 101, 105, 256} {
		for _, action := range listingActions {
			t.Run(fmt.Sprintf("%d/%s", n, action), func(t *testing.T) {
				h := newListingFixture(t)
				rows, want := ownGrants(h.org, n)
				trap := newCatalogTrap(t, h.member(h.org, "viewer", rows), 300)
				h.plant(h.org, trap)
				d := h.list(h.listQuestion(action, h.org))
				if !d.Allowed || d.ReasonCode != "allowed_by_repository_grant" {
					t.Fatalf("%d grants: %+v, want an allow by repository grant", n, d)
				}
				if _, org := d.Obligations["repository_scope"]; org {
					t.Fatalf("%d grants: a named reader got repository_scope: %+v", n, d.Obligations)
				}
				got := allowlistOf(t, d)
				if len(got) != n || !slices.Equal(got, want) {
					t.Fatalf("%d grants: allowlist of %d keys, want exactly the %d own keys", n, len(got), len(want))
				}
				if d.Obligations["tenant_scope"] != h.org {
					t.Fatalf("tenant_scope %v, want %s", d.Obligations["tenant_scope"], h.org)
				}
			})
		}
	}
}

// C2: 257 grants is the decision point's own refusal, which names the allowlist cap; never a truncated list.
func TestRepositoryListingOverTheCapIsThePDPRefusal(t *testing.T) {
	for _, action := range listingActions {
		h := newListingFixture(t)
		rows, _ := ownGrants(h.org, authorization.MaxRepositoryAllowlist+1)
		h.plant(h.org, newCatalogTrap(t, h.member(h.org, "member", rows), 300))
		d := h.list(h.listQuestion(action, h.org))
		if d.Allowed || d.ReasonCode != "repository_allowlist_too_large" || d.Obligations != nil {
			t.Fatalf("%s, 257 grants: %+v, want deny repository_allowlist_too_large with no list", action, d)
		}
	}
}

// C3: an org-wide reader (membership default, or a live wildcard grant) in a 105- and a 300-repository organization is
// "all": repository_scope organization, no allowlist, and no read of the organization's catalog.
func TestRepositoryListingOrgWideReaderIsAllWithNoCatalogRead(t *testing.T) {
	for _, repos := range []int{105, 300} {
		for name, grants := range map[string]func(org string) []authorization.Grant{
			"membership default": func(string) []authorization.Grant { return nil },
			"wildcard grant":     func(org string) []authorization.Grant { return []authorization.Grant{listedGrant(org, "*")} },
		} {
			for _, action := range listingActions {
				t.Run(fmt.Sprintf("%d/%s/%s", repos, name, action), func(t *testing.T) {
					h := newListingFixture(t)
					trap := newCatalogTrap(t, h.member(h.org, "admin", grants(h.org)), repos)
					h.plant(h.org, trap)
					d := h.list(h.listQuestion(action, h.org))
					if !d.Allowed || d.Obligations["repository_scope"] != "organization" {
						t.Fatalf("%+v, want an allow with repository_scope organization", d)
					}
					if _, listed := d.Obligations["repository_allowlist"]; listed {
						t.Fatalf("an org-wide reader got an allowlist: %+v", d.Obligations)
					}
					if trap.pointReads != 0 {
						t.Fatalf("%d repository identity reads for an org-wide reader, want 0", trap.pointReads)
					}
				})
			}
		}
	}
}

// C4: canonical keys are the decision point's: an id-bearing grant lists the live path of its id (never its stale
// stored path), an id with no live path lists nothing, and a key reached twice is listed once.
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
	got := allowlistOf(t, h.list(h.listQuestion("acr.context.read", h.org)))
	if want := []string{"acme/api", "acme/new-name"}; !slices.Equal(got, want) {
		t.Fatalf("allowlist %v, want %v", got, want)
	}
}

// C5: impersonation follows the decision point's delegation rules for the read actions.
func TestRepositoryListingUnderImpersonation(t *testing.T) {
	t.Run("named reader, action permitted: own keys at the session's assurance", func(t *testing.T) {
		h := newListingFixture(t)
		rows, want := ownGrants(h.org, 105)
		h.plant(h.org, h.member(h.org, "member", rows))
		h.delegate(listingActions...)
		for _, action := range listingActions {
			d := h.list(h.listQuestion(action, h.org))
			if !d.Allowed || !slices.Equal(allowlistOf(t, d), want) || d.Obligations["minimum_assurance"] != "aal1" {
				t.Fatalf("%s: %+v, want the 105 own keys with minimum_assurance aal1", action, d)
			}
		}
	})
	t.Run("action not permitted by the session: impersonation_denied", func(t *testing.T) {
		h := newListingFixture(t)
		rows, _ := ownGrants(h.org, 5)
		h.plant(h.org, h.member(h.org, "member", rows))
		h.delegate("acr.evidence.read")
		if d := h.list(h.listQuestion("acr.context.read", h.org)); d.Allowed || d.ReasonCode != "impersonation_denied" {
			t.Fatalf("%+v, want deny impersonation_denied", d)
		}
	})
	t.Run("org-wide subject: impersonation_denied, as the decision point refuses a delegated *", func(t *testing.T) {
		h := newListingFixture(t)
		h.plant(h.org, h.member(h.org, "admin", nil))
		h.delegate(listingActions...)
		for _, action := range listingActions {
			d := h.list(h.listQuestion(action, h.org))
			if d.Allowed || d.ReasonCode != "impersonation_denied" {
				t.Fatalf("%s: %+v, want deny impersonation_denied", action, d)
			}
			// The per-key route gives the same answer to the same session for the whole organization.
			pdp := h.decideOne(h.question(action, "repository", "*", h.org))
			if pdp.Allowed || pdp.ReasonCode != d.ReasonCode {
				t.Fatalf("%s: /v1/authorize answers %+v for *, the listing %+v", action, pdp, d)
			}
		}
	})
	t.Run("session for another organization: delegation_invalid", func(t *testing.T) {
		h := newListingFixture(t)
		other := uuidOther()
		h.plant(other, h.member(other, "member", []authorization.Grant{listedGrant(other, "acme/api")}))
		h.delegate(listingActions...)
		if d := h.list(h.listQuestion("acr.context.read", other)); d.Allowed || d.ReasonCode != "delegation_invalid" {
			t.Fatalf("%+v, want deny delegation_invalid", d)
		}
	})
}

// C6: a platform admin with no membership row is refused the read listing, as the decision point refuses it each
// repository read (no repository action takes the platform-admin flag: legacy parity, product rule 1).
func TestRepositoryListingPlatformAdminWithoutMembership(t *testing.T) {
	for _, role := range []string{"platform_admin", "platform_owner"} {
		h := newListingFixture(t)
		s := h.member(h.org, "", nil)
		s.membership, s.platformRole = nil, role
		h.plant(h.org, s)
		for _, action := range listingActions {
			d := h.list(h.listQuestion(action, h.org))
			if d.Allowed || d.ReasonCode != "no_membership" {
				t.Fatalf("%s %s: %+v, want deny no_membership", role, action, d)
			}
			pdp := h.decideOne(h.question(action, "repository", "acme/api", h.org))
			if pdp.Allowed || pdp.ReasonCode != d.ReasonCode {
				t.Fatalf("%s %s: /v1/authorize answers %+v, the listing %+v", role, action, pdp, d)
			}
		}
	}
}

// C7: a principal of another organization is refused: a user with no membership in the asked organization, and a
// service account whose account row is in another organization.
func TestRepositoryListingRefusesAPrincipalOfAnotherOrganization(t *testing.T) {
	h := newListingFixture(t)
	other := uuidOther()
	stranger := h.member(other, "admin", nil)
	stranger.membership = nil
	h.plant(other, stranger)
	if d := h.list(h.listQuestion("acr.context.read", other)); d.Allowed || d.ReasonCode != "no_membership" {
		t.Fatalf("user of another org: %+v, want deny no_membership", d)
	}
	account := h.member(other, "", []authorization.Grant{listedGrant(other, "acme/api")})
	account.membership = nil
	account.account = &authorization.ServiceAccount{OrgID: h.org, Status: authorization.StatusActive}
	h.plant(other, account)
	if d := h.list(h.listQuestion("acr.context.read", other)); d.Allowed || d.ReasonCode != "org_mismatch" {
		t.Fatalf("service account of another org: %+v, want deny org_mismatch", d)
	}
}

// C8: a store error fails closed: the call is a 503, never a list and never a deny.
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

// C9: the route lists; it never decides one repository, and an action that is not a repository read is the decision
// point's resource_mismatch. A caller without a workload token is refused before any read.
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

// C10: the listing never answers from the decision cache: a cached org-wide allow of the action does not widen a
// named reader's own list.
func TestRepositoryListingBypassesTheDecisionCache(t *testing.T) {
	planted := map[string]authorization.Decision{}
	h := newListingFixture(t, withPlanted(planted))
	planted["acr.context.read"] = plantedAllow(&authorization.Obligations{TenantScope: h.org, RepositoryScope: authorization.RepositoryScopeOrganization})
	rows, want := ownGrants(h.org, 3)
	h.plant(h.org, h.member(h.org, "member", rows))
	if got := allowlistOf(t, h.list(h.listQuestion("acr.context.read", h.org))); !slices.Equal(got, want) {
		t.Fatalf("allowlist %v, want the own keys %v", got, want)
	}
}
