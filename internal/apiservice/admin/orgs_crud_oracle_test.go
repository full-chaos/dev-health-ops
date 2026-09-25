//go:build integration

package admin_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestOrgCRUDMatchesThePythonAPI is the venue-oracle proof for CHAOS-6305's
// org and member routes: org list/get/create/patch, member
// list/add/patch-role/remove, and transfer-ownership. User routes have
// their own oracle test in CHAOS-6304 (users_oracle_test.go); this test
// seeds users directly by SQL only as membership targets. DELETE
// /orgs/{org_id} has its own oracle test, CHAOS-6306's
// orgdeletion_oracle_test.go.
func TestOrgCRUDMatchesThePythonAPI(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-org-crud-flow-32-bytes!!!"

	orgID := uuid.New()
	ownerID := uuid.New()
	memberID := uuid.New()
	newMemberID := uuid.New()
	superID := uuid.New()
	inviterTargetID := uuid.New()
	emptyRoleTargetID := uuid.New()
	nullRoleTargetID := uuid.New()

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   root,
		JWTKey: jwtKey,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, v *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			exec := func(sql string, args ...any) {
				t.Helper()
				if _, err := admin.Exec(ctx, sql, args...); err != nil {
					t.Fatalf("seed: %v\n%s", err, sql)
				}
			}
			exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, 'venue-org-3', 'Venue Org 3', 'community', 'stripe', true, now(), now())`, orgID)
			// An existing org_licenses row for the "patch org tier" case:
			// _sync_license_tier only updates a PRE-EXISTING row, never
			// inserts one, so this seed is required for that case to
			// exercise the sync path at all.
			exec(`INSERT INTO org_licenses (id, org_id, tier, is_valid, license_type, managed_by, created_at, updated_at)
VALUES ($1, $2, 'community', true, 'saas', 'stripe', now(), now())`, uuid.New(), orgID)
			for _, row := range []struct {
				id    uuid.UUID
				email string
				super bool
			}{
				{ownerID, "venue-owner@example.com", false},
				{memberID, "venue-member@example.com", false},
				{newMemberID, "venue-newmember@example.com", false},
				{superID, "venue-super@example.com", true},
				{inviterTargetID, "venue-invitertarget@example.com", false},
				{emptyRoleTargetID, "venue-emptyroletarget@example.com", false},
				{nullRoleTargetID, "venue-nullroletarget@example.com", false},
			} {
				exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, $3, 0, now(), now())`, row.id, row.email, row.super)
			}
			exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'owner', now(), now(), now())`, uuid.New(), orgID, ownerID)
			exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'member', now(), now(), now())`, uuid.New(), orgID, memberID)
			exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'member', now(), now(), now())`, uuid.New(), orgID, newMemberID)
			return map[string]map[string]any{
				"owner": {"user_id": ownerID.String(), "email": "venue-owner@example.com", "org_id": orgID.String(), "role": "owner"},
				"super": {"user_id": superID.String(), "email": "venue-super@example.com", "is_superuser": true},
			}
		},
	})

	bearer := func(name string) string { return "Bearer " + venue.Tokens[name] }
	jsonHeaders := func(name string) map[string]string {
		return map[string]string{"Authorization": bearer(name), "Content-Type": "application/json"}
	}
	authHeaders := func(name string) map[string]string { return map[string]string{"Authorization": bearer(name)} }

	requests := []venueoracle.Request{
		// Organizations (superuser only).
		{Name: "list orgs", Method: "GET", Path: "/api/v1/admin/orgs", Headers: authHeaders("super")},
		// An explicitly empty int query value is present, not absent --
		// FastAPI 422s it, it does not fall back to the default. A live
		// round found Go treating "limit=" as absent.
		{Name: "list orgs empty limit", Method: "GET", Path: "/api/v1/admin/orgs?limit=", Headers: authHeaders("super")},
		// FastAPI's bool query coercion is case-insensitive ("YeS" is
		// True) -- a live round found Go's exact-case switch rejecting it.
		{Name: "list orgs mixed-case bool", Method: "GET", Path: "/api/v1/admin/orgs?active_only=YeS", Headers: authHeaders("super")},
		// A repeated query key resolves to FastAPI's LAST value, never Go's
		// stdlib url.Values.Get's first -- a live round found this reversed
		// for limit/active_only/role. Positioned before any org-creating
		// request below: the list body is a top-level JSON ARRAY, which
		// this test's own Normalize helper cannot redact field-by-field
		// (it only unwraps a top-level object), so every case that lists
		// orgs must run while the only matching row is the seeded org
		// (deterministic id/timestamps, copied byte-identical to both
		// planes) -- never after a create has minted a fresh, genuinely
		// per-plane-random id.
		{Name: "list orgs duplicate limit", Method: "GET", Path: "/api/v1/admin/orgs?limit=0&limit=1", Headers: authHeaders("super")},
		// FastAPI's int query coercion trims surrounding whitespace (it is
		// not a bare strconv.Atoi) -- a live round found Go 422ing this.
		{Name: "list orgs padded limit", Method: "GET", Path: "/api/v1/admin/orgs?limit=%201%20", Headers: authHeaders("super")},
		{Name: "get org", Method: "GET", Path: "/api/v1/admin/orgs/" + orgID.String(), Headers: authHeaders("super")},
		{Name: "get org as non-superuser refused", Method: "GET", Path: "/api/v1/admin/orgs/" + orgID.String(), Headers: authHeaders("owner")},
		{Name: "create org", Method: "POST", Path: "/api/v1/admin/orgs", Headers: jsonHeaders("super"),
			Body: venueoracle.B64(`{"name":"A New Org"}`)},
		// OrganizationCreate.settings: dict[str, Any] = Field(default_factory=dict)
		// -- a present object is stored verbatim, never dropped.
		{Name: "create org with settings", Method: "POST", Path: "/api/v1/admin/orgs", Headers: jsonHeaders("super"),
			Body: venueoracle.B64(`{"name":"Org With Settings","settings":{"flag":true}}`)},
		// A response_model body renders floats as pydantic-core dump_json
		// does (1e-7, 0.00001, 1e+21), not as json.dumps (1e-07, 1e-05).
		{Name: "create org with float settings", Method: "POST", Path: "/api/v1/admin/orgs", Headers: jsonHeaders("super"),
			Body: venueoracle.B64(`{"name":"Org With Float Settings","settings":{"tiny":1e-7,"edge":0.00001,"below":9.99e-6,"big":1e21,"neg":-2.5e-9,"whole":3.0}}`)},
		// validate_name: pydantic strips the name and rejects an
		// all-whitespace result -- a live round found Go accepting this.
		{Name: "create org whitespace name", Method: "POST", Path: "/api/v1/admin/orgs", Headers: jsonHeaders("super"),
			Body: venueoracle.B64(`{"name":"   "}`)},
		// OrganizationService.create inserts the org and its owner
		// membership in ONE session/transaction -- a valid but nonexistent
		// owner_user_id fails the membership insert, and the org insert
		// must not survive that failure either. A live round found Go
		// leaving a committed, ownerless org behind.
		{Name: "create org owner insert fails", Method: "POST", Path: "/api/v1/admin/orgs", Headers: jsonHeaders("super"),
			Body: venueoracle.B64(fmt.Sprintf(`{"name":"Orphan Owner Org","owner_user_id":%q}`, uuid.New().String()))},
		// A MALFORMED owner_user_id is a different failure than a
		// well-formed-but-nonexistent one above: uuid.UUID() itself raises,
		// unhandled, all the way to a generic 500 -- a live round found Go
		// silently skipping the membership insert and still committing the
		// org (201).
		{Name: "create org malformed owner id", Method: "POST", Path: "/api/v1/admin/orgs", Headers: jsonHeaders("super"),
			Body: venueoracle.B64(`{"name":"Malformed Owner Org","owner_user_id":"not-a-uuid"}`)},
		// description is genuinely Optional (`str | None`): a present ""
		// is a REAL value, not null -- a live round found Go storing null.
		{Name: "create org empty description", Method: "POST", Path: "/api/v1/admin/orgs", Headers: jsonHeaders("super"),
			Body: venueoracle.B64(`{"name":"Org Empty Description","description":""}`)},
		// _slugify keeps every Unicode \w character, not just ASCII -- a
		// live round found Go dropping non-ASCII letters entirely.
		{Name: "create org unicode name", Method: "POST", Path: "/api/v1/admin/orgs", Headers: jsonHeaders("super"),
			Body: venueoracle.B64(`{"name":"Café 東京"}`)},
		// CHAOS-6712: Python's str.strip()/str.lower()/\s are NOT Go's. U+001C..U+001F
		// are whitespace to Python only: an all-FS name is "Workspace name is
		// required", the same characters around a name are stripped, in the middle
		// they separate slug words, and U+0130 lowers to two code points.
		{Name: "create org control-whitespace name", Method: "POST", Path: "/api/v1/admin/orgs", Headers: jsonHeaders("super"),
			Body: venueoracle.B64(`{"name":"\u001c"}`)},
		{Name: "create org name padded with control whitespace", Method: "POST", Path: "/api/v1/admin/orgs", Headers: jsonHeaders("super"),
			Body: venueoracle.B64(`{"name":"\u001f\u001eStripped Name\u001c"}`)},
		{Name: "create org name with control whitespace inside", Method: "POST", Path: "/api/v1/admin/orgs", Headers: jsonHeaders("super"),
			Body: venueoracle.B64(`{"name":"Left\u001cRight"}`)},
		{Name: "create org dotted capital I name", Method: "POST", Path: "/api/v1/admin/orgs", Headers: jsonHeaders("super"),
			Body: venueoracle.B64(`{"name":"\u001c\u0130stanbul\u001f"}`)},
		{Name: "patch org", Method: "PATCH", Path: "/api/v1/admin/orgs/" + orgID.String(), Headers: jsonHeaders("super"),
			Body: venueoracle.B64(`{"description":"updated description"}`)},
		{Name: "patch org not found", Method: "PATCH", Path: "/api/v1/admin/orgs/" + uuid.New().String(), Headers: jsonHeaders("super"),
			Body: venueoracle.B64(`{"description":"x"}`)},
		{Name: "patch org tier", Method: "PATCH", Path: "/api/v1/admin/orgs/" + orgID.String(), Headers: jsonHeaders("super"),
			Body: venueoracle.B64(`{"tier":"enterprise"}`)},
		// OrganizationUpdate.settings is genuinely Optional (`dict | None`):
		// a present non-object value is a dict_type 422 -- a live round
		// found Go marshaling any JSON value with no type check at all.
		{Name: "patch org invalid settings type", Method: "PATCH", Path: "/api/v1/admin/orgs/" + orgID.String(), Headers: jsonHeaders("super"),
			Body: venueoracle.B64(`{"settings":[]}`)},

		// Members.
		{Name: "list members", Method: "GET", Path: "/api/v1/admin/orgs/" + orgID.String() + "/members", Headers: authHeaders("owner")},
		{Name: "add member missing user", Method: "POST", Path: "/api/v1/admin/orgs/" + orgID.String() + "/members", Headers: jsonHeaders("owner"),
			Body: venueoracle.B64(fmt.Sprintf(`{"user_id":%q,"role":"member"}`, uuid.New().String()))},
		{Name: "add member already exists", Method: "POST", Path: "/api/v1/admin/orgs/" + orgID.String() + "/members", Headers: jsonHeaders("owner"),
			Body: venueoracle.B64(fmt.Sprintf(`{"user_id":%q,"role":"member"}`, memberID.String()))},
		// MembershipService.add_member: `uuid.UUID(invited_by_id)` is a
		// bare, un-try/excepted call -- any ValueError propagates to the
		// router's 400. A live round found Go silently discarding this and
		// creating the membership anyway, without its inviter.
		{Name: "add member malformed inviter id", Method: "POST", Path: "/api/v1/admin/orgs/" + orgID.String() + "/members", Headers: jsonHeaders("owner"),
			Body: venueoracle.B64(fmt.Sprintf(`{"user_id":%q,"role":"member","invited_by_id":"not-a-uuid"}`, inviterTargetID.String()))},
		// role is a pydantic DEFAULT (`str = "member"`): a present "" is a
		// real, distinct-from-default value; a present null is a
		// string_type 422 -- a live round found Go collapsing "" to the
		// default and silently accepting null.
		{Name: "add member empty role", Method: "POST", Path: "/api/v1/admin/orgs/" + orgID.String() + "/members", Headers: jsonHeaders("owner"),
			Body: venueoracle.B64(fmt.Sprintf(`{"user_id":%q,"role":""}`, emptyRoleTargetID.String()))},
		{Name: "add member null role", Method: "POST", Path: "/api/v1/admin/orgs/" + orgID.String() + "/members", Headers: jsonHeaders("owner"),
			Body: venueoracle.B64(fmt.Sprintf(`{"user_id":%q,"role":null}`, nullRoleTargetID.String()))},
		{Name: "patch member role", Method: "PATCH", Path: "/api/v1/admin/orgs/" + orgID.String() + "/members/" + newMemberID.String(),
			Headers: jsonHeaders("owner"), Body: venueoracle.B64(`{"role":"admin"}`)},
		{Name: "patch member role invalid", Method: "PATCH", Path: "/api/v1/admin/orgs/" + orgID.String() + "/members/" + newMemberID.String(),
			Headers: jsonHeaders("owner"), Body: venueoracle.B64(`{"role":"not-a-role"}`)},
		// Same duplicate-query-key rule as list orgs above, for the
		// members-list `role` filter.
		{Name: "list members duplicate role", Method: "GET", Path: "/api/v1/admin/orgs/" + orgID.String() + "/members?role=owner&role=member",
			Headers: authHeaders("owner")},
		{Name: "remove member", Method: "DELETE", Path: "/api/v1/admin/orgs/" + orgID.String() + "/members/" + newMemberID.String(),
			Headers: authHeaders("owner")},
		{Name: "remove last owner refused", Method: "DELETE", Path: "/api/v1/admin/orgs/" + orgID.String() + "/members/" + ownerID.String(),
			Headers: authHeaders("owner")},
	}
	python := venue.ServePython(t, requests)
	goBase, _ := startGoServer(t, ctx, venue, jwtKey)

	receipt := venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{
		Normalize: func(request venueoracle.Request, body string) string {
			// joined_at: a fresh add-member INSERT sets it independently on
			// each plane (now(), genuinely wall-clock-different), same
			// reason id/created_at/updated_at are redacted -- a seeded
			// membership's own joined_at is copied byte-identical from the
			// source Postgres and never needed this, which is why the gap
			// was invisible until a live round's own new add-member case.
			for _, field := range []string{"id", "created_at", "updated_at", "expires_at", "joined_at"} {
				body = redactField(t, body, field)
			}
			return body
		},
	})
	t.Log(receipt)

	// Finding: OrganizationService.create's org insert and its owner
	// membership insert are one transaction -- a failed owner insert (a
	// valid but nonexistent owner_user_id) must leave ZERO matching
	// organizations behind on both planes, never a committed orphan.
	orphanQuery := `SELECT count(*) FROM organizations WHERE name = 'Orphan Owner Org'`
	sourceOrphans := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), orphanQuery)
	goOrphans := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), orphanQuery)
	if sourceOrphans != goOrphans {
		t.Errorf("orphan organization count differs after a failed owner insert:\n python: %s\n go:     %s", sourceOrphans, goOrphans)
	}

	// Finding: a tier PATCH that actually changes the tier must sync a
	// PRE-EXISTING org_licenses row's own tier/managed_by, on both planes.
	licenseQuery := fmt.Sprintf(`SELECT tier, managed_by, coalesce(features_override::text, '<null>'), coalesce(limits_override::text, '<null>') FROM org_licenses WHERE org_id = '%s'`, orgID)
	sourceLicense := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), licenseQuery)
	goLicense := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), licenseQuery)
	if sourceLicense != goLicense {
		t.Errorf("org_licenses row differs after a tier patch:\n python: %s\n go:     %s", sourceLicense, goLicense)
	}

	// transfer-ownership is NOT diffed against Python: it is the one ruled,
	// intentional shape divergence in this PR (team-lead: "Go serves the
	// web's shape... no Python follow-up"). The web calls
	// POST /orgs/{org_id}/transfer-ownership with no from_user_id path
	// segment; Python's own route is
	// POST /orgs/{org_id}/transfer-ownership/{from_user_id}, so that path on
	// the Python plane matches NO route at all and 404s at the router, never
	// reaching transfer_ownership's own logic -- proving the Python path is
	// genuinely dead code, not a route this test can honestly diff. Assert
	// only the Go plane's own documented behavior: resolve the org's current
	// owner server-side, then fail the same way Python's transfer_ownership
	// would for a non-member target.
	transferToNonMember := venueoracle.Request{
		Name: "transfer ownership to non-member (go-only, web shape)", Method: "POST",
		Path:    "/api/v1/admin/orgs/" + orgID.String() + "/transfer-ownership",
		Headers: jsonHeaders("owner"), Body: venueoracle.B64(fmt.Sprintf(`{"new_owner_user_id":%q}`, uuid.New().String())),
	}
	if got := venueoracle.Do(t, goBase, transferToNonMember); got.Status != 400 || got.Body != `{"detail":"Target user is not a member"}` {
		t.Errorf("transfer ownership to non-member: got %d %s, want 400 Target user is not a member", got.Status, got.Body)
	}
}
