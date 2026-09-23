//go:build integration

package admin_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestUserCRUDAndPasswordChangeMatchesThePythonAPI is the venue-oracle
// proof for CHAOS-6304's 6 user routes: list/get/create/patch/delete and
// set_user_password (the audit-writing route team-lead named explicitly
// for row-diff proof). The org and its members are seeded directly by SQL
// -- no org route is exercised here, that is CHAOS-6305's own oracle.
func TestUserCRUDAndPasswordChangeMatchesThePythonAPI(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-user-crud-flow-32-bytes!!"
	const adminPlaintextPassword = "correct horse battery staple 9"

	orgID := uuid.New()
	adminID := uuid.New()
	memberID := uuid.New()

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
			adminHash, err := bcrypt.GenerateFromPassword([]byte(adminPlaintextPassword), bcrypt.DefaultCost)
			if err != nil {
				t.Fatalf("bcrypt: %v", err)
			}
			exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, 'venue-users-org', 'Venue Users Org', 'community', 'stripe', true, now(), now())`, orgID)
			exec(`INSERT INTO users (id, email, password_hash, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'venue-uadmin@example.com', $2, true, true, false, 0, now(), now())`, adminID, string(adminHash))
			exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'venue-umember@example.com', true, true, false, 0, now(), now())`, memberID)
			exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'admin', now(), now(), now())`, uuid.New(), orgID, adminID)
			exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'member', now(), now(), now())`, uuid.New(), orgID, memberID)
			return map[string]map[string]any{
				"admin": {"user_id": adminID.String(), "email": "venue-uadmin@example.com", "org_id": orgID.String(), "role": "admin"},
			}
		},
	})

	jsonHeaders := map[string]string{"Authorization": "Bearer " + venue.Tokens["admin"], "Content-Type": "application/json"}
	authHeaders := map[string]string{"Authorization": "Bearer " + venue.Tokens["admin"]}

	requests := []venueoracle.Request{
		{Name: "list users by org", Method: "GET", Path: "/api/v1/admin/users",
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["admin"], "X-Org-Id": orgID.String()}},
		// A repeated query key resolves to FastAPI's LAST value, never Go's
		// stdlib url.Values.Get's first -- a live round found this
		// reversed for limit/active_only/role/q across the admin routes
		// (queryLastValue, shared by queryInt/queryBool/querySearch).
		// Positioned before "create user" below: list's body is a
		// top-level JSON array this test's own Normalize cannot redact
		// field-by-field, so it must run while only the seeded, byte-
		// identical-on-both-planes rows exist.
		{Name: "list users duplicate limit", Method: "GET", Path: "/api/v1/admin/users?limit=0&limit=1",
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["admin"], "X-Org-Id": orgID.String()}},
		// FastAPI's int query coercion trims surrounding whitespace (it is
		// not a bare strconv.Atoi) -- a live round found Go 422ing this.
		{Name: "list users padded limit", Method: "GET", Path: "/api/v1/admin/users?limit=%201%20",
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["admin"], "X-Org-Id": orgID.String()}},
		{Name: "get user", Method: "GET", Path: "/api/v1/admin/users/" + memberID.String(), Headers: authHeaders},
		{Name: "get user not found", Method: "GET", Path: "/api/v1/admin/users/" + uuid.New().String(), Headers: authHeaders},
		{Name: "create user", Method: "POST", Path: "/api/v1/admin/users", Headers: jsonHeaders,
			Body: venueoracle.B64(`{"email":"venue-newuser@example.com","password":"a brand new password 7"}`)},
		// create_user carries no route-level auth dependency of its own --
		// admin/router.py's whole router requires require_admin -- so an
		// unauthenticated request (no Authorization header at all) must
		// 401 the same as any other admin route, never succeed. A live
		// round found this route registered Public, letting an
		// unauthenticated caller create an arbitrary user, including
		// is_superuser:true.
		{Name: "unauthenticated create user", Method: "POST", Path: "/api/v1/admin/users",
			Headers: map[string]string{"Content-Type": "application/json"},
			Body:    venueoracle.B64(`{"email":"venue-unauth-root@example.com","password":"a brand new password 9","is_superuser":true}`)},
		// A whitespace-only username is truthy in Python (any non-empty
		// string is), so UserService.create lowers/strips it to "" and
		// stores/returns an EMPTY STRING, never null -- a live round found
		// Go returning null here.
		{Name: "create user whitespace username", Method: "POST", Path: "/api/v1/admin/users", Headers: jsonHeaders,
			Body: venueoracle.B64(`{"email":"venue-username-ws@example.com","username":" "}`)},
		// auth_provider is `str = "local"`, a pydantic DEFAULT, not
		// Optional: an explicit "" is a valid, present string and is
		// stored verbatim, never coerced to the default -- a live round
		// found Go defaulting to "local" here.
		{Name: "create user empty auth_provider", Method: "POST", Path: "/api/v1/admin/users", Headers: jsonHeaders,
			Body: venueoracle.B64(`{"email":"venue-authprovider-empty@example.com","auth_provider":""}`)},
		// auth_provider/is_verified are non-Optional pydantic fields with a
		// default (`str = "local"` / `bool = False`): a present null is a
		// TYPE error there, not a silent fall-back to the default -- a
		// live round found Go accepting this as 201.
		{Name: "create user null-typed defaulted fields", Method: "POST", Path: "/api/v1/admin/users", Headers: jsonHeaders,
			Body: venueoracle.B64(`{"email":"venue-nulltyped@example.com","auth_provider":null,"is_verified":null}`)},
		{Name: "patch user", Method: "PATCH", Path: "/api/v1/admin/users/" + memberID.String(), Headers: jsonHeaders,
			Body: venueoracle.B64(`{"full_name":"A New Name"}`)},
		// pydantic's bool validator is lax: it coerces the JSON int 1/0 to
		// True/False, not just a native JSON bool -- a live round found Go
		// answering 422 bool_type for this.
		{Name: "patch user int-coerced bool", Method: "PATCH", Path: "/api/v1/admin/users/" + memberID.String(), Headers: jsonHeaders,
			Body: venueoracle.B64(`{"is_active":1}`)},
		// request_metadata is compared row-for-row below (audit_logs), so
		// this request sets explicit, stable User-Agent/X-Forwarded-For/
		// X-Request-ID headers: leaving them unset would let each plane's
		// own default HTTP client identity (httpx TestClient's "testclient"
		// vs Go's http.Client's "Go-http-client/1.1" and 127.0.0.1) leak
		// into the comparison, which is a test-harness artifact, not a
		// product difference.
		{Name: "set password", Method: "POST", Path: "/api/v1/admin/users/" + memberID.String() + "/password",
			Headers: map[string]string{
				"Authorization": "Bearer " + venue.Tokens["admin"], "Content-Type": "application/json",
				"User-Agent": "venue-oracle-test/1.0", "X-Forwarded-For": "203.0.113.42", "X-Request-ID": "venue-set-password-req",
			},
			Body: venueoracle.B64(fmt.Sprintf(`{"admin_password":%q,"password":"a new strong password 42"}`, adminPlaintextPassword))},
		{Name: "set password wrong admin password", Method: "POST", Path: "/api/v1/admin/users/" + memberID.String() + "/password", Headers: jsonHeaders,
			Body: venueoracle.B64(`{"admin_password":"totally the wrong password","password":"a new strong password 42"}`)},
		// A new password whose only digit is a Unicode "Digit but not
		// Decimal" character (U+00B2 SUPERSCRIPT TWO) satisfies Python's
		// str.isdigit()-based check but not a decimal-only one -- a live
		// round found Go's password policy rejecting this password Python
		// accepts.
		// Same test-harness-artifact note as "set password" above: this
		// request also succeeds and writes a second password_changed audit
		// row, so it needs the same explicit, stable headers or the
		// audit_logs row comparison sees each plane's own default HTTP
		// client identity instead of a real difference.
		{Name: "set password unicode digit", Method: "POST", Path: "/api/v1/admin/users/" + memberID.String() + "/password",
			Headers: map[string]string{
				"Authorization": "Bearer " + venue.Tokens["admin"], "Content-Type": "application/json",
				"User-Agent": "venue-oracle-test/1.0", "X-Forwarded-For": "203.0.113.42", "X-Request-ID": "venue-set-password-unicode-req",
			},
			Body: venueoracle.B64(fmt.Sprintf(`{"admin_password":%q,"password":"Abcdefghijk²"}`, adminPlaintextPassword))},
		// A 73-byte admin_password: Python's route calls bcrypt.checkpw
		// directly (not the try/except-wrapped _verify_password helper),
		// which RAISES for a plaintext longer than 72 bytes -- unhandled,
		// the generic 500 -- a live round found Go mapping this to the
		// ordinary "verification failed" 403 instead.
		{Name: "set password admin password over bcrypt limit", Method: "POST", Path: "/api/v1/admin/users/" + memberID.String() + "/password", Headers: jsonHeaders,
			Body: venueoracle.B64(fmt.Sprintf(`{"admin_password":"%s","password":"a new strong password 43"}`, strings.Repeat("a", 73)))},
		{Name: "delete user", Method: "DELETE", Path: "/api/v1/admin/users/" + memberID.String(), Headers: authHeaders},
		{Name: "delete user again", Method: "DELETE", Path: "/api/v1/admin/users/" + memberID.String(), Headers: authHeaders},
		// Unauthenticated + malformed body: FastAPI validates the pydantic
		// body parameter before the auth Depends() ever runs, so this is a
		// 422 on both planes, never a 401 -- see the impersonation oracle's
		// identical case for the P1 this pins.
		{Name: "unauthenticated malformed body", Method: "PATCH", Path: "/api/v1/admin/users/" + memberID.String(),
			Headers: map[string]string{"Content-Type": "application/json"}, Body: venueoracle.B64(`{`)},
	}
	python := venue.ServePython(t, requests)
	goBase, _ := startGoServer(t, ctx, venue, jwtKey)

	receipt := venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{
		Normalize: func(request venueoracle.Request, body string) string {
			for _, field := range []string{"id", "created_at", "updated_at"} {
				body = redactField(t, body, field)
			}
			return body
		},
	})
	t.Log(receipt)

	sourceRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), userPasswordAuditQuery(orgID, adminID))
	goRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), userPasswordAuditQuery(orgID, adminID))
	if sourceRows != goRows {
		t.Errorf("audit_logs rows differ:\n python: %s\n go:     %s", sourceRows, goRows)
	}
	compareAuditJSONWithSpacingGap(t, ctx, venue, fmt.Sprintf("org_id = '%s' AND user_id = '%s'", orgID, adminID), "request_metadata")
}

func userPasswordAuditQuery(orgID, adminID uuid.UUID) string {
	return fmt.Sprintf(`SELECT org_id, user_id, action, resource_type, status, changes::text
FROM audit_logs WHERE org_id = '%s' AND user_id = '%s' ORDER BY created_at`, orgID, adminID)
}
