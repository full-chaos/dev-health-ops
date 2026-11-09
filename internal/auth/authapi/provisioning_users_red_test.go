package authapi

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/passwordhash"
)

// The user-admin routes go-api calls while auth-service owns users. The paths are literals on purpose: these cells
// run unchanged on a tree that has no such routes.
const (
	usersCreatePath   = "/internal/v1/users/create"
	usersUpdatePath   = "/internal/v1/users/update"
	usersPasswordPath = "/internal/v1/users/password"
	usersDeletePath   = "/internal/v1/users/delete"
)

var usersAdminPaths = []string{usersCreatePath, usersUpdatePath, usersPasswordPath, usersDeletePath}

func newUsersAdminFixture(t *testing.T) *provFixture {
	t.Helper()
	f := newProvFixtureFor(t, docWithRows(t, append([]string{PatternOrgRename}, usersAdminPaths...)...), nil)
	f.roles.held = map[uuid.UUID]bool{f.super: true} // the stored assignment of the fixture's active superuser
	return f
}

func testHash(t *testing.T, password string) string {
	t.Helper()
	hash, err := passwordhash.Hash(password)
	if err != nil {
		t.Fatal(err)
	}
	return hash
}

func usersAnswerOf(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal(body, &doc); err != nil {
		t.Fatalf("not JSON: %s", body)
	}
	return doc
}

// Each route mounts when its endpoint-profile row is in the document: an empty body is a 400 malformed, not a 404.
// Without the rows no route mounts.
func TestUsersAdminRoutesMountWithTheirRows(t *testing.T) {
	t.Parallel()
	with := newUsersAdminFixture(t)
	without := newProvFixtureWith(t, embeddedDocWithoutRoutes(t, usersAdminPaths...))
	for _, path := range usersAdminPaths {
		expectRefusal(t, with.post(path, `{}`, nil), http.StatusBadRequest, reasonMalformed)
		if got := without.post(path, `{}`, nil).Code; got != http.StatusNotFound {
			t.Errorf("%s without its row: %d, want 404", path, got)
		}
	}
}

// The actor rule runs on auth-service again: a platform create (no organization) or a platform admin grant needs a
// superuser actor; an org admin may create a member of its own organization.
func TestUsersAdminRoutesRerunActorRule(t *testing.T) {
	t.Parallel()
	f := newUsersAdminFixture(t)
	hash := testHash(t, "correct horse battery staple 1")
	platform := map[string]any{"actor_principal_id": f.viewer.String(), "email": "p@example.com", "password_hash": hash,
		"auth_provider": "local", "is_verified": false, "grant_platform_admin": false}
	expectRefusal(t, f.post(usersCreatePath, encode(platform), nil), http.StatusForbidden, reasonSuperuserRequired)

	grant := map[string]any{"actor_principal_id": f.admin.String(), "email": "g@example.com", "auth_provider": "local",
		"is_verified": false, "grant_platform_admin": true, "organization_id": f.org.String(), "role": "member"}
	expectRefusal(t, f.post(usersCreatePath, encode(grant), nil), http.StatusForbidden, reasonSuperuserRequired)

	orgAdd := map[string]any{"actor_principal_id": f.admin.String(), "email": "m@example.com", "auth_provider": "local",
		"is_verified": false, "grant_platform_admin": false, "organization_id": f.org.String(), "role": "member"}
	if response := f.post(usersCreatePath, encode(orgAdd), nil); response.Code != http.StatusOK {
		t.Fatalf("org admin create in its own org: %d %s, want 200", response.Code, response.Body)
	}

	verify := map[string]any{"actor_principal_id": f.viewer.String(), "principal_id": f.target.String(), "is_verified": true}
	expectRefusal(t, f.post(usersUpdatePath, encode(verify), nil), http.StatusForbidden, reasonSuperuserRequired)
}

// The create keeps go-api's email form (lower case, outer spaces cut) and takes a service account's .invalid mail:
// the register normalization is not used.
func TestUsersCreateKeepsGoAPIEmailForm(t *testing.T) {
	t.Parallel()
	f := newUsersAdminFixture(t)
	for raw, want := range map[string]string{
		"  New.User@Example.COM ":          "new.user@example.com",
		"Bot-1@service.dev-health.invalid": "bot-1@service.dev-health.invalid",
	} {
		body := map[string]any{"actor_principal_id": f.super.String(), "email": raw, "auth_provider": "service",
			"is_verified": false, "grant_platform_admin": false}
		response := f.post(usersCreatePath, encode(body), nil)
		if response.Code != http.StatusOK {
			t.Fatalf("create %q: %d %s, want 200", raw, response.Code, response.Body)
		}
		if got := usersAnswerOf(t, response.Body.Bytes())["email"]; got != want {
			t.Errorf("create %q: email %v, want %q", raw, got, want)
		}
	}
}

// The admin password check runs on auth-service against the acting admin's auth.users verifier.
func TestUsersPasswordRouteChecksTheActingAdminPassword(t *testing.T) {
	t.Parallel()
	f := newUsersAdminFixture(t)
	body := func(admin string) string {
		return encode(map[string]any{"actor_principal_id": f.super.String(), "principal_id": f.target.String(),
			"admin_password": admin, "password_hash": testHash(t, "a new long password 2")})
	}
	expectRefusal(t, f.post(usersPasswordPath, body("not the admin password"), nil), http.StatusForbidden, reasonAdminPasswordInvalidLiteral)
	if response := f.post(usersPasswordPath, body(usersAdminFixturePassword), nil); response.Code != http.StatusOK {
		t.Fatalf("set password with the right admin password: %d %s, want 200", response.Code, response.Body)
	}
}

// A soft delete answers whether it was already done, so go-api keeps its 404 on a repeat.
func TestUsersSoftDeleteRouteAnswersAlreadyDone(t *testing.T) {
	t.Parallel()
	f := newUsersAdminFixture(t)
	body := encode(map[string]any{"actor_principal_id": f.super.String(), "principal_id": f.target.String(), "mode": "soft"})
	response := f.post(usersDeletePath, body, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("soft delete: %d %s, want 200", response.Code, response.Body)
	}
	if got := usersAnswerOf(t, response.Body.Bytes())["already_done"]; got != false {
		t.Errorf("first soft delete: already_done %v, want false", got)
	}
	expectRefusal(t, f.post(usersDeletePath, encode(map[string]any{"actor_principal_id": f.super.String(),
		"principal_id": f.target.String(), "mode": "force"}), nil), http.StatusBadRequest, reasonMalformed)
}

// Literals the cells above share: the refusal code of a wrong admin password and the password the fixture's acting
// admin holds.
const (
	reasonAdminPasswordInvalidLiteral = "admin_password_invalid"
	usersAdminFixturePassword         = "the acting admin password 3"
)
