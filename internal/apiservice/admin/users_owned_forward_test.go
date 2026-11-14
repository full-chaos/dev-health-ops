package admin

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// While auth-service owns users (AUTH_USERS_OWNED), the admin users writes keep go-api's checks and send the write
// to auth-service's user-admin routes; go-api writes nothing and answers from its read of public.users. Each auth
// refusal maps to the answer go-api gives when it writes itself (PLAN-13380-v4 section 2).

const ownedNewEmail = "New@Example.com"

type fakeUsersAdmin struct {
	verbs     []string
	creates   []authprovision.UserCreateRequest
	updates   []authprovision.UserUpdateRequest
	passwords []authprovision.UserPasswordRequest
	deletes   []authprovision.UserDeleteRequest
	err       error
	deleted   authprovision.DeletedUser
	// onUpdate runs inside UpdateUser: the write auth-service makes, as the read-back sees it.
	onUpdate func()
}

func (f *fakeUsersAdmin) CreateUser(_ context.Context, in authprovision.UserCreateRequest) (authprovision.CreatedUser, error) {
	f.verbs = append(f.verbs, "create")
	f.creates = append(f.creates, in)
	if f.err != nil {
		return authprovision.CreatedUser{}, f.err
	}
	return authprovision.CreatedUser{ID: provTarget, Email: strings.ToLower(in.Email)}, nil
}

func (f *fakeUsersAdmin) UpdateUser(_ context.Context, in authprovision.UserUpdateRequest) error {
	f.verbs = append(f.verbs, "update")
	f.updates = append(f.updates, in)
	if f.onUpdate != nil && f.err == nil {
		f.onUpdate()
	}
	return f.err
}

func (f *fakeUsersAdmin) SetUserPassword(_ context.Context, in authprovision.UserPasswordRequest) error {
	f.verbs = append(f.verbs, "password")
	f.passwords = append(f.passwords, in)
	return f.err
}

func (f *fakeUsersAdmin) DeleteUser(_ context.Context, in authprovision.UserDeleteRequest) (authprovision.DeletedUser, error) {
	f.verbs = append(f.verbs, "delete")
	f.deletes = append(f.deletes, in)
	if f.err != nil {
		return authprovision.DeletedUser{}, f.err
	}
	return f.deleted, nil
}

// ownedStored is the public.users row of provTarget as fullUserByID scans it.
type ownedStored struct {
	email              string
	username, fullName *string
	active, verified   bool
	superuser          bool
}

func (s ownedStored) row() []any {
	provider := "local"
	return []any{provTarget, s.email, s.username, (*string)(nil), s.fullName, (*string)(nil), &provider, (*string)(nil),
		s.active, s.verified, s.superuser, 0, (*time.Time)(nil), provStamp, provStamp}
}

const (
	ownedByIDSQL       = "FROM users WHERE id = $1"
	ownedByEmailSQL    = "FROM users WHERE lower(email) = $1"
	ownedByUsernameSQL = "FROM users WHERE lower(username) = $1"
)

func ownedDB(stored ownedStored) *provisionDB {
	rows := provisionRowsFor()
	rows[ownedByIDSQL] = stored.row()
	return &provisionDB{rows: rows}
}

func activeStored() ownedStored { return ownedStored{email: "a@example.com", active: true} }

func ownedHandlers(db *provisionDB, users authprovision.UsersAdmin, rolesOwned bool) *handlers {
	h := newProvisionHandlers(db, nil)
	h.store.UsersOwned = true
	h.store.PlatformRolesOwned = rolesOwned
	h.usersAdmin = users
	return h
}

func ownedSuperuser() *policy.User {
	return &policy.User{IsSuperuser: true, UserID: validUUID, ID: provActor, Email: "root@example.com"}
}

func ownedOrgAdmin() *policy.User {
	return &policy.User{UserID: validUUID, ID: provActor, Email: "admin@example.com", OrgID: provOrg.String()}
}

type ownedCall struct {
	route  string // create, update, password, delete
	body   string
	query  string
	caller *policy.User
	header map[string]string
	input  *setPasswordInput
	// session is a live impersonation row in the request context, as Scope.Impersonation sets it.
	session *policy.Impersonation
}

func (c ownedCall) run(h *handlers) (int, string) {
	method, path, handler := http.MethodPost, "/api/v1/admin/users", h.createUser
	switch c.route {
	case "update":
		method, path, handler = http.MethodPatch, "/api/v1/admin/users/"+provTarget.String(), h.updateUser
	case "password":
		path, handler = "/api/v1/admin/users/"+provTarget.String()+"/password", h.setUserPassword
	case "delete":
		method, path, handler = http.MethodDelete, "/api/v1/admin/users/"+provTarget.String(), h.deleteUser
	}
	req := httptest.NewRequest(method, path+c.query, nil)
	caller := c.caller
	if caller == nil {
		caller = ownedSuperuser()
	}
	req = req.WithContext(policy.WithUser(req.Context(), caller))
	if c.session != nil {
		req = req.WithContext(policy.WithImpersonation(req.Context(), c.session))
	}
	req.SetPathValue("user_id", provTarget.String())
	for name, value := range c.header {
		req.Header.Set(name, value)
	}
	if c.body != "" {
		req = withBody(c.body)(req)
	}
	if c.route == "password" {
		input := setPasswordInput{AdminPassword: "Admin-Password-123!", Password: "Correct-Horse-Battery-9"}
		if c.input != nil {
			input = *c.input
		}
		req = req.WithContext(context.WithValue(req.Context(), setPasswordInputKey{}, input))
	}
	rec := httptest.NewRecorder()
	handler(rec, req)
	return rec.Code, strings.TrimSpace(rec.Body.String())
}

// wrote reports a write or a transaction in db's log: the owned path writes nothing of its own.
func wrote(db *provisionDB) []string {
	var out []string
	for _, step := range db.log {
		if step == "BEGIN" || strings.HasPrefix(step, "tx:") || strings.HasPrefix(step, "INSERT") || strings.HasPrefix(step, "UPDATE") ||
			strings.HasPrefix(step, "DELETE") {
			out = append(out, step)
		}
	}
	return out
}

func mustOwnedAnswer(t *testing.T, code int, body string, wantCode int, wantBody string) {
	t.Helper()
	if code != wantCode || body != wantBody {
		t.Fatalf("answer %d %s, want %d %s", code, body, wantCode, wantBody)
	}
}

func mustForwardOnly(t *testing.T, db *provisionDB, users *fakeUsersAdmin, verb string) {
	t.Helper()
	if len(users.verbs) != 1 || users.verbs[0] != verb {
		t.Fatalf("auth calls %v, want one %s", users.verbs, verb)
	}
	if w := wrote(db); len(w) != 0 {
		t.Fatalf("go-api wrote on the owned path: %q", w)
	}
}

func mustNoForward(t *testing.T, db *provisionDB, users *fakeUsersAdmin) {
	t.Helper()
	if len(users.verbs) != 0 {
		t.Fatalf("auth calls %v, want none", users.verbs)
	}
	if w := wrote(db); len(w) != 0 {
		t.Fatalf("go-api wrote: %q", w)
	}
}

// R1
func TestUsersOwnedPlatformCreateForwardsToAuth(t *testing.T) {
	db, users := ownedDB(activeStored()), &fakeUsersAdmin{}
	code, body := ownedCall{route: "create", body: `{"email":"` + ownedNewEmail + `","password":"Correct-Horse-Battery-9","full_name":"New"}`}.run(ownedHandlers(db, users, true))
	if code != http.StatusCreated || !strings.Contains(body, `"id":"`+provTarget.String()+`"`) {
		t.Fatalf("answer %d %s, want 201 with the read-back row", code, body)
	}
	mustForwardOnly(t, db, users, "create")
	in := users.creates[0]
	if in.Email != ownedNewEmail || in.Actor != provActor || in.OrgID != nil || in.GrantPlatformAdmin || in.PasswordHash == nil ||
		*in.PasswordHash == "" || in.FullName == nil || *in.FullName != "New" || in.Username != nil {
		t.Fatalf("create request %+v", in)
	}
}

// R2
func TestUsersOwnedServiceAccountCreateForwardsToAuth(t *testing.T) {
	db, users := ownedDB(activeStored()), &fakeUsersAdmin{}
	code, _ := ownedCall{route: "create", body: `{"email":"ci-bot@acme.invalid","auth_provider":"service"}`}.run(ownedHandlers(db, users, true))
	if code != http.StatusCreated {
		t.Fatalf("answer %d, want 201", code)
	}
	mustForwardOnly(t, db, users, "create")
	in := users.creates[0]
	if in.Email != "ci-bot@acme.invalid" || in.AuthProvider == nil || *in.AuthProvider != "service" || in.PasswordHash != nil {
		t.Fatalf("create request %+v", in)
	}
}

// R3
func TestUsersOwnedCreateSuperuserGrantsPlatformAdmin(t *testing.T) {
	db, users := ownedDB(ownedStored{email: "new@example.com", active: true, superuser: true}), &fakeUsersAdmin{}
	code, body := ownedCall{route: "create", body: `{"email":"` + ownedNewEmail + `","is_superuser":true}`}.run(ownedHandlers(db, users, true))
	if code != http.StatusCreated || !strings.Contains(body, `"is_superuser":true`) {
		t.Fatalf("answer %d %s, want 201 is_superuser true", code, body)
	}
	mustForwardOnly(t, db, users, "create")
	if !users.creates[0].GrantPlatformAdmin {
		t.Fatalf("create request %+v, want grant_platform_admin", users.creates[0])
	}
}

// R4
func TestUsersOwnedOrgScopedCreateForwardsMembership(t *testing.T) {
	db, users := ownedDB(activeStored()), &fakeUsersAdmin{}
	code, _ := ownedCall{route: "create", body: `{"email":"` + ownedNewEmail + `"}`,
		header: map[string]string{"X-Org-Id": provOrg.String(), "User-Agent": "owned-test-agent"}}.run(ownedHandlers(db, users, true))
	if code != http.StatusCreated {
		t.Fatalf("answer %d, want 201", code)
	}
	mustForwardOnly(t, db, users, "create")
	in := users.creates[0]
	if in.OrgID == nil || *in.OrgID != provOrg || in.Role != "member" || !strings.Contains(string(in.RequestMetadata), `"user_agent":"owned-test-agent"`) {
		t.Fatalf("create request org %v role %q metadata %s", in.OrgID, in.Role, in.RequestMetadata)
	}
}

// R17 (pm D6914): a create into a suspended organization is the legacy 201; go-api reads no organization state.
func TestUsersOwnedCreateIntoASuspendedOrgIs201(t *testing.T) {
	db, users := ownedDB(activeStored()), &fakeUsersAdmin{}
	db.rows["FROM organizations WHERE id = $1"] = []any{provOrg, "acme", "Acme", (*string)(nil), []byte("{}"), "community", "self", false, provStamp, provStamp}
	code, _ := ownedCall{route: "create", body: `{"email":"` + ownedNewEmail + `","role":"viewer"}`,
		header: map[string]string{"X-Org-Id": provOrg.String()}}.run(ownedHandlers(db, users, true))
	if code != http.StatusCreated {
		t.Fatalf("answer %d, want 201", code)
	}
	mustForwardOnly(t, db, users, "create")
	if users.creates[0].Role != "viewer" {
		t.Fatalf("role %q", users.creates[0].Role)
	}
}

// R5
func TestUsersOwnedPatchVerifiedForwardsToAuth(t *testing.T) {
	for _, verified := range []bool{true, false} {
		stored := activeStored()
		stored.verified = !verified
		db, users := ownedDB(stored), &fakeUsersAdmin{}
		body := `{"is_verified":false}`
		if verified {
			body = `{"is_verified":true}`
		}
		code, _ := ownedCall{route: "update", body: body}.run(ownedHandlers(db, users, true))
		if code != http.StatusOK {
			t.Fatalf("verified %v: answer %d, want 200", verified, code)
		}
		mustForwardOnly(t, db, users, "update")
		in := users.updates[0]
		if in.IsVerified == nil || *in.IsVerified != verified || in.Principal != provTarget || in.Actor != provActor {
			t.Fatalf("verified %v: update request %+v", verified, in)
		}
	}
}

// R6
func TestUsersOwnedPatchSuperuserGrantsAndRevokes(t *testing.T) {
	for _, grant := range []bool{true, false} {
		stored := activeStored()
		stored.superuser = !grant
		db, users := ownedDB(stored), &fakeUsersAdmin{}
		body := `{"is_superuser":false}`
		if grant {
			body = `{"is_superuser":true}`
		}
		code, _ := ownedCall{route: "update", body: body}.run(ownedHandlers(db, users, true))
		if code != http.StatusOK {
			t.Fatalf("grant %v: answer %d, want 200", grant, code)
		}
		mustForwardOnly(t, db, users, "update")
		if in := users.updates[0]; in.IsSuperuser == nil || *in.IsSuperuser != grant {
			t.Fatalf("grant %v: update request %+v", grant, in)
		}
	}
}

// R18 (pm D6914): is_superuser true on an inactive user is the legacy 200.
func TestUsersOwnedPatchSuperuserOnAnInactiveUserIs200(t *testing.T) {
	stored := activeStored()
	stored.active = false
	db, users := ownedDB(stored), &fakeUsersAdmin{}
	code, _ := ownedCall{route: "update", body: `{"is_superuser":true}`}.run(ownedHandlers(db, users, true))
	if code != http.StatusOK {
		t.Fatalf("answer %d, want 200", code)
	}
	mustForwardOnly(t, db, users, "update")
}

// R6b: the web edit form sends all six fields; only the changed verified flag is a change, and the unchanged
// is_active is no is_active change.
func TestUsersOwnedPatchWebFormNormalizedEqualFieldsForwardOnlyTheChange(t *testing.T) {
	username, name := "neo", "Neo"
	db, users := ownedDB(ownedStored{email: "a@example.com", username: &username, fullName: &name, active: true}), &fakeUsersAdmin{}
	code, body := ownedCall{route: "update", body: `{"email":"A@Example.com","username":"Neo","full_name":"Neo","is_active":true,"is_verified":true,"is_superuser":false}`}.run(ownedHandlers(db, users, true))
	if code != http.StatusOK {
		t.Fatalf("answer %d %s, want 200", code, body)
	}
	mustForwardOnly(t, db, users, "update")
	for _, step := range db.log {
		if strings.Contains(step, ownedByEmailSQL) || strings.Contains(step, ownedByUsernameSQL) {
			t.Fatalf("an unchanged email or username was checked for a duplicate: %q", step)
		}
	}
	if in := users.updates[0]; in.IsVerified == nil || !*in.IsVerified || in.IsActive != nil {
		t.Fatalf("update request %+v, want is_verified true and no is_active (unchanged)", in)
	}
}

// R7
func TestUsersOwnedPatchProfileFieldsForwardToAuth(t *testing.T) {
	db, users := ownedDB(activeStored()), &fakeUsersAdmin{}
	code, _ := ownedCall{route: "update", body: `{"username":"Trinity","full_name":"T","avatar_url":"https://example.com/a.png"}`}.run(ownedHandlers(db, users, true))
	if code != http.StatusOK {
		t.Fatalf("answer %d, want 200", code)
	}
	mustForwardOnly(t, db, users, "update")
	in := users.updates[0]
	if in.Username == nil || *in.Username != "Trinity" || in.FullName == nil || *in.FullName != "T" || in.AvatarURL == nil ||
		*in.AvatarURL != "https://example.com/a.png" || in.Email != nil || in.IsVerified != nil {
		t.Fatalf("update request %+v", in)
	}
}

// R23 (pm D6915, parity): an empty PATCH reaches auth-service, which bumps updated_at as legacy does, and go-api
// answers the row it reads after the call: the bumped updated_at, not the one it read before.
func TestUsersOwnedNoOpPatchAnswersTheBumpedUpdatedAt(t *testing.T) {
	db := ownedDB(activeStored())
	bumped := provStamp.Add(time.Hour)
	users := &fakeUsersAdmin{onUpdate: func() {
		row := activeStored().row()
		row[14] = bumped
		db.rows[ownedByIDSQL] = row
	}}
	code, body := ownedCall{route: "update", body: `{}`}.run(ownedHandlers(db, users, true))
	if code != http.StatusOK || !strings.Contains(body, `"updated_at":"`+pyTimeString(bumped)+`"`) {
		t.Fatalf("answer %d %s, want 200 with updated_at %s", code, body, pyTimeString(bumped))
	}
	mustForwardOnly(t, db, users, "update")
}

// R8
func TestUsersOwnedSetPasswordForwardsToAuth(t *testing.T) {
	db, users := ownedDB(activeStored()), &fakeUsersAdmin{}
	code, body := ownedCall{route: "password"}.run(ownedHandlers(db, users, true))
	mustOwnedAnswer(t, code, body, http.StatusOK, `{"success":true}`)
	mustForwardOnly(t, db, users, "password")
	in := users.passwords[0]
	if in.AdminPassword != "Admin-Password-123!" || in.PasswordHash == "" || in.PasswordHash == "Correct-Horse-Battery-9" ||
		in.Actor != provActor || in.Principal != provTarget || in.OrgID != nil {
		t.Fatalf("password request %+v", in)
	}
}

// R16 (Q8): the acting admin's password over bcrypt's 72 bytes is go-api's 500, before any call (ledger residual
// row known-divergence-admin-password-no-hash-long).
func TestUsersOwnedSetPasswordAdminPasswordOver72BytesIs500BeforeTheForward(t *testing.T) {
	mustLedgerRow(t, "known-divergence-admin-password-no-hash-long")
	db, users := ownedDB(activeStored()), &fakeUsersAdmin{}
	long := strings.Repeat("a", 73)
	code, body := ownedCall{route: "password", input: &setPasswordInput{AdminPassword: long, Password: "Correct-Horse-Battery-9"}}.run(ownedHandlers(db, users, true))
	mustOwnedAnswer(t, code, body, http.StatusInternalServerError, internalBody())
	mustNoForward(t, db, users)
}

// R9
func TestUsersOwnedSoftDeleteForwardsAndRepeatIs404(t *testing.T) {
	db, users := ownedDB(activeStored()), &fakeUsersAdmin{deleted: authprovision.DeletedUser{WasActive: true}}
	code, body := ownedCall{route: "delete"}.run(ownedHandlers(db, users, true))
	mustOwnedAnswer(t, code, body, http.StatusOK, `{"deleted":true}`)
	mustForwardOnly(t, db, users, "delete")
	if in := users.deletes[0]; in.Mode != "soft" || in.Principal != provTarget || in.Actor != provActor {
		t.Fatalf("delete request %+v", in)
	}
	db, users = ownedDB(activeStored()), &fakeUsersAdmin{deleted: authprovision.DeletedUser{AlreadyDone: true}}
	code, body = ownedCall{route: "delete"}.run(ownedHandlers(db, users, true))
	mustOwnedAnswer(t, code, body, http.StatusNotFound, `{"detail":"User not found"}`)
	mustForwardOnly(t, db, users, "delete")
}

// The force delete stays the moved operation until S4, before any database work.
func TestUsersOwnedForceDeleteStaysMovedUntilS4(t *testing.T) {
	db, users := &provisionDB{}, &fakeUsersAdmin{}
	code, body := ownedCall{route: "delete", query: "?force=true"}.run(ownedHandlers(db, users, true))
	mustOwnedAnswer(t, code, body, http.StatusConflict, movedBody())
	if len(db.log) != 0 || len(users.verbs) != 0 {
		t.Fatalf("database %q, auth %v: want none", db.log, users.verbs)
	}
}

// R11
func TestUsersOwnedWithoutUsersClientIs503(t *testing.T) {
	for _, call := range []ownedCall{{route: "create", body: `{"email":"` + ownedNewEmail + `"}`}, {route: "update", body: `{"is_verified":true}`},
		{route: "password"}, {route: "delete"}} {
		db := ownedDB(activeStored())
		code, body := call.run(ownedHandlers(db, nil, true))
		mustOwnedAnswer(t, code, body, http.StatusServiceUnavailable, unavailableBody())
		if w := wrote(db); len(w) != 0 {
			t.Fatalf("%s: go-api wrote %q", call.route, w)
		}
	}
}

// R12 (13377 ruling): the provider-id check runs before the forward, with go-api's exact 422.
func TestUsersOwnedCreateRefusesAnInvalidProviderIDBeforeTheForward(t *testing.T) {
	body := `{"email":"a@example.test","auth_provider":"github","auth_provider_id":""}`
	_, legacy, _, _ := createUserWithBody(t, body)
	db, users := ownedDB(activeStored()), &fakeUsersAdmin{}
	code, got := ownedCall{route: "create", body: body}.run(ownedHandlers(db, users, true))
	mustOwnedAnswer(t, code, got, http.StatusUnprocessableEntity, strings.TrimSpace(legacy))
	mustNoForward(t, db, users)
}

// R13: a duplicate go-api's own check finds is its 400, with no call.
func TestUsersOwnedDuplicateFoundBeforeTheForwardIsTheGoAPI400(t *testing.T) {
	other := ownedStored{email: "new@example.com", active: true}.row()
	other[0] = provOwner
	for name, c := range map[string]struct {
		call ownedCall
		key  string
		want string
	}{
		"create email":    {ownedCall{route: "create", body: `{"email":"` + ownedNewEmail + `"}`}, ownedByEmailSQL, `{"detail":"User with email New@Example.com already exists"}`},
		"create username": {ownedCall{route: "create", body: `{"email":"` + ownedNewEmail + `","username":"Neo"}`}, ownedByUsernameSQL, `{"detail":"User with username Neo already exists"}`},
		"update email":    {ownedCall{route: "update", body: `{"email":"New@Example.com"}`}, ownedByEmailSQL, `{"detail":"Email New@Example.com already in use"}`},
		"update username": {ownedCall{route: "update", body: `{"username":"Neo"}`}, ownedByUsernameSQL, `{"detail":"Username Neo already in use"}`},
	} {
		t.Run(name, func(t *testing.T) {
			db, users := ownedDB(activeStored()), &fakeUsersAdmin{}
			db.rows[c.key] = other
			code, body := c.call.run(ownedHandlers(db, users, true))
			mustOwnedAnswer(t, code, body, http.StatusBadRequest, c.want)
			mustNoForward(t, db, users)
		})
	}
}

// R14 (D6908): a username auth-service finds taken after go-api's check passed is the race: 409 username_taken
// (ledger row known-stricter-duplicate-race).
func TestUsersOwnedUsernameRaceIs409UsernameTaken(t *testing.T) {
	mustLedgerRow(t, "known-stricter-duplicate-race")
	db, users := ownedDB(activeStored()), &fakeUsersAdmin{err: refusedWith(http.StatusConflict, "username_taken")}
	code, body := ownedCall{route: "create", body: `{"email":"` + ownedNewEmail + `","username":"Neo"}`}.run(ownedHandlers(db, users, true))
	mustOwnedAnswer(t, code, body, http.StatusConflict, `{"detail":{"message":"User with username Neo already exists","reason":"username_taken"}}`)
}

// R19 (D6908 class): the email race, the same row.
func TestUsersOwnedEmailRaceIs409EmailTaken(t *testing.T) {
	mustLedgerRow(t, "known-stricter-duplicate-race")
	db, users := ownedDB(activeStored()), &fakeUsersAdmin{err: refusedWith(http.StatusConflict, "email_taken")}
	code, body := ownedCall{route: "update", body: `{"email":"New@Example.com"}`}.run(ownedHandlers(db, users, true))
	mustOwnedAnswer(t, code, body, http.StatusConflict, `{"detail":{"message":"Email New@Example.com already in use","reason":"email_taken"}}`)
}

// R20 (acp Q4): an org-scoped create on a missing organization is 404 (legacy: a 500 foreign-key failure; ledger
// row known-stricter-create-missing-org).
func TestUsersOwnedCreateOnAMissingOrgIs404OrganizationNotFound(t *testing.T) {
	mustLedgerRow(t, "known-stricter-create-missing-org")
	db, users := ownedDB(activeStored()), &fakeUsersAdmin{err: refusedWith(http.StatusNotFound, "organization_not_found")}
	code, body := ownedCall{route: "create", body: `{"email":"` + ownedNewEmail + `"}`, header: map[string]string{"X-Org-Id": provOrg.String()}}.run(ownedHandlers(db, users, true))
	mustOwnedAnswer(t, code, body, http.StatusNotFound, `{"detail":"Organization not found"}`)
}

// R21 (acp Q7): a lock race is 409 state_changed, never a 503.
func TestUsersOwnedStateChangedIs409(t *testing.T) {
	db, users := ownedDB(activeStored()), &fakeUsersAdmin{err: refusedWith(http.StatusConflict, "state_changed")}
	code, body := ownedCall{route: "update", body: `{"is_verified":true}`}.run(ownedHandlers(db, users, true))
	mustOwnedAnswer(t, code, body, http.StatusConflict, `{"detail":{"message":"`+usersStateChangedText+`","reason":"state_changed"}}`)
}

// R15: go-api's own superuser guard refuses a non-superuser's grant before any call.
func TestUsersOwnedNonSuperuserGrantRefusedBeforeTheForward(t *testing.T) {
	mustLedgerRow(t, "known-stricter-superuser-create-promote")
	db, users := ownedDB(activeStored()), &fakeUsersAdmin{}
	db.rows["SELECT role FROM memberships WHERE org_id = $1 AND user_id = $2"] = []any{"admin"}
	code, body := ownedCall{route: "create", body: `{"email":"` + ownedNewEmail + `","is_superuser":true}`, caller: ownedOrgAdmin()}.run(ownedHandlers(db, users, true))
	mustOwnedAnswer(t, code, body, http.StatusForbidden, `{"detail":"Superuser access required"}`)
	mustNoForward(t, db, users)
}

// R22 (acp Q10): with users owned and platform roles not owned, a real is_superuser change is the moved operation.
func TestUsersOwnedRolesNotOwnedSuperuserChangeIs409OperationMoved(t *testing.T) {
	for _, call := range []ownedCall{{route: "update", body: `{"is_superuser":true}`}, {route: "create", body: `{"email":"` + ownedNewEmail + `","is_superuser":true}`}} {
		db, users := ownedDB(activeStored()), &fakeUsersAdmin{}
		code, body := call.run(ownedHandlers(db, users, false))
		mustOwnedAnswer(t, code, body, http.StatusConflict, movedBody())
		mustNoForward(t, db, users)
	}
}

// A real is_active change with other fields in the patch is one forward carrying them all (CHAOS-13684).
func TestUsersOwnedPatchIsActiveWithOtherFieldsIsOneForward(t *testing.T) {
	db, users := ownedDB(activeStored()), &fakeUsersAdmin{}
	code, _ := ownedCall{route: "update", body: `{"is_active":false,"is_verified":true}`}.run(ownedHandlers(db, users, true))
	if code != http.StatusOK {
		t.Fatalf("answer %d, want 200", code)
	}
	mustForwardOnly(t, db, users, "update")
	if in := users.updates[0]; in.IsActive == nil || *in.IsActive || in.IsVerified == nil || !*in.IsVerified {
		t.Fatalf("update request %+v, want is_active false and is_verified true", in)
	}
}

// R10: every auth refusal maps to go-api's own answer for the same fact (PLAN-13380-v4 section 2).
func TestUsersOwnedAuthReasonsMapToGoAPIAnswers(t *testing.T) {
	_, providerBody, _, _ := createUserWithBody(t, `{"email":"a@example.test","auth_provider":"github","auth_provider_id":""}`)
	unavailable := unavailableBody()
	create := ownedCall{route: "create", body: `{"email":"` + ownedNewEmail + `","username":"Neo"}`}
	update := ownedCall{route: "update", body: `{"email":"New@Example.com","username":"Neo","is_superuser":false}`}
	password := ownedCall{route: "password"}
	remove := ownedCall{route: "delete"}
	all := map[string]ownedCall{"create": create, "update": update, "password": password, "delete": remove}
	type answer struct {
		code int
		body string
	}
	cases := map[string]map[string]answer{
		"provider_id_invalid": {"create": {422, strings.TrimSpace(providerBody)}},
		"superuser_required": {"create": {403, `{"detail":"Superuser access required"}`}, "update": {403, `{"detail":"Superuser access required"}`},
			"password": {403, `{"detail":"Superuser access required"}`}, "delete": {403, `{"detail":"Superuser access required"}`}},
		"admin_required":         {"create": {403, `{"detail":"Admin access required for organization"}`}, "delete": {403, `{"detail":"Admin access required for organization"}`}},
		"principal_deactivated":  {"create": {403, `{"detail":"Account is deactivated"}`}, "update": {403, `{"detail":"Account is deactivated"}`}},
		"organization_suspended": {"create": {403, `{"detail":"Organization is suspended"}`}},
		"principal_not_found": {"update": {404, `{"detail":"User not found"}`}, "password": {404, `{"detail":"User not found"}`},
			"delete": {404, `{"detail":"User not found"}`}},
		"email_taken": {"create": {409, `{"detail":{"message":"User with email New@Example.com already exists","reason":"email_taken"}}`},
			"update": {409, `{"detail":{"message":"Email New@Example.com already in use","reason":"email_taken"}}`}},
		"username_taken": {"create": {409, `{"detail":{"message":"User with username Neo already exists","reason":"username_taken"}}`},
			"update": {409, `{"detail":{"message":"Username Neo already in use","reason":"username_taken"}}`}},
		"last_platform_admin": {"update": {409, `{"detail":{"message":"` + usersLastPlatformAdminText + `","reason":"last_platform_admin"}}`},
			"delete": {409, `{"detail":{"message":"` + usersLastPlatformAdminText + `","reason":"last_platform_admin"}}`}},
		"admin_password_invalid": {"password": {403, `{"detail":"Admin password verification failed"}`}},
		"organization_not_found": {"create": {404, `{"detail":"Organization not found"}`}},
		"state_changed": {"update": {409, `{"detail":{"message":"` + usersStateChangedText + `","reason":"state_changed"}}`},
			"delete": {409, `{"detail":{"message":"` + usersStateChangedText + `","reason":"state_changed"}}`}},
		"malformed":           {"create": {503, unavailable}, "update": {503, unavailable}, "password": {503, unavailable}, "delete": {503, unavailable}},
		"invalid_credentials": {"create": {503, unavailable}, "delete": {503, unavailable}},
	}
	status := map[string]int{"provider_id_invalid": 400, "superuser_required": 403, "admin_required": 403, "principal_deactivated": 403,
		"organization_suspended": 403, "principal_not_found": 404, "email_taken": 409, "username_taken": 409, "last_platform_admin": 409,
		"admin_password_invalid": 403, "organization_not_found": 404, "state_changed": 409, "malformed": 400, "invalid_credentials": 401}
	for reason, routes := range cases {
		for route, want := range routes {
			t.Run(reason+"/"+route, func(t *testing.T) {
				db, users := ownedDB(activeStored()), &fakeUsersAdmin{err: refusedWith(status[reason], reason)}
				code, body := all[route].run(ownedHandlers(db, users, true))
				mustOwnedAnswer(t, code, body, want.code, want.body)
				mustForwardOnly(t, db, users, map[string]string{"create": "create", "update": "update", "password": "password", "delete": "delete"}[route])
			})
		}
	}
	for name, err := range map[string]error{"unavailable": authprovision.ErrUnavailable, "5xx refusal": refusedWith(http.StatusServiceUnavailable, "store_unavailable"),
		"no route": refusedWith(http.StatusNotFound, ""), "other": errors.New("transport")} {
		for route, call := range all {
			t.Run(name+"/"+route, func(t *testing.T) {
				db, users := ownedDB(activeStored()), &fakeUsersAdmin{err: err}
				code, body := call.run(ownedHandlers(db, users, true))
				mustOwnedAnswer(t, code, body, http.StatusServiceUnavailable, unavailable)
			})
		}
	}
}

// A read-back that finds no row after a create is the 503: go-api cannot show the row it was asked for.
func TestUsersOwnedCreateReadBackMissingIs503(t *testing.T) {
	db, users := ownedDB(activeStored()), &fakeUsersAdmin{}
	delete(db.rows, ownedByIDSQL)
	code, body := ownedCall{route: "create", body: `{"email":"` + ownedNewEmail + `"}`}.run(ownedHandlers(db, users, true))
	mustOwnedAnswer(t, code, body, http.StatusServiceUnavailable, unavailableBody())
}

// mustLedgerRow fails unless ci/auth_exception_ledger.tsv holds the row id, anchored in a file of this tree that
// holds its needle: the stricter or divergent answer a cell pins is named in the ledger.
func mustLedgerRow(t *testing.T, id string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "ci", "auth_exception_ledger.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		cols := strings.Split(line, "\t")
		if len(cols) != 7 || cols[0] != id {
			continue
		}
		anchor, err := os.ReadFile(filepath.Join("..", "..", "..", filepath.FromSlash(cols[2])))
		if err != nil || !strings.Contains(string(anchor), cols[3]) {
			t.Fatalf("ledger row %s: anchor %s does not hold its needle (%v)", id, cols[2], err)
		}
		return
	}
	t.Fatalf("ci/auth_exception_ledger.tsv has no row %s", id)
}

func movedBody() string {
	return `{"detail":{"message":"` + "This operation is served by the authentication service while it owns users" + `","reason":"operation_moved"}}`
}
