package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
)

const (
	orgCreateSlugRead     = "SELECT id, slug, name, description, settings, tier, managed_by, is_active, created_at, updated_at FROM organizations WHERE lower(slug) = $1"
	orgCreateOwnerRead    = "SELECT id, email, is_active, is_superuser FROM users WHERE id = $1"
	orgCreateOrgInsert    = "INSERT INTO organizations (id, slug, name, description, settings, tier, managed_by, is_active, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"
	orgCreateMemberRead   = "SELECT id, org_id, user_id, role, invited_by_id, joined_at, created_at, updated_at FROM memberships WHERE org_id = $1 AND user_id = $2"
	orgCreateMemberInsert = "INSERT INTO memberships (id, org_id, user_id, role, invited_by_id, joined_at, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"
	orgCreateGuardLock    = "SELECT 1 FROM organizations WHERE id = $1 FOR UPDATE"
	orgCreateGuardMembers = "SELECT EXISTS (SELECT 1 FROM memberships WHERE org_id = $1)"
	orgCreateGuardDelete  = "DELETE FROM organizations WHERE id = $1"
	orgCreateOwnerEmail   = "owner@example.com"
)

// The guarded delete of an ownerless organization: the org row locked, then
// the membership check after the lock, then the delete, in one transaction.
var (
	orgCreateGuardDeletes = []string{"BEGIN", "tx:" + orgCreateGuardLock, "tx:" + orgCreateGuardMembers, "tx:" + orgCreateGuardDelete, "COMMIT", "ROLLBACK"}
	orgCreateGuardKeeps   = []string{"BEGIN", "tx:" + orgCreateGuardLock, "tx:" + orgCreateGuardMembers, "ROLLBACK"}
)

var orgCreateWithOwner = `{"name": "Acme", "owner_user_id": "` + provOwner.String() + `"}`

// orgCreateDB is a provisionDB whose guard transaction finds a membership of
// the org when memberExists is set (it then keeps the org), and whose owner
// membership row exists when ownerRow is set, or appears once the guard ran
// when ownerAfterGuard is set.
type orgCreateDB struct {
	*provisionDB
	memberExists    bool
	ownerRow        bool
	ownerAfterGuard bool
	ownerReadFails  bool
}

// orgCreateTx is the guard's transaction on an orgCreateDB.
type orgCreateTx struct {
	*provisionTx
	d *orgCreateDB
}

func (t *orgCreateTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	switch fold(sql) {
	case orgCreateGuardLock:
		t.d.add("tx:" + fold(sql))
		return provisionRow{values: []any{1}, ok: true}
	case orgCreateGuardMembers:
		t.d.add("tx:" + fold(sql))
		return provisionRow{values: []any{t.d.memberExists}, ok: true}
	}
	return t.provisionTx.QueryRow(ctx, sql, args...)
}

func (d *orgCreateDB) deleted() bool {
	for _, step := range d.log {
		if step == "tx:"+orgCreateGuardDelete {
			return true
		}
	}
	return false
}

func (d *orgCreateDB) guardRan() bool {
	for _, step := range d.log {
		if step == "tx:"+orgCreateGuardMembers {
			return true
		}
	}
	return false
}

func (d *orgCreateDB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if fold(sql) == orgCreateMemberRead {
		d.add(fold(sql))
		if d.ownerReadFails {
			return provisionRow{values: []any{failRead{}}, ok: true}
		}
		if d.ownerRow || (d.ownerAfterGuard && d.guardRan()) {
			joined := provStamp
			return provisionRow{values: []any{provMember, provOrg, provOwner, "owner", (*uuid.UUID)(nil), &joined, provStamp, provStamp}, ok: true}
		}
		return provisionRow{}
	}
	return d.provisionDB.QueryRow(ctx, sql, args...)
}

func (d *orgCreateDB) Begin(ctx context.Context) (pgx.Tx, error) {
	d.add("BEGIN")
	return &orgCreateTx{provisionTx: &provisionTx{db: d.provisionDB}, d: d}, nil
}

func (d *orgCreateDB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return d.provisionDB.Query(ctx, sql, args...)
}

func newOrgCreateDB() *orgCreateDB {
	email := orgCreateOwnerEmail
	return &orgCreateDB{provisionDB: &provisionDB{rows: map[string][]any{
		orgCreateOwnerRead: {provOwner, email, true, false},
	}}}
}

// orgCreateProvisioner answers the add and the deprovision on their own.
type orgCreateProvisioner struct {
	fakeProvisioner
	addErr, deprovisionErr error
}

func (f *orgCreateProvisioner) AddMember(ctx context.Context, org, principal, actor uuid.UUID, role string, invitedBy *uuid.UUID) (authprovision.Membership, error) {
	m, _ := f.fakeProvisioner.AddMember(ctx, org, principal, actor, role, invitedBy)
	return m, f.addErr
}

func (f *orgCreateProvisioner) Deprovision(ctx context.Context, org, actor uuid.UUID) error {
	_ = f.fakeProvisioner.Deprovision(ctx, org, actor)
	return f.deprovisionErr
}

// callCreateOrg runs POST /api/v1/admin/orgs as the superuser provActor; auth
// nil is the switch off. It returns the answer and the log text.
func callCreateOrg(db *orgCreateDB, auth authprovision.Provisioner, body string) (int, string, string) {
	h := newProvisionHandlers(db.provisionDB, auth)
	h.store.Pool = db
	var logs bytes.Buffer
	h.logger = slog.New(slog.NewTextHandler(&logs, nil))
	r := answerWith(h.createOrganization, http.MethodPost, func(req *http.Request) *http.Request {
		req = req.WithContext(policy.WithUser(req.Context(), &policy.User{IsSuperuser: true, UserID: validUUID, ID: provActor, Email: "root@example.com"}))
		return withBody(body)(req)
	})
	return r.Code, strings.TrimSpace(r.Body.String()), logs.String()
}

// bodyWithoutID is an organization answer with its random id taken out, and
// that id.
func bodyWithoutID(t *testing.T, body string) (map[string]any, string) {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("answer %q: %v", body, err)
	}
	id, _ := out["id"].(string)
	delete(out, "id")
	return out, id
}

func internalBody() string {
	r := answerWith(func(w http.ResponseWriter, _ *http.Request) { policy.WriteInternal(w) }, http.MethodPost, nil)
	return strings.TrimSpace(r.Body.String())
}

func unavailableBody() string {
	r := answerWith(func(w http.ResponseWriter, _ *http.Request) {
		policy.WriteDetail(w, http.StatusServiceUnavailable, policy.ErrorDetail(sessionissue.UnavailableText), nil)
	}, http.MethodPost, nil)
	return strings.TrimSpace(r.Body.String())
}

func assertNoAudit(t *testing.T, log []string) {
	t.Helper()
	for _, step := range log {
		if strings.Contains(step, "audit_logs") {
			t.Fatalf("an audit row was written: %v", log)
		}
	}
}

func assertOwnerlessError(t *testing.T, logs, orgID string) {
	t.Helper()
	found := false
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, "level=ERROR") && strings.Contains(line, "org_id="+orgID) {
			found = true
		}
	}
	if !found {
		t.Fatalf("no ERROR line naming org_id=%s in:\n%s", orgID, logs)
	}
	if strings.Contains(logs, "@") {
		t.Fatalf("the log carries an address:\n%s", logs)
	}
}

// the org id the add was sent for.
func addedOrg(t *testing.T, auth *orgCreateProvisioner) string {
	t.Helper()
	if len(auth.args) == 0 || auth.calls[0] != "add" {
		t.Fatalf("auth calls %v, want the add first", auth.calls)
	}
	return auth.args[0][0].(uuid.UUID).String()
}

// The switch-off create with an owner: today's one transaction, pinned.
var orgCreateSwitchOffSteps = []string{orgCreateSlugRead, "BEGIN", "tx:" + orgCreateOrgInsert, "tx:" + orgCreateMemberRead, "tx:" + orgCreateMemberInsert, "COMMIT", "ROLLBACK"}

// U7: the switch off makes no auth call and runs today's statements in
// today's order; the answer is 201 with the organization.
func TestOrgCreateSwitchOffRunsTodaysSQL(t *testing.T) {
	db := newOrgCreateDB()
	code, body, _ := callCreateOrg(db, nil, orgCreateWithOwner)
	if code != http.StatusCreated {
		t.Fatalf("answer %d %s", code, body)
	}
	if !reflect.DeepEqual(db.log, orgCreateSwitchOffSteps) {
		t.Fatalf("switch-off steps:\n got %q\nwant %q", db.log, orgCreateSwitchOffSteps)
	}
}

// U6: the switch on and no owner: no auth call, today's statements.
func TestOrgCreateSwitchOnWithoutOwnerIsToday(t *testing.T) {
	offDB := newOrgCreateDB()
	offCode, offBody, _ := callCreateOrg(offDB, nil, `{"name": "Acme"}`)
	db := newOrgCreateDB()
	auth := &orgCreateProvisioner{fakeProvisioner: fakeProvisioner{db: db.provisionDB}}
	code, body, _ := callCreateOrg(db, auth, `{"name": "Acme"}`)
	if len(auth.calls) != 0 {
		t.Fatalf("auth calls %v with no owner", auth.calls)
	}
	if code != offCode || !reflect.DeepEqual(db.log, offDB.log) {
		t.Fatalf("no owner, switch on: %d %q; switch off: %d %q", code, db.log, offCode, offDB.log)
	}
	got, _ := bodyWithoutID(t, body)
	want, _ := bodyWithoutID(t, offBody)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("answer %v, switch off %v", got, want)
	}
}

// U1: the switch on with an owner: the org row is written on its own, then
// one add of the first owner, by the calling superuser, to an organization
// with no members (the bootstrap actor); go-api writes no membership. The
// answer is the switch-off answer.
func TestOrgCreateSwitchOnAddsTheOwnerInAuthAfterTheOrgRow(t *testing.T) {
	offDB := newOrgCreateDB()
	_, offBody, _ := callCreateOrg(offDB, nil, orgCreateWithOwner)
	db := newOrgCreateDB()
	auth := &orgCreateProvisioner{fakeProvisioner: fakeProvisioner{db: db.provisionDB}}
	code, body, _ := callCreateOrg(db, auth, orgCreateWithOwner)
	if code != http.StatusCreated {
		t.Fatalf("answer %d %s", code, body)
	}
	want := []string{orgCreateSlugRead, orgCreateOwnerRead, orgCreateOrgInsert, "auth:add"}
	if !reflect.DeepEqual(db.log, want) {
		t.Fatalf("switch-on steps:\n got %q\nwant %q", db.log, want)
	}
	got, id := bodyWithoutID(t, body)
	if len(auth.calls) != 1 {
		t.Fatalf("auth calls %v, want one add", auth.calls)
	}
	if wantArgs := []any{uuid.MustParse(id), provOwner, provActor, "owner", (*uuid.UUID)(nil)}; !reflect.DeepEqual(auth.args[0], wantArgs) {
		t.Fatalf("add args %v, want %v", auth.args[0], wantArgs)
	}
	offGot, _ := bodyWithoutID(t, offBody)
	if !reflect.DeepEqual(got, offGot) {
		t.Fatalf("answer %v, switch off %v", got, offGot)
	}
	assertNoAudit(t, db.log)
}

// An owner go-api does not hold answers today's 500 (the foreign key failure),
// before any write or auth call.
func TestOrgCreateSwitchOnUnknownOwnerIsTodays500(t *testing.T) {
	db := newOrgCreateDB()
	delete(db.rows, orgCreateOwnerRead)
	auth := &orgCreateProvisioner{fakeProvisioner: fakeProvisioner{db: db.provisionDB}}
	code, body, _ := callCreateOrg(db, auth, orgCreateWithOwner)
	if code != http.StatusInternalServerError || body != internalBody() {
		t.Fatalf("answer %d %s, want today's 500", code, body)
	}
	if want := []string{orgCreateSlugRead, orgCreateOwnerRead}; !reflect.DeepEqual(db.log, want) {
		t.Fatalf("steps %q, want %q", db.log, want)
	}
}

// U2: auth refuses an owner go-api holds, no owner row is there: the org is
// denied in auth and deleted with the guarded statement. With the switch off
// the same input is a 201 (go-api writes the owner), which cannot be given
// without an owner: the answer is the 503 of the other proxied routes for a
// refusal with no go-api answer; a 500 is only today's unknown-owner answer.
func TestOrgCreateRefusedOwnerCleansUp(t *testing.T) {
	for _, reason := range []string{"principal_not_found", "principal_deactivated"} {
		t.Run(reason, func(t *testing.T) {
			db := newOrgCreateDB()
			auth := &orgCreateProvisioner{fakeProvisioner: fakeProvisioner{db: db.provisionDB}, addErr: refusedWith(http.StatusConflict, reason)}
			code, body, _ := callCreateOrg(db, auth, orgCreateWithOwner)
			if code != http.StatusServiceUnavailable || body != unavailableBody() {
				t.Fatalf("answer %d %s, want the 503", code, body)
			}
			want := append([]string{orgCreateSlugRead, orgCreateOwnerRead, orgCreateOrgInsert, "auth:add", orgCreateMemberRead, "auth:deprovision"}, orgCreateGuardDeletes...)
			if !reflect.DeepEqual(db.log, want) {
				t.Fatalf("steps:\n got %q\nwant %q", db.log, want)
			}
			if id := addedOrg(t, auth); auth.args[1][0].(uuid.UUID).String() != id || auth.args[1][1] != provActor {
				t.Fatalf("deprovision args %v, want the new org by the caller", auth.args[1])
			}
			assertNoAudit(t, db.log)
		})
	}
}

// A refusal of the actor rule keeps its 403, after the cleanup.
func TestOrgCreateActorRefusalKeepsIts403(t *testing.T) {
	db := newOrgCreateDB()
	auth := &orgCreateProvisioner{fakeProvisioner: fakeProvisioner{db: db.provisionDB}, addErr: refusedWith(http.StatusForbidden, "superuser_required")}
	code, body, _ := callCreateOrg(db, auth, orgCreateWithOwner)
	if code != http.StatusForbidden || !strings.Contains(body, "Superuser access required") || !db.deleted() {
		t.Fatalf("answer %d %s, deleted %v; want the 403 after the cleanup", code, body, db.deleted())
	}
}

// U3: auth unavailable, no owner row: cleanup, then the 503.
func TestOrgCreateUnavailableWithoutOwnerCleansUpAnd503(t *testing.T) {
	db := newOrgCreateDB()
	auth := &orgCreateProvisioner{fakeProvisioner: fakeProvisioner{db: db.provisionDB}, addErr: authprovision.ErrUnavailable}
	code, body, _ := callCreateOrg(db, auth, orgCreateWithOwner)
	if code != http.StatusServiceUnavailable || body != unavailableBody() {
		t.Fatalf("answer %d %s, want the 503", code, body)
	}
	want := append([]string{orgCreateSlugRead, orgCreateOwnerRead, orgCreateOrgInsert, "auth:add", orgCreateMemberRead, "auth:deprovision"}, orgCreateGuardDeletes...)
	if !reflect.DeepEqual(db.log, want) {
		t.Fatalf("steps:\n got %q\nwant %q", db.log, want)
	}
}

// U4: auth unavailable but its commit landed (the owner row is there): 201,
// no cleanup.
func TestOrgCreateLostAnswerWithOwnerIs201(t *testing.T) {
	db := newOrgCreateDB()
	db.ownerRow = true
	auth := &orgCreateProvisioner{fakeProvisioner: fakeProvisioner{db: db.provisionDB}, addErr: authprovision.ErrUnavailable}
	code, body, _ := callCreateOrg(db, auth, orgCreateWithOwner)
	if code != http.StatusCreated {
		t.Fatalf("answer %d %s, want 201", code, body)
	}
	if _, id := bodyWithoutID(t, body); id != addedOrg(t, auth) {
		t.Fatalf("answer id %s, add sent for %s", id, addedOrg(t, auth))
	}
	want := []string{orgCreateSlugRead, orgCreateOwnerRead, orgCreateOrgInsert, "auth:add", orgCreateMemberRead}
	if !reflect.DeepEqual(db.log, want) {
		t.Fatalf("steps:\n got %q\nwant %q", db.log, want)
	}
}

// A failed read-back leaves the owner state unknown: 500 (a half-written state
// a retry must not meet) and an ERROR naming the org; no deprovision and no
// DELETE.
func TestOrgCreateReadBackFailsIs500WithoutCleanup(t *testing.T) {
	db := newOrgCreateDB()
	db.ownerReadFails = true
	auth := &orgCreateProvisioner{fakeProvisioner: fakeProvisioner{db: db.provisionDB}, addErr: authprovision.ErrUnavailable}
	code, body, logs := callCreateOrg(db, auth, orgCreateWithOwner)
	if code != http.StatusInternalServerError || body != internalBody() {
		t.Fatalf("answer %d %s, want the 500 of a failed cleanup", code, body)
	}
	if len(auth.calls) != 1 || db.deleted() {
		t.Fatalf("auth calls %v, deleted %v: an unknown owner state must not deny or delete", auth.calls, db.deleted())
	}
	assertOwnerlessError(t, logs, addedOrg(t, auth))
}

// U5: the cleanup's deprovision fails: 500, an ERROR naming the org and no
// address, no DELETE.
func TestOrgCreateCleanupDeprovisionFailsIs500(t *testing.T) {
	db := newOrgCreateDB()
	auth := &orgCreateProvisioner{fakeProvisioner: fakeProvisioner{db: db.provisionDB}, addErr: refusedWith(http.StatusConflict, "principal_deactivated"),
		deprovisionErr: authprovision.ErrUnavailable}
	code, body, logs := callCreateOrg(db, auth, orgCreateWithOwner)
	if code != http.StatusInternalServerError || body != internalBody() {
		t.Fatalf("answer %d %s, want the 500 of a failed cleanup", code, body)
	}
	if db.deleted() {
		t.Fatalf("the org was deleted after a failed deprovision: %v", db.log)
	}
	assertOwnerlessError(t, logs, addedOrg(t, auth))
	assertNoAudit(t, db.log)
}

// A deprovision answering organization_not_found has nothing to deny: the
// DELETE runs.
func TestOrgCreateCleanupOrgNotInAuthStillDeletes(t *testing.T) {
	db := newOrgCreateDB()
	auth := &orgCreateProvisioner{fakeProvisioner: fakeProvisioner{db: db.provisionDB}, addErr: refusedWith(http.StatusNotFound, "principal_not_found"),
		deprovisionErr: refusedWith(http.StatusNotFound, "organization_not_found")}
	code, _, _ := callCreateOrg(db, auth, orgCreateWithOwner)
	if code != http.StatusServiceUnavailable || !db.deleted() {
		t.Fatalf("answer %d, deleted %v; want the 503 after the DELETE", code, db.deleted())
	}
}

// U8: the guarded DELETE takes no row and no owner row is there (a member
// was added meanwhile): 500 and an ERROR naming the org, no address.
func TestOrgCreateGuardKeepsOrgWithoutOwnerIs500(t *testing.T) {
	db := newOrgCreateDB()
	db.memberExists = true
	auth := &orgCreateProvisioner{fakeProvisioner: fakeProvisioner{db: db.provisionDB}, addErr: authprovision.ErrUnavailable}
	code, body, logs := callCreateOrg(db, auth, orgCreateWithOwner)
	if code != http.StatusInternalServerError || body != internalBody() {
		t.Fatalf("answer %d %s, want the 500 of a failed cleanup", code, body)
	}
	want := append(append([]string{orgCreateSlugRead, orgCreateOwnerRead, orgCreateOrgInsert, "auth:add", orgCreateMemberRead, "auth:deprovision"}, orgCreateGuardKeeps...), orgCreateMemberRead)
	if !reflect.DeepEqual(db.log, want) {
		t.Fatalf("steps:\n got %q\nwant %q", db.log, want)
	}
	assertOwnerlessError(t, logs, addedOrg(t, auth))
}

// U9: the guarded DELETE takes no row and the owner row is there now: 201.
func TestOrgCreateGuardKeepsOrgWithOwnerIs201(t *testing.T) {
	db := newOrgCreateDB()
	db.memberExists = true
	db.ownerAfterGuard = true
	auth := &orgCreateProvisioner{fakeProvisioner: fakeProvisioner{db: db.provisionDB}, addErr: authprovision.ErrUnavailable}
	code, body, _ := callCreateOrg(db, auth, orgCreateWithOwner)
	if code != http.StatusCreated {
		t.Fatalf("answer %d %s, want 201", code, body)
	}
	if _, id := bodyWithoutID(t, body); id != addedOrg(t, auth) {
		t.Fatalf("answer id %s, add sent for %s", id, addedOrg(t, auth))
	}
}

// C1-C3: the CLI create.
func TestOperatorCreateOrgWithOwnerRefusesWithTheSwitchOn(t *testing.T) {
	db := &provisionDB{rows: operatorRows()}
	_, err := Operator{db: db, OrgProvisioning: true}.CreateOrg(context.Background(), CreateOrgInput{Name: "Acme", Tier: "community", OwnerEmail: strp("a@example.test")})
	if !errors.Is(err, ErrOrgProvisioningOn) || len(db.log) != 0 {
		t.Fatalf("err %v, database calls %v; want ErrOrgProvisioningOn and none", err, db.log)
	}
}

var operatorCreateOrgSQL = []string{"BEGIN",
	"tx:SELECT id, email, username, password_hash, full_name, avatar_url, auth_provider, auth_provider_id, is_active, is_verified, is_superuser, token_version, last_login_at, created_at, updated_at FROM users WHERE lower(email) = $1",
	"tx:" + orgCreateSlugRead, "tx:" + orgCreateOrgInsert, "tx:" + orgCreateMemberRead, "tx:" + orgCreateMemberInsert, "COMMIT", "ROLLBACK"}

func TestOperatorCreateOrgWithoutOwnerRunsWithTheSwitchOn(t *testing.T) {
	on := &provisionDB{}
	if _, err := (Operator{db: on, OrgProvisioning: true}).CreateOrg(context.Background(), CreateOrgInput{Name: "Acme", Tier: "community"}); err != nil {
		t.Fatalf("switch on, no owner: %v", err)
	}
	off := &provisionDB{}
	if _, err := (Operator{db: off}).CreateOrg(context.Background(), CreateOrgInput{Name: "Acme", Tier: "community"}); err != nil {
		t.Fatalf("switch off, no owner: %v", err)
	}
	want := []string{"BEGIN", "tx:" + orgCreateSlugRead, "tx:" + orgCreateOrgInsert, "COMMIT", "ROLLBACK"}
	if !reflect.DeepEqual(on.log, want) || !reflect.DeepEqual(off.log, want) {
		t.Fatalf("no owner:\n on  %q\n off %q\nwant %q", on.log, off.log, want)
	}
}

func TestOperatorCreateOrgSwitchOffRunsTodaysSQL(t *testing.T) {
	db := &provisionDB{rows: map[string][]any{"FROM users WHERE lower(email) = $1": {zeroFill{}}}}
	if _, err := (Operator{db: db}).CreateOrg(context.Background(), CreateOrgInput{Name: "Acme", Tier: "community", OwnerEmail: strp("a@example.test")}); err != nil {
		t.Fatalf("switch off: %v", err)
	}
	if !reflect.DeepEqual(db.log, operatorCreateOrgSQL) {
		t.Fatalf("switch-off SQL:\n got %q\nwant %q", db.log, operatorCreateOrgSQL)
	}
}
