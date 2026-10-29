package admin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

var (
	provActor   = uuid.MustParse(validUUID)
	provOrg     = uuid.MustParse("0a000000-0000-4000-8000-00000000000a")
	provTarget  = uuid.MustParse("0b000000-0000-4000-8000-00000000000b")
	provOwner   = uuid.MustParse("0c000000-0000-4000-8000-00000000000c")
	provInvite  = uuid.MustParse("0d000000-0000-4000-8000-00000000000d")
	provMember  = uuid.MustParse("0e000000-0000-4000-8000-00000000000e")
	provStamp   = time.Date(2026, 10, 29, 9, 0, 0, 0, time.UTC)
	errProvNoDB = errors.New("provisionDB: no such row in this test")
)

// provisionDB records every database call, in order, with its SQL folded to
// one line; a transaction's calls are recorded as "tx:<sql>". BEGIN, every
// statement and COMMIT/ROLLBACK are steps, and each auth call is the step
// "auth:<verb>" in the same log.
type provisionDB struct {
	log []string
	// rows answers a QueryRow or Query: the first key the folded SQL
	// contains decides the scanned values.
	rows map[string][]any
}

func fold(sql string) string { return strings.Join(strings.Fields(sql), " ") }

func (d *provisionDB) add(step string) { d.log = append(d.log, step) }

func (d *provisionDB) answer(sql string) ([]any, bool) {
	for key, values := range d.rows {
		if strings.Contains(sql, key) {
			return values, true
		}
	}
	return nil, false
}

func (d *provisionDB) Begin(context.Context) (pgx.Tx, error) {
	d.add("BEGIN")
	return &provisionTx{db: d}, nil
}

func (d *provisionDB) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	d.add(fold(sql))
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func (d *provisionDB) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	d.add(fold(sql))
	values, _ := d.answer(fold(sql))
	return &provisionRows{values: values}, nil
}

func (d *provisionDB) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	d.add(fold(sql))
	values, ok := d.answer(fold(sql))
	return provisionRow{values: values, ok: ok}
}

// provisionTx is the transaction of a provisionDB: pgx.Tx is embedded so a
// method the handlers never call panics.
type provisionTx struct {
	pgx.Tx
	db *provisionDB
}

func (t *provisionTx) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	t.db.add("tx:" + fold(sql))
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func (t *provisionTx) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	t.db.add("tx:" + fold(sql))
	values, _ := t.db.answer(fold(sql))
	return &provisionRows{values: values}, nil
}

func (t *provisionTx) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	t.db.add("tx:" + fold(sql))
	values, ok := t.db.answer(fold(sql))
	return provisionRow{values: values, ok: ok}
}

func (t *provisionTx) Commit(context.Context) error   { t.db.add("COMMIT"); return nil }
func (t *provisionTx) Rollback(context.Context) error { t.db.add("ROLLBACK"); return nil }

type provisionRow struct {
	values []any
	ok     bool
}

func (r provisionRow) Scan(dest ...any) error {
	if !r.ok {
		return pgx.ErrNoRows
	}
	return scanValues(r.values, dest)
}

func scanValues(values, dest []any) error {
	if len(values) != len(dest) {
		return errProvNoDB
	}
	for i, value := range values {
		target := reflect.ValueOf(dest[i]).Elem()
		if value == nil {
			target.Set(reflect.Zero(target.Type()))
			continue
		}
		target.Set(reflect.ValueOf(value))
	}
	return nil
}

// provisionRows is a one-row result (or none when values is nil).
type provisionRows struct {
	pgx.Rows
	values []any
	done   bool
}

func (r *provisionRows) Next() bool {
	if r.done || r.values == nil {
		return false
	}
	r.done = true
	return true
}
func (r *provisionRows) Scan(dest ...any) error { return scanValues(r.values, dest) }
func (r *provisionRows) Err() error             { return nil }
func (r *provisionRows) Close()                 {}

// fakeProvisioner is auth-service's provisioning routes: it records each call
// in the database log (so the order against the SQL is one list) and answers
// err, or a success naming the requested rows.
type fakeProvisioner struct {
	db    *provisionDB
	err   error
	calls []string
	args  [][]any
	// role, when set, is the role a success answers (an accept's invite role).
	role string
}

func (f *fakeProvisioner) record(verb string, args ...any) {
	f.db.add("auth:" + verb)
	f.calls = append(f.calls, verb)
	f.args = append(f.args, args)
}

func (f *fakeProvisioner) AddMember(_ context.Context, org, principal, actor uuid.UUID, role string, invitedBy *uuid.UUID) (authprovision.Membership, error) {
	f.record("add", org, principal, actor, role, invitedBy)
	return authprovision.Membership{ID: provMember, OrgID: org, PrincipalID: principal, Role: role, Status: "active"}, f.err
}

func (f *fakeProvisioner) ChangeRole(_ context.Context, org, principal, actor uuid.UUID, role string) (authprovision.Membership, error) {
	f.record("role", org, principal, actor, role)
	return authprovision.Membership{ID: provMember, OrgID: org, PrincipalID: principal, Role: role, Status: "active"}, f.err
}

func (f *fakeProvisioner) RemoveMember(_ context.Context, org, principal, actor uuid.UUID) error {
	f.record("remove", org, principal, actor)
	return f.err
}

func (f *fakeProvisioner) TransferOwnership(_ context.Context, org, from, to, actor uuid.UUID) error {
	f.record("transfer", org, from, to, actor)
	return f.err
}

func (f *fakeProvisioner) CreateInvite(_ context.Context, in authprovision.InviteRequest) (authprovision.Invite, error) {
	f.record("invite", in)
	return authprovision.Invite{ID: provInvite, Token: "invite.token", ExpiresAt: provStamp.Add(inviteTTL)}, f.err
}

func (f *fakeProvisioner) AcceptInvite(_ context.Context, in authprovision.AcceptRequest) (authprovision.Accepted, error) {
	f.record("accept", in)
	return authprovision.Accepted{InviteID: provInvite, OrgID: provOrg, MembershipID: provMember, Role: f.role}, f.err
}

func (f *fakeProvisioner) Deprovision(_ context.Context, org, actor uuid.UUID) error {
	f.record("deprovision", org, actor)
	return f.err
}

func refusedWith(status int, reason string) error {
	return &authprovision.Refused{Status: status, Reason: reason}
}

// provisionRows answers the rows the five handlers read.
func provisionRowsFor() map[string][]any {
	joined := provStamp
	inviter := provActor
	return map[string][]any{
		// membershipByOrgUser, the re-read after a proxied add or role change.
		"SELECT id, org_id, user_id, role, invited_by_id, joined_at, created_at, updated_at FROM memberships WHERE org_id = $1 AND user_id = $2": {
			provMember, provOrg, provTarget, "admin", (*uuid.UUID)(nil), &joined, provStamp, provStamp},
		// orgByID.
		"FROM organizations WHERE id = $1": {provOrg, "acme", "Acme", (*string)(nil), []byte("{}"), "community", "self", true, provStamp, provStamp},
		// inviterIdentity.
		"SELECT full_name, email FROM users WHERE id = $1": {(*string)(nil), (*string)(nil)},
		// the re-read of a proxied invite.
		"FROM org_invites WHERE id = $1": {provInvite, provOrg, "new@example.com", "member", &inviter, "pending", provStamp.Add(inviteTTL),
			(*time.Time)(nil), provStamp, provStamp},
		// the current owner of a transfer.
		"FROM memberships WHERE org_id = $1 AND role = 'owner'": {provOwner},
		// refuse's target lock: no target is a superuser.
		"SELECT is_superuser FROM users WHERE id = ANY($1) ORDER BY id FOR UPDATE": {false},
		// the superuser read of a transfer target with no single owner.
		"SELECT is_superuser FROM users WHERE id = $1": {false},
		// transferOwnership's target role read (switch off).
		"SELECT role FROM memberships WHERE org_id = $1 AND user_id = $2": {"owner"},
		// transferOwnership's target membership check (switch off).
		"SELECT EXISTS(SELECT 1 FROM memberships WHERE org_id = $1 AND user_id = $2)": {true},
		// createInvite's pending-duplicate check (switch off).
		"FROM org_invites WHERE org_id = $1 AND lower(email) = $2": {false},
	}
}

type provisionRoute struct {
	name    string
	handler func(h *handlers) http.HandlerFunc
	method  string
	path    []string
	body    string
	verb    string
}

var provisionRoutes = []provisionRoute{
	{"add", func(h *handlers) http.HandlerFunc { return h.addMember }, http.MethodPost,
		[]string{"org_id", provOrg.String()}, `{"user_id": "` + provTarget.String() + `", "role": "admin"}`, "add"},
	{"role", func(h *handlers) http.HandlerFunc { return h.updateMemberRole }, http.MethodPatch,
		[]string{"org_id", provOrg.String(), "user_id", provTarget.String()}, `{"role": "admin"}`, "role"},
	{"remove", func(h *handlers) http.HandlerFunc { return h.removeMember }, http.MethodDelete,
		[]string{"org_id", provOrg.String(), "user_id", provTarget.String()}, ``, "remove"},
	{"transfer", func(h *handlers) http.HandlerFunc { return h.transferOwnership }, http.MethodPost,
		[]string{"org_id", provOrg.String()}, `{"new_owner_user_id": "` + provTarget.String() + `"}`, "transfer"},
	{"invite", func(h *handlers) http.HandlerFunc { return h.createOrgInvite }, http.MethodPost,
		[]string{"org_id", provOrg.String()}, `{"email": " New@Example.com ", "role": "member"}`, "invite"},
}

func newProvisionHandlers(db *provisionDB, auth authprovision.Provisioner) *handlers {
	h := &handlers{store: pgStore{Pool: db, Now: func() time.Time { return provStamp }}, logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		audit: audit.PGWriter{Now: func() time.Time { return provStamp }}}
	if auth != nil {
		h.orgProvision = auth
	}
	return h
}

// callRoute runs route as the superuser provActor.
func callRoute(h *handlers, route provisionRoute) (int, string) {
	r := answerWith(route.handler(h), route.method, func(req *http.Request) *http.Request {
		req = req.WithContext(policy.WithUser(req.Context(), &policy.User{IsSuperuser: true, UserID: validUUID, ID: provActor, Email: "root@example.com"}))
		req = pathValues(route.path...)(req)
		if route.name == "invite" {
			req = req.WithContext(context.WithValue(req.Context(), inviteInputKey{}, inviteInput{Email: " New@Example.com ", Role: "member"}))
		}
		if route.body != "" {
			req = withBody(route.body)(req)
		}
		return req
	})
	return r.Code, strings.TrimSpace(r.Body.String())
}

// The SQL each route runs today, switch off, superuser caller: the writer
// transaction with its statements in order. Pinned so the switch-off path
// stays byte-equal (X2).
var switchOffSQL = map[string][]string{
	"add": {"BEGIN", "tx:SELECT is_superuser FROM users WHERE id = ANY($1) ORDER BY id FOR UPDATE", "tx:SELECT id, org_id, user_id, role, invited_by_id, joined_at, created_at, updated_at FROM memberships WHERE org_id = $1 AND user_id = $2",
		"tx:INSERT INTO memberships (id, org_id, user_id, role, invited_by_id, joined_at, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
		"COMMIT", "ROLLBACK"},
	"role": {"BEGIN", "tx:SELECT is_superuser FROM users WHERE id = ANY($1) ORDER BY id FOR UPDATE", "tx:SELECT id, org_id, user_id, role, invited_by_id, joined_at, created_at, updated_at FROM memberships WHERE org_id = $1 AND user_id = $2",
		"tx:UPDATE memberships SET role = $3, updated_at = $4 WHERE org_id = $1 AND user_id = $2", "COMMIT", "ROLLBACK"},
	"remove": {"BEGIN", "tx:SELECT is_superuser FROM users WHERE id = ANY($1) ORDER BY id FOR UPDATE", "tx:SELECT id, org_id, user_id, role, invited_by_id, joined_at, created_at, updated_at FROM memberships WHERE org_id = $1 AND user_id = $2",
		"tx:DELETE FROM memberships WHERE org_id = $1 AND user_id = $2", "COMMIT", "ROLLBACK"},
	"transfer": {"BEGIN", "tx:SELECT user_id FROM memberships WHERE org_id = $1 AND role = 'owner'", "tx:SELECT is_superuser FROM users WHERE id = ANY($1) ORDER BY id FOR UPDATE",
		"tx:SELECT role FROM memberships WHERE org_id = $1 AND user_id = $2",
		"tx:SELECT EXISTS(SELECT 1 FROM memberships WHERE org_id = $1 AND user_id = $2)",
		"tx:UPDATE memberships SET role = 'admin', updated_at = $3 WHERE org_id = $1 AND user_id = $2",
		"tx:UPDATE memberships SET role = 'owner', updated_at = $3 WHERE org_id = $1 AND user_id = $2", "COMMIT", "ROLLBACK"},
	"invite": {"SELECT id, slug, name, description, settings, tier, managed_by, is_active, created_at, updated_at FROM organizations WHERE id = $1",
		"SELECT full_name, email FROM users WHERE id = $1", "BEGIN",
		"tx:SELECT EXISTS ( SELECT 1 FROM org_invites WHERE org_id = $1 AND lower(email) = $2 AND status = 'pending' AND expires_at >= $3 )",
		"tx:INSERT INTO org_invites (id, org_id, email, role, token_hash, invited_by_id, status, expires_at, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
		"tx:INSERT INTO audit_logs (id, org_id, user_id, action, resource_type, resource_id, description, changes, request_metadata, status, error_message, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
		"COMMIT", "ROLLBACK"},
}

func TestProvisionSwitchOffMakesNoAuthCallAndTheSameSQL(t *testing.T) {
	for _, route := range provisionRoutes {
		t.Run(route.name, func(t *testing.T) {
			db := &provisionDB{rows: provisionRowsFor()}
			if route.name == "add" {
				// a new member, so the add runs to its insert
				delete(db.rows, "SELECT id, org_id, user_id, role, invited_by_id, joined_at, created_at, updated_at FROM memberships WHERE org_id = $1 AND user_id = $2")
			}
			h := newProvisionHandlers(db, nil)
			callRoute(h, route)
			for _, step := range db.log {
				if strings.HasPrefix(step, "auth:") {
					t.Fatalf("an auth call with the switch off: %v", db.log)
				}
			}
			want := switchOffSQL[route.name]
			if !reflect.DeepEqual(db.log, want) {
				t.Fatalf("switch-off SQL:\n got %q\nwant %q", db.log, want)
			}
		})
	}
}

// The steps of each route with the switch on: the reads go-api made before
// the write today, the one auth call, then only the read-back of the row auth
// wrote. Nothing else, so no second write can hide in a read that finds a row.
var switchOnSteps = map[string][]string{
	"add":      {"auth:add", "SELECT id, org_id, user_id, role, invited_by_id, joined_at, created_at, updated_at FROM memberships WHERE org_id = $1 AND user_id = $2"},
	"role":     {"auth:role", "SELECT id, org_id, user_id, role, invited_by_id, joined_at, created_at, updated_at FROM memberships WHERE org_id = $1 AND user_id = $2"},
	"remove":   {"auth:remove"},
	"transfer": {"SELECT user_id FROM memberships WHERE org_id = $1 AND role = 'owner'", "auth:transfer"},
	"invite": {"SELECT id, slug, name, description, settings, tier, managed_by, is_active, created_at, updated_at FROM organizations WHERE id = $1",
		"SELECT full_name, email FROM users WHERE id = $1", "auth:invite",
		"SELECT id, org_id, email, role, invited_by_id, status, expires_at, accepted_at, created_at, updated_at FROM org_invites WHERE id = $1"},
}

// With the switch on every route calls auth once, as the caller, before any
// read of its answer, and go-api opens no transaction and writes nothing.
func TestProvisionSwitchOnCallsAuthOnceAndWritesNothing(t *testing.T) {
	wantArgs := map[string][]any{
		"add":      {provOrg, provTarget, provActor, "admin", (*uuid.UUID)(nil)},
		"role":     {provOrg, provTarget, provActor, "admin"},
		"remove":   {provOrg, provTarget, provActor},
		"transfer": {provOrg, provOwner, provTarget, provActor},
	}
	for _, route := range provisionRoutes {
		t.Run(route.name, func(t *testing.T) {
			db := &provisionDB{rows: provisionRowsFor()}
			auth := &fakeProvisioner{db: db}
			code, body := callRoute(newProvisionHandlers(db, auth), route)
			if code >= 300 {
				t.Fatalf("answer %d %s", code, body)
			}
			if len(auth.calls) != 1 || auth.calls[0] != route.verb {
				t.Fatalf("auth calls %v, want one %s", auth.calls, route.verb)
			}
			if want, ok := wantArgs[route.name]; ok && !reflect.DeepEqual(auth.args[0], want) {
				t.Fatalf("auth args %v, want %v", auth.args[0], want)
			}
			if want := switchOnSteps[route.name]; !reflect.DeepEqual(db.log, want) {
				t.Fatalf("switch-on steps:\n got %q\nwant %q", db.log, want)
			}
			for _, step := range db.log {
				if step == "BEGIN" || strings.HasPrefix(step, "tx:") || strings.Contains(step, "INSERT") || strings.Contains(step, "UPDATE") ||
					strings.Contains(step, "DELETE") || strings.Contains(step, "FOR UPDATE") || strings.Contains(step, "FOR SHARE") {
					t.Fatalf("go-api opened a transaction, took a lock or wrote: %v", db.log)
				}
			}
		})
	}
}

func TestProvisionInviteSendsTheRouteRequest(t *testing.T) {
	db := &provisionDB{rows: provisionRowsFor()}
	auth := &fakeProvisioner{db: db}
	code, body := callRoute(newProvisionHandlers(db, auth), provisionRoutes[4])
	if code != http.StatusCreated {
		t.Fatalf("answer %d %s", code, body)
	}
	in, _ := auth.args[0][0].(authprovision.InviteRequest)
	if in.OrgID != provOrg || in.Email != "new@example.com" || in.Role != "member" || in.TTL != inviteTTL || in.Actor != provActor ||
		in.Description != "Organization invite created" {
		t.Fatalf("invite request %+v", in)
	}
	if !strings.Contains(body, `"id":"`+provInvite.String()+`"`) || !strings.Contains(body, `"status":"pending"`) {
		t.Fatalf("invite answer %s, want the re-read row", body)
	}
}

func TestProvisionSuccessBodiesAreTodays(t *testing.T) {
	want := map[string]struct {
		code int
		body string
	}{
		"add": {http.StatusCreated, `{"id":"` + provMember.String() + `","org_id":"` + provOrg.String() + `","user_id":"` + provTarget.String() +
			`","role":"admin","invited_by_id":null,"joined_at":"2026-10-29T09:00:00Z","created_at":"2026-10-29T09:00:00Z","updated_at":"2026-10-29T09:00:00Z"}`},
		"role": {http.StatusOK, `{"id":"` + provMember.String() + `","org_id":"` + provOrg.String() + `","user_id":"` + provTarget.String() +
			`","role":"admin","invited_by_id":null,"joined_at":"2026-10-29T09:00:00Z","created_at":"2026-10-29T09:00:00Z","updated_at":"2026-10-29T09:00:00Z"}`},
		"remove":   {http.StatusOK, `{"deleted":true}`},
		"transfer": {http.StatusOK, `{"success":true}`},
	}
	for _, route := range provisionRoutes[:4] {
		db := &provisionDB{rows: provisionRowsFor()}
		code, body := callRoute(newProvisionHandlers(db, &fakeProvisioner{db: db}), route)
		if code != want[route.name].code || body != want[route.name].body {
			t.Fatalf("%s: %d %s, want %d %s", route.name, code, body, want[route.name].code, want[route.name].body)
		}
	}
}

const provUnavailable = `{"detail":{"message":"Authentication service unavailable"}}`

func provDetail(text string) string {
	raw, _ := json.Marshal(map[string]string{"detail": text})
	return string(raw)
}

// PARITY-10988: each auth answer maps to today's go-api answer; every other
// refusal or failure is the 503 of #5239, with nothing written.
func TestProvisionRefusalsMapToTodaysAnswers(t *testing.T) {
	type row struct {
		err  error
		code int
		body string
	}
	unavailable := []row{
		{authprovision.ErrUnavailable, 503, provUnavailable},
		{errors.New("other"), 503, provUnavailable},
		{refusedWith(409, "users_not_owned"), 503, provUnavailable},
		{refusedWith(409, "state_changed"), 503, provUnavailable},
		{refusedWith(409, "organization_suspended"), 503, provUnavailable},
		{refusedWith(400, "malformed"), 503, provUnavailable},
		{refusedWith(401, ""), 503, provUnavailable},
	}
	actorRows := []row{
		{refusedWith(403, "superuser_required"), 403, provDetail("Superuser access required")},
		{refusedWith(403, "admin_required"), 403, provDetail("Admin access required for organization")},
	}
	table := map[string][]row{
		"add": append(append([]row{
			{refusedWith(409, "already_member"), 400, provDetail("User is already a member of this organization")},
			{refusedWith(400, "role_invalid"), 400, provDetail("Invalid role: admin")},
		}, actorRows...), unavailable...),
		"role": append(append([]row{
			{refusedWith(400, "role_invalid"), 400, provDetail("Invalid role: admin")},
			{refusedWith(404, "membership_not_found"), 404, provDetail("Membership not found")},
			{refusedWith(409, "last_owner"), 400, provDetail("Cannot remove the last owner of an organization")},
		}, actorRows...), unavailable...),
		"remove": append(append([]row{
			{refusedWith(404, "membership_not_found"), 404, provDetail("Membership not found")},
			{refusedWith(409, "last_owner"), 400, provDetail("Cannot remove the last owner of an organization")},
		}, actorRows...), unavailable...),
		"transfer": append(append([]row{
			{refusedWith(409, "not_owner"), 400, provDetail("Source user is not an owner")},
			{refusedWith(409, "target_not_member"), 400, provDetail("Target user is not a member")},
		}, actorRows...), unavailable...),
		"invite": append([]row{
			{refusedWith(409, "invite_pending"), 409, provDetail("A pending invite already exists for this email")},
			{refusedWith(404, "organization_not_found"), 404, provDetail("Organization not found")},
			{refusedWith(400, "role_invalid"), 400, provDetail("Invalid role: member")},
			{refusedWith(403, "admin_required"), 403, provDetail("Admin access required for organization")},
			{refusedWith(400, "email_invalid"), 503, provUnavailable},
		}, unavailable...),
	}
	for _, route := range provisionRoutes {
		for _, tc := range table[route.name] {
			db := &provisionDB{rows: provisionRowsFor()}
			code, body := callRoute(newProvisionHandlers(db, &fakeProvisioner{db: db, err: tc.err}), route)
			if code != tc.code || body != tc.body {
				t.Fatalf("%s %v: %d %s, want %d %s", route.name, tc.err, code, body, tc.code, tc.body)
			}
			if i := indexOf(db.log, "auth:"+route.verb); i < 0 || len(db.log) != i+1 {
				t.Fatalf("%s %v: steps after the refused call: %v", route.name, tc.err, db.log)
			}
		}
	}
}

func indexOf(log []string, step string) int {
	for i, s := range log {
		if s == step {
			return i
		}
	}
	return -1
}

// Refusals go-api decides before the call: a role outside the catalog (add,
// invite) and a transfer with no single owner. Auth is not called.
func TestProvisionLocalRefusalsMakeNoAuthCall(t *testing.T) {
	for _, tc := range []struct {
		route provisionRoute
		rows  func(map[string][]any)
		code  int
		body  string
	}{
		{provisionRoute{"add", provisionRoutes[0].handler, http.MethodPost, provisionRoutes[0].path,
			`{"user_id": "` + provTarget.String() + `", "role": ""}`, "add"}, nil, 400, provDetail("Invalid role: ")},
		{provisionRoute{"add", provisionRoutes[0].handler, http.MethodPost, provisionRoutes[0].path,
			`{"user_id": "` + provTarget.String() + `", "role": "superadmin"}`, "add"}, nil, 400, provDetail("Invalid role: superadmin")},
		{provisionRoutes[3], func(rows map[string][]any) { delete(rows, "FROM memberships WHERE org_id = $1 AND role = 'owner'") },
			400, provDetail("Source user is not an owner")},
	} {
		db := &provisionDB{rows: provisionRowsFor()}
		if tc.rows != nil {
			tc.rows(db.rows)
		}
		auth := &fakeProvisioner{db: db}
		code, body := callRoute(newProvisionHandlers(db, auth), tc.route)
		if code != tc.code || body != tc.body || len(auth.calls) != 0 {
			t.Fatalf("%s %s: %d %s calls %v, want %d %s and no call", tc.route.name, tc.route.body, code, body, auth.calls, tc.code, tc.body)
		}
	}
}

// The invite role check: an invite of an unknown role answers 400 before the call.
func TestProvisionInviteRoleOutsideTheCatalogMakesNoAuthCall(t *testing.T) {
	db := &provisionDB{rows: provisionRowsFor()}
	auth := &fakeProvisioner{db: db}
	h := newProvisionHandlers(db, auth)
	r := answerWith(h.createOrgInvite, http.MethodPost, func(req *http.Request) *http.Request {
		req = req.WithContext(policy.WithUser(req.Context(), &policy.User{IsSuperuser: true, UserID: validUUID, ID: provActor}))
		req = pathValues("org_id", provOrg.String())(req)
		return req.WithContext(context.WithValue(req.Context(), inviteInputKey{}, inviteInput{Email: "a@example.com", Role: "boss"}))
	})
	if r.Code != 400 || strings.TrimSpace(r.Body.String()) != provDetail("Invalid role: boss") || len(auth.calls) != 0 {
		t.Fatalf("%d %s calls %v", r.Code, r.Body.String(), auth.calls)
	}
}

// A refused or failed deprovision stops the delete; an organization auth does
// not hold (404) lets the purge run, as today's delete of a missing org does.
func TestDeprovisionInAuthDecidesWhetherTheDeleteGoesOn(t *testing.T) {
	for _, tc := range []struct {
		err     error
		goesOn  bool
		authErr bool
	}{
		{nil, true, false},
		{refusedWith(404, "organization_not_found"), true, false},
		{refusedWith(403, "superuser_required"), false, true},
		{refusedWith(409, "users_not_owned"), false, true},
		{refusedWith(409, "state_changed"), false, true},
		{authprovision.ErrUnavailable, false, true},
	} {
		db := &provisionDB{}
		auth := &fakeProvisioner{db: db, err: tc.err}
		h := newProvisionHandlers(db, auth)
		ctx := policy.WithUser(context.Background(), &policy.User{IsSuperuser: true, UserID: validUUID, ID: provActor})
		err := h.deprovisionInAuth(ctx, provOrg)
		var authErr *orgDeletionAuthError
		if (err == nil) != tc.goesOn || errors.As(err, &authErr) != tc.authErr {
			t.Fatalf("%v: deprovisionInAuth = %v", tc.err, err)
		}
		if len(auth.calls) != 1 || !reflect.DeepEqual(auth.args[0], []any{provOrg, provActor}) {
			t.Fatalf("%v: calls %v %v", tc.err, auth.calls, auth.args)
		}
	}
}

// With the switch on and no usable ClickHouse the delete is refused before
// auth is asked: nothing is deprovisioned for a delete that cannot purge.
func TestOrgDeletionRefusedByThePreflightMakesNoAuthCall(t *testing.T) {
	db := &provisionDB{}
	auth := &fakeProvisioner{db: db}
	h := newProvisionHandlers(db, auth)
	r := answerWith(h.deleteOrganization, http.MethodDelete, func(req *http.Request) *http.Request {
		req = req.WithContext(policy.WithUser(req.Context(), &policy.User{IsSuperuser: true, UserID: validUUID, ID: provActor}))
		return pathValues("org_id", provOrg.String())(req)
	})
	if r.Code != http.StatusServiceUnavailable || len(auth.calls) != 0 || len(db.log) != 0 {
		t.Fatalf("%d calls %v db %v", r.Code, auth.calls, db.log)
	}
}

// The delete's auth refusal renders the 503 of #5239.
func TestOrgDeletionAuthErrorIsTheUnavailableAnswer(t *testing.T) {
	r := answerWith(func(w http.ResponseWriter, _ *http.Request) {
		writeOrgDeletionAuthError(w)
	}, http.MethodDelete, nil)
	if r.Code != http.StatusServiceUnavailable || strings.TrimSpace(r.Body.String()) != provUnavailable {
		t.Fatalf("%d %s", r.Code, r.Body.String())
	}
}
