//go:build integration

package local

import (
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/clientaddr"
	"github.com/full-chaos/dev-health-ops/internal/auth/publicaudit"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
)

// CHAOS-11400: while users are owned by auth-service, go-api refuses register, verify, forgot, reset and change
// (internal/api/session refuseWhileUsersOwned), so the go-api public.audit_logs row each of them wrote comes from
// auth-service, in the flow's own transaction, with go-api's columns (internal/api/audit.PGWriter through
// session.emitAudit / auditUserEvent). Rulings (acp, PLAN-11400-2): org_id is the issued or current session's
// organization, else the user's first public membership in the login order, else no row; request_metadata is
// go-api's {ip_address, user_agent, request_id} with request_id the forwarded X-Request-Id, else auth-service's own
// request id; created_at is the auth event's database time (one instant for both rows); a failed insert aborts the
// flow, except forgot, which answers as a success and commits nothing.

const (
	parityClientIP      = "203.0.113.7"
	parityUserAgent     = "venue-oracle/1"
	parityForwardedID   = "venue-oracle-req-1"
	parityMetadata      = `{"ip_address": "203.0.113.7", "user_agent": "venue-oracle/1", "request_id": "venue-oracle-req-1"}`
	parityOwnRequestID  = "6f2b1c1e-6a0e-4c43-9e0f-0b6c7a1d2e3f"
	parityOwnIDMetadata = `{"ip_address": "203.0.113.7", "user_agent": "venue-oracle/1", "request_id": "6f2b1c1e-6a0e-4c43-9e0f-0b6c7a1d2e3f"}`
)

// parityContext binds what auth-service's middleware binds for a request: the
// client address and user agent, auth-service's own request id and the
// correlation id the go-api proxy forwards in X-Request-Id.
func parityContext() context.Context {
	ctx := clientaddr.With(context.Background(), netip.MustParseAddr(parityClientIP))
	ctx = clientaddr.WithUserAgent(ctx, parityUserAgent)
	ctx = requestid.With(ctx, uuid.MustParse(parityOwnRequestID))
	return publicaudit.WithForwardedRequestID(ctx, parityForwardedID)
}

// goAPIAuditRow is one public.audit_logs row, every column go-api writes.
type goAPIAuditRow struct {
	ID              uuid.UUID
	OrgID           string
	UserID          *string
	Action          string
	ResourceType    string
	ResourceID      string
	Description     *string
	Changes         *string
	RequestMetadata *string
	Status          string
	ErrorMessage    *string
	CreatedAt       time.Time
}

func strp(s string) *string { return &s }

func deref(p *string) string {
	if p == nil {
		return "<NULL>"
	}
	return *p
}

// publicAuditRows reads every public.audit_logs row of action, as the admin.
func (r *endRig) publicAuditRows(t *testing.T, ctx context.Context, action string) []goAPIAuditRow {
	t.Helper()
	rows, err := r.env.admin.Query(ctx, `SELECT id, org_id::text, user_id::text, action, resource_type, resource_id, description,
	changes::text, request_metadata::text, status, error_message, created_at
FROM public.audit_logs WHERE action = $1 ORDER BY created_at, id`, action)
	if err != nil {
		t.Fatalf("read public.audit_logs: %v", err)
	}
	defer rows.Close()
	var out []goAPIAuditRow
	for rows.Next() {
		var row goAPIAuditRow
		if err := rows.Scan(&row.ID, &row.OrgID, &row.UserID, &row.Action, &row.ResourceType, &row.ResourceID, &row.Description,
			&row.Changes, &row.RequestMetadata, &row.Status, &row.ErrorMessage, &row.CreatedAt); err != nil {
			t.Fatalf("scan public.audit_logs: %v", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read public.audit_logs: %v", err)
	}
	return out
}

// eventTime is the occurred_at of the one auth event of eventType for subject.
func (r *endRig) eventTime(t *testing.T, ctx context.Context, eventType string, subject uuid.UUID) time.Time {
	t.Helper()
	var at time.Time
	if err := r.env.admin.QueryRow(ctx, `SELECT occurred_at FROM auth.security_audit_events WHERE event_type = $1 AND subject_principal_id = $2`,
		eventType, subject).Scan(&at); err != nil {
		t.Fatalf("the one %s event of %s: %v", eventType, subject, err)
	}
	return at
}

// requireOneGoAPIRow asserts exactly one public.audit_logs row of want.Action,
// every column equal to want, and created_at equal to the occurred_at of the
// flow's auth event (eventType of subject): one instant for both rows.
func (r *endRig) requireOneGoAPIRow(t *testing.T, ctx context.Context, want goAPIAuditRow, eventType string, subject uuid.UUID) {
	t.Helper()
	got := r.publicAuditRows(t, ctx, want.Action)
	if len(got) != 1 {
		t.Fatalf("public.audit_logs rows of action %q = %d, want exactly 1 (go-api writes one; auth-service must write it while users are owned)",
			want.Action, len(got))
	}
	row := got[0]
	if row.ID == uuid.Nil {
		t.Errorf("id is the nil uuid")
	}
	check := func(column, got, want string) {
		if got != want {
			t.Errorf("%s = %q, want %q", column, got, want)
		}
	}
	check("org_id", row.OrgID, want.OrgID)
	check("user_id", deref(row.UserID), deref(want.UserID))
	check("action", row.Action, want.Action)
	check("resource_type", row.ResourceType, want.ResourceType)
	check("resource_id", row.ResourceID, want.ResourceID)
	check("description", deref(row.Description), deref(want.Description))
	check("changes", deref(row.Changes), deref(want.Changes))
	check("request_metadata", deref(row.RequestMetadata), deref(want.RequestMetadata))
	check("status", row.Status, want.Status)
	check("error_message", deref(row.ErrorMessage), deref(want.ErrorMessage))
	if at := r.eventTime(t, ctx, eventType, subject); !row.CreatedAt.Equal(at) {
		t.Errorf("created_at = %s, want the %s event's occurred_at %s", row.CreatedAt, eventType, at)
	}
}

// requireNoGoAPIRow asserts no public.audit_logs row of action.
func (r *endRig) requireNoGoAPIRow(t *testing.T, ctx context.Context, action, why string) {
	t.Helper()
	if got := r.publicAuditRows(t, ctx, action); len(got) != 0 {
		t.Fatalf("%s: public.audit_logs rows of action %q = %d, want 0", why, action, len(got))
	}
}

// registerOwner signs up email with an organization through flows and returns
// the new user and its one public membership's organization.
func (r *endRig) registerOwner(t *testing.T, ctx context.Context, flows *Flows, email, password, orgName string) (user, org uuid.UUID) {
	t.Helper()
	outcome, err := flows.Register(ctx, RegisterRequest{Email: email, Password: password, OrganizationName: &orgName})
	if err != nil || outcome.Kind != FlowOK {
		t.Fatalf("Register(%s) = %v, %v", email, outcome.Kind, err)
	}
	if err := r.env.admin.QueryRow(ctx, `SELECT u.id, m.org_id FROM public.users u JOIN public.memberships m ON m.user_id = u.id
WHERE u.email = $1`, email).Scan(&user, &org); err != nil {
		t.Fatalf("the registered user and its one membership: %v", err)
	}
	return user, org
}

// registerAlone signs up email without an organization: a user with no membership.
func (r *endRig) registerAlone(t *testing.T, ctx context.Context, email, password string) uuid.UUID {
	t.Helper()
	if out, err := r.flows.Register(ctx, RegisterRequest{Email: email, Password: password}); err != nil || out.Kind != FlowOK {
		t.Fatalf("Register(%s, no org) = %v, %v", email, out.Kind, err)
	}
	var user uuid.UUID
	if err := r.env.admin.QueryRow(ctx, `SELECT id FROM public.users WHERE email = $1`, email).Scan(&user); err != nil {
		t.Fatal(err)
	}
	return user
}

func (r *endRig) authEvents(t *testing.T, ctx context.Context, eventType string, subject uuid.UUID) int {
	t.Helper()
	return r.env.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = $1 AND subject_principal_id = $2`,
		eventType, subject)
}

// mailFlows is rig.flows with a mail sink the test reads.
func (r *endRig) mailFlows(t *testing.T) (*Flows, *liveMail) {
	t.Helper()
	mail := &liveMail{}
	flows, err := NewFlows(FlowsConfig{Accounts: r.accounts, Links: r.links, Sessions: r.sessions, Mail: mail,
		Locker: r.links, UsersOwnedByAuth: true, Hash: cheapHash})
	if err != nil {
		t.Fatal(err)
	}
	return flows, mail
}

// go-api register.go:167: with an organization, create/user "User registered",
// org_id = the new organization, user_id = resource_id = the new user,
// changes {"email": <normalized>, "organization_id": <org>} in that key order.
func TestRegisterWritesGoAPIsPublicAuditRow(t *testing.T) {
	ctx := parityContext()
	rig := newEndRig(t, ctx)
	user, org := rig.registerOwner(t, ctx, rig.flows, "nina.new@example.com", "Correct-horse-battery-9", "Nina's Team")
	rig.requireOneGoAPIRow(t, ctx, goAPIAuditRow{
		OrgID: org.String(), UserID: strp(user.String()), Action: "create", ResourceType: "user", ResourceID: user.String(),
		Description:     strp("User registered"),
		Changes:         strp(`{"email": "nina.new@example.com", "organization_id": "` + org.String() + `"}`),
		RequestMetadata: strp(parityMetadata), Status: "success",
	}, "auth.principal.provisioned", user)
}

// Without a forwarded correlation id the row's request_id is auth-service's own
// request id (its audit form, the one auth.security_audit_events.request_id holds).
func TestTheRowCarriesAuthServicesOwnRequestIDWithoutAForwardedOne(t *testing.T) {
	ctx := clientaddr.With(context.Background(), netip.MustParseAddr(parityClientIP))
	ctx = clientaddr.WithUserAgent(ctx, parityUserAgent)
	ctx = requestid.With(ctx, uuid.MustParse(parityOwnRequestID))
	rig := newEndRig(t, ctx)
	user, org := rig.registerOwner(t, ctx, rig.flows, "own.id@example.com", "Correct-horse-battery-9", "Own Id Org")
	rig.requireOneGoAPIRow(t, ctx, goAPIAuditRow{
		OrgID: org.String(), UserID: strp(user.String()), Action: "create", ResourceType: "user", ResourceID: user.String(),
		Description:     strp("User registered"),
		Changes:         strp(`{"email": "own.id@example.com", "organization_id": "` + org.String() + `"}`),
		RequestMetadata: strp(parityOwnIDMetadata), Status: "success",
	}, "auth.principal.provisioned", user)
}

// go-api writes the register row only when it creates an organization (the
// row's org_id is NOT NULL); a refused sign-up writes none.
func TestRegisterWithoutAnOrganizationOrRefusedWritesNoPublicAuditRow(t *testing.T) {
	ctx := parityContext()
	rig := newEndRig(t, ctx)
	rig.registerAlone(t, ctx, "no.org@example.com", "Correct-horse-battery-9")
	name := "Short Org"
	outcome, err := rig.flows.Register(ctx, RegisterRequest{Email: "short@example.com", Password: "short", OrganizationName: &name})
	if err != nil || outcome.Kind != FlowPolicy {
		t.Fatalf("Register with a short password = %v, %v; want %s", outcome.Kind, err, FlowPolicy)
	}
	rig.requireNoGoAPIRow(t, ctx, "create", "a sign-up without an organization and a policy refusal")
}

// go-api's verify (verify.go:85) writes no audit row: auth-service must not add one.
func TestVerifyWritesNoPublicAuditRow(t *testing.T) {
	ctx := parityContext()
	rig := newEndRig(t, ctx)
	flows, mail := rig.mailFlows(t)
	user, _ := rig.registerOwner(t, ctx, flows, "verify.me@example.com", "Correct-horse-battery-9", "Verify Org")
	link := mail.last(t)
	if link.Purpose != PurposeEmailVerification {
		t.Fatalf("last mail purpose = %s, want %s", link.Purpose, PurposeEmailVerification)
	}
	out, err := flows.Verify(ctx, link.Secret.Reveal())
	if err != nil || out.Kind != FlowOK {
		t.Fatalf("Verify = %v, %v", out.Kind, err)
	}
	if n := rig.env.count(t, ctx, `SELECT count(*) FROM public.audit_logs WHERE action <> 'create'`); n != 0 {
		t.Fatalf("public.audit_logs rows other than the sign-up's = %d, want 0: go-api's verify writes none", n)
	}
	if n := rig.authEvents(t, ctx, "auth.user.email_verified", user); n != 1 {
		t.Errorf("auth.user.email_verified rows = %d, want 1: the auth trail is kept", n)
	}
}

// go-api verify.go:231: password_reset_requested/user "Password reset requested", changes {}. Only for a user with a
// membership; an unknown address and a user with no membership write none.
func TestForgotWritesGoAPIsPublicAuditRow(t *testing.T) {
	ctx := parityContext()
	rig := newEndRig(t, ctx)
	user, org := rig.registerOwner(t, ctx, rig.flows, "forgot.me@example.com", "Correct-horse-battery-9", "Forgot Org")
	if err := rig.flows.Forgot(ctx, "nobody@example.com"); err != nil {
		t.Fatalf("Forgot(unknown) = %v", err)
	}
	rig.registerAlone(t, ctx, "lonely@example.com", "Correct-horse-battery-9")
	if err := rig.flows.Forgot(ctx, "lonely@example.com"); err != nil {
		t.Fatalf("Forgot(no membership) = %v", err)
	}
	rig.requireNoGoAPIRow(t, ctx, "password_reset_requested", "an unknown address and a user with no membership")
	if err := rig.flows.Forgot(ctx, "forgot.me@example.com"); err != nil {
		t.Fatalf("Forgot = %v", err)
	}
	rig.requireOneGoAPIRow(t, ctx, goAPIAuditRow{
		OrgID: org.String(), UserID: strp(user.String()), Action: "password_reset_requested", ResourceType: "user",
		ResourceID: user.String(), Description: strp("Password reset requested"), Changes: strp("{}"),
		RequestMetadata: strp(parityMetadata), Status: "success",
	}, "auth.password.reset_requested", user)
}

// go-api verify.go:286: password_reset/user "User reset password", changes {}. A refused reset writes no row.
func TestResetWritesGoAPIsPublicAuditRow(t *testing.T) {
	ctx := parityContext()
	rig := newEndRig(t, ctx)
	user, org := rig.registerOwner(t, ctx, rig.flows, "reset.me@example.com", "Correct-horse-battery-9", "Reset Org")
	if err := rig.links.IssueBound(ctx, IssueRequest{PrincipalID: user, Purpose: PurposePasswordReset, TokenHash: linkHash("parity-reset"), TTL: ResetTTL}); err != nil {
		t.Fatal(err)
	}
	if out, err := rig.flows.Reset(ctx, "parity-reset", "short"); err != nil || out.Kind != FlowPolicy {
		t.Fatalf("Reset(short) = %v, %v; want %s", out.Kind, err, FlowPolicy)
	}
	if out, err := rig.flows.Reset(ctx, "not-a-live-link", "A-new-pass-7777"); err != nil || out.Kind != FlowInvalidToken {
		t.Fatalf("Reset(unknown link) = %v, %v; want %s", out.Kind, err, FlowInvalidToken)
	}
	rig.requireNoGoAPIRow(t, ctx, "password_reset", "a refused reset")
	out, err := rig.flows.Reset(ctx, "parity-reset", "A-new-pass-7777")
	if err != nil || out.Kind != FlowOK {
		t.Fatalf("Reset = %v, %v", out.Kind, err)
	}
	rig.requireOneGoAPIRow(t, ctx, goAPIAuditRow{
		OrgID: org.String(), UserID: strp(user.String()), Action: "password_reset", ResourceType: "user",
		ResourceID: user.String(), Description: strp("User reset password"), Changes: strp("{}"),
		RequestMetadata: strp(parityMetadata), Status: "success",
	}, "auth.password.reset", user)
}

// go-api changepassword.go:178: password_changed/user "User changed own password", changes {}, in the organization
// of the session the change issues, else the first membership (here: a session without an organization, the user's
// one membership). A refused change and a user with no organization at all write no row.
func TestChangeWritesGoAPIsPublicAuditRow(t *testing.T) {
	ctx := parityContext()
	rig := newEndRig(t, ctx)
	user, org := rig.registerOwner(t, ctx, rig.flows, "change.me@example.com", "Correct-horse-battery-9", "Change Org")
	if out, err := rig.flows.Change(ctx, ChangeRequest{PrincipalID: user, Current: "Wrong-horse-battery-9", New: "A-new-pass-7777"}); err != nil || out.Kind != FlowCurrentRejected {
		t.Fatalf("Change(wrong current) = %v, %v; want %s", out.Kind, err, FlowCurrentRejected)
	}
	lonely := rig.registerAlone(t, ctx, "lonely.change@example.com", "Correct-horse-battery-9")
	if out, err := rig.flows.Change(ctx, ChangeRequest{PrincipalID: lonely, Current: "Correct-horse-battery-9", New: "A-new-pass-7777"}); err != nil || out.Kind != FlowOK {
		t.Fatalf("Change(no organization) = %v, %v", out.Kind, err)
	}
	rig.requireNoGoAPIRow(t, ctx, "password_changed", "a refused change and a user with no organization")
	out, err := rig.flows.Change(ctx, ChangeRequest{PrincipalID: user, Current: "Correct-horse-battery-9", New: "A-new-pass-7777"})
	if err != nil || out.Kind != FlowOK {
		t.Fatalf("Change = %v, %v", out.Kind, err)
	}
	rig.requireOneGoAPIRow(t, ctx, goAPIAuditRow{
		OrgID: org.String(), UserID: strp(user.String()), Action: "password_changed", ResourceType: "user",
		ResourceID: user.String(), Description: strp("User changed own password"), Changes: strp("{}"),
		RequestMetadata: strp(parityMetadata), Status: "success",
	}, "auth.user.password_changed", user)
}

// refusePublicAuditInserts makes every INSERT into public.audit_logs fail, as a
// lost grant or a constraint would.
func (r *endRig) refusePublicAuditInserts(t *testing.T, ctx context.Context) {
	t.Helper()
	for _, statement := range []string{
		`CREATE FUNCTION public.chaos11400_refuse_audit_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'chaos11400: injected public.audit_logs insert failure'; END $$`,
		`CREATE TRIGGER chaos11400_refuse_audit_insert BEFORE INSERT ON public.audit_logs
FOR EACH ROW EXECUTE FUNCTION public.chaos11400_refuse_audit_insert()`,
	} {
		if _, err := r.env.admin.Exec(ctx, statement); err != nil {
			t.Fatalf("install the insert refusal: %v", err)
		}
	}
}

// The public row is in the flow's transaction: a failed insert fails register, reset and change and commits nothing
// (no user, organization, auth event or password change). Forgot answers as a success (no enumeration) and commits
// nothing: no reset link, no auth event, no mail delivered.
func TestAPublicAuditInsertFailureAbortsTheFlow(t *testing.T) {
	ctx := parityContext()
	rig := newEndRig(t, ctx)
	flows, mail := rig.mailFlows(t)
	user, _ := rig.registerOwner(t, ctx, flows, "abort.change@example.com", "Correct-horse-battery-9", "Abort Org")
	if err := rig.links.IssueBound(ctx, IssueRequest{PrincipalID: user, Purpose: PurposePasswordReset, TokenHash: linkHash("abort-reset"), TTL: ResetTTL}); err != nil {
		t.Fatal(err)
	}
	hashBefore := rig.passwordHash(t, ctx, user)
	rig.refusePublicAuditInserts(t, ctx)

	name := "Never Org"
	outcome, err := flows.Register(ctx, RegisterRequest{Email: "abort.register@example.com", Password: "Correct-horse-battery-9", OrganizationName: &name})
	if err == nil {
		t.Errorf("Register with a failing public audit insert = %v, nil error; want the flow to fail", outcome.Kind)
	}
	if n := rig.env.count(t, ctx, `SELECT count(*) FROM public.users WHERE email = 'abort.register@example.com'`); n != 0 {
		t.Errorf("public.users rows of the aborted sign-up = %d, want 0", n)
	}
	if n := rig.env.count(t, ctx, `SELECT count(*) FROM auth.users WHERE email_lower = 'abort.register@example.com'`); n != 0 {
		t.Errorf("auth.users rows of the aborted sign-up = %d, want 0", n)
	}
	if n := rig.env.count(t, ctx, `SELECT count(*) FROM public.organizations WHERE name = 'Never Org'`); n != 0 {
		t.Errorf("organizations of the aborted sign-up = %d, want 0", n)
	}

	if out, err := flows.Change(ctx, ChangeRequest{PrincipalID: user, Current: "Correct-horse-battery-9", New: "A-new-pass-7777"}); err == nil {
		t.Errorf("Change with a failing public audit insert = %v, nil error; want the flow to fail", out.Kind)
	}
	if out, err := flows.Reset(ctx, "abort-reset", "A-new-pass-8888"); err == nil {
		t.Errorf("Reset with a failing public audit insert = %v, nil error; want the flow to fail", out.Kind)
	}
	if got := rig.passwordHash(t, ctx, user); got != hashBefore {
		t.Errorf("the password changed although its public audit row failed")
	}
	for _, eventType := range []string{"auth.user.password_changed", "auth.password.reset"} {
		if n := rig.authEvents(t, ctx, eventType, user); n != 0 {
			t.Errorf("%s rows = %d, want 0: the aborted flow left an auth event", eventType, n)
		}
	}

	links := rig.env.count(t, ctx, `SELECT count(*) FROM auth.link_credentials WHERE principal_id = $1 AND purpose = 'password_reset'`, user)
	requested := rig.authEvents(t, ctx, "auth.password.reset_requested", user)
	sent := len(mail.sent)
	if err := flows.Forgot(ctx, "abort.change@example.com"); err != nil {
		t.Errorf("Forgot with a failing public audit insert = %v; want the answer of a success", err)
	}
	if n := rig.authEvents(t, ctx, "auth.password.reset_requested", user); n != requested {
		t.Errorf("auth.password.reset_requested rows = %d, want %d: the skipped request left an auth event", n, requested)
	}
	if n := rig.env.count(t, ctx, `SELECT count(*) FROM auth.link_credentials WHERE principal_id = $1 AND purpose = 'password_reset'`, user); n != links {
		t.Errorf("reset links = %d, want %d: the skipped request changed the links", n, links)
	}
	for _, m := range mail.sent[sent:] {
		if m.Deliver {
			t.Errorf("the skipped request delivered a %s mail", m.Purpose)
		}
	}
	if len(mail.sent) != sent+1 {
		t.Errorf("mails after the skipped request = %d, want one decoy (the call sequence of every other address)", len(mail.sent)-sent)
	}
}
