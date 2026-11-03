//go:build integration

package oidc

import (
	"context"
	"encoding/base64"
	"errors"
	"maps"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

const jitWaitBound = 20 * time.Second

// jitHarness is the OIDC harness with the real PGJIT over the real identity and
// session stores: only the token mint is a double.
type jitHarness struct {
	*harness
	mintErr error
}

func newJITHarness(t *testing.T, opts harnessOpts) *jitHarness {
	t.Helper()
	opts.signup = true
	if opts.allowedDomains == nil {
		opts.allowedDomains = []string{"example.com"}
	}
	h := newHarness(t, opts)
	sessions, err := session.New(session.Config{Pool: h.env.runtime, Schema: h.env.schema, UsersOwned: true,
		SuccessorKey: contracts.NewSecretString(base64.RawURLEncoding.EncodeToString(make([]byte, 32)))})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.ids.on.Store.ProjectOrganization(context.Background(), h.orgID); err != nil {
		t.Fatal(err)
	}
	j := &jitHarness{harness: h}
	h.svc.cfg.MembershipJIT = PGJIT{Identities: h.ids.on.Store, Sessions: sessions, LoginEvent: jitLoginEvent, Mint: j.mint}
	return j
}

func jitLoginEvent(_ context.Context, principal uuid.UUID, org *uuid.UUID) audit.AuditEvent {
	actor, orgText := principal.String(), org.String()
	return audit.AuditEvent{EventType: "auth.login.succeeded", Outcome: audit.OutcomeAllowed, ActorPrincipalID: &actor,
		SubjectPrincipalID: &actor, OrganizationID: &orgText,
		Attributes: map[string]any{"reason_code": "accepted", "target_kind": "session", "provider": "oidc"}}
}

func (j *jitHarness) mint(context.Context, session.Issued, SessionRequest) (Tokens, error) {
	if j.mintErr != nil {
		return Tokens{}, j.mintErr
	}
	return Tokens{AccessToken: "jit-access", RefreshToken: "jit-refresh"}, nil
}

func (j *jitHarness) signIn(t *testing.T, sub, email string) (Login, *Error) {
	t.Helper()
	s := j.start(t)
	return j.finish(t, s, j.sign(t, j.claims(s.nonce, sub, email)))
}

// account is an existing account with this provider's identity and no membership.
func (j *jitHarness) account(t *testing.T, sub, email string) uuid.UUID {
	t.Helper()
	id, err := j.ids.on.Store.ProvisionFederated(context.Background(), identity.ProvisionRequest{
		Key:   identity.ExternalKey{Issuer: j.issuer, Provider: identity.Provider(j.providerID.String()), Subject: sub},
		Email: email, Actor: identity.SelfSignUp(nil), LegacyProvider: "oidc", LegacySubject: sub})
	if err != nil {
		t.Fatalf("provision the existing account: %v", err)
	}
	return id
}

func (j *jitHarness) setDefaultRole(t *testing.T, role string) {
	t.Helper()
	if _, err := j.env.admin.Exec(context.Background(), `UPDATE public.sso_providers SET default_role = $2 WHERE id = $1`, j.providerID, role); err != nil {
		t.Fatal(err)
	}
}

// jitTableCounts is the row count of every table a sign-in join writes.
func (j *jitHarness) jitTableCounts(t *testing.T) map[string]int {
	t.Helper()
	counts := map[string]int{}
	for _, table := range []string{"auth.principals", "auth.users", "auth.external_identities", "auth.memberships", "auth.sessions",
		"auth.refresh_credentials", "auth.security_audit_events", "auth.auth_outbox_events", "public.users", "public.memberships"} {
		counts[table] = j.count(t, `SELECT count(*) FROM `+table)
	}
	return counts
}

func wantSameCounts(t *testing.T, before, after map[string]int) {
	t.Helper()
	if !maps.Equal(before, after) {
		t.Fatalf("rows changed: before %v after %v", before, after)
	}
}

// failInsertsOf makes every INSERT on table raise, in the writer's transaction, until the cell ends.
func (j *jitHarness) failInsertsOf(t *testing.T, table string) {
	t.Helper()
	ctx := context.Background()
	if _, err := j.env.admin.Exec(ctx, `CREATE OR REPLACE FUNCTION public.jit_forced_failure() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'forced mid-transaction failure'; END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := j.env.admin.Exec(ctx, `CREATE TRIGGER jit_forced_failure BEFORE INSERT ON `+table+
		` FOR EACH ROW EXECUTE FUNCTION public.jit_forced_failure()`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := j.env.admin.Exec(context.Background(), `DROP TRIGGER jit_forced_failure ON `+table); err != nil {
			t.Errorf("drop the trigger: %v", err)
		}
	})
}

// seedAuthMembership writes an auth.memberships row with no public row, as a
// status other than active leaves it.
func (j *jitHarness) seedAuthMembership(t *testing.T, principal, org uuid.UUID, status string) {
	t.Helper()
	ctx := context.Background()
	if err := j.ids.on.Store.ProjectOrganization(ctx, org); err != nil {
		t.Fatal(err)
	}
	id, subject, orgText := uuid.New(), principal.String(), org.String()
	wireStatus := status
	if status == "invited" {
		wireStatus = "pending"
	}
	err := audit.Commit(ctx, j.env.runtime, j.env.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, `INSERT INTO auth.memberships (id, organization_id, principal_id, status, role) VALUES ($1, $2, $3, $4, 'member')`,
				id, org, principal, status)
			return err
		},
		Audit: audit.AuditEvent{EventType: "auth.membership.added", Outcome: audit.OutcomeAllowed, SubjectPrincipalID: &subject, OrganizationID: &orgText,
			Attributes: map[string]any{"reason_code": "membership_added", "target_kind": "membership", "target_id": id.String()}},
		Events: []audit.OutboxEvent{{AggregateType: "membership", AggregateID: provisioning.WireID("membership", id.String()),
			EventType: "membership." + status, Change: map[string]any{"status": wireStatus}}},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func (j *jitHarness) waiters(t *testing.T) int {
	t.Helper()
	return j.count(t, `SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
WHERE NOT l.granted AND a.datname = current_database()`)
}

type jitResult struct {
	login   Login
	failure *Error
	err     error
}

// callback runs one sign-in in its own goroutine.
func (j *jitHarness) callback(t *testing.T, sub, email string) <-chan jitResult {
	t.Helper()
	s := j.start(t)
	idToken := j.sign(t, j.claims(s.nonce, sub, email))
	code := j.idp.register(s.challenge, idToken)
	out := make(chan jitResult, 1)
	go func() {
		login, _, err := j.svc.Callback(context.Background(), j.providerID, CallbackInput{Code: code, State: s.state, RequestID: &j.requestID,
			Cookie: func(name string) (string, bool) {
				if name == s.cookie.Name {
					return s.cookie.Value, true
				}
				return "", false
			}})
		var failure *Error
		if errors.As(err, &failure) {
			err = nil
		}
		out <- jitResult{login: login, failure: failure, err: err}
	}()
	return out
}

// whileHeld runs a sign-in while tx holds the account's public.users row:
// the sign-in's plan read sees none of tx's rows, then it waits at the lock.
// tx commits and the sign-in ends.
func (j *jitHarness) whileHeld(t *testing.T, sub, email string, statements func(ctx context.Context, tx pgx.Tx) error) jitResult {
	t.Helper()
	ctx := context.Background()
	tx, err := j.env.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := statements(ctx, tx); err != nil {
		t.Fatal(err)
	}
	out := j.callback(t, sub, email)
	deadline := time.Now().Add(jitWaitBound)
	for j.waiters(t) == 0 {
		select {
		case r := <-out:
			t.Fatalf("the sign-in ended before it waited on a lock: %+v", r)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("the sign-in never waited on a lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-out:
		if r.err != nil {
			t.Fatalf("callback: %v", r.err)
		}
		return r
	case <-time.After(2 * jitWaitBound):
		t.Fatal("the sign-in did not end")
		return jitResult{}
	}
}

func (j *jitHarness) wantNoMembership(t *testing.T, user uuid.UUID) {
	t.Helper()
	if n := j.count(t, `SELECT (SELECT count(*) FROM public.memberships WHERE user_id = $1 AND org_id = $2)
  + (SELECT count(*) FROM auth.memberships WHERE principal_id = $1 AND organization_id = $2 AND status = 'active')`, user, j.orgID); n != 0 {
		t.Fatalf("%d membership rows, want none", n)
	}
}

func (j *jitHarness) wantOneMembership(t *testing.T, user uuid.UUID, role string) {
	t.Helper()
	if n := j.count(t, `SELECT count(*) FROM public.memberships p JOIN auth.memberships a ON a.id = p.id
WHERE p.user_id = $1 AND p.org_id = $2 AND p.role = $3 AND a.principal_id = $1 AND a.organization_id = $2 AND a.role = $3 AND a.status = 'active'`,
		user, j.orgID, role); n != 1 {
		t.Fatalf("%d membership pairs with one id and role %q, want 1", n, role)
	}
	if n := j.count(t, `SELECT count(*) FROM public.memberships WHERE user_id = $1 AND org_id = $2`, user, j.orgID); n != 1 {
		t.Fatalf("%d public rows, want 1", n)
	}
}

func (j *jitHarness) principalOf(t *testing.T, email string) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	if err := j.env.admin.QueryRow(context.Background(), `SELECT principal_id FROM auth.users WHERE email_lower = $1`, strings.ToLower(email)).Scan(&id); err != nil {
		t.Fatalf("the account of %s: %v", email, err)
	}
	return id
}

func TestJITNewAccountOneOfEach(t *testing.T) {
	t.Run("oidc", func(t *testing.T) {
		j := newJITHarness(t, harnessOpts{})
		email, sub := uniqueEmail("jit-new"), "sub-"+uuid.NewString()
		before := j.jitTableCounts(t)
		login, failure := j.signIn(t, sub, email)
		if failure != nil {
			t.Fatalf("sign-in: %v", failure)
		}
		if login.Tokens.AccessToken != "jit-access" || login.Role != "member" || login.OrgID == nil || *login.OrgID != j.orgID {
			t.Fatalf("login %+v", login)
		}
		user := j.principalOf(t, email)
		if login.PrincipalID != user {
			t.Fatalf("login principal %v, account %v", login.PrincipalID, user)
		}
		after := j.jitTableCounts(t)
		for table, grew := range map[string]int{"auth.principals": 1, "auth.users": 1, "auth.external_identities": 1, "auth.memberships": 1,
			"auth.sessions": 1, "auth.refresh_credentials": 1, "public.users": 1, "public.memberships": 1} {
			if after[table]-before[table] != grew {
				t.Errorf("%s grew by %d, want %d", table, after[table]-before[table], grew)
			}
		}
		j.wantOneMembership(t, user, "member")
		if n := j.count(t, `SELECT count(*) FROM auth.sessions s JOIN public.users u ON u.id = s.principal_id
WHERE s.principal_id = $1 AND s.organization_id = $2 AND u.last_login_at = s.issued_at`, user, j.orgID); n != 1 {
			t.Fatalf("%d sessions in the organization with the sign-in stamp, want 1", n)
		}
		if j.sessions.count() != 0 {
			t.Fatal("the member path issued a second session")
		}
	})
}

func TestJITFailureAtEachStepWritesNothing(t *testing.T) {
	for name, table := range map[string]string{"oidc after identity": "public.memberships", "oidc after membership": "auth.sessions",
		"oidc after session": "auth.refresh_credentials"} {
		t.Run(name, func(t *testing.T) {
			j := newJITHarness(t, harnessOpts{})
			email := uniqueEmail("jit-fail")
			j.failInsertsOf(t, table)
			before := j.jitTableCounts(t)
			_, failure := j.signIn(t, "sub-"+uuid.NewString(), email)
			if failure == nil || failure.Status < 500 {
				t.Fatalf("sign-in = %v, want a 5xx", failure)
			}
			wantSameCounts(t, before, j.jitTableCounts(t))
		})
	}
}

func TestJITJoinFailureWritesNothing(t *testing.T) {
	t.Run("oidc", func(t *testing.T) {
		j := newJITHarness(t, harnessOpts{})
		email, sub := uniqueEmail("jit-join-fail"), "sub-"+uuid.NewString()
		j.account(t, sub, email)
		j.failInsertsOf(t, "auth.sessions")
		before := j.jitTableCounts(t)
		_, failure := j.signIn(t, sub, email)
		if failure == nil || failure.Status < 500 {
			t.Fatalf("sign-in = %v, want a 5xx", failure)
		}
		wantSameCounts(t, before, j.jitTableCounts(t))
	})
}

func TestJITSwitchOffRefusesBeforeAnyWrite(t *testing.T) {
	t.Run("oidc", func(t *testing.T) {
		j := newJITHarness(t, harnessOpts{})
		j.svc.cfg.MembershipJIT = nil
		email, sub := uniqueEmail("jit-off"), "sub-"+uuid.NewString()
		before := j.jitTableCounts(t)
		_, failure := j.signIn(t, sub, email)
		wantDenied(t, failure, http.StatusForbidden, ReasonMembershipRequired)
		wantSameCounts(t, before, j.jitTableCounts(t))
	})
}

func TestJITAutoProvisionOffRefusesAJoin(t *testing.T) {
	t.Run("oidc", func(t *testing.T) {
		j := newJITHarness(t, harnessOpts{noAutoProvision: true})
		email, sub := uniqueEmail("jit-auto-off"), "sub-"+uuid.NewString()
		user := j.account(t, sub, email)
		before := j.jitTableCounts(t)
		_, failure := j.signIn(t, sub, email)
		wantDenied(t, failure, http.StatusForbidden, ReasonMembershipRequired)
		wantSameCounts(t, before, j.jitTableCounts(t))
		j.wantNoMembership(t, user)
	})
}

func TestJITRefusesAMemberOfAnotherOrg(t *testing.T) {
	t.Run("oidc", func(t *testing.T) {
		for _, shape := range []string{"public", "auth suspended"} {
			t.Run(shape, func(t *testing.T) {
				j := newJITHarness(t, harnessOpts{})
				email, sub := uniqueEmail("jit-other"), "sub-"+uuid.NewString()
				user := j.account(t, sub, email)
				other := j.seedOrg(t)
				if shape == "public" {
					j.seedMembership(t, user, other, "member")
				} else {
					j.seedAuthMembership(t, user, other, "suspended")
				}
				before := j.jitTableCounts(t)
				_, failure := j.signIn(t, sub, email)
				wantDenied(t, failure, http.StatusForbidden, ReasonMembershipRequired)
				wantSameCounts(t, before, j.jitTableCounts(t))
			})
		}
	})
}

func TestJITOtherOrgCommittedAfterThePlanRead(t *testing.T) {
	t.Run("oidc", func(t *testing.T) {
		j := newJITHarness(t, harnessOpts{})
		email, sub := uniqueEmail("jit-parked"), "sub-"+uuid.NewString()
		user := j.account(t, sub, email)
		other := j.seedOrg(t)
		r := j.whileHeld(t, sub, email, func(ctx context.Context, tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, `SELECT 1 FROM public.users WHERE id = $1 FOR UPDATE`, user); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `INSERT INTO public.memberships (id, user_id, org_id, role) VALUES ($1, $2, $3, 'member')`, uuid.New(), user, other)
			return err
		})
		wantDenied(t, r.failure, http.StatusForbidden, ReasonMembershipRequired)
		j.wantNoMembership(t, user)
	})
}

// jitRoleRows are the default_role rows and the literal role each gives; "" is a refusal.
var jitRoleRows = []struct{ name, row, want string }{
	{"empty", "", "member"}, {"viewer", "viewer", "viewer"}, {"member", "member", "member"}, {"admin", "admin", "admin"},
	{"owner", "owner", ""}, {"unknown", "root", ""},
}

func TestJITRoleTable(t *testing.T) {
	t.Run("oidc", func(t *testing.T) {
		for _, row := range jitRoleRows {
			t.Run(row.name, func(t *testing.T) {
				j := newJITHarness(t, harnessOpts{})
				j.setDefaultRole(t, row.row)
				newEmail, joinEmail, joinSub := uniqueEmail("jit-role"), uniqueEmail("jit-role-join"), "sub-"+uuid.NewString()
				joiner := j.account(t, joinSub, joinEmail)
				before := j.jitTableCounts(t)
				login, newFailure := j.signIn(t, "sub-"+uuid.NewString(), newEmail)
				joined, joinFailure := j.signIn(t, joinSub, joinEmail)
				if row.want == "" {
					wantDenied(t, newFailure, http.StatusForbidden, ReasonMembershipRequired)
					wantDenied(t, joinFailure, http.StatusForbidden, ReasonMembershipRequired)
					wantSameCounts(t, before, j.jitTableCounts(t))
					return
				}
				if newFailure != nil || joinFailure != nil {
					t.Fatalf("new %v, join %v", newFailure, joinFailure)
				}
				if login.Role != row.want || joined.Role != row.want {
					t.Fatalf("login roles %q and %q, want %q", login.Role, joined.Role, row.want)
				}
				j.wantOneMembership(t, j.principalOf(t, newEmail), row.want)
				j.wantOneMembership(t, joiner, row.want)
			})
		}
	})
}

func TestJITSuspendedOrgRefused(t *testing.T) {
	t.Run("oidc", func(t *testing.T) {
		j := newJITHarness(t, harnessOpts{})
		email, sub := uniqueEmail("jit-suspended"), "sub-"+uuid.NewString()
		user := j.account(t, sub, email)
		if _, err := j.env.admin.Exec(context.Background(), `UPDATE public.organizations SET is_active = false WHERE id = $1`, j.orgID); err != nil {
			t.Fatal(err)
		}
		_, failure := j.signIn(t, sub, email)
		wantDenied(t, failure, http.StatusForbidden, ReasonMembershipRequired)
		j.wantNoMembership(t, user)
	})
}

// The account is disabled after the plan read: the check under the lock refuses it.
func TestJITDisabledAccountRefused(t *testing.T) {
	t.Run("oidc", func(t *testing.T) {
		j := newJITHarness(t, harnessOpts{})
		email, sub := uniqueEmail("jit-disabled"), "sub-"+uuid.NewString()
		user := j.account(t, sub, email)
		r := j.whileHeld(t, sub, email, func(ctx context.Context, tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `UPDATE public.users SET is_active = false WHERE id = $1`, user)
			return err
		})
		wantDenied(t, r.failure, http.StatusForbidden, ReasonPrincipalDeactivated)
		j.wantNoMembership(t, user)
	})
}

// A sign-in never undoes a status: the auth row stays as it was.
func TestJITRemovedMemberStaysOut(t *testing.T) {
	t.Run("oidc", func(t *testing.T) {
		for _, status := range []string{"removed", "suspended", "invited"} {
			t.Run(status, func(t *testing.T) {
				j := newJITHarness(t, harnessOpts{})
				email, sub := uniqueEmail("jit-out"), "sub-"+uuid.NewString()
				user := j.account(t, sub, email)
				j.seedAuthMembership(t, user, j.orgID, status)
				before := j.jitTableCounts(t)
				_, failure := j.signIn(t, sub, email)
				wantDenied(t, failure, http.StatusForbidden, ReasonMembershipRequired)
				wantSameCounts(t, before, j.jitTableCounts(t))
				if n := j.count(t, `SELECT count(*) FROM auth.memberships WHERE principal_id = $1 AND organization_id = $2 AND status = $3`,
					user, j.orgID, status); n != 1 {
					t.Fatalf("%d rows with status %s, want 1", n, status)
				}
			})
		}
	})
}

func TestJITTwoFirstSignInsOneRow(t *testing.T) {
	t.Run("oidc", func(t *testing.T) {
		j := newJITHarness(t, harnessOpts{})
		email, sub := uniqueEmail("jit-two"), "sub-"+uuid.NewString()
		first, second := j.callback(t, sub, email), j.callback(t, sub, email)
		won := 0
		for _, r := range []jitResult{<-first, <-second} {
			switch {
			case r.err != nil:
				t.Fatalf("callback: %v", r.err)
			case r.failure == nil:
				won++
			case r.failure.Status != http.StatusConflict:
				t.Fatalf("sign-in = %v, want a success or 409", r.failure)
			}
		}
		if won == 0 {
			t.Fatal("no sign-in won")
		}
		j.wantOneMembership(t, j.principalOf(t, email), "member")
	})
}

func TestJITTwoJoinsOneRow(t *testing.T) {
	t.Run("oidc", func(t *testing.T) {
		j := newJITHarness(t, harnessOpts{})
		email, sub := uniqueEmail("jit-two-join"), "sub-"+uuid.NewString()
		user := j.account(t, sub, email)
		var wg sync.WaitGroup
		results := make([]jitResult, 2)
		first, second := j.callback(t, sub, email), j.callback(t, sub, email)
		wg.Add(2)
		for i, c := range []<-chan jitResult{first, second} {
			go func() { defer wg.Done(); results[i] = <-c }()
		}
		wg.Wait()
		for i, r := range results {
			if r.err != nil || r.failure != nil {
				t.Fatalf("sign-in %d: %v %v", i, r.err, r.failure)
			}
		}
		j.wantOneMembership(t, user, "member")
	})
}

// The account became a member after the plan read: a session, no second row.
func TestJITAlreadyMemberGetsASessionOnly(t *testing.T) {
	t.Run("oidc", func(t *testing.T) {
		j := newJITHarness(t, harnessOpts{})
		email, sub := uniqueEmail("jit-already"), "sub-"+uuid.NewString()
		user := j.account(t, sub, email)
		if err := j.ids.on.Store.ProjectOrganization(context.Background(), j.orgID); err != nil {
			t.Fatal(err)
		}
		r := j.whileHeld(t, sub, email, func(ctx context.Context, tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, `SELECT 1 FROM public.users WHERE id = $1 FOR UPDATE`, user); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `INSERT INTO public.memberships (id, user_id, org_id, role) VALUES ($1, $2, $3, 'admin')`, uuid.New(), user, j.orgID)
			return err
		})
		if r.failure != nil {
			t.Fatalf("sign-in: %v", r.failure)
		}
		if r.login.Role != "admin" || j.sessions.count() != 1 {
			t.Fatalf("login role %q, member sessions %d; want admin and 1", r.login.Role, j.sessions.count())
		}
		if n := j.count(t, `SELECT count(*) FROM auth.memberships WHERE principal_id = $1`, user); n != 0 {
			t.Fatalf("%d auth rows, want none", n)
		}
	})
}

func TestJITTokenFailureNextSignInWorks(t *testing.T) {
	j := newJITHarness(t, harnessOpts{})
	email, sub := uniqueEmail("jit-mint"), "sub-"+uuid.NewString()
	j.mintErr = errors.New("mint failed")
	_, failure := j.signIn(t, sub, email)
	if failure == nil || failure.Status < 500 {
		t.Fatalf("sign-in = %v, want a 5xx", failure)
	}
	user := j.principalOf(t, email)
	j.wantOneMembership(t, user, "member")
	j.mintErr = nil
	login, failure := j.signIn(t, sub, email)
	if failure != nil {
		t.Fatalf("next sign-in: %v", failure)
	}
	if login.PrincipalID != user || j.sessions.count() != 1 {
		t.Fatalf("login %+v, member sessions %d", login, j.sessions.count())
	}
	j.wantOneMembership(t, user, "member")
}

func (j *jitHarness) setAllowedDomains(t *testing.T, list any) {
	t.Helper()
	if _, err := j.env.admin.Exec(context.Background(), `UPDATE public.sso_providers SET allowed_domains = $2::json WHERE id = $1`, j.providerID, list); err != nil {
		t.Fatal(err)
	}
}

func TestJITNewAccountNeedsAListedDomain(t *testing.T) {
	t.Run("oidc", func(t *testing.T) {
		for name, list := range map[string]any{"empty": "[]", "null": nil} {
			t.Run(name, func(t *testing.T) {
				j := newJITHarness(t, harnessOpts{})
				j.setAllowedDomains(t, list)
				before := j.jitTableCounts(t)
				_, failure := j.signIn(t, "sub-"+uuid.NewString(), uniqueEmail("jit-list"))
				wantDenied(t, failure, http.StatusBadRequest, ReasonProvisioningDisabled)
				wantSameCounts(t, before, j.jitTableCounts(t))
				if n := j.count(t, `SELECT count(*) FROM public.sso_providers WHERE id = $1 AND last_error = $2`, j.providerID, msgAutoProvisionOff); n != 1 {
					t.Fatalf("%d provider rows with the auto-provision failure, want 1", n)
				}
			})
		}
		t.Run("listed", func(t *testing.T) {
			j := newJITHarness(t, harnessOpts{})
			email := uniqueEmail("jit-list")
			if _, failure := j.signIn(t, "sub-"+uuid.NewString(), email); failure != nil {
				t.Fatalf("sign-in: %v", failure)
			}
			j.wantOneMembership(t, j.principalOf(t, email), "member")
		})
		t.Run("unlisted", func(t *testing.T) {
			j := newJITHarness(t, harnessOpts{allowedDomains: []string{"other.example"}})
			before := j.jitTableCounts(t)
			_, failure := j.signIn(t, "sub-"+uuid.NewString(), uniqueEmail("jit-list"))
			wantDenied(t, failure, http.StatusForbidden, ReasonDomainNotAllowed)
			wantSameCounts(t, before, j.jitTableCounts(t))
		})
		t.Run("switch_off", func(t *testing.T) {
			j := newJITHarness(t, harnessOpts{})
			j.svc.cfg.MembershipJIT = nil
			j.setAllowedDomains(t, "[]")
			_, failure := j.signIn(t, "sub-"+uuid.NewString(), uniqueEmail("jit-list"))
			wantDenied(t, failure, http.StatusForbidden, ReasonMembershipRequired)
		})
	})
}

// A key the row holds decides the account: the email and the empty list do not.
func TestJITKnownKeyIgnoresAnEmptyList(t *testing.T) {
	t.Run("oidc", func(t *testing.T) {
		j := newJITHarness(t, harnessOpts{})
		email, sub := uniqueEmail("jit-known"), "sub-"+uuid.NewString()
		user := j.account(t, sub, email)
		j.setAllowedDomains(t, "[]")
		if _, failure := j.signIn(t, sub, email); failure != nil {
			t.Fatalf("known key refused: %v", failure)
		}
		j.wantOneMembership(t, user, "member")
	})
}
