//go:build integration

package local

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

type memberRig struct {
	*endRig
	ids     *identity.Store
	members *membership.Store
}

func newMemberRig(t *testing.T, ctx context.Context) *memberRig {
	t.Helper()
	rig := newEndRig(t, ctx)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	ids, err := identity.New(identity.Config{Pool: rig.env.runtime, Schema: schema, UsersOwnedByAuth: true})
	if err != nil {
		t.Fatal(err)
	}
	members, err := membership.New(membership.Config{Pool: rig.env.runtime, Schema: schema, OwnsUsers: true, Sessions: rig.sessions})
	if err != nil {
		t.Fatal(err)
	}
	return &memberRig{endRig: rig, ids: ids, members: members}
}

func (r *memberRig) org(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := r.env.admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'password-membership')`, id, "org-"+id.String()[:8]); err != nil {
		t.Fatalf("seed public.organizations: %v", err)
	}
	if err := r.ids.ProjectOrganization(ctx, id); err != nil {
		t.Fatalf("ProjectOrganization: %v", err)
	}
	return id
}

func (r *memberRig) join(t *testing.T, ctx context.Context, org, user uuid.UUID) {
	t.Helper()
	allow := func(context.Context, audit.TxOps, membership.Locked) error { return nil }
	if _, err := r.members.Add(ctx, membership.Change{OrgID: org, PrincipalID: user, Role: "member", Authorize: allow}); err != nil {
		t.Fatalf("membership Add: %v", err)
	}
}

func (r *memberRig) dropMembership(t *testing.T, ctx context.Context, org, user uuid.UUID) {
	t.Helper()
	if _, err := r.env.admin.Exec(ctx, `DELETE FROM public.memberships WHERE org_id = $1 AND user_id = $2`, org, user); err != nil {
		t.Fatalf("delete the membership: %v", err)
	}
}

func (r *memberRig) change(ctx context.Context, user uuid.UUID, org *uuid.UUID) (PasswordEndResult, error) {
	hash, err := cheapHash("Brand-new-pass-42")
	if err != nil {
		return PasswordEndResult{}, err
	}
	return r.accounts.SetPasswordEndingSessions(ctx, PasswordEnd{ID: user, Hash: hash, NewSession: &NewSession{OrganizationID: org, AMR: []string{"pwd"}}})
}

// rowOrg is the organization of one auth.sessions row; "" is NULL.
func (r *memberRig) rowOrg(t *testing.T, ctx context.Context, id uuid.UUID) string {
	t.Helper()
	var org *string
	if err := r.env.admin.QueryRow(ctx, `SELECT organization_id::text FROM auth.sessions WHERE id = $1`, id).Scan(&org); err != nil {
		t.Fatal(err)
	}
	if org == nil {
		return ""
	}
	return *org
}

func (r *memberRig) revoked(t *testing.T, ctx context.Context, id uuid.UUID) bool {
	t.Helper()
	return r.env.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE id = $1 AND revoked_at IS NOT NULL`, id) == 1
}

// hold locks one row with a separate transaction and returns its backend pid.
func (r *memberRig) hold(t *testing.T, ctx context.Context, table, key string, id uuid.UUID) (pgx.Tx, int32) {
	t.Helper()
	holder, err := r.env.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = holder.Rollback(context.Background()) })
	var pid int32
	if err := holder.QueryRow(ctx, `SELECT pg_backend_pid() FROM `+table+` WHERE `+key+` = $1 FOR UPDATE`, id).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	return holder, pid
}

// blockedBy returns the pids of the backends that wait for the given backend, once there is one.
func (r *memberRig) blockedBy(t *testing.T, ctx context.Context, pid int32, done <-chan error) []int32 {
	t.Helper()
	bound := time.After(10 * time.Second)
	for {
		rows, err := r.env.admin.Query(ctx, `SELECT pid FROM pg_stat_activity WHERE $1::int = ANY(pg_blocking_pids(pid))`, pid)
		if err != nil {
			t.Fatal(err)
		}
		var pids []int32
		for rows.Next() {
			var p int32
			if err := rows.Scan(&p); err != nil {
				t.Fatal(err)
			}
			pids = append(pids, p)
		}
		rows.Close()
		if len(pids) > 0 {
			return pids
		}
		select {
		case err := <-done:
			t.Fatalf("the writer ended while the row was held: %v", err)
		case <-bound:
			t.Fatal("no writer waited for the row")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (r *memberRig) setup(t *testing.T, ctx context.Context) (org, user uuid.UUID, old uuid.UUID) {
	t.Helper()
	org = r.org(t, ctx)
	user = r.account(t, ctx, "Old-pass-12345")
	r.join(t, ctx, org, user)
	issued, err := r.sessions.Create(ctx, user, &org, []string{"pwd"})
	if err != nil {
		t.Fatal(err)
	}
	return org, user, issued.Session.ID
}

// C1: the member row is gone while the old session is live: the new row has no organization.
func TestC1PasswordChangeOfALostMemberGetsASessionWithNoOrganization(t *testing.T) {
	ctx := context.Background()
	rig := newMemberRig(t, ctx)
	org, user, old := rig.setup(t, ctx)
	rig.dropMembership(t, ctx, org, user)
	res, err := rig.change(ctx, user, &org)
	if err != nil || res.Issued == nil {
		t.Fatalf("change = %+v, %v", res, err)
	}
	if got := res.Issued.Session.OrganizationID; got != nil {
		t.Errorf("issued session organization = %v, want none", got)
	}
	if got := rig.rowOrg(t, ctx, res.Issued.Session.ID); got != "" {
		t.Errorf("new session row organization = %s, want NULL", got)
	}
	if !rig.revoked(t, ctx, old) {
		t.Error("the old session is live")
	}
}

// C2: a member keeps the organization on the new row.
func TestC2PasswordChangeOfAMemberKeepsTheOrganization(t *testing.T) {
	ctx := context.Background()
	rig := newMemberRig(t, ctx)
	org, user, old := rig.setup(t, ctx)
	res, err := rig.change(ctx, user, &org)
	if err != nil || res.Issued == nil {
		t.Fatalf("change = %+v, %v", res, err)
	}
	if got := rig.rowOrg(t, ctx, res.Issued.Session.ID); got != org.String() {
		t.Errorf("new session row organization = %q, want %s", got, org)
	}
	if !rig.revoked(t, ctx, old) {
		t.Error("the old session is live")
	}
}

// C3: a session with no organization stays so, for a user who is a member somewhere.
func TestC3PasswordChangeOfASessionWithNoOrganizationStaysNull(t *testing.T) {
	ctx := context.Background()
	rig := newMemberRig(t, ctx)
	_, user, _ := rig.setup(t, ctx)
	res, err := rig.change(ctx, user, nil)
	if err != nil || res.Issued == nil {
		t.Fatalf("change = %+v, %v", res, err)
	}
	if got := rig.rowOrg(t, ctx, res.Issued.Session.ID); got != "" {
		t.Errorf("new session row organization = %s, want NULL", got)
	}
}

// C4: a removal commits while the change waits for the user row: the new row has no organization.
func TestC4RemovalThatCommitsWhileTheChangeWaitsLeavesTheNewSessionWithNoOrganization(t *testing.T) {
	ctx := context.Background()
	rig := newMemberRig(t, ctx)
	org, user, _ := rig.setup(t, ctx)
	holder, pid := rig.hold(t, ctx, "auth.users", "principal_id", user)
	type outcome struct {
		res PasswordEndResult
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		res, err := rig.change(ctx, user, &org)
		done <- outcome{res, err}
	}()
	rig.blockedBy(t, ctx, pid, nil)
	rig.dropMembership(t, ctx, org, user)
	if err := holder.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	out := <-done
	if out.err != nil || out.res.Issued == nil {
		t.Fatalf("change = %+v, %v", out.res, out.err)
	}
	if got := rig.rowOrg(t, ctx, out.res.Issued.Session.ID); got != "" {
		t.Errorf("new session row organization = %s, want NULL", got)
	}
}

// C5: a removal waits for the change's commit and then ends the new session.
func TestC5RemovalBehindTheChangeEndsTheNewSession(t *testing.T) {
	ctx := context.Background()
	rig := newMemberRig(t, ctx)
	org, user, old := rig.setup(t, ctx)
	holder, held := rig.hold(t, ctx, "auth.sessions", "id", old)
	type outcome struct {
		res PasswordEndResult
		err error
	}
	changed := make(chan outcome, 1)
	go func() {
		res, err := rig.change(ctx, user, &org)
		changed <- outcome{res, err}
	}()
	changePids := rig.blockedBy(t, ctx, held, nil)
	removed := make(chan error, 1)
	go func() {
		allow := func(context.Context, audit.TxOps, membership.Locked) error { return nil }
		_, err := rig.members.Remove(ctx, membership.Change{OrgID: org, PrincipalID: user, Authorize: allow})
		removed <- err
	}()
	rig.blockedBy(t, ctx, changePids[0], removed)
	select {
	case err := <-removed:
		t.Fatalf("the removal ended before the change committed: %v", err)
	case out := <-changed:
		t.Fatalf("the change ended while the session row was held: %+v", out)
	default:
	}
	if err := holder.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	out := <-changed
	if out.err != nil || out.res.Issued == nil {
		t.Fatalf("change = %+v, %v", out.res, out.err)
	}
	if err := <-removed; err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if got := rig.rowOrg(t, ctx, out.res.Issued.Session.ID); got != org.String() {
		t.Errorf("new session row organization = %q, want %s", got, org)
	}
	if !rig.revoked(t, ctx, out.res.Issued.Session.ID) {
		t.Error("the removal left the new session live")
	}
}
