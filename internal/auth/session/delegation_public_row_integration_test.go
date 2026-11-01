//go:build integration

package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

const (
	publicStart = "impersonation_start"
	publicStop  = "impersonation_stop"
)

func (d *delegationEnv) publicRows(t *testing.T, ctx context.Context, actor uuid.UUID) int {
	return d.count(t, ctx, `SELECT count(*) FROM public.impersonation_sessions WHERE admin_user_id = $1`, actor)
}

func (d *delegationEnv) openPublicRows(t *testing.T, ctx context.Context, actor uuid.UUID) int {
	return d.count(t, ctx, `SELECT count(*) FROM public.impersonation_sessions WHERE admin_user_id = $1 AND ended_at IS NULL`, actor)
}

func (d *delegationEnv) publicEnded(t *testing.T, ctx context.Context, id uuid.UUID) bool {
	t.Helper()
	return d.count(t, ctx, `SELECT count(*) FROM public.impersonation_sessions WHERE id = $1 AND ended_at IS NOT NULL`, id) == 1
}

// publicAudit counts go-api's audit_logs rows of a delegation: the actor in the
// target's organization, resource session/<target>, as admin/impersonation.go.
func (d *delegationEnv) publicAudit(t *testing.T, ctx context.Context, action string, c delegationCase) int {
	return d.count(t, ctx, `SELECT count(*) FROM public.audit_logs WHERE action = $1 AND user_id = $2 AND org_id = $3
		AND resource_type = 'session' AND resource_id = $4 AND status = 'success'`, action, c.actor, c.org, c.target.String())
}

func (d *delegationEnv) wantPublicEnd(t *testing.T, ctx context.Context, row Record, c delegationCase) {
	t.Helper()
	if !d.publicEnded(t, ctx, row.ID) {
		t.Fatalf("public row %s is not ended", row.ID)
	}
	if got := d.publicAudit(t, ctx, publicStop, c); got != 1 {
		t.Fatalf("impersonation_stop rows = %d, want 1", got)
	}
}

func TestDelegationWritesThePublicRow(t *testing.T) {
	ctx := testContext(t)
	d := newDelegationEnv(t, ctx)
	c := d.fixture(t, ctx)
	row := d.start(t, ctx, c)
	if got := d.publicRows(t, ctx, c.actor); got != 1 {
		t.Fatalf("public rows = %d, want 1", got)
	}
	if got := d.count(t, ctx, `SELECT count(*) FROM public.impersonation_sessions p JOIN auth.sessions s ON s.id = p.id
		WHERE p.id = $1 AND p.admin_user_id = $2 AND p.target_user_id = $3 AND p.target_org_id = $4 AND p.target_role = 'member'
		AND p.created_at = s.created_at AND p.expires_at = s.expires_at AND p.ended_at IS NULL`,
		row.ID, c.actor, c.target, c.org); got != 1 {
		t.Fatalf("the public row does not equal its session row: %d", got)
	}
	if got := d.publicAudit(t, ctx, publicStart, c); got != 1 {
		t.Fatalf("impersonation_start rows = %d, want 1", got)
	}
	other := d.fixture(t, ctx)
	if _, err := d.admin.Exec(ctx, `UPDATE public.memberships SET role = 'admin' WHERE user_id = $1`, other.target); err != nil {
		t.Fatal(err)
	}
	second := d.start(t, ctx, other)
	if got := d.count(t, ctx, `SELECT count(*) FROM public.impersonation_sessions WHERE id = $1 AND target_role = 'admin'`, second.ID); got != 1 {
		t.Fatalf("the public row does not carry the locked role: %d", got)
	}
}

// Either row of a Start refused by the database leaves neither row.
func TestDelegationAuditFailureLeavesNoPublicRow(t *testing.T) {
	ctx := testContext(t)
	d := newDelegationEnv(t, ctx)
	t.Run("the auth event", func(t *testing.T) {
		c := d.fixture(t, ctx)
		d.refuseAudit(t, ctx, eventStarted)
		if _, err := d.owned.StartDelegation(ctx, c.input()); err == nil {
			t.Fatal("StartDelegation committed")
		}
		if got := d.publicRows(t, ctx, c.actor); got != 0 {
			t.Fatalf("public rows = %d, want 0", got)
		}
		if got := d.publicAudit(t, ctx, publicStart, c); got != 0 {
			t.Fatalf("impersonation_start rows = %d, want 0", got)
		}
	})
	t.Run("the audit_logs row", func(t *testing.T) {
		c := d.fixture(t, ctx)
		if _, err := d.admin.Exec(ctx, `ALTER TABLE public.audit_logs
			ADD CONSTRAINT refuse_start CHECK (action <> 'impersonation_start') NOT VALID`); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if _, err := d.admin.Exec(context.Background(), `ALTER TABLE public.audit_logs DROP CONSTRAINT refuse_start`); err != nil {
				t.Errorf("drop constraint: %v", err)
			}
		})
		if _, err := d.owned.StartDelegation(ctx, c.input()); err == nil {
			t.Fatal("StartDelegation committed")
		}
		if got := d.publicRows(t, ctx, c.actor); got != 0 {
			t.Fatalf("public rows = %d, want 0", got)
		}
		if got := d.rowsOf(t, ctx, c.actor); got != 0 {
			t.Fatalf("session rows = %d, want 0", got)
		}
	})
}

// The key compromise path is TestCompromiseEndsTheDelegatedChildOfARevokedSession
// in internal/auth/keyrotation.
func TestDelegationPublicRowEndsOnEveryEndPath(t *testing.T) {
	ctx := testContext(t)
	d := newDelegationEnv(t, ctx)
	for name, end := range map[string]func(t *testing.T, c delegationCase, row Record){
		"self_revoke": func(t *testing.T, _ delegationCase, row Record) {
			if ok, err := d.owned.Revoke(ctx, row.ID, Actor{}); err != nil || !ok {
				t.Fatalf("Revoke = %v, %v", ok, err)
			}
		},
		"end_delegation": func(t *testing.T, _ delegationCase, row Record) {
			if ok, err := d.owned.EndDelegation(ctx, row.ID, Actor{}); err != nil || !ok {
				t.Fatalf("EndDelegation = %v, %v", ok, err)
			}
		},
		"base_revoke": func(t *testing.T, c delegationCase, _ Record) {
			if ok, err := d.owned.Revoke(ctx, c.base.Session.ID, Actor{}); err != nil || !ok {
				t.Fatalf("Revoke = %v, %v", ok, err)
			}
		},
		"target_revoke_all": func(t *testing.T, c delegationCase, _ Record) {
			if _, err := d.owned.RevokeAll(ctx, c.target, Actor{}); err != nil {
				t.Fatalf("RevokeAll: %v", err)
			}
		},
		"actor_role_lost": func(t *testing.T, c delegationCase, _ Record) {
			other := d.user(t, ctx, true)
			if err := d.platform.Revoke(ctx, platformrole.RevokeInput{Actor: other, Principal: c.actor, Role: platformrole.PlatformAdmin}); err != nil {
				t.Fatalf("platform Revoke: %v", err)
			}
		},
		"actor_disabled": func(t *testing.T, c delegationCase, _ Record) {
			if _, err := d.admin.Exec(ctx, `UPDATE public.users SET is_active = false WHERE id = $1`, c.actor); err != nil {
				t.Fatal(err)
			}
			if err := d.identity.Project(ctx, c.actor); err != nil {
				t.Fatalf("Project: %v", err)
			}
		},
		"superseded_by_start": func(t *testing.T, c delegationCase, row Record) {
			next := d.start(t, ctx, c)
			if d.publicEnded(t, ctx, next.ID) {
				t.Fatal("the next delegation's public row is ended")
			}
		},
		"organization_end": func(t *testing.T, c delegationCase, _ Record) {
			if err := d.inCommit(ctx, func(ctx context.Context, tx audit.TxOps) error {
				_, err := d.owned.EndOrganizationInTx(ctx, tx, c.org)
				return err
			}); err != nil {
				t.Fatalf("EndOrganizationInTx: %v", err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := d.fixture(t, ctx)
			row := d.start(t, ctx, c)
			end(t, c, row)
			d.wantPublicEnd(t, ctx, row, c)
		})
	}
}

// A stop row is the fact that this end ended the public row: a row go-api
// already ended, or no row, writes none, and the end still commits.
func TestDelegationEndWritesOneStopRow(t *testing.T) {
	ctx := testContext(t)
	d := newDelegationEnv(t, ctx)
	end := func(t *testing.T, row Record) {
		t.Helper()
		if ok, err := d.owned.EndDelegation(ctx, row.ID, Actor{}); err != nil || !ok {
			t.Fatalf("EndDelegation = %v, %v", ok, err)
		}
	}
	t.Run("twice", func(t *testing.T) {
		c := d.fixture(t, ctx)
		row := d.start(t, ctx, c)
		end(t, row)
		if ok, err := d.owned.EndDelegation(ctx, row.ID, Actor{}); err != nil || ok {
			t.Fatalf("second EndDelegation = %v, %v", ok, err)
		}
		d.wantPublicEnd(t, ctx, row, c)
	})
	t.Run("go-api ended the row", func(t *testing.T) {
		c := d.fixture(t, ctx)
		row := d.start(t, ctx, c)
		if _, err := d.admin.Exec(ctx, `UPDATE public.impersonation_sessions SET ended_at = now() WHERE id = $1`, row.ID); err != nil {
			t.Fatal(err)
		}
		end(t, row)
		if got := d.publicAudit(t, ctx, publicStop, c); got != 0 {
			t.Fatalf("impersonation_stop rows = %d, want 0", got)
		}
	})
	t.Run("no public row", func(t *testing.T) {
		c := d.fixture(t, ctx)
		row := d.start(t, ctx, c)
		if _, err := d.admin.Exec(ctx, `DELETE FROM public.impersonation_sessions WHERE id = $1`, row.ID); err != nil {
			t.Fatal(err)
		}
		end(t, row)
		if got := d.publicAudit(t, ctx, publicStop, c); got != 0 {
			t.Fatalf("impersonation_stop rows = %d, want 0", got)
		}
		d.wantEnded(t, ctx, row, c)
	})
}

// go-api's own row of the actor ends at a Start, expired or not, as go-api's
// start ends it, and its end is in audit_logs. Another actor's row stays.
func TestStartEndsALegacyOpenRow(t *testing.T) {
	ctx := testContext(t)
	d := newDelegationEnv(t, ctx)
	c := d.fixture(t, ctx)
	other := d.user(t, ctx, true)
	seed := func(admin uuid.UUID, expires time.Time) uuid.UUID {
		id := uuid.New()
		pgseed.ImpersonationSession(ctx, t, d.admin, id.String(), admin.String(), c.target.String(), c.org.String(), "member", expires, nil)
		return id
	}
	open := seed(c.actor, time.Now().Add(time.Hour))
	expired := seed(c.actor, time.Now().Add(-time.Hour))
	bystander := seed(other, time.Now().Add(time.Hour))
	row := d.start(t, ctx, c)
	for _, id := range []uuid.UUID{open, expired} {
		if !d.publicEnded(t, ctx, id) {
			t.Fatalf("legacy row %s is not ended", id)
		}
	}
	if d.publicEnded(t, ctx, bystander) {
		t.Fatal("another actor's row ended")
	}
	if d.publicEnded(t, ctx, row.ID) {
		t.Fatal("the new row is ended")
	}
	if got := d.publicAudit(t, ctx, publicStop, c); got != 2 {
		t.Fatalf("impersonation_stop rows = %d, want 2", got)
	}
}

// A legacy row whose target user was deleted (0150 made the reference
// nullable) still ends, and its stop row names no target.
func TestStartEndsALegacyRowWithNoTarget(t *testing.T) {
	ctx := testContext(t)
	d := newDelegationEnv(t, ctx)
	c := d.fixture(t, ctx)
	id := uuid.New()
	if _, err := d.admin.Exec(ctx, `INSERT INTO public.impersonation_sessions
		(id, admin_user_id, target_user_id, target_org_id, target_role, created_at, expires_at)
		VALUES ($1, $2, NULL, $3, 'member', now(), now() + interval '1 hour')`, id, c.actor, c.org); err != nil {
		t.Fatal(err)
	}
	d.start(t, ctx, c)
	if !d.publicEnded(t, ctx, id) {
		t.Fatal("the legacy row is not ended")
	}
	if got := d.count(t, ctx, `SELECT count(*) FROM public.audit_logs WHERE action = $1 AND user_id = $2 AND resource_id = ''`,
		publicStop, c.actor); got != 1 {
		t.Fatalf("impersonation_stop rows with no target = %d, want 1", got)
	}
}

// Rolled back to the switch off, the runtime role cannot start a public row,
// and a live delegated row still ends. A compromise runs as the migration role:
// TestCompromiseEndsTheDelegatedChildOfARevokedSession has its switch off.
func TestSwitchOffStillEndsThePublicRow(t *testing.T) {
	ctx := testContext(t)
	d := newDelegationEnv(t, ctx)
	c := d.fixture(t, ctx)
	row := d.start(t, ctx, c)
	bystander := d.fixture(t, ctx)
	byRow := d.start(t, ctx, bystander)
	other := d.user(t, ctx, true)
	var role string
	if err := d.runtime.QueryRow(ctx, `SELECT current_user`).Scan(&role); err != nil {
		t.Fatal(err)
	}
	off := authschema.Options{Schema: "auth", RuntimeRole: role}
	_, err := authschema.Apply(ctx, d.admin, off)
	var narrowed *authschema.NarrowingError
	if !errors.As(err, &narrowed) {
		t.Fatalf("Apply with the switch off: %v, want the narrowing refusal", err)
	}
	off.AcknowledgeNarrowing = narrowed.Digest
	if _, err := authschema.Apply(ctx, d.admin, off); err != nil {
		t.Fatalf("Apply with the switch off: %v", err)
	}
	_, err = d.runtime.Exec(ctx, `INSERT INTO public.impersonation_sessions
		(id, admin_user_id, target_user_id, target_org_id, target_role, created_at, expires_at)
		VALUES ($1, $2, $3, $4, 'member', now(), now() + interval '1 minute')`, uuid.New(), c.actor, c.target, c.org)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
		t.Fatalf("runtime INSERT with the switch off: %v, want 42501", err)
	}
	if err := d.platform.Revoke(ctx, platformrole.RevokeInput{Actor: other, Principal: c.actor, Role: platformrole.PlatformAdmin}); err != nil {
		t.Fatalf("platform Revoke: %v", err)
	}
	d.wantPublicEnd(t, ctx, row, c)
	if ok, err := d.owned.Revoke(ctx, byRow.ID, Actor{}); err != nil || !ok {
		t.Fatalf("Revoke = %v, %v", ok, err)
	}
	d.wantPublicEnd(t, ctx, byRow, bystander)
}
