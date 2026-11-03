//go:build integration

package restorefence

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

func inviteID(name string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-invite-"+name))
}

// insertInvite writes one auth.invites row (a guarded table: inside audit.Commit). status is pending or accepted.
func (d *fenceDB) insertInvite(t *testing.T, ctx context.Context, name, status string) {
	t.Helper()
	principal := uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-principal"))
	d.commit(t, ctx, "invite.created", 0, func(ctx context.Context, tx audit.TxOps) error {
		if status == "accepted" {
			_, err := tx.Exec(ctx, `INSERT INTO auth.invites (id, organization_id, email_normalized, role, token_hash, status, expires_at, accepted_at, accepted_principal_id)
VALUES ($1, $2, $3, 'member', md5($3) || md5($3 || 'b'), 'accepted', now() + interval '7 days', now(), $4)`, inviteID(name), fenceOrgID(0), name+"@example.invalid", principal)
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO auth.invites (id, organization_id, email_normalized, role, token_hash, status, expires_at)
VALUES ($1, $2, $3, 'member', md5($3) || md5($3 || 'b'), 'pending', now() + interval '7 days')`, inviteID(name), fenceOrgID(0), name+"@example.invalid")
		return err
	})
}

// D6459 am.1: an invite accepted or revoked after T, or gone at the source, is revoked in the restored database: never
// accepted (no membership was restored) and never pending. An invite still pending at the source stays pending, an
// invite accepted before T stays accepted, and an invite created after T is absent (expected).
func TestTheFenceRevokesAnInviteTheSourceNoLongerHoldsPending(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := newFenceDB(t, ctx), newFenceDB(t, ctx)
	principal := uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-principal"))
	for _, d := range []*fenceDB{restored, end} {
		d.stateT(t, ctx)
		for _, n := range []string{"accepted-later", "revoked-later", "still-pending", "deleted-later"} {
			d.insertInvite(t, ctx, n, "pending")
		}
		d.insertInvite(t, ctx, "accepted-before", "accepted")
	}
	end.commit(t, ctx, "invite.accepted", 0, func(ctx context.Context, tx audit.TxOps) error {
		_, err := tx.Exec(ctx, `UPDATE auth.invites SET status = 'accepted', accepted_at = now(), accepted_principal_id = $2 WHERE id = $1`, inviteID("accepted-later"), principal)
		return err
	})
	end.commit(t, ctx, "invite.revoked", 0, func(ctx context.Context, tx audit.TxOps) error {
		_, err := tx.Exec(ctx, `UPDATE auth.invites SET status = 'revoked' WHERE id = $1`, inviteID("revoked-later"))
		return err
	})
	end.commit(t, ctx, "invite.deleted", 0, func(ctx context.Context, tx audit.TxOps) error {
		_, err := tx.Exec(ctx, `DELETE FROM auth.invites WHERE id = $1`, inviteID("deleted-later"))
		return err
	})
	end.insertInvite(t, ctx, "created-later", "pending")
	manifest := exportEnd(t, ctx, end)
	report, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{Apply: true})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	status := func(name string) string {
		var s *string
		if err := restored.admin.QueryRow(ctx, `SELECT status FROM auth.invites WHERE id = $1`, inviteID(name)).Scan(&s); err != nil {
			return "absent"
		}
		return *s
	}
	for name, want := range map[string]string{
		"accepted-later": "revoked", "revoked-later": "revoked", "deleted-later": "revoked",
		"still-pending": "pending", "accepted-before": "accepted", "created-later": "absent",
	} {
		if got := status(name); got != want {
			t.Errorf("invite %s ends %s, want %s", name, got, want)
		}
	}
	if n := restored.count(t, ctx, `SELECT count(*) FROM auth.invites WHERE status = 'accepted' AND id = '`+inviteID("accepted-later").String()+`'`); n != 0 {
		t.Errorf("an invite accepted after T must not end accepted in the restored database (no membership exists): %d", n)
	}
	changed := int64(0)
	for _, s := range report.Sections {
		if s.Name == "auth.invites.ids" {
			changed = s.Changed
		}
	}
	if changed != 3 {
		t.Errorf("the invite section revoked %d invites, want 3", changed)
	}
}

// A blind margin has no source state: every pending invite is revoked (fail closed; admins re-invite).
func TestABlindMarginRevokesEveryPendingInvite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	db := newFenceDB(t, ctx)
	db.stateT(t, ctx)
	db.insertInvite(t, ctx, "blind-a", "pending")
	db.insertInvite(t, ctx, "blind-b", "pending")
	db.insertInvite(t, ctx, "blind-c", "accepted")
	report, err := Apply(ctx, db.admin, db.schema, nil, Options{Apply: true, BlindMargin: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if n := db.count(t, ctx, `SELECT count(*) FROM auth.invites WHERE status = 'pending'`); n != 0 {
		t.Errorf("%d pending invites remain after a blind margin", n)
	}
	if n := db.count(t, ctx, `SELECT count(*) FROM auth.invites WHERE status = 'accepted'`); n != 1 {
		t.Errorf("a blind margin changed an accepted invite (%d accepted remain, want 1)", n)
	}
	for _, s := range report.Sections {
		if s.Name == "auth.invites.ids" && s.Changed != 2 {
			t.Errorf("the report counts %d invites revoked, want 2", s.Changed)
		}
	}
}
