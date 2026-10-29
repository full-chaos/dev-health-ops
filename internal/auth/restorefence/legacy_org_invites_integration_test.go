//go:build integration

package restorefence

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestTheFenceRevokesALegacyInviteTheSourceDeletedAndABlindMarginRevokesAll(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := newFenceDB(t, ctx), newFenceDB(t, ctx)
	org := uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-legacy-org"))
	deleted := uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-legacy-invite-deleted"))
	pending := uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-legacy-invite-pending"))
	insert := `INSERT INTO public.org_invites (id, org_id, email, token_hash, expires_at) VALUES ($1, $2, $3, $4, now() + interval '1 day')`
	for _, d := range []*fenceDB{restored, end} {
		d.stateT(t, ctx)
		d.must(t, ctx, insert, deleted, org, "del@example.invalid", "d-hash")
		d.must(t, ctx, insert, pending, org, "pen@example.invalid", "p-hash")
	}
	end.must(t, ctx, `DELETE FROM public.org_invites WHERE id = $1`, deleted)
	manifest := exportEnd(t, ctx, end)
	if _, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{Apply: true}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	status := func(id uuid.UUID) string {
		var s string
		if err := restored.admin.QueryRow(ctx, `SELECT status FROM public.org_invites WHERE id = $1`, id).Scan(&s); err != nil {
			return "absent"
		}
		return s
	}
	if got := status(deleted); got != "revoked" {
		t.Errorf("the legacy invite the source deleted ends %s, want revoked", got)
	}
	if got := status(pending); got != "pending" {
		t.Errorf("a legacy invite the source still holds pending ends %s, want pending", got)
	}
	blind := newFenceDB(t, ctx)
	blind.stateT(t, ctx)
	blind.must(t, ctx, insert, pending, org, "pen@example.invalid", "p-hash")
	if _, err := Apply(ctx, blind.admin, blind.schema, nil, Options{Apply: true, BlindMargin: 1000}); err != nil {
		t.Fatal(err)
	}
	if n := blind.count(t, ctx, `SELECT count(*) FROM public.org_invites WHERE status = 'pending'`); n != 0 {
		t.Errorf("%d legacy invites stay pending after a blind margin", n)
	}
}
