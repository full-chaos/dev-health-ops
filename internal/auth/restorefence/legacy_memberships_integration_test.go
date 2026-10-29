//go:build integration

package restorefence

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

// The legacy membership mirror: a row deleted after T is deleted again; a row the source keeps survives; a blind
// margin (no manifest) removes no member.
func TestTheFenceCarriesALegacyMembershipMirrorDelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := newFenceDB(t, ctx), newFenceDB(t, ctx)
	user := uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-user"))
	org := uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-legacy-org"))
	user2 := uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-user-m2"))
	gone := uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-public-membership-gone"))
	kept := uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-public-membership-kept"))
	for _, d := range []*fenceDB{restored, end} {
		d.stateT(t, ctx)
		d.must(t, ctx, `INSERT INTO public.users (id, email) VALUES ($1, 'fence-m2@example.invalid')`, user2)
		d.must(t, ctx, `INSERT INTO public.memberships (id, user_id, org_id, role) VALUES ($1, $2, $3, 'member'), ($4, $5, $3, 'member')`, gone, user, org, kept, user2)
	}
	end.must(t, ctx, `DELETE FROM public.memberships WHERE id = $1`, gone)
	manifest := exportEnd(t, ctx, end)
	if _, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{Apply: true}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if n := restored.count(t, ctx, `SELECT count(*) FROM public.memberships WHERE id = '`+gone.String()+`'`); n != 0 {
		t.Errorf("the mirror row deleted after T is back (%d)", n)
	}
	if n := restored.count(t, ctx, `SELECT count(*) FROM public.memberships WHERE id = '`+kept.String()+`'`); n != 1 {
		t.Errorf("a mirror row the source kept was removed (%d)", n)
	}
}

func TestABlindMarginRemovesNoLegacyMember(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	db := newFenceDB(t, ctx)
	db.stateT(t, ctx)
	user := uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-user"))
	org := uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-legacy-org"))
	db.must(t, ctx, `INSERT INTO public.memberships (id, user_id, org_id, role) VALUES ($1, $2, $3, 'member')`, uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-blind-m")), user, org)
	if _, err := Apply(ctx, db.admin, db.schema, nil, Options{Apply: true, BlindMargin: 1000}); err != nil {
		t.Fatal(err)
	}
	if n := db.count(t, ctx, `SELECT count(*) FROM public.memberships`); n != 1 {
		t.Errorf("a blind margin left %d memberships, want 1", n)
	}
}
