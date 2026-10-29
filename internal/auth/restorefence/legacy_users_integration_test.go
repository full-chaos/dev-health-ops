//go:build integration

package restorefence

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

// A legacy user force-deleted at the source after T: the restored row stays (foreign keys), is disabled and its
// token_version is bumped, so every token minted before T (version v) is refused; the id is reported.
func TestTheFenceTombstonesALegacyUserTheSourceDeleted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := newFenceDB(t, ctx), newFenceDB(t, ctx)
	gone := uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-user-deleted"))
	kept := uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-user"))
	for _, d := range []*fenceDB{restored, end} {
		d.stateT(t, ctx)
		d.must(t, ctx, `INSERT INTO public.users (id, email) VALUES ($1, 'fence-deleted@example.invalid')`, gone)
	}
	end.afterT(t, ctx)
	end.must(t, ctx, `DELETE FROM public.users WHERE id = $1`, gone)
	manifest := exportEnd(t, ctx, end)
	report, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{Apply: true})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if n := restored.count(t, ctx, `SELECT count(*) FROM public.users WHERE id = '`+gone.String()+`' AND NOT is_active AND token_version = 1`); n != 1 {
		t.Errorf("the force-deleted user is not tombstoned (kept, is_active false, token_version 1): %d", n)
	}
	if n := restored.count(t, ctx, `SELECT count(*) FROM public.users WHERE id = '`+kept.String()+`' AND NOT is_active AND token_version = 3`); n != 1 {
		t.Errorf("the user the source only disabled keeps its counter-section result: %d", n)
	}
	var ids int
	for _, a := range report.Actions {
		if a.Kind == ActionUserTombstoned {
			ids = len(a.IDs)
		}
	}
	if ids != 1 {
		t.Errorf("the tombstone action names %d ids, want 1 (actions: %+v)", ids, report.Actions)
	}
}

func TestABlindMarginTombstonesNoLegacyUser(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	db := newFenceDB(t, ctx)
	db.stateT(t, ctx)
	if _, err := Apply(ctx, db.admin, db.schema, nil, Options{Apply: true, BlindMargin: 1000}); err != nil {
		t.Fatal(err)
	}
	if n := db.count(t, ctx, `SELECT count(*) FROM public.users WHERE is_active`); n != 1 {
		t.Errorf("a blind margin disabled users (%d active, want 1)", n)
	}
}
