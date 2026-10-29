//go:build integration

package restorefence

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// D6459 P2-b: a link credential the source replaced after T (Issue deletes the old unconsumed row and inserts a new
// one) must not stay usable in the restored database. The new link, issued after T, is absent after the restore:
// that is expected (the user asks for a link again). A link the source kept, and a link consumed after T, behave as
// before.
func TestTheFenceRemovesALinkCredentialTheSourceReplaced(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := newFenceDB(t, ctx), newFenceDB(t, ctx)
	member := uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-principal"))
	old := uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-link-old"))
	reissued := uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-link-new"))
	kept := uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-link-kept"))
	used := uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-link-used"))
	insert := `INSERT INTO auth.link_credentials (id, principal_id, purpose, token_hash, expires_at) VALUES ($1, $2, $3, decode(md5($4::text) || md5($4::text || 'b'), 'hex'), now() + interval '1 day')`
	for _, d := range []*fenceDB{restored, end} {
		d.stateT(t, ctx)
		d.must(t, ctx, insert, old, member, "password_reset", "lk-old")
		d.must(t, ctx, insert, kept, member, "email_verification", "lk-kept")
		d.must(t, ctx, insert, used, member, "email_verification", "lk-used")
	}
	end.must(t, ctx, `DELETE FROM auth.link_credentials WHERE id = $1`, old)
	end.must(t, ctx, insert, reissued, member, "password_reset", "lk-new")
	// A link consumed after T: consumed_at is guarded (0010 link_credentials_security_guard), so it runs in a commit.
	end.commit(t, ctx, "link_credential.consumed", 0, func(ctx context.Context, tx audit.TxOps) error {
		_, err := tx.Exec(ctx, `UPDATE auth.link_credentials SET consumed_at = now() WHERE id = $1`, used)
		return err
	})
	manifest := exportEnd(t, ctx, end)
	if _, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{Apply: true}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	count := func(id uuid.UUID, where string) int64 {
		return restored.count(t, ctx, `SELECT count(*) FROM auth.link_credentials WHERE id = '`+id.String()+`'`+where)
	}
	if n := count(old, ""); n != 0 {
		t.Errorf("the replaced link credential is still usable after the restore (%d rows)", n)
	}
	if n := count(reissued, ""); n != 0 {
		t.Errorf("a link issued after T cannot exist in the restored database (%d rows); it is expected to be absent", n)
	}
	if n := count(kept, " AND consumed_at IS NULL"); n != 1 {
		t.Errorf("a link the source kept was removed or consumed (%d rows)", n)
	}
	if n := count(used, " AND consumed_at IS NOT NULL"); n != 1 {
		t.Errorf("a link consumed after T must stay as a consumed row, not be deleted (%d rows)", n)
	}
}

// A blind margin has no consumed-after-T proof: every unconsumed link credential and every legacy reset or
// verification token goes (fail closed), and the report counts them.
func TestABlindMarginRemovesUnconsumedLinksAndLegacyTokensAndCountsThem(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	db := newFenceDB(t, ctx)
	db.stateT(t, ctx)
	member := uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-principal"))
	db.must(t, ctx, `INSERT INTO auth.link_credentials (id, principal_id, purpose, token_hash, expires_at) VALUES ($1, $2, 'password_reset', decode(md5('lk-b') || md5('lk-b2'), 'hex'), now() + interval '1 day')`,
		uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-link-blind")), member)
	legacy := db.count(t, ctx, `SELECT count(*) FROM public.password_reset_tokens`) + db.count(t, ctx, `SELECT count(*) FROM public.email_verification_tokens`)
	if legacy == 0 {
		t.Fatal("the fixture holds no legacy token: the test would measure nothing")
	}
	report, err := Apply(ctx, db.admin, db.schema, nil, Options{Apply: true, BlindMargin: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if n := db.count(t, ctx, `SELECT count(*) FROM auth.link_credentials WHERE consumed_at IS NULL`); n != 0 {
		t.Errorf("%d unconsumed link credentials remain after a blind margin", n)
	}
	if n := db.count(t, ctx, `SELECT count(*) FROM public.password_reset_tokens`) + db.count(t, ctx, `SELECT count(*) FROM public.email_verification_tokens`); n != 0 {
		t.Errorf("%d legacy tokens remain after a blind margin", n)
	}
	byName := map[string]int64{}
	for _, s := range report.Sections {
		byName[s.Name] = s.Changed
	}
	if got := byName["public.password_reset_tokens"] + byName["public.email_verification_tokens"]; got != legacy {
		t.Errorf("the report counts %d legacy tokens removed, want %d", got, legacy)
	}
	if byName["auth.link_credentials.ids"] != 1 {
		t.Errorf("the report counts %d link credentials removed, want 1", byName["auth.link_credentials.ids"])
	}
}
