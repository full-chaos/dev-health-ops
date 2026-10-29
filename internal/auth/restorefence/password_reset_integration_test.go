//go:build integration

package restorefence

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// pwHash is a stand-in verifier built at run time: it must never reach a manifest, a report or the stored actions.
func pwHash(name string) string {
	return strings.Join([]string{"fence", "pw", "verifier", name}, "-")
}

func pwUser(name string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-pw-"+name))
}

// pwSeed puts one user into both tables: auth.users (guarded: inside audit.Commit) with a hash when password is true,
// and the legacy public.users mirror with the same hash.
func (d *fenceDB) pwSeed(t *testing.T, ctx context.Context, name string, password bool) {
	t.Helper()
	id := pwUser(name)
	d.must(t, ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', $2)`, id, name)
	var hash, algorithm, at any
	if password {
		hash, algorithm, at = pwHash(name+"-old"), "argon2id", time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	}
	d.commit(t, ctx, "user.created", 0, func(ctx context.Context, tx audit.TxOps) error {
		_, err := tx.Exec(ctx, `INSERT INTO auth.users (principal_id, email, email_lower, password_hash, password_algorithm, password_updated_at)
VALUES ($1, $2, $2, $3, $4, $5)`, id, name+"@pw.example.invalid", hash, algorithm, at)
		return err
	})
	d.must(t, ctx, `INSERT INTO public.users (id, email, password_hash) VALUES ($1, $2, $3)`, id, name+"@pw.example.invalid", hash)
}

// pwChangeViaAuthService is a password write of auth-service after T: a new hash and password_updated_at in auth.users,
// and the mirror write of publicusers.go (new legacy hash, token_version + 1).
func (d *fenceDB) pwChangeViaAuthService(t *testing.T, ctx context.Context, name string) {
	t.Helper()
	d.commit(t, ctx, "user.password_changed", 0, func(ctx context.Context, tx audit.TxOps) error {
		_, err := tx.Exec(ctx, `UPDATE auth.users SET password_hash = $2, password_algorithm = 'argon2id', password_updated_at = '2026-10-20T00:00:00Z' WHERE principal_id = $1`,
			pwUser(name), pwHash(name+"-new"))
		return err
	})
	d.must(t, ctx, `UPDATE public.users SET password_hash = $2, token_version = token_version + 1 WHERE id = $1`, pwUser(name), pwHash(name+"-new"))
}

// pwChangeViaGoAPI is a legacy password write after T (changepassword.go / verify.go: new hash, token_version + 1)
// that reaches auth.users only through the legacy_token_version mirror.
func (d *fenceDB) pwChangeViaGoAPI(t *testing.T, ctx context.Context, name string) {
	t.Helper()
	d.must(t, ctx, `UPDATE public.users SET password_hash = $2, token_version = token_version + 1 WHERE id = $1`, pwUser(name), pwHash(name+"-new"))
	d.commit(t, ctx, "user.legacy_token_version", 0, func(ctx context.Context, tx audit.TxOps) error {
		_, err := tx.Exec(ctx, `UPDATE auth.users SET legacy_token_version = legacy_token_version + 1 WHERE principal_id = $1`, pwUser(name))
		return err
	})
}

type pwState struct {
	authHash, authAlgorithm, legacyHash *string
	tokenVersion                        int
}

func (d *fenceDB) pwState(t *testing.T, ctx context.Context, name string) pwState {
	t.Helper()
	var s pwState
	if err := d.admin.QueryRow(ctx, `SELECT a.password_hash, a.password_algorithm, p.password_hash, p.token_version
FROM auth.users a JOIN public.users p ON p.id = a.principal_id WHERE a.principal_id = $1`, pwUser(name)).Scan(&s.authHash, &s.authAlgorithm, &s.legacyHash, &s.tokenVersion); err != nil {
		t.Fatalf("user %s: %v", name, err)
	}
	return s
}

func assertNoPasswordHash(t *testing.T, ctx context.Context, d *fenceDB, m *Manifest, report Report) {
	t.Helper()
	needle := strings.Join([]string{"fence", "pw", "verifier"}, "-")
	if m != nil {
		raw, err := m.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), needle) || strings.Contains(string(raw), "password_hash") || strings.Contains(string(raw), "password_algorithm") {
			t.Error("the manifest carries a password hash or its column")
		}
	}
	raw, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), needle) {
		t.Error("the apply report carries a password hash")
	}
	if n := d.count(t, ctx, `SELECT count(*) FROM auth.restore_fence_state WHERE operator_actions::text LIKE '%`+needle+`%'`); n != 0 {
		t.Error("the stored operator actions carry a password hash")
	}
}

// 11834 + P1 + P2 + P5: a password changed after T through auth-service (password_updated_at) or through go-api
// (token_version / legacy_token_version) has its restored hash cleared in BOTH tables, the CHECK pair holds, and the user
// is named; the legacy clear needs the section before the counter (the order mutant fails here); an unchanged user and a
// user without a password are never changed or named.
func TestTheFenceClearsAPasswordChangedAfterTAndNamesTheUser(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := newFenceDB(t, ctx), newFenceDB(t, ctx)
	for _, d := range []*fenceDB{restored, end} {
		d.stateT(t, ctx)
		d.pwSeed(t, ctx, "via-auth", true)
		d.pwSeed(t, ctx, "via-goapi", true)
		d.pwSeed(t, ctx, "same", true)
		d.pwSeed(t, ctx, "sso", false)
	}
	end.pwChangeViaAuthService(t, ctx, "via-auth")
	end.pwChangeViaGoAPI(t, ctx, "via-goapi")
	// P5: an SSO user's token is bumped after T (no password on either side).
	end.must(t, ctx, `UPDATE public.users SET token_version = token_version + 1 WHERE id = $1`, pwUser("sso"))
	end.commit(t, ctx, "user.legacy_token_version", 0, func(ctx context.Context, tx audit.TxOps) error {
		_, err := tx.Exec(ctx, `UPDATE auth.users SET legacy_token_version = legacy_token_version + 1 WHERE principal_id = $1`, pwUser("sso"))
		return err
	})
	manifest := exportEnd(t, ctx, end)
	report, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{Apply: true})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	for _, name := range []string{"via-auth", "via-goapi"} {
		s := restored.pwState(t, ctx, name)
		if s.authHash != nil || s.authAlgorithm != nil || s.legacyHash != nil {
			t.Errorf("%s: a hash survived the fence (auth %v/%v, legacy %v)", name, s.authHash != nil, s.authAlgorithm != nil, s.legacyHash != nil)
		}
		if s.tokenVersion != 1 {
			t.Errorf("%s: token_version %d, want the source's 1 (the counter still runs)", name, s.tokenVersion)
		}
	}
	if s := restored.pwState(t, ctx, "same"); s.authHash == nil || *s.authHash != pwHash("same-old") || s.legacyHash == nil || *s.legacyHash != pwHash("same-old") {
		t.Error("an unchanged password was cleared")
	}
	if s := restored.pwState(t, ctx, "sso"); s.authHash != nil || s.legacyHash != nil || s.authAlgorithm != nil {
		t.Error("a user without a password gained one")
	}
	if n := restored.count(t, ctx, `SELECT count(*) FROM auth.users WHERE (password_hash IS NULL) <> (password_algorithm IS NULL)`); n != 0 {
		t.Errorf("%d auth.users rows break the hash/algorithm pair", n)
	}
	want := sortedIDs(pwUser("via-auth"), pwUser("via-goapi"))
	sameSet(t, "force_password_reset (auth.users.password)", reviewIDs(report, "force_password_reset", authPasswordSection), want)
	sameSet(t, "force_password_reset (public.users.password)", reviewIDs(report, "force_password_reset", legacyPasswordSection), want)
	for _, s := range report.Sections {
		if (s.Name == authPasswordSection || s.Name == legacyPasswordSection) && s.Changed != 2 {
			t.Errorf("%s cleared %d users, want 2", s.Name, s.Changed)
		}
	}
	for _, src := range []string{authPasswordSection, legacyPasswordSection} {
		for _, id := range reviewIDs(report, "force_password_reset", src) {
			if id == pwUser("sso").String() || id == pwUser("same").String() {
				t.Errorf("%s names %s, which needs no reset", src, id)
			}
		}
	}
	assertNoPasswordHash(t, ctx, restored, manifest, report)
}

// A user whose password the source holds unchanged is not written.
func TestAnUnchangedPasswordIsKeptWithNoWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := newFenceDB(t, ctx), newFenceDB(t, ctx)
	for _, d := range []*fenceDB{restored, end} {
		d.stateT(t, ctx)
		d.pwSeed(t, ctx, "steady", true)
	}
	report, err := Apply(ctx, restored.admin, restored.schema, exportEnd(t, ctx, end), Options{Apply: true})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	for _, s := range report.Sections {
		if (s.Name == authPasswordSection || s.Name == legacyPasswordSection) && s.Changed != 0 {
			t.Errorf("%s wrote %d rows for an unchanged password, want 0", s.Name, s.Changed)
		}
	}
	if s := restored.pwState(t, ctx, "steady"); s.authHash == nil || s.legacyHash == nil {
		t.Error("an unchanged password was cleared")
	}
}

// A blind margin has no source state: no password is cleared, and every user that holds one is listed (ids only).
func TestABlindMarginKeepsPasswordsAndListsTheirUsers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	db := newFenceDB(t, ctx)
	db.stateT(t, ctx)
	db.pwSeed(t, ctx, "blind-a", true)
	db.pwSeed(t, ctx, "blind-sso", false)
	report, err := Apply(ctx, db.admin, db.schema, nil, Options{Apply: true, BlindMargin: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if s := db.pwState(t, ctx, "blind-a"); s.authHash == nil || s.legacyHash == nil {
		t.Error("a blind margin cleared a password")
	}
	for src, query := range map[string]string{
		authPasswordSection:   `SELECT principal_id::text FROM auth.users WHERE password_hash IS NOT NULL ORDER BY 1`,
		legacyPasswordSection: `SELECT id::text FROM public.users WHERE password_hash IS NOT NULL ORDER BY 1`,
	} {
		rows, err := db.admin.Query(ctx, query)
		if err != nil {
			t.Fatal(err)
		}
		var want []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			want = append(want, id)
		}
		rows.Close()
		if len(want) == 0 {
			t.Fatalf("fixture: no user with a password for %s", src)
		}
		sameSet(t, "blind-margin review of "+src, reviewIDs(report, "review_after_blind_margin", src), want)
	}
	assertNoPasswordHash(t, ctx, db, nil, report)
}
