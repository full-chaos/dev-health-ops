//go:build integration

package restorefence

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// identitySentinel is the IdP subject and lock email of the fixture, built at run time; it must never reach a manifest,
// a report or the stored operator actions.
var identitySentinel = strings.Join([]string{"fence", "identity", "sentinel", "61b2"}, "-")

func identityPrincipal(name string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-identity-principal-"+name))
}

func identityID(name string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-identity-"+name))
}

func (d *fenceDB) identityPrincipalRow(t *testing.T, ctx context.Context, name string) {
	t.Helper()
	d.must(t, ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', $2)`, identityPrincipal(name), name)
}

func (d *fenceDB) link(t *testing.T, ctx context.Context, name string, id uuid.UUID) {
	t.Helper()
	d.commit(t, ctx, "auth.identity.linked", 0, func(ctx context.Context, tx audit.TxOps) error {
		_, err := tx.Exec(ctx, `INSERT INTO auth.external_identities (id, principal_id, issuer, provider, subject) VALUES ($1, $2, 'https://idp.example.invalid', 'oidc', $3)`,
			id, identityPrincipal(name), identitySentinel+"-"+name)
		return err
	})
}

func (d *fenceDB) unlink(t *testing.T, ctx context.Context, id uuid.UUID) {
	t.Helper()
	d.commit(t, ctx, "auth.identity.unlinked", 0, func(ctx context.Context, tx audit.TxOps) error {
		_, err := tx.Exec(ctx, `DELETE FROM auth.external_identities WHERE id = $1`, id)
		return err
	})
}

func (d *fenceDB) linkPresent(t *testing.T, ctx context.Context, id uuid.UUID) bool {
	t.Helper()
	return d.count(t, ctx, `SELECT count(*) FROM auth.external_identities WHERE id = '`+id.String()+`'`) == 1
}

func assertNoIdentitySentinel(t *testing.T, ctx context.Context, d *fenceDB, m *Manifest, report Report) {
	t.Helper()
	if m != nil {
		raw, err := m.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), identitySentinel) || strings.Contains(string(raw), "subject") {
			t.Error("the manifest carries an identity subject, a lock email or the subject column")
		}
	}
	raw, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), identitySentinel) {
		t.Error("the apply report carries an identity subject or a lock email")
	}
	if n := d.count(t, ctx, `SELECT count(*) FROM auth.restore_fence_state WHERE operator_actions::text LIKE '%`+identitySentinel+`%'`); n != 0 {
		t.Error("the stored operator actions carry an identity subject or a lock email")
	}
}

// Q1: a link the source unlinked after T is deleted and named; a link unlinked and linked again after T (new id) is
// deleted too (id match only, fail closed); a link the source holds is kept; a link made after T is not inserted.
func TestTheFenceRemovesAnIdentityUnlinkedAfterT(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := newFenceDB(t, ctx), newFenceDB(t, ctx)
	for _, d := range []*fenceDB{restored, end} {
		d.stateT(t, ctx)
		for _, name := range []string{"kept", "unlinked", "relinked", "late"} {
			d.identityPrincipalRow(t, ctx, name)
		}
		for _, name := range []string{"kept", "unlinked", "relinked"} {
			d.link(t, ctx, name, identityID(name))
		}
	}
	end.unlink(t, ctx, identityID("unlinked"))
	end.unlink(t, ctx, identityID("relinked"))
	end.link(t, ctx, "relinked", identityID("relinked-2"))
	end.link(t, ctx, "late", identityID("late"))
	manifest := exportEnd(t, ctx, end)
	report, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{Apply: true})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	for name, want := range map[string]bool{"kept": true, "unlinked": false, "relinked": false, "relinked-2": false, "late": false} {
		if got := restored.linkPresent(t, ctx, identityID(name)); got != want {
			t.Errorf("link %s present = %v, want %v", name, got, want)
		}
	}
	want := []string{identityID("unlinked").String(), identityID("relinked").String()}
	sort.Strings(want)
	sameSet(t, "identity_unlinked", reviewIDs(report, "identity_unlinked", "auth.external_identities"), want)
	for _, s := range report.Sections {
		if s.Name == "auth.external_identities" && s.Changed != 2 {
			t.Errorf("the identity section deleted %d rows, want 2", s.Changed)
		}
	}
	assertNoIdentitySentinel(t, ctx, restored, manifest, report)
}

// Q2: a blind margin deletes no link and lists every restored link as id/principal/provider (never the subject).
func TestABlindMarginKeepsEveryIdentityAndListsIt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	db := newFenceDB(t, ctx)
	db.stateT(t, ctx)
	db.identityPrincipalRow(t, ctx, "blind")
	db.link(t, ctx, "blind", identityID("blind"))
	report, err := Apply(ctx, db.admin, db.schema, nil, Options{Apply: true, BlindMargin: 1000})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if !db.linkPresent(t, ctx, identityID("blind")) {
		t.Error("a blind margin deleted a link")
	}
	sameSet(t, "review_after_blind_margin (identities)", reviewIDs(report, "review_after_blind_margin", "auth.external_identities"),
		[]string{identityID("blind").String() + "/" + identityPrincipal("blind").String() + "/oidc"})
	assertNoIdentitySentinel(t, ctx, db, nil, report)
}

func lockHash(name string) []byte {
	sum := sha256.Sum256([]byte(identitySentinel + "-" + name))
	return sum[:]
}

func (d *fenceDB) authLock(t *testing.T, ctx context.Context, name string, count int, until string) {
	t.Helper()
	d.must(t, ctx, `INSERT INTO auth.login_attempts (identifier_hash, attempt_count, first_attempt_at, locked_until) VALUES ($1, $2, now() - interval '5 minutes', now() + $3::interval)`,
		lockHash(name), count, until)
}

func (d *fenceDB) legacyLock(t *testing.T, ctx context.Context, name string, count int, until string) {
	t.Helper()
	d.must(t, ctx, `INSERT INTO public.login_attempts (id, email, attempt_count, first_attempt_at, locked_until, created_at, updated_at) VALUES (gen_random_uuid(), $1, $2, now() - interval '5 minutes', now() + $3::interval, now(), now())`,
		identitySentinel+"-"+name+"@lock.example.invalid", count, until)
}

type lockState struct {
	present bool
	count   int
	minutes float64
}

func (d *fenceDB) authLockState(t *testing.T, ctx context.Context, name string) lockState {
	t.Helper()
	var s lockState
	var until *float64
	err := d.admin.QueryRow(ctx, `SELECT attempt_count, EXTRACT(EPOCH FROM (locked_until - now())) / 60 FROM auth.login_attempts WHERE identifier_hash = $1`, lockHash(name)).Scan(&s.count, &until)
	if err == nil {
		s.present = true
		if until != nil {
			s.minutes = *until
		}
	}
	return s
}

func (d *fenceDB) legacyLockState(t *testing.T, ctx context.Context, name string) lockState {
	t.Helper()
	var s lockState
	var until *float64
	err := d.admin.QueryRow(ctx, `SELECT attempt_count, EXTRACT(EPOCH FROM (locked_until - now())) / 60 FROM public.login_attempts WHERE email = $1`, identitySentinel+"-"+name+"@lock.example.invalid").Scan(&s.count, &until)
	if err == nil {
		s.present = true
		if until != nil {
			s.minutes = *until
		}
	}
	return s
}

// Q3: with the source set, a live lock the source holds (both tables) is present and locked after the apply; a
// restored lock is never shortened or lowered; an expired source lock is not copied. Without the source nothing moves.
func TestTheFenceCarriesLiveLockoutsFromTheSourceOnlyRaising(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := newFenceDB(t, ctx), newFenceDB(t, ctx)
	for _, d := range []*fenceDB{restored, end} {
		d.stateT(t, ctx)
	}
	end.authLock(t, ctx, "new", 5, "12 minutes")
	end.legacyLock(t, ctx, "new", 5, "12 minutes")
	end.authLock(t, ctx, "raise", 7, "12 minutes")
	restored.authLock(t, ctx, "raise", 5, "2 minutes")
	end.authLock(t, ctx, "keep", 5, "3 minutes")
	restored.authLock(t, ctx, "keep", 9, "20 minutes")
	end.legacyLock(t, ctx, "keep", 5, "3 minutes")
	restored.legacyLock(t, ctx, "keep", 9, "20 minutes")
	end.authLock(t, ctx, "expired", 5, "-1 minutes")
	end.legacyLock(t, ctx, "expired", 5, "-1 minutes")
	manifest := exportEnd(t, ctx, end)

	report, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{Apply: true, Source: PGRevocationSource{Pool: end.admin}})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if s := restored.authLockState(t, ctx, "new"); !s.present || s.count != 5 || s.minutes < 10 {
		t.Errorf("a live auth lock of the source after apply = %+v, want present, count 5, ~12 min left", s)
	}
	if s := restored.legacyLockState(t, ctx, "new"); !s.present || s.count != 5 || s.minutes < 10 {
		t.Errorf("a live legacy lock of the source after apply = %+v, want present, count 5, ~12 min left", s)
	}
	if s := restored.authLockState(t, ctx, "raise"); s.count != 7 || s.minutes < 10 {
		t.Errorf("a shorter restored lock = %+v, want raised to count 7, ~12 min", s)
	}
	if s := restored.authLockState(t, ctx, "keep"); s.count != 9 || s.minutes < 18 {
		t.Errorf("a longer restored auth lock = %+v, want kept at count 9, ~20 min", s)
	}
	if s := restored.legacyLockState(t, ctx, "keep"); s.count != 9 || s.minutes < 18 {
		t.Errorf("a longer restored legacy lock = %+v, want kept at count 9, ~20 min", s)
	}
	if restored.authLockState(t, ctx, "expired").present || restored.legacyLockState(t, ctx, "expired").present {
		t.Error("an expired source lock was copied")
	}
	counts := map[string]int64{}
	for _, s := range report.Sections {
		counts[s.Name] = s.Changed
	}
	if counts["auth.login_attempts.source"] != 2 || counts["public.login_attempts.source"] != 1 {
		t.Errorf("lockout counts = %v, want auth 2 (new, raise), legacy 1 (new)", counts)
	}
	assertNoIdentitySentinel(t, ctx, restored, manifest, report)
}

func TestWithoutTheSourceNoLockoutMoves(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := newFenceDB(t, ctx), newFenceDB(t, ctx)
	for _, d := range []*fenceDB{restored, end} {
		d.stateT(t, ctx)
	}
	end.authLock(t, ctx, "new", 5, "12 minutes")
	end.legacyLock(t, ctx, "new", 5, "12 minutes")
	if _, err := Apply(ctx, restored.admin, restored.schema, exportEnd(t, ctx, end), Options{Apply: true}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if restored.authLockState(t, ctx, "new").present || restored.legacyLockState(t, ctx, "new").present {
		t.Error("a lock moved without the source")
	}
}

// v5: a manifest exported by a version 4 binary is refused by the version 5 apply, before any write.
func TestAVersionFourManifestIsRefusedAndNothingIsWritten(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := newFenceDB(t, ctx), newFenceDB(t, ctx)
	for _, d := range []*fenceDB{restored, end} {
		d.stateT(t, ctx)
		d.identityPrincipalRow(t, ctx, "v4")
		d.link(t, ctx, "v4", identityID("v4"))
	}
	end.unlink(t, ctx, identityID("v4"))
	manifest := exportEnd(t, ctx, end)
	manifest.Version = 4
	if err := manifest.Seal(); err != nil {
		t.Fatal(err)
	}
	if _, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{Apply: true}); !errors.Is(err, ErrManifest) {
		t.Fatalf("a version 4 manifest = %v, want ErrManifest", err)
	}
	if !restored.linkPresent(t, ctx, identityID("v4")) {
		t.Error("a refused version 4 apply deleted a link")
	}
	if n := restored.count(t, ctx, `SELECT count(*) FROM auth.restore_fence_state WHERE source = 'manifest'`); n != 0 {
		t.Errorf("a refused version 4 apply wrote %d fence row(s)", n)
	}
}
