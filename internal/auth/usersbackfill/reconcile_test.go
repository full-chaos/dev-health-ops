package usersbackfill

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

func byID(a, b uuid.UUID) bool { return bytes.Compare(a[:], b[:]) < 0 }

type fakeSource struct {
	rows []Row
	err  error
}

func (s fakeSource) Page(_ context.Context, after *uuid.UUID, limit int) ([]Row, error) {
	if s.err != nil {
		return nil, s.err
	}
	rows := append([]Row(nil), s.rows...)
	sort.Slice(rows, func(i, j int) bool { return byID(rows[i].ID, rows[j].ID) })
	var out []Row
	for _, r := range rows {
		if after == nil || byID(*after, r.ID) {
			out = append(out, r)
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

type fakeTarget struct {
	rows    []AuthRow
	holders map[identity.ExternalKey]uuid.UUID
	err     error
	idErr   error
	pages   int
}

func (t *fakeTarget) Page(_ context.Context, after *uuid.UUID, limit int) ([]AuthRow, error) {
	t.pages++
	if t.err != nil {
		return nil, t.err
	}
	rows := append([]AuthRow(nil), t.rows...)
	sort.Slice(rows, func(i, j int) bool { return byID(rows[i].ID, rows[j].ID) })
	var out []AuthRow
	for _, r := range rows {
		if after == nil || byID(*after, r.ID) {
			out = append(out, r)
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

func (t *fakeTarget) IdentityHolder(_ context.Context, key identity.ExternalKey) (uuid.UUID, bool, error) {
	if t.idErr != nil {
		return uuid.Nil, false, t.idErr
	}
	h, ok := t.holders[key]
	return h, ok, nil
}

// pair returns a public row and the auth row that agrees with it in every field.
func pair(t *testing.T) (Row, AuthRow) {
	t.Helper()
	id := uuid.New()
	hash := bcryptHash(t)
	name := "Display Name"
	login := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	row := Row{ID: id, Email: "Pat.Example@Example.test", FullName: &name, PasswordHash: hash, Active: true, Verified: true,
		TokenVersion: 3, LastLoginAt: &login}
	auth := AuthRow{ID: id, Kind: "user", HasUser: true, Email: row.Email, EmailLower: identity.StoredLoginKey(row.Email),
		Active: true, Verified: true, PasswordHash: hash, TokenVersion: 3, LastLoginAt: &login, DisplayName: name}
	return row, auth
}

func reconcile(t *testing.T, rows []Row, auth []AuthRow, tgt *fakeTarget) Reconciliation {
	t.Helper()
	if tgt == nil {
		tgt = &fakeTarget{}
	}
	tgt.rows = auth
	rep, err := Reconcile(context.Background(), fakeSource{rows: rows}, tgt, 2)
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

func TestReconcileZeroDiffAcrossPages(t *testing.T) {
	var rows []Row
	var auth []AuthRow
	for i := 0; i < 7; i++ {
		r, a := pair(t)
		rows, auth = append(rows, r), append(auth, a)
	}
	rep := reconcile(t, rows, auth, nil)
	if rep.Seen != 7 || rep.Matched != 7 || rep.GatedTotal() != 0 {
		t.Fatalf("zero-diff data reported %+v", rep)
	}
}

func TestReconcileEmptyBothSidesIsZeroDiff(t *testing.T) {
	rep := reconcile(t, nil, nil, nil)
	if rep.Seen != 0 || rep.GatedTotal() != 0 {
		t.Fatalf("%+v", rep)
	}
}

// Each gated class is planted alone and must be the only thing reported.
func TestReconcileEachGatedClassIsReported(t *testing.T) {
	social := identity.ExternalKey{Issuer: "https://github.com", Provider: identity.ProviderGitHub, Subject: "subject-1"}
	other := uuid.New()
	cases := []struct {
		class string
		plant func(row *Row, auth *AuthRow, tgt *fakeTarget)
	}{
		{DiffPrincipalKind, func(_ *Row, a *AuthRow, _ *fakeTarget) { a.Kind = "service_account" }},
		{DiffPrincipalKind, func(_ *Row, a *AuthRow, _ *fakeTarget) { a.HasUser = false }},
		{DiffEmail, func(_ *Row, a *AuthRow, _ *fakeTarget) { a.Email = "other@example.test" }},
		{DiffEmail, func(_ *Row, a *AuthRow, _ *fakeTarget) { a.EmailLower = "other@example.test" }},
		{DiffActive, func(_ *Row, a *AuthRow, _ *fakeTarget) { a.Active = false }},
		{DiffVerified, func(_ *Row, a *AuthRow, _ *fakeTarget) { a.Verified = false }},
		{DiffPasswordHash, func(_ *Row, a *AuthRow, _ *fakeTarget) { a.PasswordHash = nil }},
		{DiffPasswordHash, func(r *Row, _ *AuthRow, _ *fakeTarget) { r.PasswordHash = nil }},
		{DiffPlatformRole, func(r *Row, _ *AuthRow, _ *fakeTarget) { r.Superuser = true }},
		{DiffPlatformRole, func(_ *Row, a *AuthRow, _ *fakeTarget) { a.RoleHeld, a.RoleBackfilled = true, true }},
		{DiffTokenVersion, func(_ *Row, a *AuthRow, _ *fakeTarget) { a.TokenVersion = 4 }},
		{DiffIdentityMissing, func(r *Row, _ *AuthRow, tgt *fakeTarget) {
			r.AuthProvider, r.AuthProviderID = str("github"), str("subject-1")
			tgt.holders = map[identity.ExternalKey]uuid.UUID{}
		}},
		{DiffIdentityConflict, func(r *Row, _ *AuthRow, tgt *fakeTarget) {
			r.AuthProvider, r.AuthProviderID = str("github"), str("subject-1")
			tgt.holders = map[identity.ExternalKey]uuid.UUID{social: other}
		}},
		{SkipUnknownHashFormat, func(r *Row, _ *AuthRow, _ *fakeTarget) { r.PasswordHash = str("not-a-bcrypt-verifier") }},
		{SkipInvalidIdentity, func(r *Row, _ *AuthRow, _ *fakeTarget) { r.AuthProvider, r.AuthProviderID = str("github"), str("") }},
	}
	for _, tc := range cases {
		row, auth := pair(t)
		tgt := &fakeTarget{}
		tc.plant(&row, &auth, tgt)
		rep := reconcile(t, []Row{row}, []AuthRow{auth}, tgt)
		if rep.Diffs[tc.class] != 1 || rep.GatedTotal() != 1 || rep.Matched != 0 {
			t.Errorf("%s: reported %+v", tc.class, rep.Diffs)
		}
	}
}

func TestReconcileSocialIdentityHeldByTheUserIsNoDiff(t *testing.T) {
	row, auth := pair(t)
	row.AuthProvider, row.AuthProviderID = str("github"), str("subject-1")
	key := identity.ExternalKey{Issuer: "https://github.com", Provider: identity.ProviderGitHub, Subject: "subject-1"}
	rep := reconcile(t, []Row{row}, []AuthRow{auth}, &fakeTarget{holders: map[identity.ExternalKey]uuid.UUID{key: row.ID}})
	if rep.GatedTotal() != 0 || rep.Matched != 1 {
		t.Fatalf("%+v", rep)
	}
}

func TestReconcileMissingAndExtraAreReportedBothWays(t *testing.T) {
	keep, keepAuth := pair(t)
	onlyPublic, _ := pair(t)
	_, onlyAuth := pair(t)
	serviceOnly := AuthRow{ID: uuid.New(), Kind: "service_account"}
	noUser := AuthRow{ID: uuid.New(), Kind: "user", HasUser: false}
	rep := reconcile(t, []Row{keep, onlyPublic}, []AuthRow{keepAuth, onlyAuth, serviceOnly, noUser}, nil)
	if rep.Diffs[DiffMissingInAuth] != 1 || rep.Diffs[DiffExtraInAuth] != 2 || rep.GatedTotal() != 3 || rep.Seen != 2 || rep.Matched != 1 {
		t.Fatalf("reported %+v seen %d matched %d (a user principal without a users row is an extra; a service account with no users row is not)", rep.Diffs, rep.Seen, rep.Matched)
	}
}

// An auth.users row hangs off any principal; one with no public.users row fails
// the gate whatever the principal's kind.
func TestReconcileExtraUsersRowOnANonUserPrincipalFailsTheGate(t *testing.T) {
	for _, kind := range []string{"service_account", "workload", "user"} {
		extra := AuthRow{ID: uuid.New(), Kind: kind, HasUser: true}
		rep := reconcile(t, nil, []AuthRow{extra}, nil)
		if rep.Diffs[DiffExtraInAuth] != 1 || rep.GatedTotal() != 1 {
			t.Errorf("auth.users row on a %s principal with no public row: %+v", kind, rep.Diffs)
		}
	}
}

// A public row that backfill skips, with no auth row, is the skip class and is
// not also reported as missing.
func TestReconcileSkippedRowWithNoAuthRowIsTheSkipClassOnly(t *testing.T) {
	for _, tc := range []struct {
		class string
		plant func(*Row)
	}{
		{SkipUnknownHashFormat, func(r *Row) { r.PasswordHash = str("not-a-bcrypt-verifier") }},
		{SkipInvalidIdentity, func(r *Row) { r.AuthProvider, r.AuthProviderID = str("github"), str("") }},
	} {
		row, _ := pair(t)
		tc.plant(&row)
		rep := reconcile(t, []Row{row}, nil, nil)
		if rep.Diffs[tc.class] != 1 || rep.Diffs[DiffMissingInAuth] != 0 || rep.GatedTotal() != 1 {
			t.Errorf("%s with no auth row: %+v", tc.class, rep.Diffs)
		}
	}
}

// A skipped row still has its independent fields compared.
func TestReconcileSkippedRowStillShowsIndependentDiffs(t *testing.T) {
	row, auth := pair(t)
	row.PasswordHash = str("not-a-bcrypt-verifier")
	auth.Active = false
	auth.TokenVersion = 9
	rep := reconcile(t, []Row{row}, []AuthRow{auth}, nil)
	if rep.Diffs[SkipUnknownHashFormat] != 1 || rep.Diffs[DiffActive] != 1 || rep.Diffs[DiffTokenVersion] != 1 || rep.GatedTotal() != 3 || rep.Matched != 0 {
		t.Fatalf("%+v", rep.Diffs)
	}
	// An unmappable verifier is not also a password diff: the skip is the reason.
	if rep.Diffs[DiffPasswordHash] != 0 {
		t.Fatalf("a skipped hash was also counted as password_hash_diff: %+v", rep.Diffs)
	}
}

func TestReconcileAuthOnlyAfterTheLastPublicRowIsExtra(t *testing.T) {
	_, onlyAuth := pair(t)
	rep := reconcile(t, nil, []AuthRow{onlyAuth}, nil)
	if rep.Diffs[DiffExtraInAuth] != 1 {
		t.Fatalf("%+v", rep.Diffs)
	}
}

func TestReconcileInformationalClassesNeverFailTheGate(t *testing.T) {
	row, auth := pair(t)
	later := time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)
	auth.LastLoginAt = &later
	auth.DisplayName = "Renamed"
	row.AuthProvider, row.AuthProviderID = str("saml"), str("name-id")
	rep := reconcile(t, []Row{row}, []AuthRow{auth}, nil)
	if rep.GatedTotal() != 0 || rep.Matched != 1 || rep.LastLoginDrift != 1 || rep.DisplayNameDrift != 1 || rep.IdentityNotCarried != 1 {
		t.Fatalf("%+v", rep)
	}
}

func TestReconcileUserWithTwoDiffsCountsInBothClassesButIsNotMatched(t *testing.T) {
	row, auth := pair(t)
	auth.Active, auth.Verified = false, false
	rep := reconcile(t, []Row{row}, []AuthRow{auth}, nil)
	if rep.Diffs[DiffActive] != 1 || rep.Diffs[DiffVerified] != 1 || rep.Matched != 0 {
		t.Fatalf("%+v", rep)
	}
}

func TestReconcileSourceFailureStopsWithAnErrorThatNamesNoValue(t *testing.T) {
	secret := "pg-error-text-with-an-address@example.test"
	_, err := Reconcile(context.Background(), fakeSource{err: errors.New(secret)}, &fakeTarget{}, 2)
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("source failure error = %v", err)
	}
	_, err = Reconcile(context.Background(), fakeSource{}, &fakeTarget{err: errors.New(secret)}, 2)
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("target failure error = %v", err)
	}
	row, auth := pair(t)
	row.AuthProvider, row.AuthProviderID = str("github"), str("s")
	_, err = Reconcile(context.Background(), fakeSource{rows: []Row{row}}, &fakeTarget{rows: []AuthRow{auth}, idErr: errors.New(secret)}, 2)
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("identity failure error = %v", err)
	}
}

func TestReconcileStopsOnACancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	row, auth := pair(t)
	if _, err := Reconcile(ctx, fakeSource{rows: []Row{row}}, &fakeTarget{rows: []AuthRow{auth}}, 2); err == nil {
		t.Fatal("a cancelled run returned no error")
	}
}

// The report and the errors carry class names and counts only: a planted
// address, verifier and identity subject never appear.
func TestReconcileOutputCarriesNoAddressVerifierOrSubject(t *testing.T) {
	row, auth := pair(t)
	row.Email = "planted.address@example.test"
	auth.Email, auth.EmailLower = "other.address@example.test", "other.address@example.test"
	hash := *row.PasswordHash
	other := hash[:len(hash)-1] + "x"
	auth.PasswordHash = &other
	row.AuthProvider, row.AuthProviderID = str("github"), str("planted-subject-4242")
	rep := reconcile(t, []Row{row}, []AuthRow{auth}, nil)
	if rep.GatedTotal() == 0 {
		t.Fatal("nothing planted was reported")
	}
	out := strings.Join(rep.Lines(), "\n")
	for _, secret := range []string{"planted.address", "other.address", hash, other, "planted-subject-4242", row.ID.String(), "$2"} {
		if strings.Contains(out, secret) {
			t.Fatalf("report carries %q:\n%s", secret, out)
		}
	}
}

func TestReconcileLinesAreStableAndListEveryGatedClass(t *testing.T) {
	lines := reconcile(t, nil, nil, nil).Lines()
	want := []string{"mode=read-only", "seen=0", "matched=0", "gated_diffs=0"}
	for _, class := range GatedClasses {
		want = append(want, "diff."+class+"=0")
	}
	want = append(want, "info.last_login_drift=0", "info.display_name_drift=0", "info.identity_not_carried=0")
	if strings.Join(lines, "\n") != strings.Join(want, "\n") {
		t.Fatalf("lines:\n%s", strings.Join(lines, "\n"))
	}
}

// A matched row whose auth principal has the wrong kind still has every
// independent field compared.
func TestReconcileWrongKindWithAUsersRowStillComparesOtherFields(t *testing.T) {
	row, auth := pair(t)
	auth.Kind = "service_account"
	auth.Active = false
	auth.TokenVersion = 9
	rep := reconcile(t, []Row{row}, []AuthRow{auth}, nil)
	if rep.Diffs[DiffPrincipalKind] != 1 || rep.Diffs[DiffActive] != 1 || rep.Diffs[DiffTokenVersion] != 1 || rep.GatedTotal() != 3 || rep.Matched != 0 {
		t.Fatalf("%+v", rep.Diffs)
	}
}

// The wrong kind without a users row has no fields to compare: one class.
func TestReconcileWrongKindWithoutAUsersRowIsOneClass(t *testing.T) {
	for _, kind := range []string{"service_account", "user"} {
		row, auth := pair(t)
		auth.Kind, auth.HasUser = kind, false
		rep := reconcile(t, []Row{row}, []AuthRow{auth}, nil)
		if rep.Diffs[DiffPrincipalKind] != 1 || rep.GatedTotal() != 1 {
			t.Errorf("kind %s without a users row: %+v", kind, rep.Diffs)
		}
	}
}

// An unreadable verifier does not hide a valid identity the auth user lacks,
// and the identity is judged from the public row, not from the mapper's early return.
func TestReconcileUnknownHashStillChecksAValidIdentity(t *testing.T) {
	row, auth := pair(t)
	row.PasswordHash = str("not-a-bcrypt-verifier")
	row.AuthProvider, row.AuthProviderID = str("github"), str("subject-1")
	rep := reconcile(t, []Row{row}, []AuthRow{auth}, &fakeTarget{holders: map[identity.ExternalKey]uuid.UUID{}})
	if rep.Diffs[SkipUnknownHashFormat] != 1 || rep.Diffs[DiffIdentityMissing] != 1 || rep.GatedTotal() != 2 {
		t.Fatalf("%+v", rep.Diffs)
	}
	key := identity.ExternalKey{Issuer: "https://github.com", Provider: identity.ProviderGitHub, Subject: "subject-1"}
	rep = reconcile(t, []Row{row}, []AuthRow{auth}, &fakeTarget{holders: map[identity.ExternalKey]uuid.UUID{key: uuid.New()}})
	if rep.Diffs[SkipUnknownHashFormat] != 1 || rep.Diffs[DiffIdentityConflict] != 1 || rep.GatedTotal() != 2 {
		t.Fatalf("conflict: %+v", rep.Diffs)
	}
}

// Both skip classes are reported when a row falls into both.
func TestReconcileRowInBothSkipClassesCountsBoth(t *testing.T) {
	row, _ := pair(t)
	row.PasswordHash = str("not-a-bcrypt-verifier")
	row.AuthProvider, row.AuthProviderID = str("github"), str("")
	rep := reconcile(t, []Row{row}, nil, nil)
	if rep.Diffs[SkipUnknownHashFormat] != 1 || rep.Diffs[SkipInvalidIdentity] != 1 || rep.Diffs[DiffMissingInAuth] != 0 || rep.GatedTotal() != 2 {
		t.Fatalf("%+v", rep.Diffs)
	}
}
