package usersbackfill

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// relinkRows is one row per SSO shape: the bound OIDC account with a keyable subject is re-linkable, every other one
// is not (a bound SAML account has no stored NameID format).
func relinkRows() (rows []Row, relinkable int) {
	rows, relinkable, _ = relinkRowsWithBuckets()
	return rows, relinkable
}

// relinkRowsWithBuckets is relinkRows with the count of the sso_subject_unverifiable bucket.
func relinkRowsWithBuckets() (rows []Row, relinkable, unverifiable int) {
	bound := uuid.New()
	shapes := []struct {
		provider, subject, protocol *string
		bound                       bool
		want                        bool
		unverifiable                bool
	}{
		{str("oidc"), str("sub-1"), str("oidc"), true, true, false},
		{str("saml"), str("nameid-1"), str("saml"), true, false, true},
		{str("oidc"), str(" sub-4"), str("oidc"), true, false, false},
		{str("oidc"), str("sub-2"), nil, false, false, false},
		{str("oidc"), str("sub-3"), nil, true, false, false},
		{str("saml"), str(""), str("saml"), true, false, false},
		{str("saml"), nil, str("saml"), true, false, false},
		{str("saml"), str("nameid-2"), str("oidc"), true, false, false},
		{str("github"), str("910001"), str("oidc"), true, false, false},
		{str("local"), nil, nil, false, false, false},
	}
	for _, s := range shapes {
		row := Row{ID: uuid.New(), Email: "u@example.test", Active: true, AuthProvider: s.provider, AuthProviderID: s.subject,
			SSOProviderProtocol: s.protocol}
		if s.bound {
			row.SSOProviderID = &bound
		}
		if s.want {
			relinkable++
		}
		if s.unverifiable {
			unverifiable++
		}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID.String() < rows[j].ID.String() })
	return rows, relinkable, unverifiable
}

func TestRelinkableIsTheSharedDefinition(t *testing.T) {
	rows, want := relinkRows()
	got := 0
	for _, row := range rows {
		shared := identity.IsSSORelinkable(row.AuthProvider, row.AuthProviderID, row.SSOProviderID, row.SSOProviderProtocol)
		if Relinkable(row) != shared {
			t.Fatalf("Relinkable(%v) = %v, identity.IsSSORelinkable = %v", row.AuthProvider, Relinkable(row), shared)
		}
		if shared {
			got++
			if _, class, notCarried := Map(row); class != "" || !notCarried {
				t.Fatalf("a re-linkable row must stay not carried: class %q notCarried %v", class, notCarried)
			}
		}
	}
	if got != want {
		t.Fatalf("re-linkable rows = %d, want %d", got, want)
	}
}

func TestRunCountsRelinkPendingInsideNotCarried(t *testing.T) {
	rows, want, unverifiable := relinkRowsWithBuckets()
	sink := &memSink{seen: map[uuid.UUID]identity.BackfillTarget{}}
	rep, err := Run(context.Background(), pagedSource{rows: rows}, sink, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if rep.IdentityRelinkPending != want || rep.SSOSubjectUnverifiable != unverifiable || rep.IdentityNotCarry < want+unverifiable {
		t.Fatalf("relink pending %d (want %d), unverifiable %d (want %d), not carried %d: %v", rep.IdentityRelinkPending, want,
			rep.SSOSubjectUnverifiable, unverifiable, rep.IdentityNotCarry, rep.Lines())
	}
	for _, target := range sink.seen {
		if target.Identity != nil && target.Identity.Provider.IsSSO() {
			t.Fatal("the backfill carried an SSO identity; a bound SSO account is re-linked at sign-in")
		}
	}
}

// Every report line that existed before the relink count keeps its text, value and order; the one new line follows
// identity_not_carried.
func TestReportLinesKeepEveryOldLineByteEqual(t *testing.T) {
	rep := Report{DryRun: true, Seen: 9, Inserted: 1, Updated: 2, Unchanged: 3, IdentityLinked: 4, IdentityNotCarry: 5,
		DisplayNameDrift: 6, LastLoginDrift: 7, RoleGranted: 8, RoleRevoked: 9, Skipped: map[string]int{SkipInvalidIdentity: 1, SkipEmailCollision: 2},
		IdentityRelinkPending: 3, SSOSubjectUnverifiable: 4}
	old := []string{"mode=dry-run", "seen=9", "inserted=1", "updated=2", "unchanged=3", "identity_linked=4", "identity_not_carried=5",
		"display_name_drift=6", "last_login_drift=7", "platform_role_granted=8", "platform_role_revoked=9", "skipped=3",
		"skipped.email_collision=2", "skipped.invalid_identity=1"}
	var kept []string
	for _, line := range rep.Lines() {
		if line != "identity_relink_pending=3" && line != "sso_subject_unverifiable=4" {
			kept = append(kept, line)
		}
	}
	if strings.Join(kept, "\n") != strings.Join(old, "\n") {
		t.Fatalf("old lines changed:\n%s", strings.Join(kept, "\n"))
	}
	if got := rep.Lines()[7]; got != "identity_relink_pending=3" {
		t.Fatalf("line 8 = %q, want the relink count after identity_not_carried", got)
	}
	if got := rep.Lines()[8]; got != "sso_subject_unverifiable=4" {
		t.Fatalf("line 9 = %q, want the sso_subject_unverifiable bucket after the relink count", got)
	}
}

func TestReconcilePrintsRelinkPendingAndNeverGatesIt(t *testing.T) {
	rows, want := relinkRows()
	var auth []AuthRow
	for _, row := range rows {
		auth = append(auth, AuthRow{ID: row.ID, Kind: "user", HasUser: true, Email: row.Email, EmailLower: identity.StoredLoginKey(row.Email), Active: true})
	}
	var tgt *fakeTarget
	for _, row := range rows {
		if row.AuthProvider != nil && *row.AuthProvider == "github" {
			tgt = &fakeTarget{holders: map[identity.ExternalKey]uuid.UUID{ghKey(*row.AuthProviderID): row.ID}}
		}
	}
	rep := reconcile(t, rows, auth, tgt)
	if rep.SSORelinkPending != want || rep.GatedTotal() != 0 {
		t.Fatalf("relink pending %d (want %d), gated %v", rep.SSORelinkPending, want, rep.Diffs)
	}
	// A public row with no auth row is still missing_in_auth; its relink count is printed too.
	rep = reconcile(t, rows, nil, tgt)
	if rep.SSORelinkPending != want || rep.Diffs[DiffMissingInAuth] != len(rows) {
		t.Fatalf("missing in auth: relink pending %d (want %d), %v", rep.SSORelinkPending, want, rep.Diffs)
	}
	if lines := strings.Join(rep.Lines(), "\n"); !strings.Contains(lines, "info.sso_identity_relink_pending=1") || !strings.Contains(lines, "info.sso_subject_unverifiable=1") {
		t.Fatalf("lines:\n%s", strings.Join(rep.Lines(), "\n"))
	}
}
