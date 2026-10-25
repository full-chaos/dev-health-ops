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
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

func TestMapOrg(t *testing.T) {
	cases := []struct {
		name  string
		row   OrgRow
		class string
	}{
		{"valid", OrgRow{Slug: "acme", Name: "Acme", Active: true}, ""},
		{"inactive is valid", OrgRow{Slug: "acme", Name: "Acme"}, ""},
		{"empty slug", OrgRow{Slug: " ", Name: "Acme"}, SkipOrgInvalid},
		{"empty name", OrgRow{Slug: "acme", Name: ""}, SkipOrgInvalid},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			target, class := MapOrg(c.row)
			if class != c.class {
				t.Fatalf("class = %q, want %q", class, c.class)
			}
			if class == "" && (target.Slug != c.row.Slug || target.Active != c.row.Active) {
				t.Fatal("fields not carried")
			}
		})
	}
}

func TestMapMembership(t *testing.T) {
	joined := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	created := joined.Add(-time.Hour)
	cases := []struct {
		name   string
		row    MemberRow
		class  string
		joined *time.Time
	}{
		{"viewer", MemberRow{Role: str("viewer"), JoinedAt: &joined, Copies: 1}, "", &joined},
		{"owner", MemberRow{Role: str("owner"), JoinedAt: &joined, Copies: 1}, "", &joined},
		{"joined_at falls back to created_at", MemberRow{Role: str("admin"), CreatedAt: &created, Copies: 1}, "", &created},
		{"no timestamps is refused", MemberRow{Role: str("member"), Copies: 1}, SkipMembershipNoTimestamp, nil},
		{"nil role is refused", MemberRow{Copies: 1}, SkipMembershipUnknownRole, nil},
		{"unknown role is refused", MemberRow{Role: str("superadmin"), Copies: 1}, SkipMembershipUnknownRole, nil},
		{"upper case role is refused", MemberRow{Role: str("Admin"), Copies: 1}, SkipMembershipUnknownRole, nil},
		{"platform role is refused", MemberRow{Role: str("platform_admin"), Copies: 1}, SkipMembershipUnknownRole, nil},
		{"duplicate source is refused", MemberRow{Role: str("admin"), Copies: 2}, SkipMembershipDuplicateSrc, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			target, class := MapMembership(c.row)
			if class != c.class {
				t.Fatalf("class = %q, want %q", class, c.class)
			}
			if class != "" {
				return
			}
			if (target.JoinedAt == nil) != (c.joined == nil) || (c.joined != nil && !target.JoinedAt.Equal(*c.joined)) {
				t.Fatalf("joined_at = %v, want %v", target.JoinedAt, c.joined)
			}
			if target.Role != *c.row.Role {
				t.Fatal("role not carried")
			}
		})
	}
}

type fakeOrgSource struct {
	orgs    []OrgRow
	members []MemberRow
	pages   int
	failOrg bool
}

func (f *fakeOrgSource) PageOrgs(_ context.Context, after *uuid.UUID, limit int) ([]OrgRow, error) {
	if f.failOrg {
		return nil, errors.New("secret detail 4f1e")
	}
	f.pages++
	var out []OrgRow
	skipping := after != nil
	for _, o := range f.orgs {
		if skipping {
			skipping = o.ID != *after
			continue
		}
		out = append(out, o)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

func (f *fakeOrgSource) PageMemberships(_ context.Context, after *MemberKey, limit int) ([]MemberRow, error) {
	var out []MemberRow
	skipping := after != nil
	for _, m := range f.members {
		if skipping {
			if m.OrgID == after.OrgID && m.UserID == after.UserID {
				skipping = false
			}
			continue
		}
		out = append(out, m)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

type fakeOrgSink struct {
	orgCalls, memberCalls int
	dryCalls              int
	org                   func(identity.OrgBackfillTarget) (identity.OrgBackfillResult, error)
	member                func(identity.MembershipBackfillTarget) (identity.MembershipBackfillResult, error)
}

func (f *fakeOrgSink) BackfillOrganization(_ context.Context, t identity.OrgBackfillTarget, dry bool) (identity.OrgBackfillResult, error) {
	f.orgCalls++
	if dry {
		f.dryCalls++
	}
	return f.org(t)
}

func (f *fakeOrgSink) BackfillMembership(_ context.Context, t identity.MembershipBackfillTarget, dry bool) (identity.MembershipBackfillResult, error) {
	f.memberCalls++
	if dry {
		f.dryCalls++
	}
	return f.member(t)
}

type fakeAuthOnly struct {
	only AuthOnly
	err  error
}

func (f fakeAuthOnly) AuthOnly(context.Context) (AuthOnly, error) { return f.only, f.err }

func orgRows(n int) []OrgRow {
	rows := make([]OrgRow, n)
	for i := range rows {
		rows[i] = OrgRow{ID: uuid.MustParse("00000000-0000-0000-0000-00000000000" + string(rune('1'+i))), Slug: "s" + string(rune('a'+i)), Name: "n", Active: true}
	}
	return rows
}

func TestRunOrgsCountsEveryOutcome(t *testing.T) {
	orgs := orgRows(4)
	src := &fakeOrgSource{orgs: orgs, members: []MemberRow{
		{OrgID: orgs[0].ID, UserID: uuid.New(), Role: str("owner"), Copies: 1, CreatedAt: &testTime},
		{OrgID: orgs[0].ID, UserID: uuid.New(), Role: str("owner"), Copies: 1, CreatedAt: &testTime},
		{OrgID: orgs[0].ID, UserID: uuid.New(), Role: str("owner"), Copies: 1, CreatedAt: &testTime},
		{OrgID: orgs[0].ID, UserID: uuid.New(), Role: str("owner"), Copies: 1, CreatedAt: &testTime},
		{OrgID: orgs[0].ID, UserID: uuid.New(), Role: str("owner"), Copies: 1, CreatedAt: &testTime},
		{OrgID: orgs[0].ID, UserID: uuid.New(), Role: str("bogus"), Copies: 1, CreatedAt: &testTime},
	}}
	orgN, memN := 0, 0
	sink := &fakeOrgSink{
		org: func(t identity.OrgBackfillTarget) (identity.OrgBackfillResult, error) {
			orgN++
			switch orgN {
			case 1:
				return identity.OrgBackfillResult{Inserted: true}, nil
			case 2:
				return identity.OrgBackfillResult{Updated: true, Reactivated: true}, nil
			case 3:
				return identity.OrgBackfillResult{}, identity.ErrOrganizationSlugCollision
			}
			return identity.OrgBackfillResult{}, nil
		},
		member: func(t identity.MembershipBackfillTarget) (identity.MembershipBackfillResult, error) {
			memN++
			switch memN {
			case 1:
				return identity.MembershipBackfillResult{Inserted: true}, nil
			case 2:
				return identity.MembershipBackfillResult{RoleUpdated: true}, nil
			case 3:
				return identity.MembershipBackfillResult{Diverged: true}, nil
			case 4:
				return identity.MembershipBackfillResult{}, identity.ErrBackfillPrincipalMissing
			}
			return identity.MembershipBackfillResult{}, identity.ErrBackfillOrgMissing
		},
	}
	rep, err := RunOrgs(context.Background(), src, sink, fakeAuthOnly{only: AuthOnly{Orgs: 2, Memberships: 3}}, Options{BatchSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{SkipOrgSlugCollision: 1, SkipMembershipNoPrincipal: 1, SkipMembershipOrgMissing: 1, SkipMembershipUnknownRole: 1}
	for class, n := range want {
		if rep.Skipped[class] != n {
			t.Errorf("skipped.%s = %d, want %d", class, rep.Skipped[class], n)
		}
	}
	if rep.OrgsSeen != 4 || rep.OrgsInserted != 1 || rep.OrgsUpdated != 1 || rep.OrgsReactivated != 1 || rep.OrgsUnchanged != 1 ||
		rep.MembershipsSeen != 6 || rep.MembershipsInserted != 1 || rep.MembershipsRoleSet != 1 || rep.AuthDiverged != 1 ||
		rep.AuthOnlyOrgs != 2 || rep.AuthOnlyMemberships != 3 || rep.SkippedTotal() != 4 {
		t.Fatalf("report = %+v", rep)
	}
	if src.pages < 3 {
		t.Fatalf("org pages = %d, paging not used", src.pages)
	}
	lines := strings.Join(rep.Lines(), "\n")
	for _, l := range []string{"auth_only_memberships=3", "auth_only_orgs=2", "skipped=4", "skipped.org_slug_collision=1"} {
		if !strings.Contains(lines, l) {
			t.Errorf("report lacks %q", l)
		}
	}
}

func TestRunOrgsDryRunTreatsPendingOrgAsPresent(t *testing.T) {
	orgs := orgRows(1)
	src := &fakeOrgSource{orgs: orgs, members: []MemberRow{{OrgID: orgs[0].ID, UserID: uuid.New(), Role: str("admin"), Copies: 1, CreatedAt: &testTime}}}
	sink := &fakeOrgSink{
		org: func(identity.OrgBackfillTarget) (identity.OrgBackfillResult, error) {
			return identity.OrgBackfillResult{Inserted: true}, nil
		},
		member: func(identity.MembershipBackfillTarget) (identity.MembershipBackfillResult, error) {
			return identity.MembershipBackfillResult{}, identity.ErrBackfillOrgMissing
		},
	}
	rep, err := RunOrgs(context.Background(), src, sink, nil, Options{DryRun: true})
	if err != nil || rep.SkippedTotal() != 0 || rep.MembershipsInserted != 1 || !rep.DryRun {
		t.Fatalf("dry run: %+v %v", rep, err)
	}
	if sink.dryCalls != 2 {
		t.Fatalf("sink calls not marked dry: %d", sink.dryCalls)
	}
	rep, _ = RunOrgs(context.Background(), src, sink, nil, Options{DryRun: false})
	if rep.Skipped[SkipMembershipOrgMissing] != 1 {
		t.Fatalf("a write run must not excuse a missing organization: %+v", rep.Skipped)
	}
}

func TestRunOrgsErrorsCarryNoSourceDetail(t *testing.T) {
	_, err := RunOrgs(context.Background(), &fakeOrgSource{failOrg: true}, &fakeOrgSink{}, nil, Options{})
	if err == nil || strings.Contains(err.Error(), "4f1e") {
		t.Fatalf("err = %v", err)
	}
	src := &fakeOrgSource{}
	_, err = RunOrgs(context.Background(), src, &fakeOrgSink{}, fakeAuthOnly{err: errors.New("secret detail 9a")}, Options{})
	if err == nil || strings.Contains(err.Error(), "9a") {
		t.Fatalf("auth-only err = %v", err)
	}
}

func TestRunOrgsReportAndErrorsCarryNoRowData(t *testing.T) {
	org := OrgRow{ID: uuid.New(), Slug: "slug-marker-7f3a", Name: "name-marker-7f3a", Active: true}
	bad := OrgRow{ID: uuid.New(), Slug: "", Name: "name-marker-9c1d"}
	user := uuid.New()
	src := &fakeOrgSource{orgs: []OrgRow{bad, org}, members: []MemberRow{
		{OrgID: org.ID, UserID: user, Role: str("role-marker-4b2e"), Copies: 1, CreatedAt: &testTime},
		{OrgID: org.ID, UserID: uuid.New(), Role: str("owner"), Copies: 1, CreatedAt: &testTime},
		{OrgID: org.ID, UserID: uuid.New(), Role: str("owner"), Copies: 1, CreatedAt: &testTime},
	}}
	secret := errors.New("detail-marker-5d8f " + org.ID.String() + " " + user.String())
	sink := &fakeOrgSink{
		org: func(identity.OrgBackfillTarget) (identity.OrgBackfillResult, error) {
			return identity.OrgBackfillResult{}, secret
		},
		member: func(identity.MembershipBackfillTarget) (identity.MembershipBackfillResult, error) {
			return identity.MembershipBackfillResult{}, secret
		},
	}
	rep, err := RunOrgs(context.Background(), src, sink, fakeAuthOnly{only: AuthOnly{Orgs: 1}}, Options{})
	if err != nil || rep.SkippedTotal() != 5 {
		t.Fatalf("run: %+v %v", rep, err)
	}
	_, notes := orgsVerdict(rep, errors.New("stopped"))
	text := strings.Join(rep.Lines(), "\n") + "\n" + strings.Join(notes, "\n")
	for _, marker := range []string{"slug-marker-7f3a", "name-marker-7f3a", "name-marker-9c1d", "role-marker-4b2e", "detail-marker-5d8f", org.ID.String(), user.String(), bad.ID.String()} {
		if strings.Contains(text, marker) {
			t.Fatalf("row data %q reached the report", marker)
		}
	}
	for _, failing := range []*fakeOrgSource{{failOrg: true}} {
		_, err := RunOrgs(context.Background(), failing, sink, nil, Options{})
		if err == nil || strings.Contains(err.Error(), "4f1e") || strings.Contains(err.Error(), org.ID.String()) {
			t.Fatalf("source error text: %v", err)
		}
	}
	_, err = RunOrgs(context.Background(), &fakeOrgSource{}, sink, fakeAuthOnly{err: secret}, Options{})
	if err == nil || strings.Contains(err.Error(), "detail-marker-5d8f") || strings.Contains(err.Error(), org.ID.String()) {
		t.Fatalf("auth-only error text: %v", err)
	}
}

func TestOrgsVerdictExitDecision(t *testing.T) {
	for _, c := range []struct {
		name         string
		orgs, mems   int
		skipped, err bool
		code         int
		notes        int
	}{
		{"neither auth-only", 0, 0, false, false, cli.ExitOK, 0},
		{"auth-only orgs only: STOP note, exit unchanged", 1, 0, false, false, cli.ExitOK, 1},
		{"auth-only memberships only: STOP note, exit unchanged", 0, 1, false, false, cli.ExitOK, 1},
		{"both: two STOP notes, exit unchanged", 2, 3, false, false, cli.ExitOK, 2},
		{"a skipped row fails the run", 0, 0, true, false, cli.ExitFailure, 0},
		{"auth-only orgs and a skipped row", 1, 0, true, false, cli.ExitFailure, 1},
		{"a stopped run fails with its note", 0, 1, false, true, cli.ExitFailure, 2},
	} {
		t.Run(c.name, func(t *testing.T) {
			rep := OrgReport{AuthOnlyOrgs: c.orgs, AuthOnlyMemberships: c.mems, Skipped: map[string]int{}}
			if c.skipped {
				rep.Skipped[SkipOrgInvalid] = 1
			}
			var err error
			if c.err {
				err = errors.New("x")
			}
			code, notes := orgsVerdict(rep, err)
			if code != c.code || len(notes) != c.notes {
				t.Fatalf("code %d notes %v, want %d and %d notes", code, notes, c.code, c.notes)
			}
			joined := strings.Join(notes, "\n")
			if (c.orgs > 0) != strings.Contains(joined, "auth_only_orgs") || (c.mems > 0) != strings.Contains(joined, "auth_only_memberships") {
				t.Fatalf("notes = %q", joined)
			}
		})
	}
}

func TestRunOrgsStopsOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	src := &fakeOrgSource{orgs: orgRows(1)}
	_, err := RunOrgs(ctx, src, &fakeOrgSink{}, nil, Options{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
}

func TestOrganizationRolesAreTheCatalogOrganizationRoles(t *testing.T) {
	var want []string
	for _, role := range policycatalog.Default().Roles() {
		if role.Scope == policycatalog.ScopeOrganization {
			want = append(want, role.ID)
		}
	}
	got := append([]string(nil), identity.OrganizationRoles...)
	sort.Strings(want)
	sort.Strings(got)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("identity.OrganizationRoles = %v, catalog = %v", got, want)
	}
}

func TestOrgsVerbRefusesWhileTheUsersOwnedSwitchIsOn(t *testing.T) {
	var verb cli.Command
	for _, c := range Command().Children {
		if c.Name == "orgs-backfill" {
			verb = c
		}
	}
	if verb.Run == nil {
		t.Fatal("orgs-backfill is not registered")
	}
	for value, want := range map[string]int{"true": cli.ExitRefused, "maybe": cli.ExitFailure} {
		lookup := func(k string) (string, bool) {
			if k == "AUTH_USERS_OWNED" {
				return value, true
			}
			return "", false
		}
		var stderr bytes.Buffer
		if code := verb.Run(context.Background(), cli.Env{Args: []string{"-dry-run"}, Lookup: lookup, Stdout: &bytes.Buffer{}, Stderr: &stderr}); code != want {
			t.Fatalf("switch %q: exit %d, want %d (%s)", value, code, want, stderr.String())
		}
	}
	if code := verb.Run(context.Background(), cli.Env{Args: []string{"extra"}, Lookup: func(string) (string, bool) { return "", false }, Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}}); code != cli.ExitUsage {
		t.Fatalf("positional argument: exit %d", code)
	}
}

var testTime = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func orgsVerbWith(t *testing.T) cli.Command {
	t.Helper()
	for _, c := range Command().Children {
		if c.Name == "orgs-backfill" {
			return c
		}
	}
	t.Fatal("orgs-backfill is not registered")
	return cli.Command{}
}

func failIfConnected(t *testing.T) {
	t.Helper()
	orig := connectFn
	connectFn = func(context.Context, cli.Env, string) (*pgxpool.Pool, authschema.ValidatedIdentifier, int) {
		t.Fatal("connect was reached: the ownership gate must refuse before any database access")
		return nil, authschema.ValidatedIdentifier{}, cli.ExitFailure
	}
	t.Cleanup(func() { connectFn = orig })
}

func TestOrgsVerbOwnershipGateRefusesBeforeConnect(t *testing.T) {
	failIfConnected(t)
	cases := []struct {
		name, switchName, value string
		args                    []string
		want                    int
	}{
		{"orgs on real run", "AUTH_ORGS_OWNED", "true", nil, cli.ExitRefused},
		{"orgs on dry run", "AUTH_ORGS_OWNED", "true", []string{"-dry-run"}, cli.ExitRefused},
		{"users on real run", "AUTH_USERS_OWNED", "true", nil, cli.ExitRefused},
		{"users on dry run", "AUTH_USERS_OWNED", "true", []string{"-dry-run"}, cli.ExitRefused},
		{"orgs malformed", "AUTH_ORGS_OWNED", "maybe", []string{"-dry-run"}, cli.ExitFailure},
		{"users malformed", "AUTH_USERS_OWNED", "maybe", nil, cli.ExitFailure},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			lookup := func(k string) (string, bool) {
				if k == c.switchName {
					return c.value, true
				}
				return "postgres://secret-user:secret-pass@db/x", k == "AUTH_DATABASE_URI"
			}
			var stdout, stderr bytes.Buffer
			code := orgsVerbWith(t).Run(context.Background(), cli.Env{Args: c.args, Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
			if code != c.want {
				t.Fatalf("exit %d, want %d (%s)", code, c.want, stderr.String())
			}
			out := stderr.String()
			if !strings.Contains(out, c.switchName) || strings.Contains(out, "secret-pass") || strings.Contains(out, "maybe") || stdout.Len() != 0 {
				t.Fatalf("stderr %q stdout %q: must name %s only", out, stdout.String(), c.switchName)
			}
		})
	}
}

func TestOrgsVerbPassesTheGateWhenNeitherSwitchIsOn(t *testing.T) {
	orig := connectFn
	reached := false
	connectFn = func(context.Context, cli.Env, string) (*pgxpool.Pool, authschema.ValidatedIdentifier, int) {
		reached = true
		return nil, authschema.ValidatedIdentifier{}, cli.ExitFailure
	}
	t.Cleanup(func() { connectFn = orig })
	for _, v := range []string{"", "false"} {
		reached = false
		lookup := func(k string) (string, bool) { return v, k == "AUTH_ORGS_OWNED" || k == "AUTH_USERS_OWNED" }
		if code := orgsVerbWith(t).Run(context.Background(), cli.Env{Lookup: lookup, Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}}); code != cli.ExitFailure || !reached {
			t.Fatalf("value %q: exit %d reached=%v; want the gate passed", v, code, reached)
		}
	}
}
