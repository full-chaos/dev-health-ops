//go:build integration

package usersbackfill

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// orgsSnapshot reads the organization aggregates in one read-only snapshot.
func (e *reconEnv) orgsSnapshot(t *testing.T, ctx context.Context) OrgDiffs {
	t.Helper()
	tx, err := OpenSnapshot(ctx, e.runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	d, err := TxTarget{Tx: tx, Schema: e.schema}.OrgDiffs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func classesOf(d OrgDiffs, id uuid.UUID) ([]string, bool) {
	for _, m := range d.Mismatches {
		if m.OrgID == id {
			return m.Classes, true
		}
	}
	return nil, false
}

// Every class is planted on its own organization, behind the backfill's back, in a database shared with the other
// tests: each organization shows exactly its classes, the matched organization (with a multi-org user) shows none.
func TestOrgsReconcileClassifiesEachOrganization(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	u1 := e.seed(t, ctx, uniq("orc"), true, true)
	u2 := e.seed(t, ctx, uniq("orc"), true, true)
	u3 := e.seed(t, ctx, uniq("orc"), true, true)
	u4 := e.seed(t, ctx, uniq("orc"), true, true)
	orgOK, _ := e.seedOrg(t, ctx, true)
	orgSlug, _ := e.seedOrg(t, ctx, true)
	orgName, _ := e.seedOrg(t, ctx, true)
	orgActive, _ := e.seedOrg(t, ctx, true)
	orgMem, _ := e.seedOrg(t, ctx, true)
	e.seedMember(t, ctx, orgOK, u1, "owner")
	e.seedMember(t, ctx, orgOK, u2, "member")
	e.seedMember(t, ctx, orgSlug, u1, "member") // u1 is in several organizations
	e.seedMember(t, ctx, orgMem, u1, "owner")
	e.seedMember(t, ctx, orgMem, u2, "member")
	e.seedMember(t, ctx, orgMem, u3, "viewer")
	e.backfill(t, ctx)
	if rep := e.runOrgs(t, ctx, false); rep.SkippedTotal() != 0 {
		t.Fatalf("the organization backfill skipped rows: %+v", rep)
	}

	// Behind the backfill's back.
	var publicOnly []uuid.UUID
	e.matchOrgOnCleanup(t, orgMem, &publicOnly)
	orgPub, _ := e.seedOrg(t, ctx, true) // public only: never copied
	e.seedMember(t, ctx, orgPub, u1, "member")
	authOnly := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO auth.organizations (id, slug, name) VALUES ($1, $2, 'Auth only')`, authOnly, "auth-only-"+authOnly.String()[:8]); err != nil {
		t.Fatalf("seed an auth-only organization: %v", err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if _, err := e.admin.Exec(c, `DELETE FROM auth.organizations WHERE id = $1`, authOnly); err != nil {
			t.Logf("could not delete the auth-only organization (the runtime role has no DELETE; the admin should): %v", err)
		}
		_, _ = e.admin.Exec(c, `DELETE FROM public.memberships WHERE org_id = $1`, orgPub)
		_, _ = e.admin.Exec(c, `DELETE FROM public.organizations WHERE id = $1`, orgPub)
	})
	for _, c := range []struct {
		org uuid.UUID
		sql string
	}{
		{orgSlug, `UPDATE public.organizations SET slug = slug || '-x' WHERE id = $1`},
		{orgName, `UPDATE public.organizations SET name = name || ' x' WHERE id = $1`},
		{orgActive, `UPDATE public.organizations SET is_active = false WHERE id = $1`},
	} {
		if _, err := e.admin.Exec(ctx, c.sql, c.org); err != nil {
			t.Fatal(err)
		}
		undo := strings.NewReplacer("slug || '-x'", "replace(slug, '-x', '')", "name || ' x'", "replace(name, ' x', '')", "is_active = false", "is_active = true").Replace(c.sql)
		org := c.org
		t.Cleanup(func() {
			if _, err := e.admin.Exec(context.Background(), undo, org); err != nil {
				t.Errorf("restore a public organization: %v", err)
			}
		})
	}
	if _, err := e.admin.Exec(ctx, `UPDATE public.memberships SET role = 'viewer' WHERE org_id = $1 AND user_id = $2`, orgMem, u1); err != nil {
		t.Fatal(err)
	}
	if _, err := e.admin.Exec(ctx, `DELETE FROM public.memberships WHERE org_id = $1 AND user_id = $2`, orgMem, u2); err != nil {
		t.Fatal(err)
	}
	e.setAuthMembershipStatus(t, ctx, orgMem, u3, "removed") // public still has u3
	publicOnly = append(publicOnly, u4)
	e.seedMember(t, ctx, orgMem, u4, "member") // u4 is a user in auth, with no auth membership

	d := e.orgsSnapshot(t, ctx)
	want := map[uuid.UUID][]string{
		orgSlug:   {ClassOrgSlug},
		orgName:   {ClassOrgName},
		orgActive: {ClassOrgActive},
		orgMem:    {ClassMemberPublicMissing, ClassMemberInactiveInPublic, ClassMemberAuthMissing, ClassMemberRole},
		orgPub:    {ClassOrgMissingInAuth, ClassMemberAuthMissing},
		authOnly:  {ClassOrgExtraInAuth},
	}
	for org, classes := range want {
		got, ok := classesOf(d, org)
		if !ok || strings.Join(got, ",") != strings.Join(classes, ",") {
			t.Errorf("organization %s: classes %v (listed %v), want %v", org, got, ok, classes)
		}
	}
	if _, ok := classesOf(d, orgOK); ok {
		t.Errorf("the matched organization %s is listed as a mismatch", orgOK)
	}
	if d.MissingInAuth < 1 || d.ExtraInAuth < 1 || d.SlugDiff < 1 || d.NameDiff < 1 || d.ActiveDiff < 1 ||
		d.Members.PublicMissing < 1 || d.Members.InactiveInPublic < 1 || d.Members.AuthMissing < 2 || d.Members.RoleDiff < 1 {
		t.Errorf("the snapshot totals miss a planted class: %+v", d)
	}
	if d.Matched < 1 || d.PublicOrgs < 1 || d.AuthOrgs < 1 {
		t.Errorf("the matched count is empty: %+v", d)
	}

	// The verb, through the real connect and snapshot path: any gated diff exits 1 in both modes.
	dsn := e.runtime.Config().ConnString()
	lookup := func(name string) (string, bool) {
		switch name {
		case "AUTH_DATABASE_URI":
			return dsn, true
		case "AUTH_DATABASE_SCHEMA":
			return "auth", true
		}
		return "", false
	}
	for _, args := range [][]string{nil, {"-exact"}} {
		var stdout, stderr bytes.Buffer
		code := runOrgsReconcile(ctx, cli.Env{Args: args, Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
		if code != cli.ExitFailure || !strings.Contains(stdout.String(), "mismatch_orgs_total=") || !strings.Contains(stdout.String(), "mode=read-only\n") {
			t.Errorf("verb %v: exit %d, stdout %q, stderr %q", args, code, stdout.String(), stderr.String())
		}
		for _, secret := range []string{"auth-only-", "@example.test", "Auth only"} {
			if strings.Contains(stdout.String(), secret) {
				t.Errorf("verb %v printed a value (%q)", args, secret)
			}
		}
	}
}

// The snapshot is read-only: a write inside it is refused by the database, and a report changes no row.
func TestOrgsReconcileSnapshotIsReadOnlyAndWritesNothing(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	tx, err := OpenSnapshot(ctx, e.runtime)
	if err != nil {
		t.Fatal(err)
	}
	_, werr := tx.Exec(ctx, `INSERT INTO auth.organizations (id, slug, name) VALUES ($1, 'ro-probe', 'ro probe')`, uuid.New())
	_ = tx.Rollback(context.Background())
	var pgErr *pgconn.PgError
	if !errors.As(werr, &pgErr) || pgErr.Code != "25006" {
		t.Fatalf("a write inside the snapshot was not refused as read-only (25006): %v", werr)
	}
	before := e.writes(t, ctx)
	orgsBefore := e.count(t, ctx, `SELECT count(*) FROM auth.organizations`)
	_ = e.orgsSnapshot(t, ctx)
	if got := e.writes(t, ctx); got != before {
		t.Fatalf("the report wrote audit/outbox/revision rows: before %v after %v", before, got)
	}
	if got := e.count(t, ctx, `SELECT count(*) FROM auth.organizations`); got != orgsBefore {
		t.Fatalf("the report changed auth.organizations: %d -> %d", orgsBefore, got)
	}
}
