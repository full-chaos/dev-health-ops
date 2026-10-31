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

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/org"
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
	authOnly, _ := e.seedOrg(t, ctx, true) // copied below, then its public row is deleted: an auth-only organization
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
	// The auth schema is written only through audit.Commit: the auth-only organization is the copy of a public one whose
	// public row goes away (the way the orgs-backfill tests make one); no raw write to an auth table here.
	if _, err := e.admin.Exec(ctx, `DELETE FROM public.organizations WHERE id = $1`, authOnly); err != nil {
		t.Fatalf("delete the public row of the auth-only organization: %v", err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
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

// The snapshot holds nothing a writer can wait on and sees one consistent state: the backend of the open snapshot holds
// only AccessShare relation locks (no row lock, no transaction id), a writer on the very rows the report read is not
// blocked (lock_timeout 2s), and the snapshot keeps its view while a fresh snapshot sees the write.
func TestOrgsReconcileSnapshotBlocksNoWriterAndKeepsItsView(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	u := e.seed(t, ctx, uniq("orw"), true, true)
	org, slug := e.seedOrg(t, ctx, true)
	name := "Name " + slug
	e.seedMember(t, ctx, org, u, "member")
	e.backfill(t, ctx)
	if rep := e.runOrgs(t, ctx, false); rep.SkippedTotal() != 0 {
		t.Fatalf("the organization backfill skipped rows: %+v", rep)
	}
	t.Cleanup(func() {
		c := context.Background()
		_, _ = e.admin.Exec(c, `UPDATE public.organizations SET slug = $2 WHERE id = $1`, org, slug)
		_, _ = e.store.BackfillOrganization(c, identity.OrgBackfillTarget{ID: org, Slug: slug, Name: name, Active: true}, false)
	})

	tx, err := OpenSnapshot(ctx, e.runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	target := TxTarget{Tx: tx, Schema: e.schema}
	first, err := target.OrgDiffs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := classesOf(first, org); ok {
		t.Fatalf("the planted organization is a mismatch before any write: %v", first.Mismatches)
	}

	rows, err := tx.Query(ctx, `SELECT locktype, mode, granted FROM pg_locks WHERE pid = pg_backend_pid()`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var locktype, mode string
		var granted bool
		if err := rows.Scan(&locktype, &mode, &granted); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		okLock := (locktype == "relation" && mode == "AccessShareLock") || (locktype == "virtualxid" && mode == "ExclusiveLock")
		if !okLock || !granted {
			t.Errorf("the snapshot holds a lock beyond a read: locktype %s mode %s granted %v", locktype, mode, granted)
		}
	}
	rows.Close()

	// Writers on the rows the report read, from other connections, while the snapshot is open. The auth write goes
	// through the store (audit.Commit): a raw write is refused by the guard before any lock and would prove nothing.
	e.writerNotBlocked(t, "public.organizations", func(c context.Context) error {
		_, err := e.admin.Exec(c, `UPDATE public.organizations SET slug = slug || '-w' WHERE id = $1`, org)
		return err
	})
	e.writerNotBlocked(t, "auth.organizations (through audit.Commit)", func(c context.Context) error {
		_, err := e.store.BackfillOrganization(c, identity.OrgBackfillTarget{ID: org, Slug: slug, Name: name + " w", Active: true}, false)
		return err
	})
	e.writerNotBlocked(t, "public.memberships", func(c context.Context) error {
		_, err := e.admin.Exec(c, `UPDATE public.memberships SET updated_at = now() WHERE org_id = $1`, org)
		return err
	})

	again, err := target.OrgDiffs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := classesOf(again, org); ok {
		t.Errorf("the snapshot did not keep its view: the committed write shows inside it")
	}
	fresh := e.orgsSnapshot(t, ctx)
	if got, ok := classesOf(fresh, org); !ok || strings.Join(got, ",") != ClassOrgSlug+","+ClassOrgName {
		t.Errorf("a fresh snapshot does not see the write: classes %v (listed %v)", got, ok)
	}
}

// writerNotBlocked runs one writer and fails if it does not finish within 5 s: it then cancels the writer's context,
// joins it for at most 10 s and only then reports. The guard costs nothing when the writer is not blocked.
func (e *reconEnv) writerNotBlocked(t *testing.T, name string, write func(ctx context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- write(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("a writer on %s failed while the snapshot was open: %v", name, err)
		}
	case <-time.After(5 * time.Second):
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
		}
		t.Fatalf("a writer on %s was blocked by the open snapshot (no commit within 5 s)", name)
	}
}

// The reactivation window, end to end: a suspension that reached the auth
// schema stays after go-api's row turns active again (org_active_diff, which
// fails the default gate), and the auth-owned reactivate (the verb behind
// POST /internal/v1/organizations/reactivate) clears it: the organization is
// then listed with no class.
func TestOrgsReconcileIsZeroAfterTheAuthReactivate(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	id, _ := e.seedOrg(t, ctx, true)
	e.runOrgs(t, ctx, false)
	if _, ok := classesOf(e.orgsSnapshot(t, ctx), id); ok {
		t.Fatal("a freshly copied organization is a mismatch")
	}
	setActive := func(active bool) {
		t.Helper()
		if _, err := e.admin.Exec(ctx, `UPDATE public.organizations SET is_active = $2 WHERE id = $1`, id, active); err != nil {
			t.Fatal(err)
		}
	}
	setActive(false)
	if err := e.store.ProjectOrganization(ctx, id); err != nil {
		t.Fatalf("refresh after a go-api suspend: %v", err)
	}
	if classes, ok := classesOf(e.orgsSnapshot(t, ctx), id); ok {
		t.Fatalf("after the refresh of a go-api suspend: classes %v, want none", classes)
	}
	setActive(true)
	if err := e.store.ProjectOrganization(ctx, id); err != nil {
		t.Fatalf("refresh after a go-api reactivate: %v", err)
	}
	if classes, ok := classesOf(e.orgsSnapshot(t, ctx), id); !ok || strings.Join(classes, ",") != ClassOrgActive {
		t.Fatalf("a go-api reactivate alone: classes %v (listed %v), want %s", classes, ok, ClassOrgActive)
	}
	orgs, err := org.New(org.Config{Pool: e.runtime, Schema: e.schema})
	if err != nil {
		t.Fatal(err)
	}
	allow := func(context.Context, audit.TxOps, uuid.UUID) error { return nil }
	if changed, err := orgs.ReactivateAuthorized(ctx, id, nil, org.Actor{}, allow); err != nil || !changed {
		t.Fatalf("auth reactivate: changed=%v err=%v", changed, err)
	}
	if classes, ok := classesOf(e.orgsSnapshot(t, ctx), id); ok {
		t.Fatalf("after the auth reactivate: classes %v, want none (a zero diff)", classes)
	}
}

// The suspension window: with the auth-owned suspend, a go-api suspend reaches the auth schema before go-api writes
// its own row (org_active_diff while only the auth side is suspended), and once go-api's row is inactive too the
// organization is listed with no class, with no refresh needed.
func TestOrgsReconcileIsZeroAfterTheAuthSuspend(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	id, _ := e.seedOrg(t, ctx, true)
	e.runOrgs(t, ctx, false)
	if _, ok := classesOf(e.orgsSnapshot(t, ctx), id); ok {
		t.Fatal("a freshly copied organization is a mismatch")
	}
	orgs, err := org.New(org.Config{Pool: e.runtime, Schema: e.schema})
	if err != nil {
		t.Fatal(err)
	}
	allow := func(context.Context, audit.TxOps, uuid.UUID) error { return nil }
	if changed, err := orgs.SuspendAuthorized(ctx, id, nil, org.Actor{}, allow); err != nil || !changed {
		t.Fatalf("auth suspend: changed=%v err=%v", changed, err)
	}
	if classes, ok := classesOf(e.orgsSnapshot(t, ctx), id); !ok || strings.Join(classes, ",") != ClassOrgActive {
		t.Fatalf("the auth side suspended alone: classes %v (listed %v), want %s", classes, ok, ClassOrgActive)
	}
	if _, err := e.admin.Exec(ctx, `UPDATE public.organizations SET is_active = false WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	if classes, ok := classesOf(e.orgsSnapshot(t, ctx), id); ok {
		t.Fatalf("after go-api's row is inactive too: classes %v, want none (a zero diff, no refresh needed)", classes)
	}
}

// A birth leaves no diff: not at the default gate and not at -exact.
func TestBirthReconcilesExact(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	user := e.seed(t, ctx, "birth-"+uuid.NewString()[:8]+"@example.com", true, true)
	if err := e.store.Project(ctx, user); err != nil {
		t.Fatal(err)
	}
	owned, err := identity.New(identity.Config{Pool: e.ownedRuntime(t), Schema: e.schema, UsersOwnedByAuth: true})
	if err != nil {
		t.Fatal(err)
	}
	born, err := owned.OnboardOrganization(ctx, identity.OnboardOrganizationRequest{Principal: user, Name: "Exact"})
	if err != nil {
		t.Fatal(err)
	}
	if classes, ok := classesOf(e.orgsSnapshot(t, ctx), born.ID); ok {
		t.Fatalf("an organization birth is a mismatch: classes %v", classes)
	}
}
