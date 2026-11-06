//go:build integration

package repoidentityprojection

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/decisionstate"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/restorefence"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

type env struct {
	admin, runtime *pgxpool.Pool
	schema         authschema.ValidatedIdentifier
	projectOrg     func(context.Context, uuid.UUID) (bool, error)
}

func newEnv(t *testing.T) *env {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	pgschema.Apply(ctx, t, admin)
	role, err := containers.RoleName("ridproj_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}
	const password = "ridproj-test-only-not-a-credential"
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role, UsersOwned: true}); err != nil {
		t.Fatal(err)
	}
	cfg := admin.Config().Copy()
	cfg.ConnConfig.User, cfg.ConnConfig.Password = role, password
	runtime, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtime.Close)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restorefence.Init(ctx, admin, schema, restorefence.InitOptions{IfAbsent: true}); err != nil {
		t.Fatal(err)
	}
	id, err := identity.New(identity.Config{Pool: runtime, Schema: schema, UsersOwnedByAuth: true})
	if err != nil {
		t.Fatal(err)
	}
	projectOrg := func(ctx context.Context, org uuid.UUID) (bool, error) {
		err := id.ProjectOrganization(ctx, org)
		if errors.Is(err, identity.ErrOrganizationNotFound) {
			return false, nil
		}
		return err == nil, err
	}
	return &env{admin: admin, runtime: runtime, schema: schema, projectOrg: projectOrg}
}

func (e *env) reader(t *testing.T, horizon time.Duration) *Reader {
	t.Helper()
	r, err := NewReader(Config{Pool: e.runtime, Schema: e.schema, Registry: health.NewRegistry(time.Second), Horizon: horizon,
		BatchLimit: 50, PollInterval: time.Second, ProjectOrganization: e.projectOrg})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func (e *env) org(t *testing.T) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := e.admin.Exec(context.Background(), `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'reader')`, id, "org-"+id.String()[:8]); err != nil {
		t.Fatal(err)
	}
	return id
}

// observe appends one event through the 0057 function in its own committed transaction, as the producer does.
func (e *env) observe(t *testing.T, org uuid.UUID, provider, id, path string, at time.Time) {
	t.Helper()
	ctx := context.Background()
	tx, err := e.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := audit.AppendRepositoryIdentity(ctx, tx, e.schema, []audit.RepositoryIdentityEvent{{
		OrganizationID: org, Provider: provider, ProviderRepoID: id, Path: path, ObservedAt: at,
	}}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

type identityRow struct {
	path     string
	revision int64
	observed *time.Time
	updated  time.Time
}

func (a identityRow) same(b identityRow) bool {
	return a.path == b.path && a.revision == b.revision && a.updated.Equal(b.updated) &&
		(a.observed == nil) == (b.observed == nil) && (a.observed == nil || a.observed.Equal(*b.observed))
}

func (e *env) row(t *testing.T, org uuid.UUID, provider, id string) (identityRow, bool) {
	t.Helper()
	var r identityRow
	err := e.admin.QueryRow(context.Background(), `SELECT current_path, source_revision, observed_at, updated_at FROM auth.repository_identities
		WHERE organization_id = $1 AND provider = $2 AND provider_repo_id = $3`, org, provider, id).Scan(&r.path, &r.revision, &r.observed, &r.updated)
	if err != nil {
		return identityRow{}, false
	}
	return r, true
}

func (e *env) count(t *testing.T, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := e.admin.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("%q: %v", sql, err)
	}
	return n
}

func (e *env) step(t *testing.T, r *Reader, now time.Time) StepReport {
	t.Helper()
	rep, err := r.Step(context.Background(), now)
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	return rep
}

func observedAudits(t *testing.T, e *env, org uuid.UUID) int {
	return e.count(t, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.repository_identity.observed' AND organization_id = $1`, org)
}

func TestTheReaderCopiesObservedRepositoriesAndFollowsARename(t *testing.T) {
	e := newEnv(t)
	r := e.reader(t, time.Hour)
	org := e.org(t)
	t0 := time.Date(2026, 11, 5, 8, 0, 0, 0, time.UTC)

	t.Run("github add: one row at revision 1 with the path, the organization copied on the way", func(t *testing.T) {
		e.observe(t, org, "github", "1001", "acme/api", t0)
		rep := e.step(t, r, t0.Add(time.Minute))
		if rep.Applied != 1 || rep.Parked != 0 || rep.Final != 0 {
			t.Fatalf("report %+v, want one applied", rep)
		}
		got, ok := e.row(t, org, "github", "1001")
		if !ok || got.path != "acme/api" || got.revision != 1 || got.observed == nil || !got.observed.Equal(t0) {
			t.Fatalf("row %+v (found %v), want acme/api at revision 1 observed %v", got, ok, t0)
		}
	})

	t.Run("gitlab rename: the same provider id moves to the new path; a path-only grant gets the id", func(t *testing.T) {
		e.observe(t, org, "gitlab", "77", "group/old-name", t0)
		e.step(t, r, t0.Add(time.Minute))
		grant := uuid.New()
		seedGrant(t, e, grant, org, "group/new-name")
		e.observe(t, org, "gitlab", "77", "group/new-name", t0.Add(time.Hour))
		if rep := e.step(t, r, t0.Add(2*time.Hour)); rep.Applied != 1 {
			t.Fatalf("report %+v, want the rename applied", rep)
		}
		got, _ := e.row(t, org, "gitlab", "77")
		if got.path != "group/new-name" || got.revision != 2 {
			t.Fatalf("row %+v, want group/new-name at revision 2", got)
		}
		if n := e.count(t, `SELECT count(*) FROM auth.repository_identities WHERE organization_id = $1 AND provider = 'gitlab'`, org); n != 1 {
			t.Fatalf("%d gitlab rows, want one (a rename moves the row)", n)
		}
		if n := e.count(t, `SELECT count(*) FROM auth.resource_grants WHERE id = $1 AND resource_repo_id = 'gitlab:77'`, grant); n != 1 {
			t.Fatal("the grant on the new path did not get the provider id")
		}
	})

	t.Run("after the rename the decision snapshot resolves the new path to the id and the old path to nothing", func(t *testing.T) {
		var principal uuid.UUID
		if err := e.admin.QueryRow(context.Background(), `SELECT principal_id FROM auth.resource_grants WHERE resource_repo_id = 'gitlab:77'`).Scan(&principal); err != nil {
			t.Fatal(err)
		}
		reader, err := decisionstate.New(decisionstate.Config{Pool: e.runtime, Schema: e.schema,
			Users: authapi.PGUsers{Pool: e.runtime, AuthSchema: e.schema}, RefreshTTL: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		var ids authorization.RepositoryIdentities
		if err := reader.Read(context.Background(), func(tx authapi.DecisionTx) error {
			snap, err := tx.Snapshot(context.Background(), principal, org.String())
			if err != nil {
				return err
			}
			var ok bool
			ids, ok = snap.(authorization.RepositoryIdentities)
			if !ok {
				return errors.New("the snapshot resolves no repository identity")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if id, ok := ids.RepositoryID(org.String(), "group/new-name"); !ok || id != "gitlab:77" {
			t.Fatalf("new path resolves to %q (%v), want gitlab:77", id, ok)
		}
		if id, ok := ids.RepositoryID(org.String(), "group/old-name"); ok || id != "" {
			t.Fatalf("old path still resolves to %q: an id-bearing grant would match the old path", id)
		}
		if path, ok := ids.PathForID(org.String(), "gitlab:77"); !ok || path != "group/new-name" {
			t.Fatalf("PathForID(gitlab:77) = %q (%v), want group/new-name", path, ok)
		}
	})

	t.Run("a step over decided rows writes nothing: no row change, no audit row", func(t *testing.T) {
		before, _ := e.row(t, org, "github", "1001")
		audits := observedAudits(t, e, org)
		rows := e.count(t, `SELECT count(*) FROM auth.repository_identities WHERE organization_id = $1`, org)
		rep := e.step(t, r, t0.Add(3*time.Hour))
		if rep.Seen != 0 {
			t.Fatalf("report %+v: the reader read a decided row again", rep)
		}
		after, _ := e.row(t, org, "github", "1001")
		if !after.same(before) || observedAudits(t, e, org) != audits ||
			e.count(t, `SELECT count(*) FROM auth.repository_identities WHERE organization_id = $1`, org) != rows {
			t.Fatalf("a decided row changed state: %+v -> %+v", before, after)
		}
	})

	t.Run("a replay of an applied revision is decided and changes nothing", func(t *testing.T) {
		before, _ := e.row(t, org, "github", "1001")
		audits := observedAudits(t, e, org)
		ev := Event{OrganizationID: org, Provider: "github", ProviderRepoID: "1001", Path: "acme/other", ObservedAt: t0.Add(5 * time.Hour), Revision: 1}
		got, err := r.applyWithOrganization(context.Background(), ev)
		if err != nil || got != decided {
			t.Fatalf("replay: outcome %v, err %v; want decided", got, err)
		}
		after, _ := e.row(t, org, "github", "1001")
		if !after.same(before) || observedAudits(t, e, org) != audits {
			t.Fatalf("the replay changed state: %+v -> %+v", before, after)
		}
	})

	t.Run("the same path at a new revision records the revision and keeps updated_at", func(t *testing.T) {
		before, _ := e.row(t, org, "github", "1001")
		e.observe(t, org, "github", "1001", "acme/api", t0.Add(4*time.Hour))
		if rep := e.step(t, r, t0.Add(5*time.Hour)); rep.Applied != 1 {
			t.Fatalf("report %+v, want the new revision recorded", rep)
		}
		after, _ := e.row(t, org, "github", "1001")
		if after.revision != 2 || after.path != "acme/api" || !after.updated.Equal(before.updated) {
			t.Fatalf("row %+v -> %+v, want revision 2, the same path and the same updated_at", before, after)
		}
	})

	t.Run("an event observed before the stored row is recorded but cannot undo a rename", func(t *testing.T) {
		e.observe(t, org, "gitlab", "77", "group/old-name", t0.Add(30*time.Minute))
		e.step(t, r, t0.Add(6*time.Hour))
		got, _ := e.row(t, org, "gitlab", "77")
		if got.path != "group/new-name" || got.revision != 3 || !got.observed.Equal(t0.Add(time.Hour)) {
			t.Fatalf("row %+v, want group/new-name kept at revision 3 with the newer observed_at", got)
		}
	})
}

func TestAnEventForAnOrganizationTheCopyLacksIsParkedThenApplied(t *testing.T) {
	e := newEnv(t)
	r := e.reader(t, time.Hour)
	org := uuid.New()
	t0 := time.Date(2026, 11, 5, 8, 0, 0, 0, time.UTC)
	e.observe(t, org, "github", "5", "lost/repo", t0)
	if rep := e.step(t, r, t0); rep.Parked != 1 {
		t.Fatalf("report %+v, want the row parked", rep)
	}
	if _, ok := e.row(t, org, "github", "5"); ok {
		t.Fatal("a row was written for an organization that does not exist")
	}
	if _, err := e.admin.Exec(context.Background(), `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'late')`, org, "org-"+org.String()[:8]); err != nil {
		t.Fatal(err)
	}
	rep := e.step(t, r, t0.Add(10*time.Minute))
	if rep.Released != 1 || rep.Applied != 1 {
		t.Fatalf("report %+v, want the parked row applied and released", rep)
	}
	if got, ok := e.row(t, org, "github", "5"); !ok || got.path != "lost/repo" {
		t.Fatalf("row %+v (found %v), want lost/repo", got, ok)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.projection_retry WHERE stream = 'repository_identity'`); n != 0 {
		t.Fatalf("%d retry rows left", n)
	}
}

func TestTheMarkPassesOnlyDecidedRowsAndTheReaperKeepsTheRest(t *testing.T) {
	e := newEnv(t)
	r := e.reader(t, time.Minute)
	org := e.org(t)
	lost := uuid.New()
	e.observe(t, org, "github", "1", "a/one", time.Now().Add(-time.Hour))
	e.observe(t, lost, "github", "2", "a/two", time.Now().Add(-time.Hour))
	later := time.Now().Add(2 * time.Hour)
	rep := e.step(t, r, later)
	if rep.Applied != 1 || rep.Parked != 1 {
		t.Fatalf("report %+v, want one applied and one parked", rep)
	}
	var newest int64
	if err := e.admin.QueryRow(context.Background(), `SELECT max(id) FROM auth.auth_outbox_events WHERE aggregate_type = 'repository_identity'`).Scan(&newest); err != nil {
		t.Fatal(err)
	}
	if rep.Mark != newest {
		t.Fatalf("mark %d, want %d: both rows are old and decided or parked", rep.Mark, newest)
	}
	e.observe(t, org, "github", "3", "a/three", time.Now())
	markOutboxPublished(t, e)
	if _, err := audit.Reap(context.Background(), e.admin, e.schema, time.Now().Add(24*time.Hour), 100); err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.auth_outbox_events WHERE aggregate_type = 'repository_identity' AND id > $1`, newest); n != 1 {
		t.Fatalf("%d rows above the mark after the reap, want the undecided one kept", n)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.auth_outbox_events WHERE aggregate_type = 'repository_identity' AND id <= $1`, newest); n != 0 {
		t.Fatalf("%d rows at or below the mark after the reap, want none", n)
	}
}

func TestAMalformedRowIsParkedForGood(t *testing.T) {
	e := newEnv(t)
	r := e.reader(t, time.Hour)
	org := e.org(t)
	e.observe(t, org, "github", "9", "x/y", time.Now())
	seedOutbox(t, e, `UPDATE auth.auth_outbox_events SET idempotency_key = 'repository_identity:other:1' WHERE aggregate_type = 'repository_identity'`)
	rep := e.step(t, r, time.Now())
	if rep.Final != 1 || rep.Applied != 0 {
		t.Fatalf("report %+v, want the row parked for good", rep)
	}
	if _, ok := e.row(t, org, "github", "9"); ok {
		t.Fatal("a malformed row was applied")
	}
	if rep := e.step(t, r, time.Now().Add(time.Hour)); rep.Seen != 0 || rep.Retried != 0 {
		t.Fatalf("report %+v: a final row was read again", rep)
	}
}

// armed runs one statement with triggers off (replica role): a test seed of a guarded table, never a product path.
func armed(t *testing.T, e *env, sql string, args ...any) {
	t.Helper()
	ctx := context.Background()
	tx, err := e.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SET LOCAL session_replication_role = replica`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func seedGrant(t *testing.T, e *env, id, org uuid.UUID, path string) {
	t.Helper()
	armed(t, e, `INSERT INTO auth.resource_grants (id, principal_id, organization_id, role_key, resource_kind, resource_id)
		VALUES ($1, $2, $3, 'viewer', 'repository', $4)`, id, uuid.New(), org, path)
}

func seedOutbox(t *testing.T, e *env, sql string, args ...any) {
	t.Helper()
	armed(t, e, sql, args...)
}

func markOutboxPublished(t *testing.T, e *env) {
	t.Helper()
	armed(t, e, `UPDATE auth.auth_outbox_events SET published_at = now() - interval '30 days' WHERE published_at IS NULL`)
}

// The 0057 function's rows, read as the wire envelope, parse under repository-identity-event.v1: the producer writes
// the contract, microsecond instants and the 20-digit id bound included.
func TestTheAppendFunctionWritesTheContract(t *testing.T) {
	e := newEnv(t)
	org := e.org(t)
	e.observe(t, org, "github", "12345678901234567890", "acme/a.b-c_d", time.Date(2026, 11, 5, 8, 0, 0, 123456000, time.UTC))
	e.observe(t, org, "gitlab", "1", "g/sub/x", time.Date(2026, 11, 5, 9, 0, 0, 0, time.UTC))
	rows, err := e.admin.Query(context.Background(), `SELECT jsonb_build_object('schema_version', 'repository-identity-event.v1',
		'aggregate_type', aggregate_type, 'aggregate_id', aggregate_id, 'event_type', event_type, 'idempotency_key', idempotency_key,
		'created_at', to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'), 'payload', payload)::text
		FROM auth.auth_outbox_events WHERE aggregate_type = 'repository_identity' ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for rows.Next() {
		var doc string
		if err := rows.Scan(&doc); err != nil {
			t.Fatal(err)
		}
		if _, err := contracts.ParseRepositoryIdentityEvent(root, []byte(doc)); err != nil {
			t.Errorf("row %d does not parse: %v\n%s", n, err, doc)
		}
		n++
	}
	if n != 2 {
		t.Fatalf("%d rows, want 2", n)
	}
}
