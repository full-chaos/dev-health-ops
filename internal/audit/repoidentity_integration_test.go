//go:build integration

package audit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

type repoIdentityFixture struct {
	admin, worker, goAPI *pgxpool.Pool
	schema               authschema.ValidatedIdentifier
	options              authschema.Options
}

func newRepoIdentityFixture(t *testing.T, ctx context.Context) *repoIdentityFixture {
	t.Helper()
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
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(admin.Close)
	names := map[string]string{}
	for _, base := range []string{"rid_runtime", "rid_worker", "rid_goapi", "rid_push_writer", "rid_writer"} {
		name, err := containers.RoleName(base, instance)
		if err != nil {
			t.Fatal(err)
		}
		names[base] = name
	}
	const password = "repo-identity-test-not-a-real-credential"
	for _, statement := range []string{
		fmt.Sprintf("CREATE ROLE %q", names["rid_runtime"]),
		fmt.Sprintf("CREATE ROLE %q LOGIN PASSWORD '%s'", names["rid_worker"], password),
		fmt.Sprintf("CREATE ROLE %q LOGIN PASSWORD '%s'", names["rid_goapi"], password),
		fmt.Sprintf("CREATE ROLE %q NOLOGIN", names["rid_push_writer"]),
		fmt.Sprintf("CREATE ROLE %q NOLOGIN", names["rid_writer"]),
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	t.Cleanup(func() {
		for _, base := range []string{"rid_writer", "rid_push_writer", "rid_goapi", "rid_worker", "rid_runtime"} {
			containers.DropRole(admin, names[base], t.Logf)
		}
	})
	options := authschema.Options{
		Schema: "auth", RuntimeRole: names["rid_runtime"], SessionReaderRoles: []string{names["rid_goapi"]},
		PushLifecycleWriterRole: names["rid_push_writer"], PushLifecycleCallerRoles: []string{names["rid_goapi"], names["rid_worker"]},
		RepoIdentityWriterRole: names["rid_writer"], RepoIdentityCallerRoles: []string{names["rid_worker"]},
	}
	if _, err := authschema.Apply(ctx, admin, options); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	login := func(role string) *pgxpool.Pool {
		parsed, err := url.Parse(instance.URI)
		if err != nil {
			t.Fatal(err)
		}
		parsed.User = url.UserPassword(role, password)
		pool, err := pgxpool.New(ctx, parsed.String())
		if err != nil {
			t.Fatalf("login pool: %v", err)
		}
		t.Cleanup(pool.Close)
		return pool
	}
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	return &repoIdentityFixture{admin: admin, worker: login(names["rid_worker"]), goAPI: login(names["rid_goapi"]), schema: schema, options: options}
}

type repoIdentityRow struct {
	eventType, key string
	payload        map[string]any
}

func repoIdentityRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []repoIdentityRow {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT event_type, idempotency_key, payload FROM auth.auth_outbox_events
		WHERE aggregate_type = 'repository_identity' ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []repoIdentityRow
	for rows.Next() {
		var r repoIdentityRow
		var raw []byte
		if err := rows.Scan(&r.eventType, &r.key, &raw); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, &r.payload); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestRepositoryIdentityAppendIsTheWorkersOnlyDoor: the worker login appends events through the 0056 function only,
// the function fixes the revision, key and payload, refuses every malformed member, and nobody else can call it.
func TestRepositoryIdentityAppendIsTheWorkersOnlyDoor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newRepoIdentityFixture(t, ctx)
	org := uuid.MustParse("00000000-0000-4000-8000-0000000000a1")
	observed := time.Date(2026, 11, 5, 7, 30, 0, 123456000, time.UTC)
	event := RepositoryIdentityEvent{OrganizationID: org, Provider: "github", ProviderRepoID: "4242", Path: "acme/api", ObservedAt: observed}

	appendAs := func(pool *pgxpool.Pool, events ...RepositoryIdentityEvent) error {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if err := AppendRepositoryIdentity(ctx, tx, env.schema, events); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}

	t.Run("an add then a rename are two revisions of one aggregate with the full state", func(t *testing.T) {
		if err := appendAs(env.worker, event); err != nil {
			t.Fatalf("append: %v", err)
		}
		renamed := event
		renamed.Path, renamed.ObservedAt = "acme/api-v2", observed.Add(time.Hour)
		if err := appendAs(env.worker, renamed); err != nil {
			t.Fatalf("append rename: %v", err)
		}
		rows := repoIdentityRows(t, ctx, env.admin)
		if len(rows) != 2 {
			t.Fatalf("outbox rows = %d, want 2", len(rows))
		}
		aggregate := org.String() + "/github/4242"
		for i, want := range []struct {
			path, observedAt string
		}{{"acme/api", "2026-11-05T07:30:00.123456Z"}, {"acme/api-v2", "2026-11-05T08:30:00.123456Z"}} {
			r := rows[i]
			if r.eventType != "repository_identity.observed" || r.key != fmt.Sprintf("repository_identity:%s:%d", aggregate, i+1) {
				t.Errorf("row %d: event %q key %q", i, r.eventType, r.key)
			}
			change, _ := r.payload["change"].(map[string]any)
			if r.payload["revision"] != float64(i+1) || r.payload["organization_id"] != org.String() || len(r.payload) != 3 ||
				change["provider"] != "github" || change["provider_repo_id"] != "4242" || change["path"] != want.path ||
				change["observed_at"] != want.observedAt || len(change) != 4 {
				t.Errorf("row %d payload = %v", i, r.payload)
			}
		}
	})

	t.Run("every malformed member is refused before anything is written", func(t *testing.T) {
		before := len(repoIdentityRows(t, ctx, env.admin))
		for name, mutate := range map[string]func(*RepositoryIdentityEvent){
			"nil org":         func(e *RepositoryIdentityEvent) { e.OrganizationID = uuid.Nil },
			"upper provider":  func(e *RepositoryIdentityEvent) { e.Provider = "GitHub" },
			"long provider":   func(e *RepositoryIdentityEvent) { e.Provider = strings.Repeat("a", 17) },
			"non-numeric id":  func(e *RepositoryIdentityEvent) { e.ProviderRepoID = "12a" },
			"empty id":        func(e *RepositoryIdentityEvent) { e.ProviderRepoID = "" },
			"long id":         func(e *RepositoryIdentityEvent) { e.ProviderRepoID = strings.Repeat("1", 21) },
			"upper path":      func(e *RepositoryIdentityEvent) { e.Path = "Acme/api" },
			"path with space": func(e *RepositoryIdentityEvent) { e.Path = "acme/a pi" },
			"long path":       func(e *RepositoryIdentityEvent) { e.Path = strings.Repeat("a", 256) },
			"zero observed":   func(e *RepositoryIdentityEvent) { e.ObservedAt = time.Time{} },
		} {
			bad := event
			mutate(&bad)
			if err := appendAs(env.worker, bad); !errors.Is(err, ErrRepositoryIdentity) {
				t.Errorf("%s: err = %v, want ErrRepositoryIdentity", name, err)
			}
		}
		if after := len(repoIdentityRows(t, ctx, env.admin)); after != before {
			t.Fatalf("outbox rows %d -> %d after refusals", before, after)
		}
	})

	t.Run("the function itself refuses what Go would refuse", func(t *testing.T) {
		for name, args := range map[string][]any{
			"upper provider": {org, "GitHub", "1", "a/b", observed},
			"bad id":         {org, "github", "-1", "a/b", observed},
			"bad path":       {org, "github", "1", "/a", observed},
			"infinity":       {org, "github", "1", "a/b", "infinity"},
			"null org":       {nil, "github", "1", "a/b", observed},
		} {
			tx, err := env.worker.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			_, err = tx.Exec(ctx, `SELECT auth.append_repository_identity_event($1, $2, $3, $4, $5)`, args...)
			_ = tx.Rollback(ctx)
			if pgCode(err) != "22023" {
				t.Errorf("%s: err = %v, want 22023", name, err)
			}
		}
	})

	t.Run("the worker has no table privilege, repository_identities included", func(t *testing.T) {
		for _, sql := range []string{
			`INSERT INTO auth.auth_outbox_events (aggregate_type, aggregate_id, event_type, payload, idempotency_key) VALUES ('repository_identity', 'x', 'repository_identity.observed', '{}', 'k')`,
			`UPDATE auth.aggregate_revisions SET revision = revision + 1`,
			`SELECT count(*) FROM auth.repository_identities`,
			`INSERT INTO auth.repository_identities (organization_id, provider, provider_repo_id, current_path) VALUES ('` + org.String() + `', 'github', '1', 'a/b')`,
		} {
			_, err := env.worker.Exec(ctx, sql)
			if pgCode(err) != "42501" {
				t.Errorf("%s: err = %v, want 42501", sql[:48], err)
			}
		}
	})

	t.Run("go-api's login, a push lifecycle caller, cannot execute the function", func(t *testing.T) {
		if err := appendAs(env.goAPI, event); pgCode(err) != "42501" {
			t.Fatalf("go-api append: err = %v, want permission denied (42501)", err)
		}
	})
}
