//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

type identityOutboxFixture struct {
	admin    *pgxpool.Pool
	repo     *PostgresRepository
	now      time.Time
	unitSeed int
}

// newIdentityOutboxFixture migrates the public sync schema and the auth schema in one PostgreSQL, and opens the
// provider sync repository on a worker login that holds the public table privileges and, on the auth side, EXECUTE on
// the repository identity append function only (the repo identity caller of the auth schema).
func newIdentityOutboxFixture(t *testing.T, ctx context.Context) *identityOutboxFixture {
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
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	pgschema.Apply(ctx, t, admin)
	names := map[string]string{}
	for _, base := range []string{"rio_runtime", "rio_worker", "rio_goapi", "rio_push_writer", "rio_writer"} {
		name, err := containers.RoleName(base, instance)
		if err != nil {
			t.Fatal(err)
		}
		names[base] = name
	}
	const password = "repo-identity-outbox-test-not-a-real-credential"
	for _, statement := range []string{
		fmt.Sprintf("CREATE ROLE %q", names["rio_runtime"]),
		fmt.Sprintf("CREATE ROLE %q LOGIN PASSWORD '%s'", names["rio_worker"], password),
		fmt.Sprintf("CREATE ROLE %q LOGIN PASSWORD '%s'", names["rio_goapi"], password),
		fmt.Sprintf("CREATE ROLE %q NOLOGIN", names["rio_push_writer"]),
		fmt.Sprintf("CREATE ROLE %q NOLOGIN", names["rio_writer"]),
		fmt.Sprintf("GRANT USAGE ON SCHEMA public TO %q", names["rio_worker"]),
		fmt.Sprintf("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO %q", names["rio_worker"]),
		fmt.Sprintf("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO %q", names["rio_worker"]),
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	t.Cleanup(func() {
		for _, base := range []string{"rio_writer", "rio_push_writer", "rio_goapi", "rio_worker", "rio_runtime"} {
			containers.DropRole(admin, names[base], t.Logf)
		}
	})
	if _, err := authschema.Apply(ctx, admin, authschema.Options{
		Schema: "auth", RuntimeRole: names["rio_runtime"], SessionReaderRoles: []string{names["rio_goapi"]},
		PushLifecycleWriterRole: names["rio_push_writer"], PushLifecycleCallerRoles: []string{names["rio_goapi"], names["rio_worker"]},
		RepoIdentityWriterRole: names["rio_writer"], RepoIdentityCallerRoles: []string{names["rio_worker"]},
	}); err != nil {
		t.Fatalf("auth schema: %v", err)
	}
	parsed, err := url.Parse(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword(names["rio_worker"], password)
	worker, err := pgxpool.New(ctx, parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(worker.Close)
	repo, err := NewPostgresRepository(worker)
	if err != nil {
		t.Fatal(err)
	}
	repo.RepositoryIdentities = &RepositoryIdentityOutbox{Schema: identityTestSchema(t, "auth")}
	f := &identityOutboxFixture{admin: admin, repo: repo, now: time.Date(2026, 11, 5, 7, 30, 0, 0, time.UTC)}
	f.seedRun(t, ctx)
	return f
}

func identityTestSchema(t *testing.T, name string) authschema.ValidatedIdentifier {
	t.Helper()
	schema, err := authschema.NewValidatedIdentifier(name)
	if err != nil {
		t.Fatal(err)
	}
	return schema
}

func (f *identityOutboxFixture) seedRun(t *testing.T, ctx context.Context) {
	t.Helper()
	for _, statement := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO public.integration_credentials (id, provider, name, created_at, updated_at)
		  VALUES ($1, 'github', 'default', NOW(), NOW())`, []any{firstCredentialID}},
		{`INSERT INTO public.integrations (id, org_id, provider, credential_id, name, config, is_active, created_at, updated_at)
		  VALUES ($1, $2, 'github', $3, 'acme', '{"api_url":"https://api.github.com"}', TRUE, NOW(), NOW())`,
			[]any{firstIntegrationID, repositoryIdentityTestOrg, firstCredentialID}},
		{`INSERT INTO public.integration_sources
		  (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata,
		   is_enabled, discovered_at, last_seen_at)
		  VALUES ($1, $2, $3, 'github', 'repository', 'acme/api', 'acme/api', 'acme/api',
		   '{"default_branch":"main"}', TRUE, NOW(), NOW())`,
			[]any{firstSourceID, repositoryIdentityTestOrg, firstIntegrationID}},
		{`INSERT INTO public.sync_runs
		  (id, org_id, integration_id, triggered_by, mode, status, credential_id, credential_fingerprint, auth_source,
		   total_units, completed_units, failed_units, created_at)
		  VALUES ($1, $2, $3, 'test', 'full', 'running', $4, 'safe-fingerprint', 'integration_credential', 0, 0, 0, NOW())`,
			[]any{firstRunID, repositoryIdentityTestOrg, firstIntegrationID, firstCredentialID}},
	} {
		if _, err := f.admin.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
}

// claim seeds one github repo-metadata unit of the run and claims it on the worker login.
func (f *identityOutboxFixture) claim(t *testing.T, ctx context.Context) Claim {
	t.Helper()
	f.unitSeed++
	unitID := fmt.Sprintf("77777777-7777-4777-8777-%012d", f.unitSeed)
	if _, err := f.admin.Exec(ctx, `INSERT INTO public.sync_run_units (
			id, org_id, sync_run_id, integration_id, source_id, provider,
			dataset_key, cost_class, mode, status, processor_flags, updated_at, created_at, attempts
		  ) VALUES ($1, $2, $3, $4, $5, 'github', 'repo-metadata', 'light', 'full', 'dispatching', '{}', NOW(), NOW(), 0)`,
		unitID, repositoryIdentityTestOrg, firstRunID, firstIntegrationID, firstSourceID); err != nil {
		t.Fatal(err)
	}
	claim, err := f.repo.Claim(ctx, ClaimRequest{
		UnitID: unitID, OrgID: repositoryIdentityTestOrg, Owner: uuid.NewString(), Now: f.now,
		LeaseDuration: time.Hour, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return claim
}

type identityOutboxRow struct {
	revision int64
	payload  map[string]any
}

func (f *identityOutboxFixture) events(t *testing.T, ctx context.Context, provider, id string) []identityOutboxRow {
	t.Helper()
	rows, err := f.admin.Query(ctx, `SELECT (payload->>'revision')::bigint, payload FROM auth.auth_outbox_events
		WHERE aggregate_type = 'repository_identity' AND aggregate_id = $1 ORDER BY id`,
		repositoryIdentityTestOrg+"/"+provider+"/"+id)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []identityOutboxRow
	for rows.Next() {
		var r identityOutboxRow
		var raw []byte
		if err := rows.Scan(&r.revision, &raw); err != nil {
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

func (f *identityOutboxFixture) allEvents(t *testing.T, ctx context.Context) int {
	t.Helper()
	var n int
	if err := f.admin.QueryRow(ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE aggregate_type = 'repository_identity'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func (f *identityOutboxFixture) status(t *testing.T, ctx context.Context, claim Claim) GenerationBlockStatus {
	t.Helper()
	state, err := f.repo.LoadEffects(ctx, claim, f.now)
	if err != nil || len(state.Effects) != 1 {
		t.Fatalf("load ledger: %+v %v", state, err)
	}
	return state.Effects[0].Status
}

func assertIdentityEvent(t *testing.T, row identityOutboxRow, revision int64, provider, id, path, observedAt string) {
	t.Helper()
	change, _ := row.payload["change"].(map[string]any)
	if row.revision != revision || row.payload["revision"] != float64(revision) ||
		row.payload["organization_id"] != repositoryIdentityTestOrg || change["provider"] != provider ||
		change["provider_repo_id"] != id || change["path"] != path || change["observed_at"] != observedAt {
		t.Fatalf("event revision=%d payload=%v; want revision %d %s/%s %q at %s", row.revision, row.payload, revision, provider, id, path, observedAt)
	}
}

// TestRepositoryIdentityEventsCommitWithTheReposEffect: the producer cells against a real ledger and the real auth
// append function, on a worker login. The event and the ledger flip to Committed are one transaction: both land, or
// neither does.
func TestRepositoryIdentityEventsCommitWithTheReposEffect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	f := newIdentityOutboxFixture(t, ctx)
	committer := func(repo *PostgresRepository, sink *scriptedSink, inspection EffectInspection) EffectCommitter {
		return EffectCommitter{Ledger: repo, Sink: sink, Readback: scriptedReadback{inspection}, Now: func() time.Time { return f.now }}
	}

	t.Run("github: the write flip appends one event", func(t *testing.T) {
		claim := f.claim(t, ctx)
		batch := repoMetadataBatch(t, "github", f.now)
		result, err := committer(f.repo, &scriptedSink{}, EffectExact).Commit(ctx, claim, []EffectBatch{batch}, f.now)
		if err != nil || result.Written != 1 || f.status(t, ctx, claim) != GenerationBlockCommitted {
			t.Fatalf("result=%+v err=%v", result, err)
		}
		rows := f.events(t, ctx, "github", "4567")
		if len(rows) != 1 {
			t.Fatalf("github events = %d, want 1", len(rows))
		}
		assertIdentityEvent(t, rows[0], 1, "github", "4567", "acme/api", "2026-11-05T07:30:00.000000Z")
		if err := f.repo.CommitEffectRows(ctx, claim, 0, batch.ContentDigest, batch, f.now); err != nil {
			t.Fatalf("repeat commit: %v", err)
		}
		if n := len(f.events(t, ctx, "github", "4567")); n != 1 {
			t.Fatalf("a repeat commit of a committed effect appended: events = %d", n)
		}
	})

	t.Run("gitlab: an add then a rename are two revisions", func(t *testing.T) {
		first := repoMetadataBatch(t, "gitlab", f.now)
		if _, err := committer(f.repo, &scriptedSink{}, EffectExact).Commit(ctx, f.claim(t, ctx), []EffectBatch{first}, f.now); err != nil {
			t.Fatal(err)
		}
		row := identityRow("gitlab", "123", "Acme/API-v2")
		row["last_synced"] = "2026-11-05T08:30:00Z"
		renamed := reposBatch(t, "repos", row)
		if _, err := committer(f.repo, &scriptedSink{}, EffectExact).Commit(ctx, f.claim(t, ctx), []EffectBatch{renamed}, f.now); err != nil {
			t.Fatal(err)
		}
		rows := f.events(t, ctx, "gitlab", "123")
		if len(rows) != 2 {
			t.Fatalf("gitlab events = %d, want 2", len(rows))
		}
		assertIdentityEvent(t, rows[0], 1, "gitlab", "123", "acme/api", "2026-11-05T07:30:00.000000Z")
		assertIdentityEvent(t, rows[1], 2, "gitlab", "123", "acme/api-v2", "2026-11-05T08:30:00.000000Z")
	})

	t.Run("a row without a provider id commits with no event", func(t *testing.T) {
		before := f.allEvents(t, ctx)
		claim := f.claim(t, ctx)
		batch := reposBatch(t, "repos", identityRow("github", "", "acme/local-only"))
		if _, err := committer(f.repo, &scriptedSink{}, EffectExact).Commit(ctx, claim, []EffectBatch{batch}, f.now); err != nil {
			t.Fatal(err)
		}
		if f.status(t, ctx, claim) != GenerationBlockCommitted || f.allEvents(t, ctx) != before {
			t.Fatalf("status=%s events %d -> %d", f.status(t, ctx, claim), before, f.allEvents(t, ctx))
		}
	})

	t.Run("recovery: readback exact appends one event; a repeat appends none", func(t *testing.T) {
		claim := f.claim(t, ctx)
		batch := reposBatch(t, "repos", identityRow("github", "9001", "acme/recovered"))
		sink := &scriptedSink{fail: true}
		if _, err := committer(f.repo, sink, EffectExact).Commit(ctx, claim, []EffectBatch{batch}, f.now); err == nil {
			t.Fatal("want the sink failure")
		}
		if f.status(t, ctx, claim) != GenerationBlockWriting || len(f.events(t, ctx, "github", "9001")) != 0 {
			t.Fatal("a failed write appended or flipped")
		}
		result, err := committer(f.repo, &scriptedSink{}, EffectExact).Commit(ctx, claim, []EffectBatch{batch}, f.now)
		if err != nil || result.MarkedCommitted != 1 || f.status(t, ctx, claim) != GenerationBlockCommitted {
			t.Fatalf("recovery result=%+v err=%v", result, err)
		}
		if err := f.repo.MarkEffectCommittedRows(ctx, claim, 0, batch.ContentDigest, batch, f.now); err != nil {
			t.Fatalf("repeat mark: %v", err)
		}
		rows := f.events(t, ctx, "github", "9001")
		if len(rows) != 1 {
			t.Fatalf("recovery events = %d, want 1", len(rows))
		}
		assertIdentityEvent(t, rows[0], 1, "github", "9001", "acme/recovered", "2026-11-05T07:30:00.000000Z")
	})

	t.Run("the ledger write fails after the append: no event, ledger still writing", func(t *testing.T) {
		claim := f.claim(t, ctx)
		batch := reposBatch(t, "repos", identityRow("github", "9002", "acme/rolled-back"))
		if _, err := committer(f.repo, &scriptedSink{fail: true}, EffectExact).Commit(ctx, claim, []EffectBatch{batch}, f.now); err == nil {
			t.Fatal("want the sink failure")
		}
		for _, statement := range []string{
			`CREATE FUNCTION public.refuse_identity_commit() RETURNS trigger LANGUAGE plpgsql AS $$
			 BEGIN
			   IF NEW.id = '` + claim.ID + `'::uuid AND
			      NEW.result::jsonb #>> '{go_effect_ledger_v1,effects,0,status}' = 'committed' THEN
			     RAISE EXCEPTION 'refused by test';
			   END IF;
			   RETURN NEW;
			 END $$`,
			`CREATE TRIGGER refuse_identity_commit BEFORE UPDATE ON public.sync_run_units
			 FOR EACH ROW EXECUTE FUNCTION public.refuse_identity_commit()`,
		} {
			if _, err := f.admin.Exec(ctx, statement); err != nil {
				t.Fatal(err)
			}
		}
		defer func() {
			if _, err := f.admin.Exec(ctx, `DROP TRIGGER refuse_identity_commit ON public.sync_run_units; DROP FUNCTION public.refuse_identity_commit()`); err != nil {
				t.Error(err)
			}
		}()
		for name, commit := range map[string]func() error{
			"write flip": func() error { return f.repo.CommitEffectRows(ctx, claim, 0, batch.ContentDigest, batch, f.now) },
			"recovery flip": func() error {
				return f.repo.MarkEffectCommittedRows(ctx, claim, 0, batch.ContentDigest, batch, f.now)
			},
		} {
			if err := commit(); err == nil {
				t.Fatalf("%s: the refused ledger write succeeded", name)
			}
			if f.status(t, ctx, claim) != GenerationBlockWriting || len(f.events(t, ctx, "github", "9002")) != 0 {
				t.Fatalf("%s: the event outlived its rolled-back ledger write", name)
			}
		}
	})

	t.Run("flag on, append function missing: the flip fails closed, then recovers", func(t *testing.T) {
		claim := f.claim(t, ctx)
		batch := reposBatch(t, "repos", identityRow("github", "9003", "acme/fenced"))
		broken := *f.repo
		broken.RepositoryIdentities = &RepositoryIdentityOutbox{Schema: identityTestSchema(t, "auth_absent")}
		if _, err := committer(&broken, &scriptedSink{}, EffectExact).Commit(ctx, claim, []EffectBatch{batch}, f.now); err == nil {
			t.Fatal("a commit without the append function succeeded")
		}
		if f.status(t, ctx, claim) != GenerationBlockWriting || len(f.events(t, ctx, "github", "9003")) != 0 {
			t.Fatal("the failed flip committed the ledger or an event")
		}
		result, err := committer(f.repo, &scriptedSink{}, EffectExact).Commit(ctx, claim, []EffectBatch{batch}, f.now)
		if err != nil || result.MarkedCommitted != 1 || len(f.events(t, ctx, "github", "9003")) != 1 {
			t.Fatalf("recovery after the fix: result=%+v err=%v", result, err)
		}
	})

	t.Run("flag off: no event", func(t *testing.T) {
		before := f.allEvents(t, ctx)
		off := *f.repo
		off.RepositoryIdentities = nil
		claim := f.claim(t, ctx)
		batch := reposBatch(t, "repos", identityRow("github", "9004", "acme/dark"))
		if _, err := committer(&off, &scriptedSink{}, EffectExact).Commit(ctx, claim, []EffectBatch{batch}, f.now); err != nil {
			t.Fatal(err)
		}
		if f.status(t, ctx, claim) != GenerationBlockCommitted || f.allEvents(t, ctx) != before {
			t.Fatalf("flag off appended: %d -> %d", before, f.allEvents(t, ctx))
		}
	})
}
