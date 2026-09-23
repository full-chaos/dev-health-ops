//go:build integration

package providerfoundation

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// integrationCredentialsDDL is alembic 0001_initial_schema.py's own
// integration_credentials table, transcribed column for column (id,
// org_id, provider, name, is_active, credentials_encrypted, config, plus
// the three last_test_*/created_at/updated_at columns ResolveEncrypted
// never reads but the real table always carries) -- NOT the stub (id uuid
// PK only) internal/testsupport/providersyncschema/schema.go deliberately
// keeps out of its own scope. A hand-typed schema that silently drifts
// from the real one is exactly the false-pass class AGENTS.md warns about;
// this table is what ResolveEncrypted's own SELECT actually runs against.
const integrationCredentialsDDL = `
CREATE TABLE public.integration_credentials (
    id uuid PRIMARY KEY,
    org_id text NOT NULL DEFAULT 'default',
    provider text NOT NULL,
    name text NOT NULL,
    is_active boolean NOT NULL DEFAULT true,
    credentials_encrypted text,
    config json,
    last_test_at timestamptz,
    last_test_success boolean,
    last_test_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (org_id, provider, name)
)`

func startCredentialsPool(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if closeErr := instance.Close(closeCtx); closeErr != nil {
			t.Errorf("terminate PostgreSQL test dependency: %v", closeErr)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, integrationCredentialsDDL); err != nil {
		t.Fatal(err)
	}
	return pool
}

func insertCredential(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id, orgID, provider, name string, active bool, ciphertext string) {
	t.Helper()
	if _, err := pool.Exec(ctx,
		`INSERT INTO public.integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config)
		 VALUES ($1::uuid, $2, $3, $4, $5, $6, '{}')`,
		id, orgID, provider, name, active, ciphertext,
	); err != nil {
		t.Fatal(err)
	}
}

// insertCredentialNullCiphertext inserts an active row with a SQL NULL
// credentials_encrypted -- a shape insertCredential's non-nullable string
// param cannot express (Go can bind "" for an empty string, never NULL,
// through that signature).
func insertCredentialNullCiphertext(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id, orgID, provider, name string) {
	t.Helper()
	if _, err := pool.Exec(ctx,
		`INSERT INTO public.integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config)
		 VALUES ($1::uuid, $2, $3, $4, true, NULL, '{}')`,
		id, orgID, provider, name,
	); err != nil {
		t.Fatal(err)
	}
}

// TestResolveEncryptedAmbiguousMatchNamesEveryCandidate is CHAOS-6351's own
// reproduction: three active credentials for one provider, no "default"
// among them, no credential_id/credential_name given. Before this fix, the
// repository's own query carried a LIMIT 2, so this exact scenario could
// only ever see two of the three candidates -- proven here by seeding a
// THIRD candidate whose name sorts last and asserting it still appears in
// the error.
func TestResolveEncryptedAmbiguousMatchNamesEveryCandidate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool := startCredentialsPool(t, ctx)

	insertCredential(t, ctx, pool, "00000000-0000-0000-0000-000000000001", "org-1", "github", "alpha", true, "v1:a")
	insertCredential(t, ctx, pool, "00000000-0000-0000-0000-000000000002", "org-1", "github", "bravo", true, "v1:b")
	insertCredential(t, ctx, pool, "00000000-0000-0000-0000-000000000003", "org-1", "github", "zulu", true, "v1:c")

	repo := PostgresCredentialRepository{Pool: pool}
	_, err := repo.ResolveEncrypted(ctx, TenantScope{OrgID: "org-1", Provider: "github", IntegrationID: "admin-discover"})
	if err == nil {
		t.Fatal("expected an ambiguous-match error, got nil")
	}
	var ambiguous *CredentialAmbiguousError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("expected *CredentialAmbiguousError, got %T: %v", err, err)
	}
	if !errors.Is(err, ErrCredentialInvalid) {
		t.Error("CredentialAmbiguousError must still satisfy errors.Is(err, ErrCredentialInvalid) for existing callers")
	}
	want := []string{"alpha", "bravo", "zulu"}
	if len(ambiguous.Names) != len(want) {
		t.Fatalf("Names = %v, want %v", ambiguous.Names, want)
	}
	for index, name := range want {
		if ambiguous.Names[index] != name {
			t.Fatalf("Names = %v, want %v", ambiguous.Names, want)
		}
	}
	wantMsg := `Multiple active credentials exist for provider 'github' (alpha, bravo, zulu); specify credential_name or credential_id`
	if ambiguous.Error() != wantMsg {
		t.Errorf("Error() = %q, want %q", ambiguous.Error(), wantMsg)
	}
}

// TestResolveEncryptedDefaultWinsAmongManyCandidates proves a "default"
// named credential is still picked deterministically even among 3+ active
// candidates (the ORDER BY CASE WHEN name='default' THEN 0 clause), not
// just the 2-candidate case the removed LIMIT 2 could still cover.
func TestResolveEncryptedDefaultWinsAmongManyCandidates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool := startCredentialsPool(t, ctx)

	insertCredential(t, ctx, pool, "00000000-0000-0000-0000-000000000011", "org-1", "gitlab", "zulu", true, "v1:a")
	insertCredential(t, ctx, pool, "00000000-0000-0000-0000-000000000012", "org-1", "gitlab", "default", true, "v1:b")
	insertCredential(t, ctx, pool, "00000000-0000-0000-0000-000000000013", "org-1", "gitlab", "alpha", true, "v1:c")

	repo := PostgresCredentialRepository{Pool: pool}
	got, err := repo.ResolveEncrypted(ctx, TenantScope{OrgID: "org-1", Provider: "gitlab", IntegrationID: "admin-discover"})
	if err != nil {
		t.Fatalf("ResolveEncrypted: %v", err)
	}
	if got.Name != "default" {
		t.Errorf("Name = %q, want %q", got.Name, "default")
	}
}

// TestResolveEncryptedSingleActiveNoDefaultResolves proves the
// exactly-one-active-candidate path still resolves without a "default"
// name and without an explicit id/name -- unaffected by the LIMIT 2
// removal, since it was already correct, but was never directly tested.
func TestResolveEncryptedSingleActiveNoDefaultResolves(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool := startCredentialsPool(t, ctx)

	insertCredential(t, ctx, pool, "00000000-0000-0000-0000-000000000021", "org-1", "jira", "only", true, "v1:a")
	insertCredential(t, ctx, pool, "00000000-0000-0000-0000-000000000022", "org-1", "jira", "inactive-one", false, "v1:b")

	repo := PostgresCredentialRepository{Pool: pool}
	got, err := repo.ResolveEncrypted(ctx, TenantScope{OrgID: "org-1", Provider: "jira", IntegrationID: "admin-discover"})
	if err != nil {
		t.Fatalf("ResolveEncrypted: %v", err)
	}
	if got.Name != "only" {
		t.Errorf("Name = %q, want %q", got.Name, "only")
	}
}

// TestResolveEncryptedNotFoundStaysNotFound proves the zero-active-
// candidates path is still ErrCredentialNotFound, never mistaken for an
// ambiguous match by the len(matches)!=1 check after the LIMIT 2 removal
// (matches is empty, and the empty-result branch above returns
// ErrCredentialNotFound before the ambiguity check ever runs).
func TestResolveEncryptedNotFoundStaysNotFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool := startCredentialsPool(t, ctx)

	repo := PostgresCredentialRepository{Pool: pool}
	_, err := repo.ResolveEncrypted(ctx, TenantScope{OrgID: "org-1", Provider: "linear", IntegrationID: "admin-discover"})
	if !errors.Is(err, ErrCredentialNotFound) {
		t.Errorf("err = %v, want ErrCredentialNotFound", err)
	}
}

// TestResolveEncryptedActiveCredentialWithNullCiphertextStillCountsAsCandidate
// is CHAOS-6351's round-1 P1 reproduction: Python's list_by_provider
// (integration_credentials.py:358) selects every org+provider row with no
// filter on credentials_encrypted at all, so an active row with a NULL
// ciphertext still counts toward AmbiguousCredentialError's candidate set
// (integration_credentials.py:283,287). Before this fix, scanning a SQL
// NULL into a non-nullable Go string errored the whole query
// (ErrCredentialNotFound), so this exact two-row shape resolved to
// "not found" instead of ambiguous.
func TestResolveEncryptedActiveCredentialWithNullCiphertextStillCountsAsCandidate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool := startCredentialsPool(t, ctx)

	insertCredentialNullCiphertext(t, ctx, pool, "00000000-0000-0000-0000-000000000031", "org-1", "github", "alpha-null")
	insertCredential(t, ctx, pool, "00000000-0000-0000-0000-000000000032", "org-1", "github", "zulu-valid", true, "v1:a")

	repo := PostgresCredentialRepository{Pool: pool}
	_, err := repo.ResolveEncrypted(ctx, TenantScope{OrgID: "org-1", Provider: "github", IntegrationID: "admin-discover"})
	var ambiguous *CredentialAmbiguousError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("expected *CredentialAmbiguousError, got %T: %v", err, err)
	}
	want := []string{"alpha-null", "zulu-valid"}
	if len(ambiguous.Names) != len(want) || ambiguous.Names[0] != want[0] || ambiguous.Names[1] != want[1] {
		t.Fatalf("Names = %v, want %v", ambiguous.Names, want)
	}
}

// TestResolveEncryptedActiveCredentialWithEmptyCiphertextStillCountsAsCandidate
// is the empty-string sibling of the NULL case above: previously dropped
// from `matches` entirely by the `cipherText == ""` skip, so this exact
// two-row shape also resolved to a single silent winner instead of
// ambiguous.
func TestResolveEncryptedActiveCredentialWithEmptyCiphertextStillCountsAsCandidate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool := startCredentialsPool(t, ctx)

	insertCredential(t, ctx, pool, "00000000-0000-0000-0000-000000000041", "org-1", "github", "alpha-empty", true, "")
	insertCredential(t, ctx, pool, "00000000-0000-0000-0000-000000000042", "org-1", "github", "zulu-valid", true, "v1:a")

	repo := PostgresCredentialRepository{Pool: pool}
	_, err := repo.ResolveEncrypted(ctx, TenantScope{OrgID: "org-1", Provider: "github", IntegrationID: "admin-discover"})
	var ambiguous *CredentialAmbiguousError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("expected *CredentialAmbiguousError, got %T: %v", err, err)
	}
	want := []string{"alpha-empty", "zulu-valid"}
	if len(ambiguous.Names) != len(want) || ambiguous.Names[0] != want[0] || ambiguous.Names[1] != want[1] {
		t.Fatalf("Names = %v, want %v", ambiguous.Names, want)
	}
}
