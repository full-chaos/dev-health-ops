//go:build integration

package projectionreader

import (
	"context"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

const testPassword = "projectionreader-test-only-not-a-credential"

type env struct {
	admin  *pgxpool.Pool
	reader *pgxpool.Pool
}

var (
	envsMu sync.Mutex
	envs   = map[*testing.T]*env{}
)

// newEnv starts a PostgreSQL, applies the schema and the auth schema with one projection reader role
// (the worker-shaped login: column SELECT on ProjectionReaderReads, nothing else), and opens a pool as it.
func newEnv(t *testing.T) *env {
	t.Helper()
	envsMu.Lock()
	defer envsMu.Unlock()
	if e := envs[t]; e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = instance.Close(c)
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	pgschema.Apply(ctx, t, admin)
	runtime, err := containers.RoleName("pr_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := containers.RoleName("pr_worker", instance)
	if err != nil {
		t.Fatal(err)
	}
	db, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		fmt.Sprintf(`CREATE ROLE %q`, runtime),
		fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, worker, testPassword),
		fmt.Sprintf(`GRANT CONNECT ON DATABASE %q TO %q`, db, worker),
	} {
		if _, err := admin.Exec(ctx, stmt); err != nil {
			t.Fatalf("setup %q: %v", stmt, err)
		}
	}
	t.Cleanup(func() { containers.DropRole(admin, worker, func(string, ...any) {}) })
	t.Cleanup(func() { containers.DropRole(admin, runtime, func(string, ...any) {}) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: runtime, ProjectionReaderRoles: []string{worker}}); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword(worker, testPassword)
	reader, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reader.Close)
	e := &env{admin: admin, reader: reader}
	envs[t] = e
	t.Cleanup(func() { envsMu.Lock(); delete(envs, t); envsMu.Unlock() })
	return e
}

type row struct {
	domain, ref string
	org         uuid.UUID
	source      *string
	status      string
	scopes      []string
	expires     *string
}

// seed writes projection rows as the table owner with the user triggers off: the commit guard admits a writer only
// inside audit.Commit, and the reader's cells need rows, not the writer.
func (e *env) seed(t *testing.T, rows ...row) {
	t.Helper()
	ctx := context.Background()
	conn, err := e.admin.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SET session_replication_role = replica`); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = conn.Exec(ctx, `RESET session_replication_role`) }()
	for _, r := range rows {
		terminal := "NULL"
		if r.status != "active" {
			terminal = "now()"
		}
		binding := "organization"
		if r.source != nil {
			binding = "source"
		}
		scopes := r.scopes
		if scopes == nil {
			scopes = []string{}
		}
		if _, err := conn.Exec(ctx, `INSERT INTO auth.credential_projection
			(domain_owner, external_reference, organization_id, binding, source_id, revision, max_seen_revision, status, fingerprint, scopes, expires_at, terminal_at, principal_id, registry_id)
			VALUES ($1, $2, $3, $4, $5, 1, 1, $6, '0123456789abcdef', $7, $8, `+terminal+`, gen_random_uuid(), gen_random_uuid())`,
			r.domain, r.ref, r.org, binding, r.source, r.status, scopes, r.expires); err != nil {
			t.Fatalf("seed %s: %v", r.ref, err)
		}
	}
}

func (e *env) newReader(t *testing.T, size int) *Reader {
	t.Helper()
	id, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewReader(e.reader, id, size)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func refs(rows []row) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.ref
	}
	return out
}

// C20 (integration): as the worker-shaped reader role the projection reads and cannot be written.
func TestC20iReaderRoleCannotWriteTheProjection(t *testing.T) {
	e := newEnv(t)
	org := uuid.New()
	e.seed(t, row{domain: "external_push", ref: "w-1", org: org, status: "active"})
	ctx := context.Background()
	for _, stmt := range []string{
		`INSERT INTO auth.credential_projection (domain_owner) VALUES ('external_push')`,
		`UPDATE auth.credential_projection SET status = 'revoked'`,
		`DELETE FROM auth.credential_projection`,
		`TRUNCATE auth.credential_projection`,
		`SELECT fingerprint FROM auth.credential_projection`,
	} {
		_, err := e.reader.Exec(ctx, stmt)
		if err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Errorf("%s as the reader: err = %v, want permission denied", stmt, err)
		}
	}
	got, err := e.newReader(t, 10).ReadPage(ctx, "")
	if err != nil || len(got) == 0 {
		t.Fatalf("read = %v, %v", got, err)
	}
}

// C21 (integration): the keyset walk covers every external_push row once, in id order, at batch 1, 2 and N;
// acr rows are never read; the byte order of the key is the order of the ids.
func TestC21iKeysetPagingCoversAllRowsAtEveryBatch(t *testing.T) {
	e := newEnv(t)
	org := uuid.New()
	var want []string
	for i := 0; i < 7; i++ {
		id := uuid.NewString()
		want = append(want, id)
		e.seed(t, row{domain: "external_push", ref: id, org: org, status: "active", scopes: []string{"ingest:write"}})
	}
	e.seed(t, row{domain: "acr", ref: "acr-row-1", org: org, status: "active"})
	slices.Sort(want)
	ctx := context.Background()
	for _, batch := range []int{1, 2, 3, 100} {
		r := e.newReader(t, batch)
		var got []string
		after := ""
		for {
			page, err := r.ReadPage(ctx, after)
			if err != nil {
				t.Fatal(err)
			}
			for _, p := range page {
				got = append(got, p.ID)
			}
			if len(page) < batch {
				break
			}
			after = page[len(page)-1].ID
		}
		if !slices.Equal(got, want) {
			t.Fatalf("batch %d: got %v, want %v", batch, got, want)
		}
	}
}

func TestC21jReaderMapsScopesSourceAndExpiry(t *testing.T) {
	e := newEnv(t)
	org := uuid.New()
	src := uuid.NewString()
	exp := "2027-01-02T03:04:05.123456Z"
	a, b := uuid.NewString(), uuid.NewString()
	e.seed(t,
		row{domain: "external_push", ref: a, org: org, source: &src, status: "active", scopes: []string{"schema:read", "ingest:write"}, expires: &exp},
		row{domain: "external_push", ref: b, org: org, status: "revoked", scopes: []string{"ingest:status"}},
	)
	got, err := e.newReader(t, 10).ReadByIDs(context.Background(), []string{a, b, uuid.NewString()})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("rows = %+v", got)
	}
	byID := map[string]int{got[0].ID: 0, got[1].ID: 1}
	ra, rb := got[byID[a]], got[byID[b]]
	want, _ := time.Parse(time.RFC3339Nano, exp)
	if ra.Organization != org.String() || ra.SourceID != src || ra.Status != "active" || ra.ExpiresAt == nil || !ra.ExpiresAt.Equal(want) ||
		!slices.Equal(ra.Scopes, []string{"schema:read", "ingest:write"}) {
		t.Fatalf("row a = %+v", ra)
	}
	if rb.SourceID != "" || rb.Status != "revoked" || rb.ExpiresAt != nil {
		t.Fatalf("row b = %+v", rb)
	}
}
