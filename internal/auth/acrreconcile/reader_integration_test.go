//go:build integration

package acrreconcile

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/pushlifecycle"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

var (
	poolsMu sync.Mutex
	pools   = map[*testing.T]*pgxpool.Pool{}
)

// adminPool is the table owner's pool on this test's own PostgreSQL, started on first use and closed with the test.
func adminPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	poolsMu.Lock()
	defer poolsMu.Unlock()
	if pool := pools[t]; pool != nil {
		return pool
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
	role, err := containers.RoleName("acrreconcile_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD 'acrreconcile-test-only-not-a-credential'`, role)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { containers.DropRole(admin, role, func(string, ...any) {}) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role, UsersOwned: true}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		poolsMu.Lock()
		delete(pools, t)
		poolsMu.Unlock()
	})
	pools[t] = admin
	return admin
}

type row struct {
	domain, ref string
	org         uuid.UUID
	revision    int64
	maxSeen     int64
	status      string
	expires     *string
}

// seed writes projection rows as the table owner with the user triggers off: the commit guard admits a writer only
// inside audit.Commit, and the reader's cells need rows, not the writer.
func seed(t *testing.T, rows ...row) {
	t.Helper()
	ctx := context.Background()
	conn, err := adminPool(t).Acquire(ctx)
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
		max := r.maxSeen
		if max < r.revision {
			max = r.revision
		}
		if _, err := conn.Exec(ctx, `INSERT INTO auth.credential_projection
			(domain_owner, external_reference, organization_id, binding, revision, max_seen_revision, status, fingerprint, scopes, expires_at, terminal_at, principal_id, registry_id)
			VALUES ($1, $2, $3, 'organization', $4, $5, $6, '0123456789abcdef', ARRAY['x'], $7, `+terminal+`, gen_random_uuid(), gen_random_uuid())`,
			r.domain, r.ref, r.org, r.revision, max, r.status, r.expires); err != nil {
			t.Fatalf("seed %s: %v", r.ref, err)
		}
	}
}

func reader(t *testing.T, pageSize int) *Reader {
	t.Helper()
	id, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewReader(adminPool(t), id, pageSize)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func ids(rows []Projected) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.ID
	}
	return out
}

func TestReaderOrgFilter(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	seed(t,
		row{domain: "acr", ref: "of-a1", org: a, revision: 1, status: "active"},
		row{domain: "acr", ref: "of-a2", org: a, revision: 1, status: "active"},
		row{domain: "acr", ref: "of-b1", org: b, revision: 1, status: "active"},
	)
	got, err := reader(t, 10).ReadOrg(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(ids(got), ",") != "of-a1,of-a2" {
		t.Fatalf("org filter: got %v", ids(got))
	}
}

func TestReaderDomainOwnerFilter(t *testing.T) {
	a := uuid.New()
	seed(t,
		row{domain: "acr", ref: "do-acr", org: a, revision: 1, status: "active"},
		row{domain: "external_push", ref: "do-push", org: a, revision: 1, status: "active"},
	)
	got, err := reader(t, 10).ReadOrg(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(ids(got), ",") != "do-acr" {
		t.Fatalf("domain filter: got %v", ids(got))
	}
}

func TestReaderKeysetOrderAcrossPages(t *testing.T) {
	a := uuid.New()
	var want []string
	for i := 0; i < 7; i++ {
		ref := fmt.Sprintf("ks-%02d", 6-i)
		seed(t, row{domain: "acr", ref: ref, org: a, revision: 1, status: "active"})
	}
	for i := 0; i < 7; i++ {
		want = append(want, fmt.Sprintf("ks-%02d", i))
	}
	got, err := reader(t, 3).ReadOrg(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(ids(got), ",") != strings.Join(want, ",") {
		t.Fatalf("keyset walk: got %v, want %v", ids(got), want)
	}
	page, err := reader(t, 3).ReadPage(context.Background(), a, "ks-02")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(ids(page), ",") != "ks-03,ks-04,ks-05" {
		t.Fatalf("one page after ks-02: got %v", ids(page))
	}
}

func TestReaderManyOrgs(t *testing.T) {
	var orgs []uuid.UUID
	for i := 0; i < 25; i++ {
		o := uuid.New()
		orgs = append(orgs, o)
		for j := 0; j <= i%4; j++ {
			seed(t, row{domain: "acr", ref: fmt.Sprintf("mo-%d-%d", i, j), org: o, revision: 1, status: "active"})
		}
	}
	listed, err := reader(t, 2).Orgs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	have := map[uuid.UUID]bool{}
	for i, o := range listed {
		have[o] = true
		if i > 0 && listed[i-1].String() >= o.String() {
			t.Fatalf("Orgs not ascending at %d", i)
		}
	}
	for i, o := range orgs {
		if !have[o] {
			t.Fatalf("org %d missing from Orgs", i)
		}
		got, err := reader(t, 2).ReadOrg(context.Background(), o)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != i%4+1 {
			t.Fatalf("org %d: %d rows, want %d", i, len(got), i%4+1)
		}
	}
}

func TestOrgsListsOnlyOrgsWithAcrRows(t *testing.T) {
	pushOnly := uuid.New()
	seed(t, row{domain: "external_push", ref: "op-push", org: pushOnly, revision: 1, status: "active"})
	listed, err := reader(t, 2).Orgs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range listed {
		if o == pushOnly {
			t.Fatal("an organization with only external_push rows is listed")
		}
	}
}

func TestReaderMapsRevisionStatusAndExpiry(t *testing.T) {
	a := uuid.New()
	exp := "2026-11-01T10:00:00Z"
	seed(t,
		row{domain: "acr", ref: "mp-1", org: a, revision: 2, maxSeen: 5, status: "expired", expires: &exp},
		row{domain: "acr", ref: "mp-2", org: a, revision: 3, status: "active"},
	)
	got, err := reader(t, 10).ReadOrg(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d rows", len(got))
	}
	want, _ := time.Parse(time.RFC3339Nano, exp)
	if got[0].Revision != 2 || got[0].Status != pushlifecycle.StatusExpired || got[0].ExpiresAt == nil || !got[0].ExpiresAt.Equal(want) {
		t.Fatalf("row 1: %+v", got[0])
	}
	if got[1].Revision != 3 || got[1].Status != pushlifecycle.StatusActive || got[1].ExpiresAt != nil {
		t.Fatalf("row 2: %+v", got[1])
	}
}

func TestReaderRefusesAnUnparsableExpiry(t *testing.T) {
	a := uuid.New()
	bad := "not-a-time"
	seed(t, row{domain: "acr", ref: "bx-1", org: a, revision: 1, status: "active", expires: &bad})
	if _, err := reader(t, 10).ReadOrg(context.Background(), a); err == nil {
		t.Fatal("an unparsable expiry read as a row")
	}
}

// The org read is served by the (domain_owner, organization_id, external_reference) index, not by the primary key
// with a filter and not by a scan.
func TestReaderPageIsServedByTheOrgIndex(t *testing.T) {
	ctx := context.Background()
	pool := adminPool(t)
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SET session_replication_role = replica`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO auth.credential_projection
		(domain_owner, external_reference, organization_id, binding, revision, max_seen_revision, status, fingerprint, scopes, principal_id, registry_id)
		SELECT 'acr', 'pl-' || o || '-' || r, ('00000000-0000-4000-8000-' || lpad(o::text, 12, '0'))::uuid, 'organization', 1, 1, 'active', '0123456789abcdef', ARRAY['x'], gen_random_uuid(), gen_random_uuid()
		FROM generate_series(1, 200) o, generate_series(1, 100) r`); err != nil {
		t.Fatal(err)
	}
	_, _ = conn.Exec(ctx, `RESET session_replication_role`)
	if _, err := conn.Exec(ctx, `ANALYZE auth.credential_projection`); err != nil {
		t.Fatal(err)
	}
	id, _ := authschema.NewValidatedIdentifier("auth")
	var raw []byte
	if err := conn.QueryRow(ctx, `EXPLAIN (FORMAT JSON) `+strings.NewReplacer("$1", "'00000000-0000-4000-8000-000000000007'::uuid", "$2", "''", "$3", "500").Replace(PageSQL(id))).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var plan []map[string]any
	if err := json.Unmarshal(raw, &plan); err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	if !strings.Contains(text, `"Index Name": "credential_projection_org_idx"`) {
		t.Fatalf("the page read does not use credential_projection_org_idx: %s", text)
	}
	if strings.Contains(text, "Seq Scan") {
		t.Fatalf("the page read scans: %s", text)
	}
}
