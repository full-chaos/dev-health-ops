//go:build integration

package publicread

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// The reader runs as the auth runtime role over the real ops schema and the auth schema, holding exactly authschema.PublicReadColumns
// (plus the posture of Apply): a statement that reads a column outside the manifest, or a table the role cannot read, fails here.
func TestReaderOverTheRealSchemasAsTheRuntimeRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		_ = instance.Close(closeCtx)
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	pgschema.Apply(ctx, t, admin)
	role, err := containers.RoleName("publicread_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	password := "rt-" + hex.EncodeToString(raw)
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role}); err != nil {
		t.Fatalf("authschema.Apply: %v", err)
	}
	for _, entry := range authschema.PublicReadColumns() {
		if _, err := admin.Exec(ctx, fmt.Sprintf(`GRANT SELECT (%s) ON public.%s TO %q`, strings.Join(entry.Columns, ", "), entry.Table, role)); err != nil {
			t.Fatalf("grant %s: %v", entry.Table, err)
		}
	}
	dsn, err := url.Parse(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	dsn.User = url.UserPassword(role, password)
	runtime, err := pgxpool.New(ctx, dsn.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtime.Close)

	orgA, orgB := uuid.MustParse("00000000-0000-4000-8000-00000000000a"), uuid.MustParse("00000000-0000-4000-8000-00000000000b")
	users := []uuid.UUID{uuid.MustParse("00000000-0000-4000-8000-0000000000a1"), uuid.MustParse("00000000-0000-4000-8000-0000000000a2"), uuid.MustParse("00000000-0000-4000-8000-0000000000a3")}
	workload := uuid.MustParse("00000000-0000-4000-8000-0000000000b1")
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := admin.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	for i, org := range []uuid.UUID{orgA, orgB} {
		exec(`INSERT INTO public.organizations (id, slug, name, is_active) VALUES ($1, $2, $3, $4)`, org, fmt.Sprintf("org-%d", i), fmt.Sprintf("Org %d", i), i == 0)
	}
	for i, u := range users {
		exec(`INSERT INTO public.users (id, email, username, is_active) VALUES ($1, $2, $3, $4)`, u, fmt.Sprintf("u%d@example.test", i), fmt.Sprintf("u%d", i), i != 2)
	}
	members := []uuid.UUID{uuid.MustParse("00000000-0000-4000-8000-0000000000c3"), uuid.MustParse("00000000-0000-4000-8000-0000000000c1"), uuid.MustParse("00000000-0000-4000-8000-0000000000c2"), uuid.MustParse("00000000-0000-4000-8000-0000000000c4")}
	exec(`INSERT INTO public.memberships (id, user_id, org_id, role, joined_at) VALUES ($1, $2, $3, 'owner', now())`, members[0], users[0], orgA)
	exec(`INSERT INTO public.memberships (id, user_id, org_id, role) VALUES ($1, $2, $3, 'member')`, members[1], users[1], orgA)
	exec(`INSERT INTO public.memberships (id, user_id, org_id, role) VALUES ($1, $2, $3, 'admin')`, members[2], users[2], orgA)
	exec(`INSERT INTO public.memberships (id, user_id, org_id, role) VALUES ($1, $2, $3, 'viewer')`, members[3], users[0], orgB)
	exec(`INSERT INTO auth.principals (id, kind) VALUES ($1, 'workload')`, workload)

	reader, err := New(Config{Querier: runtime, AuthSchema: authschema.Quote(mustIdent(t, "auth"))})
	if err != nil {
		t.Fatal(err)
	}

	o, err := reader.Organization(ctx, orgA)
	if err != nil || o.Slug != "org-0" || !o.Active || o.CreatedAt.IsZero() || o.UpdatedAt.IsZero() || o.Revision != 0 {
		t.Fatalf("organization = %+v, %v", o, err)
	}
	if _, err := reader.Organization(ctx, uuid.New()); err == nil || err != ErrNotFound {
		t.Fatalf("unknown organization: %v", err)
	}
	m, err := reader.Membership(ctx, orgA, users[1])
	if err != nil || m.ID != members[1] || m.Role != "member" || m.JoinedAt != nil || m.UpdatedAt.IsZero() {
		t.Fatalf("membership = %+v, %v", m, err)
	}
	page, more, err := reader.Memberships(ctx, orgA, nil, 2)
	if err != nil || !more || len(page) != 2 || page[0].ID != members[1] || page[1].ID != members[2] {
		t.Fatalf("first page = %+v more=%v err=%v", page, more, err)
	}
	last := page[1].ID
	page, more, err = reader.Memberships(ctx, orgA, &last, 2)
	if err != nil || more || len(page) != 1 || page[0].ID != members[0] || page[0].Role != "owner" || page[0].JoinedAt == nil {
		t.Fatalf("second page = %+v more=%v err=%v", page, more, err)
	}
	of, more, err := reader.OrganizationsOf(ctx, users[0], nil, 1)
	if err != nil || !more || len(of) != 1 || of[0].Organization.ID != orgA || of[0].Membership.ID != members[0] {
		t.Fatalf("organizations of = %+v more=%v err=%v", of, more, err)
	}
	afterA := orgA
	of, more, err = reader.OrganizationsOf(ctx, users[0], &afterA, 5)
	if err != nil || more || len(of) != 1 || of[0].Organization.ID != orgB || of[0].Organization.Active || of[0].Membership.Role != "viewer" {
		t.Fatalf("organizations of, second = %+v more=%v err=%v", of, more, err)
	}
	p, err := reader.Principal(ctx, users[2])
	if err != nil || p.Kind != "user" || p.Active {
		t.Fatalf("inactive user = %+v, %v", p, err)
	}
	p, err = reader.Principal(ctx, workload)
	if err != nil || p.Kind != "workload" || !p.Active {
		t.Fatalf("workload principal = %+v, %v", p, err)
	}
	if _, err := reader.Principal(ctx, uuid.New()); err != ErrNotFound {
		t.Fatalf("unknown principal: %v", err)
	}
	list, more, err := reader.Principals(ctx, orgA, "", nil, 10)
	if err != nil || more || len(list) != 3 || list[0].Principal.ID != users[0] {
		t.Fatalf("principals = %+v more=%v err=%v", list, more, err)
	}
	list, _, err = reader.Principals(ctx, orgA, "workload", nil, 10)
	if err != nil || len(list) != 0 {
		t.Fatalf("workload filter = %+v, %v", list, err)
	}
	exec(`UPDATE public.memberships SET role = 'superhero' WHERE id = $1`, members[1])
	if _, err := reader.Membership(ctx, orgA, users[1]); err != ErrUnknownRole {
		t.Fatalf("unknown role: %v", err)
	}
}

func mustIdent(t *testing.T, name string) authschema.ValidatedIdentifier {
	t.Helper()
	id, err := authschema.NewValidatedIdentifier(name)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
