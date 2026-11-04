package publicread

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

var t0 = time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC)

func newReader(t *testing.T, db *fakeDB) (*Reader, *bytes.Buffer, *sdkmetric.ManualReader) {
	t.Helper()
	logs := &bytes.Buffer{}
	mr := sdkmetric.NewManualReader()
	r, err := New(Config{Querier: db, AuthSchema: `"auth"`,
		Logger: slog.New(slog.NewJSONHandler(logs, nil)),
		Meter:  sdkmetric.NewMeterProvider(sdkmetric.WithReader(mr)).Meter("test")})
	if err != nil {
		t.Fatal(err)
	}
	return r, logs, mr
}

var authCopyToken = regexp.MustCompile(`(?i)\b(\w+|"\w+")\.(organizations|memberships|users)\b`)

// Every statement names the ops-schema tables as public.* and reads only
// allowlisted auth tables; no statement names an auth copy of the record.
func TestPublicReadsNeverNameAuthCopies(t *testing.T) {
	stmts := Statements(`"auth"`)
	want := []string{"org_get", "orgs_of", "membership_get", "memberships_list", "principal_get_user", "principal_get_other", "principals_list"}
	for _, name := range want {
		sql, ok := stmts[name]
		if !ok || strings.TrimSpace(sql) == "" {
			t.Errorf("statement %s is missing", name)
			continue
		}
		for _, m := range authCopyToken.FindAllStringSubmatch(sql, -1) {
			if m[1] != "public" {
				t.Errorf("%s names %s.%s: only public.%s may be read", name, m[1], m[2], m[2])
			}
		}
		for _, tbl := range regexp.MustCompile(`"auth"\.(\w+)`).FindAllStringSubmatch(sql, -1) {
			switch tbl[1] {
			case "aggregate_revisions", "principals", "service_accounts", "workloads":
			default:
				t.Errorf("%s reads auth.%s, not on the allowlist", name, tbl[1])
			}
		}
	}
	for name, sql := range stmts {
		if !slices.Contains(want, name) {
			t.Errorf("unexpected statement %s: %.40s", name, sql)
		}
	}
	for _, name := range []string{"org_get", "orgs_of", "membership_get", "memberships_list", "principal_get_user"} {
		if strings.Contains(stmts[name], `"auth".principals`) || strings.Contains(stmts[name], `"auth".service_accounts`) || strings.Contains(stmts[name], `"auth".workloads`) {
			t.Errorf("%s reads auth principals: only the non-user principal reads may", name)
		}
	}
	for _, name := range []string{"org_get", "orgs_of"} {
		if !strings.Contains(stmts[name], "public.organizations") {
			t.Errorf("%s does not read public.organizations", name)
		}
	}
	for _, name := range []string{"orgs_of", "membership_get", "memberships_list"} {
		if !strings.Contains(stmts[name], "public.memberships") {
			t.Errorf("%s does not read public.memberships", name)
		}
	}
	if !strings.Contains(stmts["principal_get_user"], "public.users") {
		t.Error("principal_get_user does not read public.users")
	}
}

func divergent(t *testing.T) (*fakeDB, uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID) {
	t.Helper()
	id := ids(4)
	org, user, mem := id[0], id[1], id[2]
	joined := t0
	db := &fakeDB{revs: map[string]int64{}}
	db.public = dataset{
		orgs:  []fOrg{{org, "p-org", "P", true, t0, t0.Add(time.Hour)}},
		mems:  []fMem{{mem, org, user, "admin", &joined, t0, t0.Add(time.Hour)}},
		users: []fUser{{user, false}},
	}
	db.auth = dataset{
		orgs:  []fOrg{{org, "a-org", "A", true, t0, t0}},
		mems:  []fMem{{mem, org, user, "member", &joined, t0, t0}},
		users: []fUser{{user, true}},
	}
	return db, org, user, mem, id[3]
}

func TestReadDivergentPublicVsAuthServesPublic(t *testing.T) {
	db, org, user, _, _ := divergent(t)
	r, _, _ := newReader(t, db)
	ctx := context.Background()
	got, err := r.Organization(ctx, org)
	if err != nil || got.Name != "P" || got.Slug != "p-org" {
		t.Fatalf("organization = %+v, %v; want the public row (name P)", got, err)
	}
	m, err := r.Membership(ctx, org, user)
	if err != nil || m.Role != "admin" {
		t.Fatalf("membership = %+v, %v; want the public role admin", m, err)
	}
	list, _, err := r.Memberships(ctx, org, nil, 10)
	if err != nil || len(list) != 1 || list[0].Role != "admin" {
		t.Fatalf("membership list = %+v, %v; want the public role admin", list, err)
	}
	p, err := r.Principal(ctx, user)
	if err != nil || p.Active || p.Kind != "user" {
		t.Fatalf("principal = %+v, %v; want the public user, inactive", p, err)
	}
	of, _, err := r.OrganizationsOf(ctx, user, nil, 10)
	if err != nil || len(of) != 1 || of[0].Organization.Name != "P" || of[0].Membership.Role != "admin" {
		t.Fatalf("organizations of = %+v, %v; want the public rows", of, err)
	}
}

func TestAuthOnlyMembershipNotServed(t *testing.T) {
	db, org, user, mem, _ := divergent(t)
	db.public.mems = nil
	r, _, _ := newReader(t, db)
	ctx := context.Background()
	_ = mem
	if _, err := r.Membership(ctx, org, user); !errors.Is(err, ErrNotFound) {
		t.Fatalf("an auth-only membership (invited, suspended, removed) was served: %v", err)
	}
	list, more, err := r.Memberships(ctx, org, nil, 10)
	if err != nil || len(list) != 0 || more {
		t.Fatalf("list = %+v more=%v err=%v; want an empty page", list, more, err)
	}
	of, _, err := r.OrganizationsOf(ctx, user, nil, 10)
	if err != nil || len(of) != 0 {
		t.Fatalf("organizations of an auth-only membership = %+v, %v", of, err)
	}
}

func TestPublicOnlyMembershipServedRevision0(t *testing.T) {
	db, org, user, _, _ := divergent(t)
	db.auth = dataset{}
	r, _, _ := newReader(t, db)
	m, err := r.Membership(context.Background(), org, user)
	if err != nil || m.Role != "admin" || m.Revision != 0 {
		t.Fatalf("public-only membership = %+v, %v; want served with revision 0", m, err)
	}
	o, err := r.Organization(context.Background(), org)
	if err != nil || o.Revision != 0 {
		t.Fatalf("public-only organization = %+v, %v; want revision 0", o, err)
	}
	db.revs["membership:mbr_"+m.ID.String()] = 7
	m, _ = r.Membership(context.Background(), org, user)
	if m.Revision != 7 {
		t.Fatalf("revision = %d, want the counter 7", m.Revision)
	}
}

// A principal read selects an id, a status and a revision: no column of the
// personal data the user row holds, in the statement or in the result type.
func TestPrincipalReadSelectsNoEmailNoHash(t *testing.T) {
	stmts := Statements(`"auth"`)
	for _, name := range []string{"principal_get_user", "principal_get_other", "principals_list"} {
		sql := strings.ToLower(stmts[name])
		if sql == "" {
			t.Fatalf("statement %s missing", name)
		}
		for _, word := range []string{"email", "password", "hash", "full_name", "username", "display_name", "is_superuser", "token_version", "auth_provider"} {
			if strings.Contains(sql, word) {
				t.Errorf("%s names %q", name, word)
			}
		}
	}
	var fields []string
	typ := reflect.TypeOf(Principal{})
	for i := 0; i < typ.NumField(); i++ {
		fields = append(fields, typ.Field(i).Name)
	}
	if !reflect.DeepEqual(fields, []string{"ID", "Kind", "Active", "Revision"}) {
		t.Errorf("Principal fields = %v", fields)
	}
}

func TestPrincipalReadUserThenNonUser(t *testing.T) {
	id := ids(3)
	db := &fakeDB{revs: map[string]int64{"principal:prn_" + id[0].String(): 3}}
	db.public.users = []fUser{{id[0], true}}
	db.auth.principals = []fPrincipal{{id[1], "service_account", true, id[2]}, {id[0], "user", true, id[2]}, {id[2], "workload", false, id[2]}}
	r, _, _ := newReader(t, db)
	ctx := context.Background()
	p, err := r.Principal(ctx, id[0])
	if err != nil || p.Kind != "user" || !p.Active || p.Revision != 3 {
		t.Fatalf("user = %+v, %v", p, err)
	}
	p, err = r.Principal(ctx, id[1])
	if err != nil || p.Kind != "service_account" || !p.Active {
		t.Fatalf("service account = %+v, %v", p, err)
	}
	p, err = r.Principal(ctx, id[2])
	if err != nil || p.Kind != "workload" || p.Active {
		t.Fatalf("workload = %+v, %v", p, err)
	}
	if _, err := r.Principal(ctx, uuid.New()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown principal: %v", err)
	}
}

func TestMembershipListOrgBoundKeysetStable(t *testing.T) {
	id := ids(12)
	orgA, orgB := id[0], id[1]
	db := &fakeDB{revs: map[string]int64{}}
	var want []uuid.UUID
	for i := 2; i < 12; i++ {
		org := orgA
		if i%3 == 0 {
			org = orgB
		} else {
			want = append(want, id[i])
		}
		db.public.mems = append(db.public.mems, fMem{id[i], org, uuid.New(), "member", nil, t0, t0})
	}
	// Insert order differs from id order.
	db.public.mems[0], db.public.mems[len(db.public.mems)-1] = db.public.mems[len(db.public.mems)-1], db.public.mems[0]
	r, _, _ := newReader(t, db)
	var got []uuid.UUID
	var after *uuid.UUID
	for page := 0; page < 10; page++ {
		items, more, err := r.Memberships(context.Background(), orgA, after, 3)
		if err != nil {
			t.Fatal(err)
		}
		if len(items) > 3 {
			t.Fatalf("page of %d over the limit 3", len(items))
		}
		for _, m := range items {
			if m.OrgID != orgA {
				t.Fatalf("membership of another organization on the page")
			}
			got = append(got, m.ID)
		}
		if !more {
			break
		}
		last := items[len(items)-1].ID
		after = &last
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("pages = %v, want %v (org-bound, ascending id, no repeat or gap)", got, want)
	}
	sql := Statements(`"auth"`)["memberships_list"]
	if missing := mustContain(sql, "m.org_id = $1", "ORDER BY m.id", "m.id > $2", "LIMIT $3"); len(missing) > 0 {
		t.Errorf("memberships_list lacks %v", missing)
	}
}

func TestOrgListForPrincipalOrder(t *testing.T) {
	id := ids(8)
	user := id[0]
	db := &fakeDB{revs: map[string]int64{}}
	orgs := []uuid.UUID{id[5], id[2], id[7], id[3]}
	for i, o := range orgs {
		db.public.orgs = append(db.public.orgs, fOrg{o, "o", "O", true, t0, t0})
		db.public.mems = append(db.public.mems, fMem{id[1], o, user, "member", nil, t0, t0})
		_ = i
	}
	db.public.mems = append(db.public.mems, fMem{uuid.New(), id[4], uuid.New(), "member", nil, t0, t0})
	r, _, _ := newReader(t, db)
	var got []string
	var after *uuid.UUID
	for page := 0; page < 6; page++ {
		items, more, err := r.OrganizationsOf(context.Background(), user, after, 2)
		if err != nil {
			t.Fatal(err)
		}
		for _, it := range items {
			got = append(got, it.Organization.ID.String())
			if it.Membership.OrgID != it.Organization.ID || it.Membership.PrincipalID != user {
				t.Fatalf("pair mismatch %+v", it)
			}
		}
		if !more {
			break
		}
		last := items[len(items)-1].Organization.ID
		after = &last
	}
	want := []string{id[2].String(), id[3].String(), id[5].String(), id[7].String()}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("organizations = %v, want %v", got, want)
	}
	sql := Statements(`"auth"`)["orgs_of"]
	if missing := mustContain(sql, "m.user_id = $1", "ORDER BY o.id", "o.id > $2", "LIMIT $3"); len(missing) > 0 {
		t.Errorf("orgs_of lacks %v", missing)
	}
}

func TestExternalSourcePrincipalReadsAsNotFound(t *testing.T) {
	id := ids(4)
	org := id[0]
	db := &fakeDB{revs: map[string]int64{}}
	db.auth.principals = []fPrincipal{{id[1], "external_source", true, org}, {id[2], "service_account", true, org}}
	r, _, _ := newReader(t, db)
	if _, err := r.Principal(context.Background(), id[1]); !errors.Is(err, ErrNotFound) {
		t.Fatalf("an external_source principal is outside the read contract kinds and reads as not found, got %v", err)
	}
	items, _, err := r.Principals(context.Background(), org, "", nil, 10)
	if err != nil || len(items) != 1 || items[0].Principal.ID != id[2] {
		t.Fatalf("a principal list holds no external_source principal, got %+v, %v", items, err)
	}
	items, _, err = r.Principals(context.Background(), org, "external_source", nil, 10)
	if err != nil || len(items) != 0 {
		t.Fatalf("an external_source kind filter lists nothing, got %+v, %v", items, err)
	}
}

func TestPrincipalListOrgBound(t *testing.T) {
	id := ids(6)
	org, other := id[0], id[1]
	db := &fakeDB{revs: map[string]int64{}}
	db.public.users = []fUser{{id[2], true}, {id[3], false}}
	db.public.mems = []fMem{{uuid.New(), org, id[2], "member", nil, t0, t0}, {uuid.New(), other, id[3], "member", nil, t0, t0}}
	db.auth.principals = []fPrincipal{{id[4], "service_account", true, org}, {id[5], "workload", true, other}}
	r, _, _ := newReader(t, db)
	items, more, err := r.Principals(context.Background(), org, "", nil, 10)
	if err != nil || more || len(items) != 2 || items[0].Principal.ID != id[2] || items[1].Principal.ID != id[4] {
		t.Fatalf("principals = %+v more=%v err=%v", items, more, err)
	}
	if m := items[0].Membership; m == nil || m.ID != db.public.mems[0].ID || m.OrgID != org || m.PrincipalID != id[2] || m.Role != "member" {
		t.Fatalf("a user item holds its membership of the organization, got %+v", m)
	}
	if items[1].Principal.Kind != "service_account" || items[1].Membership != nil {
		t.Fatalf("a service account item holds no membership, got %+v", items[1])
	}
	items, _, err = r.Principals(context.Background(), org, "service_account", nil, 10)
	if err != nil || len(items) != 1 || items[0].Principal.Kind != "service_account" || items[0].Membership != nil {
		t.Fatalf("kind filter = %+v, %v", items, err)
	}
	sql := Statements(`"auth"`)["principals_list"]
	if missing := mustContain(sql, "ORDER BY", "LIMIT $4"); len(missing) > 0 {
		t.Errorf("principals_list lacks %v", missing)
	}
}

func TestUnknownPublicRoleFailsClosed(t *testing.T) {
	db, org, user, mem, _ := divergent(t)
	db.public.mems[0].Role = "superhero"
	r, logs, mr := newReader(t, db)
	ctx := context.Background()
	if _, err := r.Membership(ctx, org, user); !errors.Is(err, ErrUnknownRole) {
		t.Fatalf("membership with an unknown role: %v, want ErrUnknownRole", err)
	}
	if _, _, err := r.Memberships(ctx, org, nil, 5); !errors.Is(err, ErrUnknownRole) {
		t.Fatalf("list with an unknown role: %v, want ErrUnknownRole", err)
	}
	if _, _, err := r.OrganizationsOf(ctx, user, nil, 5); !errors.Is(err, ErrUnknownRole) {
		t.Fatalf("organizations of with an unknown role: %v, want ErrUnknownRole", err)
	}
	if _, _, err := r.Principals(ctx, org, "", nil, 5); !errors.Is(err, ErrUnknownRole) {
		t.Fatalf("principal list with an unknown role: %v, want ErrUnknownRole", err)
	}
	out := logs.String()
	for _, want := range []string{`"level":"ERROR"`, "org_" + org.String(), "superhero"} {
		if !strings.Contains(out, want) {
			t.Errorf("log lacks %s: %s", want, out)
		}
	}
	_ = mem
	var rm metricdata.ResourceMetrics
	if err := mr.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "auth.publicread.unknown_role" {
				continue
			}
			for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
				total += dp.Value
			}
		}
	}
	if total != 4 {
		t.Fatalf("auth.publicread.unknown_role = %d, want 4", total)
	}
}

func TestKnownRolesServed(t *testing.T) {
	db, org, user, _, _ := divergent(t)
	r, _, _ := newReader(t, db)
	for _, role := range []string{"viewer", "member", "admin", "owner"} {
		db.public.mems[0].Role = role
		m, err := r.Membership(context.Background(), org, user)
		if err != nil || m.Role != role {
			t.Fatalf("role %s: %+v, %v", role, m, err)
		}
	}
}

// The statements carry the filters, the order, the cursor and the revision join the contract needs; the fake database answers by
// statement name, so this is where a changed text is seen.
func TestStatementsCarryTheirContract(t *testing.T) {
	rev := func(typ, prefix, id string) []string {
		return []string{"aggregate_type = '" + typ + "'", "aggregate_id = '" + prefix + "' || " + id + "::text", `"auth".aggregate_revisions`}
	}
	want := map[string][]string{
		"org_get":             append([]string{"WHERE o.id = $1"}, rev("organization", "org_", "o.id")...),
		"orgs_of":             append(append([]string{"JOIN public.organizations o ON o.id = m.org_id", "m.user_id = $1", "o.id > $2", "ORDER BY o.id", "LIMIT $3"}, rev("organization", "org_", "o.id")...), rev("membership", "mbr_", "m.id")...),
		"membership_get":      append([]string{"WHERE m.org_id = $1 AND m.user_id = $2"}, rev("membership", "mbr_", "m.id")...),
		"memberships_list":    append([]string{"WHERE m.org_id = $1", "m.id > $2", "ORDER BY m.id", "LIMIT $3"}, rev("membership", "mbr_", "m.id")...),
		"principal_get_user":  append([]string{"WHERE u.id = $1"}, rev("principal", "prn_", "u.id")...),
		"principal_get_other": append([]string{"WHERE p.id = $1 AND p.kind IN ('service_account', 'workload')", `"auth".principals p`}, rev("principal", "prn_", "p.id")...),
		"principals_list": append([]string{"WHERE m.org_id = $1", "sa.organization_id = $1", "w.organization_id = $1", "x.kind = $2", "x.id > $3", "ORDER BY x.id", "LIMIT $4",
			`"auth".service_accounts sa`, `"auth".workloads w`, "p.kind = 'service_account'", "p.kind = 'workload'"}, append(rev("principal", "prn_", "x.id"), rev("membership", "mbr_", "x.mid")...)...),
	}
	stmts := Statements(`"auth"`)
	for name, parts := range want {
		if missing := mustContain(stmts[name], parts...); len(missing) > 0 {
			t.Errorf("%s lacks %v", name, missing)
		}
	}
	selected := map[string][]string{
		"org_get":             {"COALESCE(ro.revision, 0)"},
		"orgs_of":             {"COALESCE(ro.revision, 0)", "COALESCE(rm.revision, 0)"},
		"membership_get":      {"COALESCE(rm.revision, 0)"},
		"memberships_list":    {"COALESCE(rm.revision, 0)"},
		"principal_get_user":  {"COALESCE(rp.revision, 0)"},
		"principal_get_other": {"COALESCE(rp.revision, 0)"},
		"principals_list":     {"COALESCE(rp.revision, 0)", "COALESCE(rm.revision, 0)"},
	}
	for name, parts := range selected {
		head, _, _ := strings.Cut(stmts[name], "\nFROM")
		if missing := mustContain(head, parts...); len(missing) > 0 {
			t.Errorf("%s selects no joined revision %v", name, missing)
		}
	}
	for _, name := range []string{"orgs_of", "memberships_list", "principals_list"} {
		if n := strings.Count(stmts[name], "ORDER BY"); n != 1 {
			t.Errorf("%s has %d ORDER BY", name, n)
		}
	}
}
