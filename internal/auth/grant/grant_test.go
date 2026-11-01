package grant

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

func TestValidKey(t *testing.T) {
	long := strings.Repeat("a", 100)
	for key, want := range map[string]bool{
		"acme/api": true, "a/b": true, "*": true, "acme/api.v2": true, "acme/my_repo-1": true, "0a/9z": true,
		long + "/" + long: true,
		"Acme/api":        false, "acme/API": false, "acme": false, "": false, "acme/api/extra": false, "a/b/c": false,
		"**": false, "a/*": false, "*/b": false, "*/*": false, " *": false, "* ": false, "acme/api\n": false,
		"/b": false, "a/": false, ".a/b": false, "a/.b": false, "-a/b": false, "a/-b": false,
		long + "a/b": false, "a/" + long + "a": false, "acme/apı": false, "acme/api\x00": false,
	} {
		if got := ValidKey(key); got != want {
			t.Errorf("ValidKey(%q) = %v, want %v", key, got, want)
		}
	}
}

func TestGrantRolesExcludeOwner(t *testing.T) {
	for role, want := range map[string]bool{"viewer": true, "member": true, "admin": true, "owner": false, "": false, "superuser": false, "Viewer": false} {
		if got := ValidRole(role); got != want {
			t.Errorf("ValidRole(%q) = %v, want %v", role, got, want)
		}
	}
}

func TestCheckCreate(t *testing.T) {
	actor := uuid.New()
	ok := CreateInput{Role: "viewer", Key: "acme/api", Actor: Actor{PrincipalID: &actor}}
	cases := []struct {
		name string
		edit func(*CreateInput)
		want error
	}{
		{"valid", func(*CreateInput) {}, nil},
		{"star", func(in *CreateInput) { in.Key = "*" }, nil},
		{"owner refused", func(in *CreateInput) { in.Role = "owner" }, ErrRoleInvalid},
		{"bad key", func(in *CreateInput) { in.Key = "a/*" }, ErrKeyInvalid},
		{"bad origin", func(in *CreateInput) { in.Origin = "inventory" }, ErrOriginInvalid},
		{"api needs actor", func(in *CreateInput) { in.Actor = Actor{} }, ErrActorRequired},
		{"backfill needs no actor", func(in *CreateInput) { in.Origin = OriginBackfill; in.Actor = Actor{} }, nil},
	}
	for _, c := range cases {
		in := ok
		c.edit(&in)
		out, err := checkCreate(in)
		if !errors.Is(err, c.want) {
			t.Errorf("%s: err = %v, want %v", c.name, err, c.want)
		}
		if err == nil && out.Origin == "" {
			t.Errorf("%s: origin not defaulted", c.name)
		}
	}
}

func TestCheckUpdate(t *testing.T) {
	role, bad := "admin", "owner"
	at := time.Now().Add(time.Hour)
	cases := []struct {
		name string
		in   UpdateInput
		want error
	}{
		{"role", UpdateInput{Role: &role}, nil},
		{"expiry", UpdateInput{ExpiresAt: &at}, nil},
		{"clear", UpdateInput{ClearExpiry: true}, nil},
		{"nothing", UpdateInput{}, ErrNothingToSet},
		{"set and clear", UpdateInput{ExpiresAt: &at, ClearExpiry: true}, ErrNothingToSet},
		{"owner", UpdateInput{Role: &bad}, ErrRoleInvalid},
	}
	for _, c := range cases {
		if err := checkUpdate(c.in); !errors.Is(err, c.want) {
			t.Errorf("%s: err = %v, want %v", c.name, err, c.want)
		}
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found")
		}
		dir = parent
	}
}

// The key pattern of the Store, of authorization_request.v1 and of the 0020
// CHECK are one pattern.
func TestKeyPatternIsTheContractAndTheMigrationPattern(t *testing.T) {
	root := repoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, "contracts", "auth", "v1", "jsonschema", "authorization_request.v1.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	var node any
	if err := json.Unmarshal(raw, &node); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"then", "properties", "resource", "properties", "key", "pattern"} {
		m, ok := node.(map[string]any)
		if !ok {
			t.Fatalf("schema path breaks at %q", k)
		}
		node = m[k]
	}
	if node != KeyPattern() {
		t.Fatalf("authorization_request.v1 key pattern = %v, Store pattern = %q", node, KeyPattern())
	}
	sql, err := os.ReadFile(filepath.Join(root, "internal", "storage", "postgres", "authschema", "migrations", "0020_grant_service_account_workload_shape.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(sql), "resource_id ~ '"+KeyPattern()+"'") {
		t.Fatalf("migration 0020 does not hold the Store key pattern %q", KeyPattern())
	}
}

func newUnownedStore(t *testing.T) (*Store, *int) {
	t.Helper()
	commits := 0
	s := &Store{ownsUsers: false, logger: nil}
	s.commit = func(context.Context, audit.Mutation) error { commits++; return nil }
	counter, err := noop.NewMeterProvider().Meter("t").Int64Counter("c")
	if err != nil {
		t.Fatal(err)
	}
	s.mutations = counter
	return s, &commits
}

// With users not owned, every write verb refuses before it opens a transaction.
func TestEveryWriteVerbRefusesWhenUsersAreNotOwned(t *testing.T) {
	s, commits := newUnownedStore(t)
	ctx := context.Background()
	actor := uuid.New()
	role := "viewer"
	if _, err := s.Create(ctx, CreateInput{OrgID: uuid.New(), PrincipalID: uuid.New(), Role: "viewer", Key: "a/b", Actor: Actor{PrincipalID: &actor}}); !errors.Is(err, ErrNotOwned) {
		t.Errorf("Create = %v, want ErrNotOwned", err)
	}
	if _, err := s.Update(ctx, UpdateInput{OrgID: uuid.New(), GrantID: uuid.New(), Role: &role}); !errors.Is(err, ErrNotOwned) {
		t.Errorf("Update = %v, want ErrNotOwned", err)
	}
	if _, err := s.Revoke(ctx, RevokeInput{OrgID: uuid.New(), GrantID: uuid.New()}); !errors.Is(err, ErrNotOwned) {
		t.Errorf("Revoke = %v, want ErrNotOwned", err)
	}
	if *commits != 0 {
		t.Fatalf("%d commits opened, want 0", *commits)
	}
}

// The events and audit rows a verb builds are ones the provisioning schema and
// the audit allowlist accept.
func TestCreateEventAndAuditRowAreWellFormed(t *testing.T) {
	s := &Store{}
	s.catalogForTest()
	id, org, principal, actor := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	exp := time.Date(2030, 1, 2, 3, 4, 5, 678900000, time.UTC)
	for _, in := range []CreateInput{
		{Role: "viewer", Key: "acme/api"},
		{Role: "admin", Key: "*", ExpiresAt: &exp},
	} {
		e := s.createEvent(id, in)
		if err := provisioning.CheckEvent(e.AggregateType, e.AggregateID, e.EventType, e.Change); err != nil {
			t.Errorf("created event %+v: %v", in, err)
		}
		row := auditRow("auth.resource_grant.created", "grant_created", org, principal, id, Actor{PrincipalID: &actor}, []string{"role_key", "status"}, map[string]any{"new_status": "active"})
		if err := audit.ValidateAttributes(row.Attributes); err != nil {
			t.Errorf("audit attributes: %v", err)
		}
	}
	if got := wireTime(exp); got != "2030-01-02T03:04:05.6789Z" {
		t.Errorf("wireTime = %q", got)
	}
}

// resource_grants.role_key references auth.roles, which nothing else fills: the
// migration that adds origin seeds exactly the roles a grant may carry, each an
// organization role of the closed catalog.
func TestMigrationSeedsExactlyTheGrantRoles(t *testing.T) {
	migrations, err := authschema.Migrations()
	if err != nil {
		t.Fatal(err)
	}
	var seeded []string
	for _, m := range migrations {
		if !strings.Contains(m.SQL, "resource_grants_history_guard") || !strings.Contains(m.SQL, "INSERT INTO roles") {
			continue
		}
		for _, row := range regexp.MustCompile(`\('([a-z_]+)', '([a-z_]+)', 'organization', true\)`).FindAllStringSubmatch(m.SQL, -1) {
			if row[1] != row[2] {
				t.Errorf("role %q is named %q; the name is the key", row[1], row[2])
			}
			seeded = append(seeded, row[1])
		}
	}
	sort.Strings(seeded)
	want := Roles()
	sort.Strings(want)
	if strings.Join(seeded, ",") != strings.Join(want, ",") {
		t.Fatalf("seeded roles = %v, grant roles = %v", seeded, want)
	}
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), "contracts", "auth", "v1", "vocabulary", "roles.v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	var catalog struct {
		Roles []struct{ Role, Scope string } `json:"roles"`
	}
	if err := json.Unmarshal(raw, &catalog); err != nil {
		t.Fatal(err)
	}
	org := map[string]bool{}
	for _, r := range catalog.Roles {
		if r.Scope == "organization" {
			org[r.Role] = true
		}
	}
	for _, r := range want {
		if !org[r] {
			t.Errorf("grant role %q is not an organization role of roles.v1.json", r)
		}
	}
	if org["owner"] && ValidRole("owner") {
		t.Error("owner must not be a grant role")
	}
}
