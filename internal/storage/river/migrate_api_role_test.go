package riverstore

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func apiOptions() MigrationOptions {
	return MigrationOptions{
		Schema: "river", DomainRole: "dev_health_domain", QueueRole: "dev_health_queue",
		CoordinatorRole: "dev_health_coordinator", CoordinatorGrants: []TableGrant{{TableName: "worker_job_routes"}},
		APIRole: "dev_health_api", APIGrants: []TableGrant{{TableName: "users"}, {TableName: "organizations", AllowUpdate: true}},
	}
}

func TestMigrationOptionsValidateTheAPIRoleLeg(t *testing.T) {
	t.Parallel()
	if err := ValidateMigrationOptions(apiOptions()); err != nil {
		t.Fatalf("valid api leg refused: %v", err)
	}
	empty := apiOptions()
	empty.APIGrants = nil
	if err := ValidateMigrationOptions(empty); err != nil {
		t.Fatalf("an api role with an empty posture is valid (the posture starts empty): %v", err)
	}
	none := apiOptions()
	none.APIRole, none.APIGrants = "", nil
	if err := ValidateMigrationOptions(none); err != nil {
		t.Fatalf("no api role is valid: %v", err)
	}
	cases := map[string]func(*MigrationOptions){
		"grants without role": func(o *MigrationOptions) { o.APIRole = "" },
		"column grants without role": func(o *MigrationOptions) {
			o.APIRole, o.APIGrants = "", nil
			o.APIColumnGrants = []ColumnGrant{{TableName: "t", ColumnName: "c", Privilege: "SELECT"}}
		},
		"sequences without role":    func(o *MigrationOptions) { o.APIRole, o.APIGrants = "", nil; o.APISequences = []string{"s"} },
		"collides with domain":      func(o *MigrationOptions) { o.APIRole = o.DomainRole },
		"collides with queue":       func(o *MigrationOptions) { o.APIRole = o.QueueRole },
		"collides with coordinator": func(o *MigrationOptions) { o.APIRole = o.CoordinatorRole },
		"unsafe role":               func(o *MigrationOptions) { o.APIRole = "api; DROP SCHEMA public" },
		"unsafe table":              func(o *MigrationOptions) { o.APIGrants = []TableGrant{{TableName: "users; DROP TABLE x"}} },
		"duplicate table":           func(o *MigrationOptions) { o.APIGrants = []TableGrant{{TableName: "users"}, {TableName: "users"}} },
		"bad column privilege": func(o *MigrationOptions) {
			o.APIColumnGrants = []ColumnGrant{{TableName: "t", ColumnName: "c", Privilege: "DELETE"}}
		},
		"table-wide and column": func(o *MigrationOptions) {
			o.APIColumnGrants = []ColumnGrant{{TableName: "users", ColumnName: "c", Privilege: "SELECT"}}
		},
		"unsafe sequence": func(o *MigrationOptions) { o.APISequences = []string{"s; x"} },
	}
	for name, mutate := range cases {
		options := apiOptions()
		mutate(&options)
		if err := ValidateMigrationOptions(options); !errors.Is(err, ErrMigrationConfiguration) {
			t.Errorf("%s: error %v, want ErrMigrationConfiguration", name, err)
		}
	}
}

func TestAPIGrantStatementsDeriveFromTheInjectedPosture(t *testing.T) {
	t.Parallel()
	none := apiOptions()
	none.APIRole, none.APIGrants = "", nil
	if statements := apiGrantStatements(none); statements != nil {
		t.Fatalf("api statements without an api role: %v", statements)
	}
	joined := strings.Join(apiGrantStatements(apiOptions()), "\n")
	for _, expected := range []string{
		"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM \"dev_health_api\"",
		"REVOKE ALL PRIVILEGES ON SCHEMA \"river\" FROM \"dev_health_api\"",
		"GRANT SELECT ON TABLE \"public\".\"users\" TO \"dev_health_api\"",
		"GRANT SELECT, UPDATE ON TABLE \"public\".\"organizations\" TO \"dev_health_api\"",
	} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("missing %q:\n%s", expected, joined)
		}
	}
	if strings.Index(joined, "REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public") > strings.Index(joined, "GRANT SELECT ON TABLE") {
		t.Fatalf("REVOKE ALL must precede the grants:\n%s", joined)
	}
	// The api leg rides after the coordinator leg in the same transaction.
	all := strings.Join(runtimeGrantStatements(apiOptions()), "\n")
	if !strings.HasSuffix(all, joined) {
		t.Fatal("runtime grant statements do not end with the api leg")
	}
	// The coordinator leg is unchanged by the shared builder.
	coordinator := apiOptions()
	if !reflect.DeepEqual(coordinatorGrantStatements(coordinator),
		postureGrantStatements(coordinator.CoordinatorRole, "river", coordinator.CoordinatorGrants, nil, nil)) {
		t.Fatal("coordinator statements differ from the shared builder")
	}
}

func TestResolveAPIRoleDecisionTable(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		role      string
		migration string
		exists    bool
		eligible  bool
		wantRole  string
		wantErr   bool
	}{
		{name: "no api role configured", role: "", exists: false, wantRole: ""},
		{name: "role absent: skipped", role: "dev_health_api", exists: false, wantRole: ""},
		{name: "role present and eligible", role: "dev_health_api", exists: true, eligible: true, wantRole: "dev_health_api"},
		{name: "role present, not eligible", role: "dev_health_api", exists: true, eligible: false, wantErr: true},
		{name: "role is the migration identity", role: "dev_health_api", migration: "dev_health_api", exists: true, eligible: true, wantErr: true},
	}
	for _, test := range cases {
		options := apiOptions()
		options.APIRole = test.role
		if test.role == "" {
			options.APIGrants = nil
		}
		migration := test.migration
		if migration == "" {
			migration = "dev_health_migrator"
		}
		got, err := resolveAPIRole(context.Background(), options, migration, test.exists, test.eligible)
		if (err != nil) != test.wantErr {
			t.Errorf("%s: error %v", test.name, err)
			continue
		}
		if err != nil {
			continue
		}
		if got.APIRole != test.wantRole {
			t.Errorf("%s: api role %q, want %q", test.name, got.APIRole, test.wantRole)
		}
		if got.APIRole == "" && (got.APIGrants != nil || apiGrantStatements(got) != nil) {
			t.Errorf("%s: a skipped api role still carries grants", test.name)
		}
		if err := ValidateMigrationOptions(got); err != nil {
			t.Errorf("%s: resolved options no longer validate: %v", test.name, err)
		}
	}
}
