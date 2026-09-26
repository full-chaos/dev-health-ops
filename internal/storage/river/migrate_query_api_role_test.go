package riverstore

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func queryAPIOptions() MigrationOptions {
	options := apiOptions()
	options.QueryAPIRole = "dev_health_query_api"
	options.QueryAPIGrants = []TableGrant{
		{TableName: "saved_reports", AllowInsert: true, AllowUpdate: true, AllowDelete: true},
		{TableName: "scheduled_jobs", AllowInsert: true, AllowUpdate: true},
		{TableName: "report_runs", AllowInsert: true},
		{TableName: "worker_job_outbox", AllowInsert: true},
		{TableName: "go_api_routing_state"},
		{TableName: "organizations"},
	}
	return options
}

func TestMigrationOptionsValidateTheQueryAPIRoleLeg(t *testing.T) {
	t.Parallel()
	if err := ValidateMigrationOptions(queryAPIOptions()); err != nil {
		t.Fatalf("valid query-api leg refused: %v", err)
	}
	cases := map[string]func(*MigrationOptions){
		"grants without role":       func(o *MigrationOptions) { o.QueryAPIRole = "" },
		"role without grants":       func(o *MigrationOptions) { o.QueryAPIGrants = nil },
		"collides with domain":      func(o *MigrationOptions) { o.QueryAPIRole = o.DomainRole },
		"collides with queue":       func(o *MigrationOptions) { o.QueryAPIRole = o.QueueRole },
		"collides with coordinator": func(o *MigrationOptions) { o.QueryAPIRole = o.CoordinatorRole },
		"collides with api":         func(o *MigrationOptions) { o.QueryAPIRole = o.APIRole },
		"unsafe role":               func(o *MigrationOptions) { o.QueryAPIRole = "q; DROP SCHEMA public" },
		"unsafe table": func(o *MigrationOptions) {
			o.QueryAPIGrants = []TableGrant{{TableName: "saved_reports; DROP TABLE x", AllowInsert: true}}
		},
		"duplicate table": func(o *MigrationOptions) {
			o.QueryAPIGrants = []TableGrant{{TableName: "saved_reports"}, {TableName: "saved_reports"}}
		},
	}
	for name, mutate := range cases {
		options := queryAPIOptions()
		mutate(&options)
		if err := ValidateMigrationOptions(options); !errors.Is(err, ErrMigrationConfiguration) {
			t.Errorf("%s: error %v, want ErrMigrationConfiguration", name, err)
		}
	}
}

// The query-api leg is the api role's policy: REVOKE ALL first, so the role's
// posture is a function of this migration alone, then one guarded GRANT per
// declared relation with SELECT always present.
func TestQueryAPIGrantStatementsRevokeThenGrantTheManifest(t *testing.T) {
	t.Parallel()
	none := queryAPIOptions()
	none.QueryAPIRole, none.QueryAPIGrants = "", nil
	if statements := queryAPIGrantStatements(none); statements != nil {
		t.Fatalf("query-api statements without a role: %v", statements)
	}

	options := queryAPIOptions()
	statements := queryAPIGrantStatements(options)
	want := postureGrantStatements(options.QueryAPIRole, options.Schema, options.QueryAPIGrants, nil, nil)
	if !reflect.DeepEqual(statements, want) {
		t.Fatalf("query-api statements are not the shared role-posture policy:\n%s", strings.Join(statements, "\n"))
	}
	joined := strings.Join(statements, "\n")
	for _, needle := range []string{
		`REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM "dev_health_query_api"`,
		`REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA "river" FROM "dev_health_query_api"`,
		`GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE "public"."saved_reports" TO "dev_health_query_api"`,
		`GRANT SELECT ON TABLE "public"."go_api_routing_state" TO "dev_health_query_api"`,
		`GRANT SELECT ON TABLE "public"."organizations" TO "dev_health_query_api"`,
	} {
		if !strings.Contains(joined, needle) {
			t.Errorf("missing statement %q in:\n%s", needle, joined)
		}
	}
	// The REVOKEs must precede every GRANT: a GRANT before the REVOKE ALL would
	// be wiped by it.
	lastRevoke, firstGrant := -1, len(statements)
	for i, statement := range statements {
		if strings.HasPrefix(statement, "REVOKE") && i > lastRevoke {
			lastRevoke = i
		}
		if strings.Contains(statement, "GRANT SELECT") && i < firstGrant {
			firstGrant = i
		}
	}
	if lastRevoke > firstGrant {
		t.Fatalf("a REVOKE (index %d) follows a table GRANT (index %d)", lastRevoke, firstGrant)
	}
}

func TestQueryAPILegRidesLastAndChangesNothingWhenAbsent(t *testing.T) {
	t.Parallel()
	with := queryAPIOptions()
	without := queryAPIOptions()
	without.QueryAPIRole, without.QueryAPIGrants = "", nil

	base := runtimeGrantStatements(without)
	all := runtimeGrantStatements(with)
	if !reflect.DeepEqual(all[:len(base)], base) {
		t.Fatal("configuring the query-api role changed a statement of another role's policy")
	}
	if !reflect.DeepEqual(all[len(base):], queryAPIGrantStatements(with)) {
		t.Fatal("the query-api leg is not the trailing block of the runtime grant statements")
	}
	// An unconfigured deployment gets byte-identical statements to the
	// pre-existing api-only options.
	if !reflect.DeepEqual(base, runtimeGrantStatements(apiOptions())) {
		t.Fatal("options without a query-api role differ from the pre-existing statements")
	}
}

func TestResolveQueryAPIRoleDecisionTable(t *testing.T) {
	t.Parallel()
	const role = "dev_health_query_api"
	cases := []struct {
		name          string
		role          string
		migrationRole string
		exists        bool
		eligible      bool
		wantRole      string
		wantErr       bool
	}{
		{name: "no role configured", role: "", migrationRole: "owner"},
		{name: "role absent: skipped", role: role, migrationRole: "owner", exists: false},
		{name: "eligible role present", role: role, migrationRole: "owner", exists: true, eligible: true, wantRole: role},
		// The leg REVOKEs: an ineligible login (superuser, owns objects) or the
		// migration identity itself must stop the migration, never be revoked.
		{name: "present but ineligible", role: role, migrationRole: "owner", exists: true, eligible: false, wantErr: true},
		{name: "present, eligible, but the migration identity", role: role, migrationRole: role, exists: true, eligible: true, wantErr: true},
	}
	for _, test := range cases {
		options := queryAPIOptions()
		options.QueryAPIRole = test.role
		if test.role == "" {
			options.QueryAPIGrants = nil
		}
		got, err := resolveQueryAPIRole(context.Background(), options, test.migrationRole, test.exists, test.eligible)
		if test.wantErr {
			if !errors.Is(err, ErrMigrationConfiguration) {
				t.Errorf("%s: error %v, want ErrMigrationConfiguration", test.name, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: unexpected error %v", test.name, err)
			continue
		}
		if got.QueryAPIRole != test.wantRole {
			t.Errorf("%s: role %q, want %q", test.name, got.QueryAPIRole, test.wantRole)
		}
		if got.QueryAPIRole == "" && (got.QueryAPIGrants != nil || queryAPIGrantStatements(got) != nil) {
			t.Errorf("%s: a skipped role still carries grants", test.name)
		}
		if err := ValidateMigrationOptions(got); err != nil {
			t.Errorf("%s: resolved options no longer validate: %v", test.name, err)
		}
	}
}
